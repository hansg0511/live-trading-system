"""Durable execution state for the live-trading system.

The database is intentionally small and single-process friendly.  A pair
operation and both leg intents are committed before a broker call.  Broker
deals are recorded in ``fill_events`` and applied with a unique event key, so
callbacks can be replayed safely after a reconnect or restart.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from typing import Any, Iterable
import uuid


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STATE_DIR = PROJECT_ROOT / "data"
DB_PATH = str(DEFAULT_STATE_DIR / "trading.db")
SCHEMA_PATH = Path(__file__).with_name("schema.sql")

TERMINAL_OPERATION_STATUSES = {"closed", "failed"}
ACTIVE_OPERATION_STATUSES = {
    "created",
    "leg1_submitting",
    "leg1_submitted",
    "leg2_submitting",
    "leg2_submitted",
    "partially_filled",
    "requires_reconciliation",
    "open",
}
LEG_STATES = {
    "created",
    "submitting",
    "submitted",
    "partially_filled",
    "filled",
    "cancelled",
    "rejected",
    "failed",
    "requires_reconciliation",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def resolve_db_path(db_path: str | Path | None = None) -> Path:
    """Resolve state paths relative to the project, never the launch cwd."""
    if db_path is None:
        return Path(DB_PATH)
    candidate = Path(db_path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _add_missing_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = _table_columns(conn, table)
    for name, declaration in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Add columns used by the hardened ledger to older databases."""
    _add_missing_columns(
        conn,
        "positions_stat_arb",
        {
            "entry_operation_id": "TEXT",
            "exit_operation_id": "TEXT",
            "environment": "TEXT NOT NULL DEFAULT 'SIMULATE'",
            "account_id": "TEXT",
        },
    )
    _add_missing_columns(
        conn,
        "orders",
        {
            "operation_id": "TEXT",
            "requested_quantity": "REAL",
            "cumulative_filled_quantity": "REAL NOT NULL DEFAULT 0",
            "remaining_quantity": "REAL",
            "average_fill_price": "REAL",
            "broker_order_status": "TEXT",
            "environment": "TEXT",
            "account_id": "TEXT",
            "idempotency_key": "TEXT",
        },
    )
    _add_missing_columns(conn, "trades", {"exit_operation_id": "TEXT"})
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_broker_order_id ON orders(broker_order_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_operation_id ON orders(operation_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_operation_legs_broker_order ON operation_legs(broker_order_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_operation_legs_status ON operation_legs(status)")


@contextmanager
def get_conn(
    db_path: str | Path | None = DB_PATH,
    *,
    create: bool = True,
):
    """Open a durable SQLite connection with rollback and concurrency settings."""
    path = resolve_db_path(db_path)
    if not path.exists() and not create:
        raise FileNotFoundError(f"State database does not exist: {path}")
    if create:
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, isolation_level="DEFERRED")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    # WAL allows a reader during a short state transition; FULL synchronous
    # keeps committed intent durable across a process or machine crash.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(
    db_path: str | Path | None = DB_PATH,
    schema_path: str | Path = SCHEMA_PATH,
    *,
    trd_env: str = "SIMULATE",
) -> Path:
    """Create/migrate the state DB.

    REAL mode never silently creates a database in a typo'd working directory.
    The default path is deterministic; an explicit REAL path must already
    exist and be openable.
    """
    path = resolve_db_path(db_path)
    env = str(trd_env).upper()
    if env not in {"SIMULATE", "REAL"}:
        raise ValueError("trd_env must be SIMULATE or REAL")
    if env == "REAL" and not path.exists():
        raise FileNotFoundError(
            f"REAL trading requires an existing state database at {path}; "
            "configure TRADING_STATE_DB or initialize it in SIMULATE first."
        )
    if env == "REAL":
        if path.is_dir() or path.stat().st_size == 0:
            raise RuntimeError(
                f"REAL trading requires an initialized state database at {path}; "
                "initialize the database in SIMULATE before arming REAL."
            )
        try:
            with sqlite3.connect(str(path)) as existing_conn:
                existing_tables = {
                    str(row[0])
                    for row in existing_conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                }
        except sqlite3.Error as exc:
            raise RuntimeError(f"REAL state database cannot be opened at {path}: {exc}") from exc
        if not {"positions_stat_arb", "orders"}.issubset(existing_tables):
            raise RuntimeError(
                f"REAL trading requires an initialized state database at {path}; "
                "initialize it in SIMULATE first."
            )
    with get_conn(path, create=env != "REAL") as conn:
        conn.executescript(Path(schema_path).read_text(encoding="utf-8"))
        _migrate_schema(conn)
        # Backfill compatibility quantities for rows created by the old schema.
        conn.execute(
            """
            UPDATE orders
            SET requested_quantity = COALESCE(requested_quantity, submitted_quantity),
                remaining_quantity = COALESCE(remaining_quantity, submitted_quantity),
                cumulative_filled_quantity = COALESCE(cumulative_filled_quantity, COALESCE(fill_quantity, 0)),
                average_fill_price = COALESCE(average_fill_price, fill_price)
            WHERE requested_quantity IS NULL OR remaining_quantity IS NULL
            """
        )
    return path


def _json(value: Any) -> str:
    return json.dumps(value or {}, sort_keys=True, separators=(",", ":"), default=str)


def _decode_json(value: str | None) -> dict[str, Any]:
    try:
        decoded = json.loads(value or "{}")
        return decoded if isinstance(decoded, dict) else {}
    except (TypeError, ValueError):
        return {}


def _finite_positive(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{field} must be finite and positive")
    return number


def _quantity_tolerance(requested: float) -> float:
    return max(1e-9, abs(float(requested)) * 1e-6)


def _validate_side(side: str) -> str:
    normalized = str(side).upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError(f"Unsupported order side: {side}")
    return normalized


def get_open_position(pair: str, db_path: str | Path | None = DB_PATH) -> dict | None:
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM positions_stat_arb WHERE pair = ? AND status = 'open' ORDER BY id DESC LIMIT 1",
            (pair,),
        ).fetchone()
        return dict(row) if row else None


def get_all_open_positions(db_path: str | Path | None = DB_PATH) -> list[dict]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM positions_stat_arb WHERE status = 'open' ORDER BY id"
        ).fetchall()
        return [dict(row) for row in rows]


def get_operation(operation_id: str, db_path: str | Path | None = DB_PATH) -> dict | None:
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM pair_operations WHERE operation_id = ?", (operation_id,)
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["metadata"] = _decode_json(result.pop("metadata_json", "{}"))
        result["legs"] = [
            dict(leg)
            for leg in conn.execute(
                "SELECT * FROM operation_legs WHERE operation_id = ? ORDER BY leg", (operation_id,)
            ).fetchall()
        ]
        return result


def get_operation_by_idempotency_key(
    idempotency_key: str, db_path: str | Path | None = DB_PATH
) -> dict | None:
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT operation_id FROM pair_operations WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
    return get_operation(str(row[0]), db_path) if row else None


def find_active_operation(
    pair: str,
    operation_type: str | None = None,
    db_path: str | Path | None = DB_PATH,
) -> dict | None:
    sql = "SELECT operation_id FROM pair_operations WHERE pair = ? AND status NOT IN ('closed', 'failed', 'open')"
    params: list[Any] = [pair]
    if operation_type:
        sql += " AND operation_type = ?"
        params.append(str(operation_type).lower())
    sql += " ORDER BY created_at DESC LIMIT 1"
    with get_conn(db_path) as conn:
        row = conn.execute(sql, params).fetchone()
    return get_operation(str(row[0]), db_path) if row else None


def get_pending_orders(
    position_id: int | None = None,
    db_path: str | Path | None = DB_PATH,
) -> list[dict]:
    with get_conn(db_path) as conn:
        if position_id is None:
            rows = conn.execute(
                """
                SELECT * FROM orders
                WHERE status IN ('pending', 'submitted', 'partially_filled')
                ORDER BY id
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM orders
                WHERE position_id = ?
                  AND status IN ('pending', 'submitted', 'partially_filled')
                ORDER BY id
                """,
                (position_id,),
            ).fetchall()
        return [dict(row) for row in rows]


def get_active_operations(db_path: str | Path | None = DB_PATH) -> list[dict]:
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT operation_id FROM pair_operations WHERE status NOT IN ('closed', 'failed') ORDER BY created_at"
        ).fetchall()
    return [operation for row in rows if (operation := get_operation(str(row[0]), db_path)) is not None]


def get_operations_pending_completion(db_path: str | Path | None = DB_PATH) -> list[dict]:
    """Return terminal-looking operations whose position/trade write is incomplete."""
    with get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT operation_id
            FROM pair_operations
            WHERE (operation_type = 'entry' AND status = 'open')
               OR (operation_type = 'exit' AND status = 'closed')
            ORDER BY updated_at
            """
        ).fetchall()
    pending: list[dict] = []
    for row in rows:
        operation = get_operation(str(row[0]), db_path)
        if operation is None:
            continue
        metadata = operation.get("metadata", {})
        if operation["operation_type"] == "entry":
            with get_conn(db_path) as conn:
                position = conn.execute(
                    "SELECT id FROM positions_stat_arb WHERE entry_operation_id = ? LIMIT 1",
                    (str(operation["operation_id"]),),
                ).fetchone()
            if position is None:
                pending.append(operation)
        else:
            position_id = metadata.get("position_id")
            if position_id is not None:
                with get_conn(db_path) as conn:
                    position = conn.execute(
                        "SELECT status FROM positions_stat_arb WHERE id = ?",
                        (int(position_id),),
                    ).fetchone()
                if position is not None and str(position[0]) == "open":
                    pending.append(operation)
    return pending


def get_all_orders(db_path: str | Path | None = DB_PATH) -> list[dict]:
    with get_conn(db_path) as conn:
        return [dict(row) for row in conn.execute("SELECT * FROM orders ORDER BY id").fetchall()]


def _reverse_side(side: str) -> str:
    return "BUY" if str(side).upper() == "SELL" else "SELL"


def _infer_entry_sides(entry_zscore: float, entry_hedge_ratio: float) -> tuple[str, str]:
    long_spread = entry_zscore < 0
    independent_side = "SELL" if entry_hedge_ratio >= 0 else "BUY"
    if not long_spread:
        independent_side = _reverse_side(independent_side)
    dependent_side = "BUY" if long_spread else "SELL"
    return independent_side, dependent_side


def _record_order_conn(
    conn: sqlite3.Connection,
    *,
    strategy_id: str,
    strategy_identifier: str,
    symbol: str,
    leg: str,
    leg_type: str,
    side: str,
    intended_price: float,
    intended_quantity_raw: float,
    submitted_quantity: float,
    submitted_time: str,
    broker: str,
    position_id: int | None = None,
    broker_order_id: str | None = None,
    status: str = "pending",
    operation_id: str | None = None,
    requested_quantity: float | None = None,
    environment: str | None = None,
    account_id: str | None = None,
    idempotency_key: str | None = None,
) -> int:
    side = _validate_side(side)
    requested = float(requested_quantity if requested_quantity is not None else submitted_quantity)
    initial_cumulative = float(submitted_quantity) if str(status).lower() == "filled" else 0.0
    cur = conn.execute(
        """
        INSERT INTO orders
            (strategy_id, strategy_identifier, position_id, symbol, leg, leg_type,
             side, intended_price, intended_quantity_raw, submitted_quantity,
             submitted_time, status, broker, broker_order_id,
             operation_id, requested_quantity, cumulative_filled_quantity,
             remaining_quantity, average_fill_price, broker_order_status,
             environment, account_id, idempotency_key)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            strategy_id,
            strategy_identifier,
            position_id,
            symbol,
            leg,
            leg_type,
            side,
            float(intended_price),
            float(intended_quantity_raw),
            float(submitted_quantity),
            submitted_time,
            str(status).lower(),
            broker,
            broker_order_id,
            operation_id,
            requested,
            initial_cumulative,
            max(requested - initial_cumulative, 0.0),
            float(intended_price) if initial_cumulative >= requested else None,
            str(status).upper(),
            environment,
            account_id,
            idempotency_key,
        ),
    )
    return int(cur.lastrowid)


def record_order(
    strategy_id: str,
    strategy_identifier: str,
    symbol: str,
    leg: str,
    leg_type: str,
    side: str,
    intended_price: float,
    intended_quantity_raw: float,
    submitted_quantity: float,
    submitted_time: str,
    broker: str,
    position_id: int | None = None,
    broker_order_id: str | None = None,
    status: str = "pending",
    db_path: str | Path | None = DB_PATH,
    *,
    operation_id: str | None = None,
    requested_quantity: float | None = None,
    environment: str | None = None,
    account_id: str | None = None,
    idempotency_key: str | None = None,
) -> int:
    """Insert a compatibility order row.

    New code should use :func:`create_pair_operation`, which inserts both
    operation legs and these rows atomically before any broker submission.
    """
    with get_conn(db_path) as conn:
        return _record_order_conn(
            conn,
            strategy_id=strategy_id,
            strategy_identifier=strategy_identifier,
            symbol=symbol,
            leg=leg,
            leg_type=leg_type,
            side=side,
            intended_price=intended_price,
            intended_quantity_raw=intended_quantity_raw,
            submitted_quantity=submitted_quantity,
            submitted_time=submitted_time,
            broker=broker,
            position_id=position_id,
            broker_order_id=broker_order_id,
            status=status,
            operation_id=operation_id,
            requested_quantity=requested_quantity,
            environment=environment,
            account_id=account_id,
            idempotency_key=idempotency_key,
        )


def _operation_status_for_legs(operation_type: str, legs: list[sqlite3.Row]) -> str:
    if not legs:
        return "created"
    statuses = [str(row["status"]) for row in legs]
    filled = [float(row["cumulative_filled_quantity"] or 0.0) > 0 for row in legs]
    if all(status == "filled" for status in statuses):
        return "open" if operation_type == "entry" else "closed"
    if any(status in {"requires_reconciliation"} for status in statuses):
        return "requires_reconciliation"
    if any(status in {"failed", "rejected"} for status in statuses):
        return "requires_reconciliation" if any(filled) or any(
            row["broker_order_id"] for row in legs
        ) else "failed"
    # A cancelled leg means the pair is no longer a clean two-leg operation.
    # Do not let the remaining ``created`` leg make it look like a normal
    # leg1_submitted operation that can be retried automatically.
    if any(status == "cancelled" for status in statuses):
        return "requires_reconciliation" if any(filled) or any(
            row["broker_order_id"] for row in legs
        ) else "failed"
    if any(filled) or any(status == "partially_filled" for status in statuses):
        return "partially_filled"
    if statuses[0] == "submitting":
        return "leg1_submitting"
    if statuses[0] in {"submitted", "cancelled"} and statuses[1] == "created":
        return "leg1_submitted"
    if statuses[1] == "submitting":
        return "leg2_submitting"
    if statuses[1] in {"submitted", "cancelled"}:
        return "leg2_submitted"
    return "created"


def _set_operation_status_conn(
    conn: sqlite3.Connection,
    operation_id: str,
    status: str,
    *,
    last_error: str | None = None,
) -> None:
    status = str(status).lower()
    if status not in ACTIVE_OPERATION_STATUSES | TERMINAL_OPERATION_STATUSES:
        raise ValueError(f"Unknown operation status: {status}")
    row = conn.execute(
        "SELECT status FROM pair_operations WHERE operation_id = ?", (operation_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"No operation with id={operation_id}")
    current = str(row[0])
    if current == status:
        conn.execute(
            "UPDATE pair_operations SET last_error = COALESCE(?, last_error), updated_at = ? WHERE operation_id = ?",
            (last_error, utc_now(), operation_id),
        )
        return
    if current in TERMINAL_OPERATION_STATUSES:
        # A broker response can arrive after a local submission exception was
        # recorded.  Preserve the failure, but allow explicit evidence to
        # reopen it into the manual-reconciliation state.
        if current == "failed" and status == "requires_reconciliation":
            conn.execute(
                "UPDATE pair_operations SET status = ?, last_error = COALESCE(?, last_error), updated_at = ? WHERE operation_id = ?",
                (status, last_error, utc_now(), operation_id),
            )
            return
        raise ValueError(f"Invalid operation transition {current} -> {status}")
    allowed: dict[str, set[str]] = {
        "created": {"leg1_submitting", "failed", "requires_reconciliation"},
        "leg1_submitting": {"leg1_submitted", "failed", "requires_reconciliation"},
        "leg1_submitted": {"leg2_submitting", "partially_filled", "failed", "requires_reconciliation"},
        "leg2_submitting": {"leg2_submitted", "partially_filled", "failed", "requires_reconciliation"},
        "leg2_submitted": {"partially_filled", "open", "closed", "failed", "requires_reconciliation"},
        "partially_filled": {"partially_filled", "open", "closed", "failed", "requires_reconciliation"},
        "open": {"closed", "requires_reconciliation"},
        "requires_reconciliation": {
            "requires_reconciliation",
            "leg1_submitted",
            "leg2_submitting",
            "leg2_submitted",
            "partially_filled",
            "open",
            "closed",
            "failed",
        },
    }
    if status not in allowed.get(current, set()):
        raise ValueError(f"Invalid operation transition {current} -> {status}")
    conn.execute(
        "UPDATE pair_operations SET status = ?, last_error = COALESCE(?, last_error), updated_at = ? WHERE operation_id = ?",
        (status, last_error, utc_now(), operation_id),
    )


def _recalculate_operation_status_conn(conn: sqlite3.Connection, operation_id: str) -> str:
    operation = conn.execute(
        "SELECT operation_type, status FROM pair_operations WHERE operation_id = ?", (operation_id,)
    ).fetchone()
    if operation is None:
        raise ValueError(f"No operation with id={operation_id}")
    legs = conn.execute(
        "SELECT * FROM operation_legs WHERE operation_id = ? ORDER BY leg", (operation_id,)
    ).fetchall()
    desired = _operation_status_for_legs(str(operation["operation_type"]), legs)
    current = str(operation["status"])
    # A legacy/exception path may have marked an operation failed before a
    # broker order ID or fill was persisted. Any later broker evidence must
    # recover it into the manual-reconciliation path, never disappear.
    if current == "failed" and desired in {"partially_filled", "open", "closed"}:
        desired = "requires_reconciliation"
    # A reconciliation block is sticky while any issue remains open; broker
    # evidence must never be hidden by a later poll. Once a complete startup
    # reconciliation has cleared all issues, a recovered pre-submission state
    # may advance (for example, from a recovered leg1 to leg1_submitted).
    if current == "requires_reconciliation" and desired not in {"open", "closed"}:
        open_issue = conn.execute(
            "SELECT 1 FROM reconciliation_issues WHERE status = 'open' LIMIT 1"
        ).fetchone()
        if open_issue:
            desired = current
    _set_operation_status_conn(conn, operation_id, desired)
    return desired


def create_pair_operation(
    *,
    strategy_id: str,
    strategy_identifier: str,
    pair: str,
    operation_type: str,
    ticker1: str,
    ticker2: str,
    legs: Iterable[dict[str, Any]],
    metadata: dict[str, Any] | None = None,
    environment: str = "SIMULATE",
    account_id: str | int | None = None,
    idempotency_key: str,
    operation_id: str | None = None,
    position_id: int | None = None,
    broker: str = "moomoo",
    db_path: str | Path | None = DB_PATH,
) -> str:
    """Atomically persist pair intent and both leg rows before submission."""
    op_type = str(operation_type).lower()
    env = str(environment).upper()
    if op_type not in {"entry", "exit"}:
        raise ValueError(f"Unsupported operation type: {operation_type}")
    if env not in {"SIMULATE", "REAL"}:
        raise ValueError(f"Unsupported trading environment: {environment}")
    leg_values = list(legs)
    if len(leg_values) != 2 or {str(leg.get("leg")) for leg in leg_values} != {"ticker1", "ticker2"}:
        raise ValueError("A pair operation requires exactly ticker1 and ticker2 legs")
    operation_id = operation_id or str(uuid.uuid4())
    created_at = utc_now()
    with get_conn(db_path) as conn:
        existing = conn.execute(
            "SELECT operation_id FROM pair_operations WHERE idempotency_key = ?",
            (idempotency_key,),
        ).fetchone()
        if existing:
            return str(existing[0])
        conn.execute(
            """
            INSERT INTO pair_operations
                (operation_id, strategy_id, strategy_identifier, pair, operation_type,
                 environment, account_id, ticker1, ticker2, idempotency_key, status,
                 metadata_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'created', ?, ?, ?)
            """,
            (
                operation_id,
                strategy_id,
                strategy_identifier,
                pair,
                op_type,
                env,
                str(account_id) if account_id is not None else None,
                ticker1,
                ticker2,
                idempotency_key,
                _json(metadata),
                created_at,
                created_at,
            ),
        )
        for leg in sorted(leg_values, key=lambda item: str(item["leg"])):
            name = str(leg["leg"])
            symbol = str(leg["symbol"])
            side = _validate_side(str(leg["side"]))
            intended_price = _finite_positive(leg["intended_price"], f"{name}.intended_price")
            intended_raw = _finite_positive(leg.get("intended_quantity_raw", leg["requested_quantity"]), f"{name}.intended_quantity_raw")
            requested = _finite_positive(leg["requested_quantity"], f"{name}.requested_quantity")
            local_order_id = _record_order_conn(
                conn,
                strategy_id=strategy_id,
                strategy_identifier=strategy_identifier,
                symbol=symbol,
                leg=name,
                leg_type=op_type,
                side=side,
                intended_price=intended_price,
                intended_quantity_raw=intended_raw,
                submitted_quantity=requested,
                submitted_time=created_at,
                broker=broker,
                position_id=position_id,
                status="pending",
                operation_id=operation_id,
                requested_quantity=requested,
                environment=env,
                account_id=str(account_id) if account_id is not None else None,
                idempotency_key=idempotency_key,
            )
            cur = conn.execute(
                """
                INSERT INTO operation_legs
                    (operation_id, leg, symbol, side, intended_price,
                     intended_quantity_raw, requested_quantity, status,
                     remaining_quantity, local_order_id, position_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'created', ?, ?, ?)
                """,
                (
                    operation_id,
                    name,
                    symbol,
                    side,
                    intended_price,
                    intended_raw,
                    requested,
                    requested,
                    local_order_id,
                    position_id,
                ),
            )
            # The operation_legs FK is intentionally set after the order insert;
            # both rows are still committed in this one transaction.
            conn.execute(
                "UPDATE orders SET operation_id = ? WHERE id = ?",
                (operation_id, local_order_id),
            )
        return operation_id


def _get_leg_conn(
    conn: sqlite3.Connection,
    *,
    operation_id: str | None = None,
    leg: str | None = None,
    local_order_id: int | None = None,
    broker_order_id: str | None = None,
) -> sqlite3.Row:
    if operation_id is not None and leg is not None:
        row = conn.execute(
            "SELECT * FROM operation_legs WHERE operation_id = ? AND leg = ?",
            (operation_id, leg),
        ).fetchone()
    elif local_order_id is not None:
        row = conn.execute(
            "SELECT * FROM operation_legs WHERE local_order_id = ?", (local_order_id,)
        ).fetchone()
    elif broker_order_id is not None:
        row = conn.execute(
            "SELECT * FROM operation_legs WHERE broker_order_id = ?", (str(broker_order_id),)
        ).fetchone()
    else:
        row = None
    if row is None:
        raise ValueError("Operation leg could not be located")
    return row


def transition_leg(
    operation_id: str,
    leg: str,
    status: str,
    *,
    error: str | None = None,
    db_path: str | Path | None = DB_PATH,
) -> None:
    status = str(status).lower()
    if status not in LEG_STATES:
        raise ValueError(f"Unknown leg status: {status}")
    allowed = {
        "created": {"submitting", "failed", "requires_reconciliation"},
        "submitting": {"submitted", "failed", "requires_reconciliation"},
        "submitted": {"partially_filled", "filled", "cancelled", "rejected", "failed", "requires_reconciliation"},
        "partially_filled": {"partially_filled", "filled", "cancelled", "rejected", "failed", "requires_reconciliation"},
        "filled": {"filled", "requires_reconciliation"},
        "cancelled": {"cancelled", "requires_reconciliation"},
        "rejected": {"rejected", "requires_reconciliation"},
        "failed": {"failed", "requires_reconciliation"},
        "requires_reconciliation": {"requires_reconciliation", "submitted", "partially_filled", "filled", "cancelled"},
    }
    with get_conn(db_path) as conn:
        current_row = _get_leg_conn(conn, operation_id=operation_id, leg=leg)
        current = str(current_row["status"])
        if current != status and status not in allowed.get(current, set()):
            raise ValueError(f"Invalid leg transition {current} -> {status}")
        if status == "filled":
            requested = float(current_row["requested_quantity"])
            cumulative = float(current_row["cumulative_filled_quantity"] or 0.0)
            if cumulative + _quantity_tolerance(requested) < requested:
                raise ValueError(
                    f"Cannot mark {operation_id}:{leg} filled before requested quantity "
                    f"is evidenced ({cumulative:g}/{requested:g})"
                )
        conn.execute(
            "UPDATE operation_legs SET status = ?, last_error = COALESCE(?, last_error) WHERE id = ?",
            (status, error, current_row["id"]),
        )
        order_status = {
            "created": "pending",
            "submitting": "pending",
            "submitted": "submitted",
            "partially_filled": "partially_filled",
            "filled": "filled",
            "cancelled": "cancelled",
            "rejected": "rejected",
            "failed": "rejected",
            "requires_reconciliation": "submitted",
        }[status]
        if current_row["local_order_id"]:
            conn.execute(
                "UPDATE orders SET status = ?, broker_order_status = ?, remaining_quantity = COALESCE(remaining_quantity, requested_quantity) WHERE id = ?",
                (order_status, status.upper(), current_row["local_order_id"]),
            )
        _recalculate_operation_status_conn(conn, operation_id)


def mark_leg_submitting(operation_id: str, leg: str, db_path: str | Path | None = DB_PATH) -> None:
    transition_leg(operation_id, leg, "submitting", db_path=db_path)


def record_broker_submission(
    operation_id: str,
    leg: str,
    broker_order_id: str,
    submitted_at: str | None = None,
    *,
    broker_order_status: str = "SUBMITTED",
    db_path: str | Path | None = DB_PATH,
) -> None:
    if not broker_order_id:
        raise ValueError("broker_order_id is required")
    with get_conn(db_path) as conn:
        leg_row = _get_leg_conn(conn, operation_id=operation_id, leg=leg)
        current = str(leg_row["status"])
        if current not in {"submitting", "submitted", "partially_filled", "requires_reconciliation"}:
            raise ValueError(f"Cannot record broker submission while leg is {current}")
        previous_broker_id = str(leg_row["broker_order_id"] or "")
        if previous_broker_id and previous_broker_id != str(broker_order_id):
            raise ValueError(
                f"Broker order ID changed for {operation_id}:{leg} "
                f"({previous_broker_id} -> {broker_order_id})"
            )
        now = submitted_at or utc_now()
        conn.execute(
            """
            UPDATE operation_legs
            SET broker_order_id = ?, submitted_at = COALESCE(submitted_at, ?),
                broker_order_status = ?, status = CASE WHEN status IN ('submitting', 'requires_reconciliation') THEN 'submitted' ELSE status END,
                last_error = NULL
            WHERE id = ?
            """,
            (str(broker_order_id), now, str(broker_order_status).upper(), leg_row["id"]),
        )
        if leg_row["local_order_id"]:
            conn.execute(
                """
                UPDATE orders
                SET broker_order_id = ?, submitted_time = COALESCE(submitted_time, ?),
                    status = CASE WHEN status IN ('pending', 'submitted', 'rejected') THEN 'submitted' ELSE status END,
                    broker_order_status = ?
                WHERE id = ?
                """,
                (str(broker_order_id), now, str(broker_order_status).upper(), leg_row["local_order_id"]),
            )
        _recalculate_operation_status_conn(conn, operation_id)


def mark_leg_failure(
    operation_id: str,
    leg: str,
    error: str,
    *,
    broker_order_id: str | None = None,
    db_path: str | Path | None = DB_PATH,
) -> None:
    with get_conn(db_path) as conn:
        row = _get_leg_conn(conn, operation_id=operation_id, leg=leg)
        if broker_order_id:
            previous_broker_id = str(row["broker_order_id"] or "")
            if previous_broker_id and previous_broker_id != str(broker_order_id):
                raise ValueError(
                    f"Broker order ID changed for {operation_id}:{leg} "
                    f"({previous_broker_id} -> {broker_order_id})"
                )
            conn.execute(
                "UPDATE operation_legs SET broker_order_id = ? WHERE id = ?",
                (str(broker_order_id), row["id"]),
            )
        current = str(row["status"])
        if current not in {"failed", "rejected", "requires_reconciliation"}:
            if current not in {"created", "submitting", "submitted", "partially_filled"}:
                raise ValueError(f"Cannot fail leg while in {current}")
            conn.execute(
                "UPDATE operation_legs SET status = 'failed', last_error = ? WHERE id = ?",
                (str(error)[:2000], row["id"]),
            )
        else:
            conn.execute(
                "UPDATE operation_legs SET last_error = ? WHERE id = ?",
                (str(error)[:2000], row["id"]),
            )
        if row["local_order_id"]:
            conn.execute(
                "UPDATE orders SET status = 'rejected', broker_order_status = 'FAILED' WHERE id = ?",
                (row["local_order_id"],),
            )
        _recalculate_operation_status_conn(conn, operation_id)


def _fill_event_key(
    broker_order_id: str | None,
    broker_fill_id: str | None,
    fill_time: str,
    quantity: float,
    price: float,
) -> str:
    if broker_fill_id:
        # The fill/deal ID is scoped by operation leg by the caller, so it
        # remains stable even if the first callback lacked the order ID and a
        # later replay includes it.
        return f"fill:{broker_fill_id}"
    raw = f"{broker_order_id or ''}|{fill_time}|{quantity:.12g}|{price:.12g}"
    return "fingerprint:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _record_fill_conn(
    conn: sqlite3.Connection,
    leg_row: sqlite3.Row,
    *,
    fill_price: float,
    fill_time: str,
    fill_quantity: float,
    broker_fill_id: str | None = None,
    broker_order_status: str | None = None,
    raw_payload: Any = None,
) -> dict[str, Any]:
    price = _finite_positive(fill_price, "fill_price")
    quantity = _finite_positive(fill_quantity, "fill_quantity")
    event_key = "leg:{0}:".format(leg_row["id"]) + _fill_event_key(
        str(leg_row["broker_order_id"] or ""), broker_fill_id, str(fill_time), quantity, price
    )
    existing_event = conn.execute(
        "SELECT 1 FROM fill_events WHERE event_key = ?", (event_key,)
    ).fetchone()
    if existing_event:
        current = conn.execute("SELECT * FROM operation_legs WHERE id = ?", (leg_row["id"],)).fetchone()
        return {"duplicate": True, "cumulative_filled_quantity": float(current["cumulative_filled_quantity"] or 0)}

    requested = float(leg_row["requested_quantity"])
    previous = float(leg_row["cumulative_filled_quantity"] or 0.0)
    tolerance = _quantity_tolerance(requested)
    remaining_before = max(requested - previous, 0.0)
    if previous + tolerance >= requested:
        raise ValueError(
            f"New fill would exceed already-filled quantity for leg {leg_row['id']}"
        )
    if quantity > remaining_before + tolerance:
        raise ValueError(
            f"Fill quantity {quantity:g} exceeds remaining requested quantity "
            f"{remaining_before:g} for leg {leg_row['id']}"
        )
    applied_quantity = min(quantity, remaining_before)
    inserted = conn.execute(
        """
        INSERT OR IGNORE INTO fill_events
            (operation_leg_id, broker_order_id, broker_fill_id, event_key,
             fill_time, quantity, price, broker_status, raw_payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            leg_row["id"],
            leg_row["broker_order_id"],
            broker_fill_id,
            event_key,
            str(fill_time),
            applied_quantity,
            price,
            broker_order_status,
            _json(raw_payload) if raw_payload is not None else None,
            utc_now(),
        ),
    )
    if inserted.rowcount == 0:
        current = conn.execute("SELECT * FROM operation_legs WHERE id = ?", (leg_row["id"],)).fetchone()
        return {"duplicate": True, "cumulative_filled_quantity": float(current["cumulative_filled_quantity"] or 0)}
    previous_avg = leg_row["average_fill_price"]
    cumulative = min(requested, previous + applied_quantity)
    average = price if previous <= 0 or previous_avg is None else (
        (float(previous_avg) * previous + price * applied_quantity) / (previous + applied_quantity)
    )
    status = "filled" if cumulative + _quantity_tolerance(requested) >= requested else "partially_filled"
    remaining = max(requested - cumulative, 0.0)
    conn.execute(
        """
        UPDATE operation_legs
        SET cumulative_filled_quantity = ?, remaining_quantity = ?,
            average_fill_price = ?, last_fill_at = ?, status = ?,
            broker_order_status = COALESCE(?, broker_order_status)
        WHERE id = ?
        """,
        (cumulative, remaining, average, str(fill_time), status, broker_order_status, leg_row["id"]),
    )
    if leg_row["local_order_id"]:
        intended = float(leg_row["intended_price"])
        side = str(leg_row["side"]).upper()
        slippage = average - intended if side == "BUY" else intended - average
        conn.execute(
            """
            UPDATE orders
            SET status = ?, fill_price = ?, fill_time = ?, fill_quantity = ?,
                slippage = ?, cumulative_filled_quantity = ?, remaining_quantity = ?,
                average_fill_price = ?, broker_order_status = COALESCE(?, broker_order_status)
            WHERE id = ?
            """,
            (
                status,
                average,
                str(fill_time),
                cumulative,
                slippage,
                cumulative,
                remaining,
                average,
                broker_order_status,
                leg_row["local_order_id"],
            ),
        )
    operation_id = str(leg_row["operation_id"])
    operation_status = _recalculate_operation_status_conn(conn, operation_id)
    return {
        "duplicate": False,
        "event_key": event_key,
        "operation_id": operation_id,
        "leg": leg_row["leg"],
        "status": status,
        "cumulative_filled_quantity": cumulative,
        "remaining_quantity": remaining,
        "average_fill_price": average,
        "operation_status": operation_status,
    }


def record_fill_event(
    *,
    operation_id: str | None = None,
    leg: str | None = None,
    local_order_id: int | None = None,
    broker_order_id: str | None = None,
    broker_fill_id: str | None = None,
    fill_price: float,
    fill_time: str,
    fill_quantity: float,
    broker_order_status: str | None = None,
    raw_payload: Any = None,
    db_path: str | Path | None = DB_PATH,
) -> dict[str, Any]:
    with get_conn(db_path) as conn:
        row = _get_leg_conn(
            conn,
            operation_id=operation_id,
            leg=leg,
            local_order_id=local_order_id,
            broker_order_id=broker_order_id,
        )
        if broker_order_id:
            supplied_broker_id = str(broker_order_id)
            stored_broker_id = str(row["broker_order_id"] or "")
            if stored_broker_id and stored_broker_id != supplied_broker_id:
                raise ValueError(
                    f"Fill broker order ID {supplied_broker_id} does not match "
                    f"stored leg order ID {stored_broker_id}"
                )
            if not stored_broker_id:
                conn.execute(
                    "UPDATE operation_legs SET broker_order_id = ? WHERE id = ?",
                    (supplied_broker_id, row["id"]),
                )
                if row["local_order_id"]:
                    conn.execute(
                        "UPDATE orders SET broker_order_id = ? WHERE id = ?",
                        (supplied_broker_id, row["local_order_id"]),
                    )
                row = conn.execute("SELECT * FROM operation_legs WHERE id = ?", (row["id"],)).fetchone()
        return _record_fill_conn(
            conn,
            row,
            fill_price=fill_price,
            fill_time=fill_time,
            fill_quantity=fill_quantity,
            broker_fill_id=broker_fill_id,
            broker_order_status=broker_order_status,
            raw_payload=raw_payload,
        )


def record_aggregate_fill(
    *,
    broker_order_id: str,
    cumulative_quantity: float,
    average_price: float,
    fill_time: str,
    broker_order_status: str | None = None,
    db_path: str | Path | None = DB_PATH,
) -> dict[str, Any]:
    """Apply an aggregate broker quantity only for the unseen delta.

    This is used when a broker has no deal ID or only exposes order-level
    ``dealt_qty``. Repeated aggregate callbacks therefore cannot double-count.
    """
    aggregate = _finite_positive(cumulative_quantity, "cumulative_quantity")
    with get_conn(db_path) as conn:
        row = _get_leg_conn(conn, broker_order_id=str(broker_order_id))
        current = float(row["cumulative_filled_quantity"] or 0.0)
        requested = float(row["requested_quantity"])
        if aggregate <= current + _quantity_tolerance(requested):
            conn.execute(
                "UPDATE operation_legs SET broker_order_status = COALESCE(?, broker_order_status) WHERE id = ?",
                (broker_order_status, row["id"]),
            )
            return {"duplicate": True, "cumulative_filled_quantity": current}
        delta = aggregate - current
        # A stable aggregate key prevents the same unseen cumulative value from
        # being applied again if two callbacks race.
        result = _record_fill_conn(
            conn,
            row,
            fill_price=average_price,
            fill_time=fill_time,
            fill_quantity=delta,
            broker_fill_id=f"aggregate:{aggregate:.12g}",
            broker_order_status=broker_order_status,
        )
        return result


def record_order_fill(
    order_id: int,
    fill_price: float,
    fill_time: str,
    fill_quantity: float,
    intended_price: float | None = None,
    db_path: str | Path | None = DB_PATH,
    *,
    broker_fill_id: str | None = None,
) -> dict[str, Any]:
    """Compatibility wrapper that now accumulates an idempotent fill event."""
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            raise ValueError(f"No order with id={order_id}")
        if row["operation_id"]:
            leg = _get_leg_conn(conn, local_order_id=order_id)
            return _record_fill_conn(
                conn,
                leg,
                fill_price=fill_price,
                fill_time=fill_time,
                fill_quantity=fill_quantity,
                broker_fill_id=broker_fill_id,
            )
        # Legacy rows without an operation still accumulate in the compatibility
        # columns and never downgrade a partial fill to full.
        price = _finite_positive(fill_price, "fill_price")
        quantity = _finite_positive(fill_quantity, "fill_quantity")
        requested = float(row["requested_quantity"] or row["submitted_quantity"])
        previous = float(row["cumulative_filled_quantity"] or row["fill_quantity"] or 0.0)
        tolerance = _quantity_tolerance(requested)
        if previous + tolerance >= requested:
            return {"duplicate": True, "status": "filled", "cumulative_filled_quantity": previous}
        remaining_before = max(requested - previous, 0.0)
        if quantity > remaining_before + tolerance:
            raise ValueError(
                f"Fill quantity {quantity:g} exceeds remaining requested quantity {remaining_before:g} for order {order_id}"
            )
        applied_quantity = min(quantity, remaining_before)
        cumulative = min(requested, previous + applied_quantity)
        previous_avg = row["average_fill_price"] or row["fill_price"]
        average = price if previous <= 0 or previous_avg is None else (
            (float(previous_avg) * previous + price * applied_quantity) / (previous + applied_quantity)
        )
        status = "filled" if cumulative + _quantity_tolerance(requested) >= requested else "partially_filled"
        basis = intended_price if intended_price is not None else float(row["intended_price"])
        side = str(row["side"]).upper()
        slippage = average - basis if side == "BUY" else basis - average
        conn.execute(
            """
            UPDATE orders
            SET fill_price = ?, fill_time = ?, fill_quantity = ?, slippage = ?,
                status = ?, cumulative_filled_quantity = ?, remaining_quantity = ?,
                average_fill_price = ?
            WHERE id = ?
            """,
            (average, str(fill_time), cumulative, slippage, status, cumulative, max(requested - cumulative, 0.0), average, order_id),
        )
        return {"duplicate": False, "status": status, "cumulative_filled_quantity": cumulative}


def update_order_status(
    order_id: int,
    status: str,
    db_path: str | Path | None = DB_PATH,
    *,
    broker_order_status: str | None = None,
) -> None:
    normalized = str(status).lower()
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            raise ValueError(f"No order with id={order_id}")
        if row["operation_id"]:
            leg = _get_leg_conn(conn, local_order_id=order_id)
            cumulative = float(leg["cumulative_filled_quantity"] or 0.0)
            requested = float(leg["requested_quantity"])
            if normalized in {"filled", "filled_all"} and cumulative + _quantity_tolerance(requested) < requested:
                normalized = "partially_filled" if cumulative > 0 else "submitted"
            mapped = {
                "pending": "created",
                "submitted": "submitted",
                "partially_filled": "partially_filled",
                "filled": "filled",
                "filled_all": "filled",
                "cancelled": "cancelled",
                "rejected": "rejected",
                "failed": "failed",
            }.get(normalized, normalized)
            if mapped not in LEG_STATES:
                raise ValueError(f"Unknown order status: {status}")
            current = str(leg["status"])
            # A late/stale broker callback must not erase a durable failure or
            # reconciliation block. Only new quantity evidence (handled by
            # record_fill_event) may advance those states.
            if current in {"requires_reconciliation", "failed", "rejected"} and mapped not in {"filled"}:
                mapped = current
                normalized = "rejected" if current in {"failed", "rejected"} else "submitted"
            elif current == "filled" and mapped not in {"filled", "requires_reconciliation"}:
                mapped = current
                normalized = "filled"
            elif current == "cancelled" and mapped not in {"cancelled", "filled", "requires_reconciliation"}:
                mapped = current
                normalized = "cancelled"
            allowed = {
                "submitted": {"submitted", "partially_filled", "filled", "cancelled", "rejected", "failed", "requires_reconciliation"},
                "partially_filled": {"partially_filled", "filled", "cancelled", "rejected", "failed", "requires_reconciliation"},
                "created": {"created", "submitting", "submitted", "failed", "requires_reconciliation"},
                "requires_reconciliation": {"requires_reconciliation", "partially_filled", "filled"},
                "failed": {"failed", "requires_reconciliation"},
                "rejected": {"rejected", "requires_reconciliation"},
                "cancelled": {"cancelled", "filled", "requires_reconciliation"},
                "filled": {"filled", "requires_reconciliation"},
            }
            if current != mapped and mapped not in allowed.get(current, {mapped}):
                raise ValueError(f"Invalid leg transition {current} -> {mapped}")
            conn.execute(
                "UPDATE operation_legs SET status = ?, broker_order_status = COALESCE(?, broker_order_status) WHERE id = ?",
                (mapped, broker_order_status or normalized.upper(), leg["id"]),
            )
            conn.execute(
                "UPDATE orders SET status = ?, broker_order_status = COALESCE(?, broker_order_status) WHERE id = ?",
                (normalized, broker_order_status or normalized.upper(), order_id),
            )
            _recalculate_operation_status_conn(conn, str(leg["operation_id"]))
            return
        conn.execute(
            "UPDATE orders SET status = ?, broker_order_status = COALESCE(?, broker_order_status) WHERE id = ?",
            (normalized, broker_order_status or normalized.upper(), order_id),
        )


def link_order_to_position(order_id: int, position_id: int, db_path: str | Path | None = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute("UPDATE orders SET position_id = ? WHERE id = ?", (position_id, order_id))
        conn.execute("UPDATE operation_legs SET position_id = ? WHERE local_order_id = ?", (position_id, order_id))


def _metadata_float(metadata: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = metadata.get(key, default)
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Durable metadata field {key} is not numeric") from None
    if not math.isfinite(number):
        raise ValueError(f"Durable metadata field {key} is not finite")
    return number


def open_position_from_operation(
    operation_id: str,
    *,
    entry_date: str,
    db_path: str | Path | None = DB_PATH,
) -> int:
    """Create the normal position only after both entry legs are fully filled."""
    with get_conn(db_path) as conn:
        op = conn.execute("SELECT * FROM pair_operations WHERE operation_id = ?", (operation_id,)).fetchone()
        if op is None or op["operation_type"] != "entry":
            raise ValueError(f"Entry operation not found: {operation_id}")
        legs = conn.execute("SELECT * FROM operation_legs WHERE operation_id = ? ORDER BY leg", (operation_id,)).fetchall()
        if len(legs) != 2 or any(str(leg["status"]) != "filled" for leg in legs):
            raise ValueError("Cannot open position until both entry legs are fully filled")
        if any(float(leg["cumulative_filled_quantity"] or 0.0) + _quantity_tolerance(float(leg["requested_quantity"])) < float(leg["requested_quantity"]) for leg in legs):
            raise ValueError("Cannot open position before requested quantities are filled")
        if any(leg["average_fill_price"] is None for leg in legs):
            raise ValueError("Cannot open position without an average fill price for both legs")
        existing = conn.execute(
            "SELECT id FROM positions_stat_arb WHERE entry_operation_id = ? ORDER BY id DESC LIMIT 1",
            (operation_id,),
        ).fetchone()
        if existing:
            return int(existing[0])
        existing = conn.execute(
            "SELECT id FROM positions_stat_arb WHERE pair = ? AND status = 'open' ORDER BY id DESC LIMIT 1",
            (op["pair"],),
        ).fetchone()
        if existing:
            return int(existing[0])
        metadata = _decode_json(op["metadata_json"])
        required_metadata = {
            "entry_hedge_ratio",
            "entry_alpha",
            "entry_residual_mean",
            "entry_residual_std",
            "entry_zscore",
        }
        missing_metadata = sorted(key for key in required_metadata if key not in metadata)
        if missing_metadata:
            raise ValueError(
                f"Entry operation {operation_id} is missing durable signal metadata: {', '.join(missing_metadata)}"
            )
        by_leg = {str(leg["leg"]): leg for leg in legs}
        leg1, leg2 = by_leg["ticker1"], by_leg["ticker2"]
        cur = conn.execute(
            """
            INSERT INTO positions_stat_arb
                (strategy_id, strategy_identifier, pair, ticker1, ticker2,
                 entry_hedge_ratio, entry_alpha, entry_residual_mean,
                 entry_residual_std, entry_zscore, entry_side1, entry_side2,
                 entry_leg1_price, entry_leg2_price, executed_size1, executed_size2,
                 intended_size1, intended_size2, entry_date, status, broker,
                 entry_operation_id, environment, account_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?)
            """,
            (
                op["strategy_id"],
                op["strategy_identifier"],
                op["pair"],
                op["ticker1"],
                op["ticker2"],
                _metadata_float(metadata, "entry_hedge_ratio"),
                _metadata_float(metadata, "entry_alpha"),
                _metadata_float(metadata, "entry_residual_mean"),
                _metadata_float(metadata, "entry_residual_std"),
                _metadata_float(metadata, "entry_zscore"),
                leg1["side"],
                leg2["side"],
                float(leg1["average_fill_price"]),
                float(leg2["average_fill_price"]),
                float(leg1["cumulative_filled_quantity"]),
                float(leg2["cumulative_filled_quantity"]),
                float(leg1["intended_quantity_raw"]),
                float(leg2["intended_quantity_raw"]),
                entry_date,
                op["broker"] if "broker" in op.keys() else "moomoo",
                operation_id,
                op["environment"],
                op["account_id"],
            ),
        )
        position_id = int(cur.lastrowid)
        conn.execute("UPDATE operation_legs SET position_id = ? WHERE operation_id = ?", (position_id, operation_id))
        conn.execute("UPDATE orders SET position_id = ? WHERE operation_id = ?", (position_id, operation_id))
        _set_operation_status_conn(conn, operation_id, "open")
        return position_id


def close_position_from_operation(
    operation_id: str,
    *,
    exit_date: str,
    db_path: str | Path | None = DB_PATH,
) -> int:
    """Close a position and write its trade summary after both exit fills."""
    with get_conn(db_path) as conn:
        op = conn.execute("SELECT * FROM pair_operations WHERE operation_id = ?", (operation_id,)).fetchone()
        if op is None or op["operation_type"] != "exit":
            raise ValueError(f"Exit operation not found: {operation_id}")
        legs = conn.execute("SELECT * FROM operation_legs WHERE operation_id = ? ORDER BY leg", (operation_id,)).fetchall()
        if len(legs) != 2 or any(str(leg["status"]) != "filled" for leg in legs):
            raise ValueError("Cannot close position until both exit legs are fully filled")
        if any(leg["average_fill_price"] is None for leg in legs):
            raise ValueError("Cannot close position without an average fill price for both legs")
        metadata = _decode_json(op["metadata_json"])
        position_id = metadata.get("position_id")
        if position_id is None:
            position_id = legs[0]["position_id"]
        if position_id is None:
            raise ValueError("Exit operation has no position_id")
        pos = conn.execute("SELECT * FROM positions_stat_arb WHERE id = ?", (int(position_id),)).fetchone()
        if pos is None:
            raise ValueError(f"No position with id={position_id}")
        if pos["status"] != "open":
            raise ValueError(f"Position {position_id} is already '{pos['status']}'")
        by_leg = {str(leg["leg"]): leg for leg in legs}
        leg1, leg2 = by_leg["ticker1"], by_leg["ticker2"]
        entry_side1, entry_side2 = str(pos["entry_side1"]), str(pos["entry_side2"])
        exit_price1, exit_price2 = float(leg1["average_fill_price"]), float(leg2["average_fill_price"])
        entry_price1, entry_price2 = float(pos["entry_leg1_price"]), float(pos["entry_leg2_price"])
        size1, size2 = float(pos["executed_size1"]), float(pos["executed_size2"])
        pnl1 = (exit_price1 - entry_price1) * size1 if entry_side1 == "BUY" else (entry_price1 - exit_price1) * size1
        pnl2 = (exit_price2 - entry_price2) * size2 if entry_side2 == "BUY" else (entry_price2 - exit_price2) * size2
        conn.execute(
            "UPDATE positions_stat_arb SET status = 'closed', exit_operation_id = ? WHERE id = ?",
            (operation_id, int(position_id)),
        )
        cur = conn.execute(
            """
            INSERT INTO trades
                (position_id, strategy_id, strategy_identifier, entry_date, exit_date,
                 entry_price1, entry_price2, exit_price1, exit_price2,
                 entry_zscore, exit_zscore, exit_reason, realized_pnl, exit_operation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(position_id),
                pos["strategy_id"],
                pos["strategy_identifier"],
                pos["entry_date"],
                exit_date,
                entry_price1,
                entry_price2,
                exit_price1,
                exit_price2,
                pos["entry_zscore"],
                metadata.get("exit_zscore"),
                metadata.get("exit_reason", "unknown"),
                pnl1 + pnl2,
                operation_id,
            ),
        )
        conn.execute("UPDATE operation_legs SET position_id = ? WHERE operation_id = ?", (int(position_id), operation_id))
        _set_operation_status_conn(conn, operation_id, "closed")
        return int(cur.lastrowid)


def open_position(
    pair: str,
    ticker1: str,
    ticker2: str,
    entry_hedge_ratio: float,
    entry_alpha: float,
    entry_residual_mean: float,
    entry_residual_std: float,
    entry_zscore: float,
    entry_date: str,
    broker: str,
    intended_size1: float | None = None,
    intended_size2: float | None = None,
    entry_side1: str | None = None,
    entry_side2: str | None = None,
    entry_leg1_price: float | None = None,
    entry_leg2_price: float | None = None,
    executed_size1: float | None = None,
    executed_size2: float | None = None,
    strategy_id: str = "stat_arb",
    strategy_identifier: str | None = None,
    order_id1: int | None = None,
    order_id2: int | None = None,
    intended_price1: float | None = None,
    intended_time1: str | None = None,
    fill_price1: float | None = None,
    fill_time1: str | None = None,
    intended_price2: float | None = None,
    intended_time2: str | None = None,
    fill_price2: float | None = None,
    fill_time2: str | None = None,
    db_path: str | Path | None = DB_PATH,
    **legacy_kwargs,
) -> int:
    """Legacy position insert retained for existing tooling."""
    identifier = strategy_identifier or pair
    entry_side1, entry_side2 = (
        entry_side1,
        entry_side2,
    ) if entry_side1 and entry_side2 else _infer_entry_sides(entry_zscore, entry_hedge_ratio)
    entry_leg1_price = entry_leg1_price if entry_leg1_price is not None else legacy_kwargs.get("latest_price_s1")
    entry_leg2_price = entry_leg2_price if entry_leg2_price is not None else legacy_kwargs.get("latest_price_s2")
    executed_size1 = executed_size1 if executed_size1 is not None else legacy_kwargs.get("target_size1") or legacy_kwargs.get("submitted_quantity1")
    executed_size2 = executed_size2 if executed_size2 is not None else legacy_kwargs.get("target_size2") or legacy_kwargs.get("submitted_quantity2")
    intended_size1 = intended_size1 if intended_size1 is not None else executed_size1
    intended_size2 = intended_size2 if intended_size2 is not None else executed_size2
    if entry_leg1_price is None or entry_leg2_price is None or executed_size1 is None or executed_size2 is None:
        raise ValueError("entry prices and executed sizes are required to open a position")
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO positions_stat_arb
                (strategy_id, strategy_identifier, pair, ticker1, ticker2,
                 entry_hedge_ratio, entry_alpha, entry_residual_mean,
                 entry_residual_std, entry_zscore, entry_side1, entry_side2,
                 entry_leg1_price, entry_leg2_price, executed_size1, executed_size2,
                 intended_size1, intended_size2, entry_date, status, broker)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
            """,
            (strategy_id, identifier, pair, ticker1, ticker2, entry_hedge_ratio, entry_alpha,
             entry_residual_mean, entry_residual_std, entry_zscore, _validate_side(entry_side1),
             _validate_side(entry_side2), entry_leg1_price, entry_leg2_price, executed_size1,
             executed_size2, intended_size1, intended_size2, entry_date, broker),
        )
        position_id = int(cur.lastrowid)
        if order_id1 is not None:
            conn.execute("UPDATE orders SET position_id = ? WHERE id = ?", (position_id, order_id1))
        if order_id2 is not None:
            conn.execute("UPDATE orders SET position_id = ? WHERE id = ?", (position_id, order_id2))
        return position_id


def close_position(
    position_id: int,
    exit_date: str,
    exit_reason: str,
    exit_zscore: float,
    exit_side1: str | None = None,
    exit_side2: str | None = None,
    exit_price1: float | None = None,
    exit_price2: float | None = None,
    order_id1: int | None = None,
    order_id2: int | None = None,
    intended_price1: float | None = None,
    intended_time1: str | None = None,
    fill_time1: str | None = None,
    intended_price2: float | None = None,
    intended_time2: str | None = None,
    fill_time2: str | None = None,
    db_path: str | Path | None = DB_PATH,
    **legacy_kwargs,
) -> int:
    with get_conn(db_path) as conn:
        pos = conn.execute("SELECT * FROM positions_stat_arb WHERE id = ?", (position_id,)).fetchone()
        if pos is None:
            raise ValueError(f"No position with id={position_id}")
        if pos["status"] != "open":
            raise ValueError(f"Position {position_id} is already '{pos['status']}'")
        exit_price1 = exit_price1 if exit_price1 is not None else legacy_kwargs.get("exit_price1") or legacy_kwargs.get("exit_price")
        exit_price2 = exit_price2 if exit_price2 is not None else legacy_kwargs.get("exit_price2") or legacy_kwargs.get("exit_price")
        if exit_price1 is None or exit_price2 is None:
            raise ValueError("exit prices are required to close a position")
        entry_side1, entry_side2 = str(pos["entry_side1"]), str(pos["entry_side2"])
        size1, size2 = float(pos["executed_size1"]), float(pos["executed_size2"])
        entry_price1, entry_price2 = float(pos["entry_leg1_price"]), float(pos["entry_leg2_price"])
        pnl1 = (float(exit_price1) - entry_price1) * size1 if entry_side1 == "BUY" else (entry_price1 - float(exit_price1)) * size1
        pnl2 = (float(exit_price2) - entry_price2) * size2 if entry_side2 == "BUY" else (entry_price2 - float(exit_price2)) * size2
        conn.execute("UPDATE positions_stat_arb SET status = 'closed' WHERE id = ?", (position_id,))
        cur = conn.execute(
            """
            INSERT INTO trades
                (position_id, strategy_id, strategy_identifier, entry_date, exit_date,
                 entry_price1, entry_price2, exit_price1, exit_price2,
                 entry_zscore, exit_zscore, exit_reason, realized_pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (position_id, pos["strategy_id"], pos["strategy_identifier"], pos["entry_date"], exit_date,
             entry_price1, entry_price2, float(exit_price1), float(exit_price2), pos["entry_zscore"],
             exit_zscore, exit_reason, pnl1 + pnl2),
        )
        return int(cur.lastrowid)


def upsert_reconciliation_issue(
    *,
    category: str,
    entity_key: str,
    details: dict[str, Any],
    severity: str = "high",
    db_path: str | Path | None = DB_PATH,
) -> str:
    issue_id = hashlib.sha256(f"{category}|{entity_key}".encode("utf-8")).hexdigest()[:32]
    now = utc_now()
    with get_conn(db_path) as conn:
        conn.execute(
            """
            INSERT INTO reconciliation_issues
                (issue_id, category, severity, entity_key, details_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'open', ?, ?)
            ON CONFLICT(issue_id) DO UPDATE SET
                severity = excluded.severity,
                details_json = excluded.details_json,
                status = 'open',
                updated_at = excluded.updated_at
            """,
            (issue_id, category, severity, entity_key, _json(details), now, now),
        )
    return issue_id


def resolve_reconciliation_issue(issue_id: str, db_path: str | Path | None = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            "UPDATE reconciliation_issues SET status = 'resolved', updated_at = ? WHERE issue_id = ?",
            (utc_now(), issue_id),
        )


def resolve_reconciliation_issue_by_key(
    category: str,
    entity_key: str,
    db_path: str | Path | None = DB_PATH,
) -> None:
    issue_id = hashlib.sha256(f"{category}|{entity_key}".encode("utf-8")).hexdigest()[:32]
    resolve_reconciliation_issue(issue_id, db_path)


def get_open_reconciliation_issues(db_path: str | Path | None = DB_PATH) -> list[dict]:
    with get_conn(db_path) as conn:
        return [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM reconciliation_issues WHERE status = 'open' ORDER BY severity DESC, created_at"
            ).fetchall()
        ]


def set_system_state(key: str, value: Any, db_path: str | Path | None = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            """
            INSERT INTO system_state(key, value, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, _json(value) if isinstance(value, (dict, list)) else str(value), utc_now()),
        )


def get_system_state(key: str, db_path: str | Path | None = DB_PATH) -> str | None:
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT value FROM system_state WHERE key = ?", (key,)).fetchone()
    return str(row[0]) if row else None
