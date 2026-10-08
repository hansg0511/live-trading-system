"""Prepare the one-time Oct 7 Stage 6 SIM fill-evidence repair.

This utility is intentionally narrower than the trading repository.  It does
not import the OMS, connect to a broker, or accept arbitrary row identifiers.
It consumes an independently captured ``broker-preflight --json`` document and
can only ever change ``core_fills.evidence_mode`` for the two fixed Oct 7
historical fills.  The default is a read-only dry run; ``--apply`` requires an
exact confirmation phrase and performs a SQLite-backup-protected transaction.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from typing import Any, Mapping


EXPECTED_DB_FILENAME = "stage6-pilot-sim-20261001.db"
EXPECTED_ACCOUNT_ID = "moomoo:sim:5077333"
EXPECTED_SOURCE_INTENT_ID = "stage5-intent-fef719dc15a0f254b383cc0e"
EXPECTED_STRATEGY_ID = "stage6-controlled-sim-20261001"
EXPECTED_BROKER = "moomoo"
EXPECTED_BOOK_ID = "stage6-book-a-20261001"
EXPECTED_MODE = "CUMULATIVE_ORDER_SNAPSHOTS"
OLD_MODE = "INDIVIDUAL_DEALS"
DEFAULT_MAX_EVIDENCE_AGE_SECONDS = 900
EXPECTED_NONFLAT_ENTRY_BLOCKER = "book stage6-book-a-20261001 is not flat before entry"
CONFIRMATION = (
    "APPLY STAGE6 OCT7 EVIDENCE MODE REPAIR "
    f"{EXPECTED_DB_FILENAME} "
    "fills=03eeacbe-159b-5300-ba9b-b60015a30c02,"
    "7181461e-d5d5-518d-ae49-3f2b73b0dbdb"
)

TARGETS: tuple[dict[str, Any], ...] = (
    {
        "fill_id": "03eeacbe-159b-5300-ba9b-b60015a30c02",
        "broker_order_id": "f368e259-d12f-484b-84ae-5b0fd89132a4",
        "order_leg_id": f"{EXPECTED_SOURCE_INTENT_ID}-leg-0",
        "external_order_id": "3449827",
        "instrument_id": "stage6:us-aapl",
        "side": "BUY",
        "quantity": "1",
        "price": "336.57",
        "filled_at": "2026-10-07T17:13:03+00:00",
        "evidence_reference": "3449827:moomoo-order-fill:3449827",
        "raw_code": "US.AAPL",
        "external_symbol": "US.AAPL",
    },
    {
        "fill_id": "7181461e-d5d5-518d-ae49-3f2b73b0dbdb",
        "broker_order_id": "53d8d07e-abf2-4b17-82a8-f8911c14b224",
        "order_leg_id": f"{EXPECTED_SOURCE_INTENT_ID}-leg-1",
        "external_order_id": "3449828",
        "instrument_id": "stage6:us-msft",
        "side": "SELL",
        "quantity": "1",
        "price": "527.58",
        "filled_at": "2026-10-07T17:13:00+00:00",
        "evidence_reference": "3449828:moomoo-order-fill:3449828",
        "raw_code": "US.MSFT",
        "external_symbol": "US.MSFT",
    },
)

ACTIVE_INTENT_STATUSES = (
    "CREATED",
    "RISK_APPROVED",
    "SUBMITTING",
    "WORKING",
    "PARTIALLY_FILLED",
    "RECONCILIATION_REQUIRED",
)
# A terminal parent intent is not an active process merely because an
# unsubmitted child leg remains in its initial PLANNED state.  Keep this list
# explicit so a terminal intent with a genuinely working leg/order still
# blocks below.
TERMINAL_INTENT_STATUSES = (
    "FILLED",
    "COMPLETED",
    "REJECTED",
    "CANCELLED",
    "FAILED",
)
ACTIVE_LEG_STATUSES = (
    "PLANNED",
    "SUBMITTING",
    "WORKING",
    "PARTIALLY_FILLED",
    "RECONCILIATION_REQUIRED",
)
ACTIVE_ORDER_STATUSES = (
    "PREPARED",
    "SUBMITTING",
    "WORKING",
    "PARTIALLY_FILLED",
    "UNKNOWN",
)
ACTIVE_LOCK_TABLES = (
    "core_active_locks",
    "core_process_locks",
    "core_execution_locks",
)


class RepairBlocked(RuntimeError):
    """A fail-closed validation or safety-gate failure."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_hash(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _parse_time(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise RepairBlocked(f"{label} is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RepairBlocked(f"{label} is not ISO-8601: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RepairBlocked(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _decimal(value: Any, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RepairBlocked(f"{label} is not a decimal: {value!r}") from exc
    if not parsed.is_finite():
        raise RepairBlocked(f"{label} is not finite")
    return parsed


def _same_decimal(left: Any, right: str, label: str) -> bool:
    try:
        return _decimal(left, label) == _decimal(right, label)
    except RepairBlocked:
        return False


def _decode_json(value: Any, label: str) -> Mapping[str, Any]:
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RepairBlocked(f"{label} is not valid JSON") from exc
    if not isinstance(decoded, Mapping):
        raise RepairBlocked(f"{label} must be a JSON object")
    return decoded


def _open_readonly(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=0.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _table_names(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [str(row[0]) for row in rows]


def _normalise_sql_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__bytes__": value.hex()}
    return value


def _database_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    for table in _table_names(connection):
        columns = [str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()]
        rows = []
        for row in connection.execute(f'SELECT * FROM "{table}"').fetchall():
            rows.append([_normalise_sql_value(row[column]) for column in columns])
        rows.sort(key=_canonical_json)
        tables[table] = {"columns": columns, "rows": rows}
    return tables


def _snapshot_digest(snapshot: Mapping[str, Any]) -> str:
    return _json_hash(snapshot)


def _target_row_snapshot(connection: sqlite3.Connection, target: Mapping[str, Any]) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT f.id AS fill_id, f.broker_order_id, f.order_leg_id,
               f.external_fill_id, f.dedupe_key, f.quantity AS fill_quantity,
               f.price AS fill_price, f.filled_at, f.received_at,
               f.evidence_mode, f.metadata_json,
               o.account_id, o.broker, o.external_order_id, o.status AS order_status,
               o.submitted_quantity, l.intent_id, l.instrument_id, l.side,
               l.quantity AS leg_quantity, l.status AS leg_status,
               l.cumulative_filled_quantity, i.book_id, i.strategy_id, i.action,
               i.status AS intent_status, i.account_id AS intent_account_id
          FROM core_fills f
          JOIN core_broker_orders o ON o.id = f.broker_order_id
          JOIN core_order_legs l ON l.id = f.order_leg_id
          JOIN core_order_intents i ON i.id = l.intent_id
         WHERE f.id = ?
        """,
        (target["fill_id"],),
    ).fetchone()
    if row is None:
        raise RepairBlocked(f"target fill row is missing: {target['fill_id']}")
    return {str(key): row[key] for key in row.keys()}


def _validate_local_target(connection: sqlite3.Connection, target: Mapping[str, Any]) -> dict[str, Any]:
    row = _target_row_snapshot(connection, target)
    exact_fields = {
        "fill_id": target["fill_id"],
        "broker_order_id": target["broker_order_id"],
        "order_leg_id": target["order_leg_id"],
        "account_id": EXPECTED_ACCOUNT_ID,
        "intent_account_id": EXPECTED_ACCOUNT_ID,
        "broker": EXPECTED_BROKER,
        "external_order_id": target["external_order_id"],
        "order_status": "FILLED",
        "intent_id": EXPECTED_SOURCE_INTENT_ID,
        "book_id": EXPECTED_BOOK_ID,
        "strategy_id": EXPECTED_STRATEGY_ID,
        "action": "ENTER",
        "intent_status": "FILLED",
        "instrument_id": target["instrument_id"],
        "side": target["side"],
        "leg_status": "FILLED",
    }
    for field, expected in exact_fields.items():
        if str(row.get(field)) != str(expected):
            raise RepairBlocked(
                f"local target {target['external_order_id']} {field} mismatch: "
                f"expected {expected!r}, observed {row.get(field)!r}"
            )
    mapping = connection.execute(
        """
        SELECT provider, purpose, external_symbol
          FROM core_instrument_mappings
         WHERE instrument_id = ? AND provider = 'moomoo' AND purpose = 'BROKER'
        """,
        (target["instrument_id"],),
    ).fetchall()
    if len(mapping) != 1 or str(mapping[0]["external_symbol"]) != target["external_symbol"]:
        raise RepairBlocked(
            f"local target {target['external_order_id']} broker mapping mismatch: "
            f"expected moomoo/BROKER/{target['external_symbol']!r}, observed {[dict(item) for item in mapping]!r}"
        )
    for field in ("fill_quantity", "leg_quantity", "cumulative_filled_quantity", "submitted_quantity"):
        if not _same_decimal(row[field], target["quantity"], f"local {field}"):
            raise RepairBlocked(
                f"local target {target['external_order_id']} {field} mismatch: "
                f"expected {target['quantity']!r}, observed {row[field]!r}"
            )
    if not _same_decimal(row["fill_price"], target["price"], "local fill price"):
        raise RepairBlocked(
            f"local target {target['external_order_id']} price mismatch: "
            f"expected {target['price']!r}, observed {row['fill_price']!r}"
        )
    if _parse_time(str(row["filled_at"]), "local filled_at") != _parse_time(target["filled_at"], "expected filled_at"):
        raise RepairBlocked(
            f"local target {target['external_order_id']} filled_at mismatch: "
            f"expected {target['filled_at']!r}, observed {row['filled_at']!r}"
        )
    if str(row["evidence_mode"]) != OLD_MODE:
        raise RepairBlocked(
            f"local target {target['external_order_id']} evidence_mode is already "
            f"{row['evidence_mode']!r}; refusing to reclassify it"
        )
    metadata = _decode_json(row["metadata_json"], f"local target {target['external_order_id']} metadata")
    if metadata.get("_external_order_id") != target["external_order_id"]:
        raise RepairBlocked(f"local target {target['external_order_id']} external-order provenance mismatch")
    if metadata.get("_evidence_reference") != target["evidence_reference"]:
        raise RepairBlocked(f"local target {target['external_order_id']} evidence-reference provenance mismatch")
    if metadata.get("evidence_mode") != EXPECTED_MODE:
        raise RepairBlocked(f"local target {target['external_order_id']} metadata evidence_mode mismatch")
    if metadata.get("evidence_reference") != target["evidence_reference"]:
        raise RepairBlocked(f"local target {target['external_order_id']} metadata evidence_reference mismatch")
    if metadata.get("evidence_scope") != "CURRENT_ORDER_SNAPSHOTS":
        raise RepairBlocked(f"local target {target['external_order_id']} metadata evidence_scope mismatch")
    if metadata.get("synthetic") is not True or metadata.get("source") != "order_list_query":
        raise RepairBlocked(f"local target {target['external_order_id']} metadata synthetic provenance mismatch")
    raw = metadata.get("raw")
    if not isinstance(raw, Mapping):
        raise RepairBlocked(f"local target {target['external_order_id']} raw broker provenance is missing")
    raw_expected = {
        "order_id": target["external_order_id"],
        "code": target["raw_code"],
        "trd_side": target["side"],
        "order_status": "FILLED_ALL",
    }
    for field, expected in raw_expected.items():
        if str(raw.get(field)) != str(expected):
            raise RepairBlocked(
                f"local target {target['external_order_id']} raw provenance {field} mismatch: "
                f"expected {expected!r}, observed {raw.get(field)!r}"
            )
    for field in ("dealt_qty", "dealt_avg_price"):
        expected = target["quantity"] if field == "dealt_qty" else target["price"]
        if not _same_decimal(raw.get(field), expected, f"local raw {field}"):
            raise RepairBlocked(
                f"local target {target['external_order_id']} raw provenance {field} mismatch"
            )
    result = dict(row)
    result["metadata"] = metadata
    return result


def _activity_gate(connection: sqlite3.Connection, account_id: str) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    blockers: list[str] = []
    placeholders_intents = ",".join("?" for _ in ACTIVE_INTENT_STATUSES)
    placeholders_legs = ",".join("?" for _ in ACTIVE_LEG_STATUSES)
    placeholders_orders = ",".join("?" for _ in ACTIVE_ORDER_STATUSES)
    placeholders_terminal_intents = ",".join("?" for _ in TERMINAL_INTENT_STATUSES)
    rows = connection.execute(
        f"""
        SELECT i.id AS intent_id, i.status AS intent_status,
               l.id AS leg_id, l.status AS leg_status,
               o.id AS broker_order_id, o.status AS order_status
          FROM core_order_intents i
          LEFT JOIN core_order_legs l ON l.intent_id = i.id
          LEFT JOIN core_broker_orders o ON o.order_leg_id = l.id
         WHERE i.account_id = ?
           AND (i.status IN ({placeholders_intents})
             OR o.status IN ({placeholders_orders})
             OR (l.status IN ({placeholders_legs})
                 AND (i.status NOT IN ({placeholders_terminal_intents})
                      OR l.status <> 'PLANNED')))
        ORDER BY i.id, l.id, o.id
        """,
        (
            account_id,
            *ACTIVE_INTENT_STATUSES,
            *ACTIVE_ORDER_STATUSES,
            *ACTIVE_LEG_STATUSES,
            *TERMINAL_INTENT_STATUSES,
        ),
    ).fetchall()
    checks["active_lifecycle_rows"] = [dict(row) for row in rows]
    if rows:
        blockers.append("active intent/leg/broker-order lifecycle rows exist")
    open_orders = connection.execute(
        """
        SELECT id, external_order_id, status
          FROM core_broker_orders
         WHERE account_id = ? AND status IN ('PREPARED','SUBMITTING','WORKING','PARTIALLY_FILLED','UNKNOWN')
         ORDER BY id
        """,
        (account_id,),
    ).fetchall()
    checks["open_broker_orders"] = [dict(row) for row in open_orders]
    if open_orders:
        blockers.append("open broker-order rows exist")
    issues = connection.execute(
        "SELECT id, issue_key, status FROM core_reconciliation_issues WHERE account_id = ? AND status = 'OPEN' ORDER BY id",
        (account_id,),
    ).fetchall()
    checks["open_reconciliation_issues"] = [dict(row) for row in issues]
    if issues:
        blockers.append("open reconciliation issues exist")
    actions = connection.execute(
        "SELECT id, action_key, status FROM core_recovery_actions WHERE account_id = ? AND status = 'OPEN' ORDER BY id",
        (account_id,),
    ).fetchall()
    checks["open_recovery_actions"] = [dict(row) for row in actions]
    if actions:
        blockers.append("open recovery actions exist")
    lock_tables: dict[str, list[dict[str, Any]]] = {}
    existing_tables = set(_table_names(connection))
    for table in ACTIVE_LOCK_TABLES:
        if table not in existing_tables:
            lock_tables[table] = []
            continue
        rows = connection.execute(f'SELECT * FROM "{table}"').fetchall()
        values = [dict(row) for row in rows]
        lock_tables[table] = values
        if values:
            blockers.append(f"active lock rows exist in {table}")
    checks["active_lock_rows"] = lock_tables
    return {"blocked": bool(blockers), "blockers": blockers, "checks": checks}


def _account_from_evidence(payload: Mapping[str, Any]) -> str | None:
    account = payload.get("account")
    if isinstance(account, Mapping):
        value = account.get("account_id") or account.get("id")
        if value:
            return str(value)
    value = payload.get("account_id")
    return str(value) if value else None


def _broker_facts_from_evidence(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    facts = payload.get("broker_facts")
    if not isinstance(facts, Mapping):
        raise RepairBlocked("evidence document is missing the normal broker_facts object")
    return facts


def _validate_historical_evidence(
    payload: Mapping[str, Any],
    *,
    evidence_path: Path,
    max_age_seconds: int,
    now: datetime,
) -> dict[str, Any]:
    """Validate the exact bounded historical-preflight export contract.

    Historical order snapshots are deliberately not treated as a current
    account preflight: they prove bounded terminal order/fill evidence, but
    do not carry current positions or RTH facts.  The fixed repair scope still
    requires every target order and fill to be present exactly once with the
    adapter-authenticated historical provenance.
    """

    if payload.get("broker_contacted") is not True:
        raise RepairBlocked("historical evidence does not prove broker_contacted=true")
    if payload.get("historical_preflight_passed") is not True:
        raise RepairBlocked("historical evidence does not prove historical_preflight_passed=true")
    account = _account_from_evidence(payload)
    if account != EXPECTED_ACCOUNT_ID:
        raise RepairBlocked(f"historical evidence account identity mismatch: {account!r}")
    facts = payload.get("historical_facts")
    if not isinstance(facts, Mapping):
        raise RepairBlocked("historical evidence is missing the historical_facts object")
    if facts.get("account_id") != EXPECTED_ACCOUNT_ID:
        raise RepairBlocked(f"historical_facts account identity mismatch: {facts.get('account_id')!r}")
    if facts.get("complete") is not True or facts.get("error") not in (None, ""):
        raise RepairBlocked("historical_facts complete/error contract is not successful")
    captured_at = _parse_time(facts.get("captured_at"), "historical_facts.captured_at")
    age = (now.astimezone(timezone.utc) - captured_at).total_seconds()
    if age < -30:
        raise RepairBlocked("historical evidence capture timestamp is in the future")
    if age > max_age_seconds:
        raise RepairBlocked(
            f"historical evidence is stale ({age:.1f}s old; maximum {max_age_seconds}s)"
        )
    if facts.get("execution_evidence_mode") != EXPECTED_MODE:
        raise RepairBlocked(
            "historical_facts execution_evidence_mode is not CUMULATIVE_ORDER_SNAPSHOTS"
        )
    scope = facts.get("execution_evidence_scope")
    if not isinstance(scope, list) or set(scope) != {"HISTORICAL_ORDER_SNAPSHOTS"}:
        raise RepairBlocked(
            "historical_facts execution_evidence_scope must be exactly HISTORICAL_ORDER_SNAPSHOTS"
        )
    metadata = facts.get("metadata")
    if not isinstance(metadata, Mapping) or metadata.get("source") != "history_order_list_query":
        raise RepairBlocked("historical_facts metadata source must be history_order_list_query")
    provenance = facts.get("provenance")
    if not isinstance(provenance, Mapping) or provenance.get("query") != "history_order_list_query":
        raise RepairBlocked("historical evidence provenance must identify history_order_list_query")
    start = _parse_time(facts.get("requested_start"), "historical_facts.requested_start")
    end = _parse_time(facts.get("requested_end"), "historical_facts.requested_end")
    if end <= start:
        raise RepairBlocked("historical evidence window is invalid")

    orders = facts.get("orders")
    fills = facts.get("fills")
    if not isinstance(orders, list) or not isinstance(fills, list):
        raise RepairBlocked("historical_facts must contain orders and fills lists")
    expected_by_order = {str(item["external_order_id"]): item for item in TARGETS}

    def index_rows(rows: list[Any], label: str) -> dict[str, Mapping[str, Any]]:
        indexed: dict[str, Mapping[str, Any]] = {}
        for raw in rows:
            if not isinstance(raw, Mapping):
                raise RepairBlocked(f"historical evidence contains a malformed {label} row")
            external_id = str(raw.get("external_order_id", "")).strip()
            if not external_id:
                raise RepairBlocked(f"historical {label} row has no external_order_id")
            if external_id in indexed:
                raise RepairBlocked(
                    f"historical evidence contains duplicate {label} identity {external_id}"
                )
            indexed[external_id] = raw
        return indexed

    order_rows = index_rows(orders, "order")
    fill_rows = index_rows(fills, "fill")
    missing_orders = sorted(set(expected_by_order) - set(order_rows))
    missing_fills = sorted(set(expected_by_order) - set(fill_rows))
    if missing_orders or missing_fills:
        raise RepairBlocked(
            "historical evidence is missing fixed-scope order/fill identities: "
            f"orders={missing_orders}, fills={missing_fills}"
        )

    normalized_fills: dict[str, dict[str, Any]] = {}
    normalized_orders: dict[str, dict[str, Any]] = {}
    for external_id, target in expected_by_order.items():
        raw_order = order_rows[external_id]
        for field in (
            "external_order_id",
            "account_id",
            "instrument_id",
            "side",
            "quantity",
            "filled_quantity",
            "status",
            "order_time",
            "authority",
            "metadata",
        ):
            if field not in raw_order:
                raise RepairBlocked(f"historical order {external_id} is missing {field}")
        if raw_order.get("account_id") != EXPECTED_ACCOUNT_ID:
            raise RepairBlocked(f"historical order {external_id} account mismatch")
        if raw_order.get("instrument_id") != target["instrument_id"]:
            raise RepairBlocked(f"historical order {external_id} instrument mismatch")
        if raw_order.get("side") != target["side"]:
            raise RepairBlocked(f"historical order {external_id} side mismatch")
        if raw_order.get("status") != "FILLED":
            raise RepairBlocked(f"historical order {external_id} is not FILLED")
        if not _same_decimal(raw_order.get("quantity"), target["quantity"], f"historical order {external_id} quantity"):
            raise RepairBlocked(f"historical order {external_id} quantity mismatch")
        if not _same_decimal(raw_order.get("filled_quantity"), target["quantity"], f"historical order {external_id} filled quantity"):
            raise RepairBlocked(f"historical order {external_id} filled quantity mismatch")
        if _parse_time(raw_order.get("order_time"), f"historical order {external_id} order_time") != _parse_time(target["filled_at"], "expected filled_at"):
            raise RepairBlocked(f"historical order {external_id} order_time mismatch")
        if raw_order.get("authority") != "ADAPTER_ORDER_SNAPSHOT":
            raise RepairBlocked(f"historical order {external_id} authority mismatch")
        order_metadata = raw_order.get("metadata")
        if not isinstance(order_metadata, Mapping) or not isinstance(order_metadata.get("raw"), Mapping):
            raise RepairBlocked(f"historical order {external_id} raw provenance is missing")
        order_raw = order_metadata["raw"]
        expected_raw = {
            "order_id": external_id,
            "code": target["raw_code"],
            "trd_side": target["side"],
            "order_status": "FILLED_ALL",
        }
        for field, expected in expected_raw.items():
            if str(order_raw.get(field)) != str(expected):
                raise RepairBlocked(f"historical order {external_id} raw {field} mismatch")
        normalized_orders[external_id] = dict(raw_order)

        raw_fill = fill_rows[external_id]
        for field in (
            "external_order_id",
            "account_id",
            "instrument_id",
            "quantity",
            "price",
            "filled_at",
            "evidence_mode",
            "evidence_reference",
            "metadata",
        ):
            if field not in raw_fill:
                raise RepairBlocked(f"historical fill {external_id} is missing {field}")
        if raw_fill.get("account_id") != EXPECTED_ACCOUNT_ID:
            raise RepairBlocked(f"historical fill {external_id} account mismatch")
        if raw_fill.get("instrument_id") != target["instrument_id"]:
            raise RepairBlocked(f"historical fill {external_id} instrument mismatch")
        if raw_fill.get("evidence_mode") != EXPECTED_MODE:
            raise RepairBlocked(f"historical fill {external_id} evidence_mode mismatch")
        if raw_fill.get("evidence_reference") != target["evidence_reference"]:
            raise RepairBlocked(f"historical fill {external_id} evidence_reference mismatch")
        if not _same_decimal(raw_fill.get("quantity"), target["quantity"], f"historical fill {external_id} quantity"):
            raise RepairBlocked(f"historical fill {external_id} quantity mismatch")
        if not _same_decimal(raw_fill.get("price"), target["price"], f"historical fill {external_id} price"):
            raise RepairBlocked(f"historical fill {external_id} price mismatch")
        if _parse_time(raw_fill.get("filled_at"), f"historical fill {external_id} filled_at") != _parse_time(target["filled_at"], "expected filled_at"):
            raise RepairBlocked(f"historical fill {external_id} filled_at mismatch")
        fill_metadata = raw_fill.get("metadata")
        if not isinstance(fill_metadata, Mapping):
            raise RepairBlocked(f"historical fill {external_id} metadata/provenance is missing")
        if fill_metadata.get("evidence_reference") != target["evidence_reference"]:
            raise RepairBlocked(f"historical fill {external_id} metadata evidence_reference mismatch")
        if fill_metadata.get("evidence_scope") != "HISTORICAL_ORDER_SNAPSHOTS":
            raise RepairBlocked(f"historical fill {external_id} metadata evidence_scope mismatch")
        if fill_metadata.get("synthetic") is not True or fill_metadata.get("source") != "history_order_list_query":
            raise RepairBlocked(f"historical fill {external_id} metadata provenance mismatch")
        fill_raw = fill_metadata.get("raw")
        if not isinstance(fill_raw, Mapping):
            raise RepairBlocked(f"historical fill {external_id} raw provenance is missing")
        for field, expected in expected_raw.items():
            if str(fill_raw.get(field)) != str(expected):
                raise RepairBlocked(f"historical fill {external_id} raw {field} mismatch")
        for field, expected in (("dealt_qty", target["quantity"]), ("dealt_avg_price", target["price"])):
            if not _same_decimal(fill_raw.get(field), expected, f"historical raw {field}"):
                raise RepairBlocked(f"historical fill {external_id} raw {field} mismatch")
        normalized_fills[external_id] = dict(raw_fill)

    return {
        "path": str(evidence_path),
        "mode": "HISTORICAL_PREFLIGHT",
        "captured_at": captured_at.isoformat(),
        "age_seconds": age,
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "orders": normalized_orders,
        "fills": normalized_fills,
        "account_id": EXPECTED_ACCOUNT_ID,
        "execution_evidence_mode": EXPECTED_MODE,
        "execution_evidence_scope": ["HISTORICAL_ORDER_SNAPSHOTS"],
    }


def _validate_evidence(
    payload: Mapping[str, Any],
    *,
    evidence_path: Path,
    max_age_seconds: int,
    now: datetime,
) -> dict[str, Any]:
    if payload.get("mode") == "HISTORICAL_PREFLIGHT":
        return _validate_historical_evidence(
            payload,
            evidence_path=evidence_path,
            max_age_seconds=max_age_seconds,
            now=now,
        )
    if payload.get("mode") != "BROKER_PREFLIGHT":
        raise RepairBlocked("evidence document mode must be BROKER_PREFLIGHT")
    if payload.get("broker_contacted") is not True:
        raise RepairBlocked("evidence does not prove broker_contacted=true")
    expected_nonflat_entry_blocker_accepted = False
    if payload.get("preflight_passed") is not True:
        stop_reasons = payload.get("stop_reasons")
        if not isinstance(stop_reasons, list) or {
            str(reason) for reason in stop_reasons
        } != {EXPECTED_NONFLAT_ENTRY_BLOCKER}:
            raise RepairBlocked("evidence preflight_passed is not true")
        expected_nonflat_entry_blocker_accepted = True
    account = _account_from_evidence(payload)
    if account != EXPECTED_ACCOUNT_ID:
        raise RepairBlocked(f"evidence account identity mismatch: {account!r}")
    facts = _broker_facts_from_evidence(payload)
    if facts.get("account_id") != EXPECTED_ACCOUNT_ID:
        raise RepairBlocked(f"broker_facts account identity mismatch: {facts.get('account_id')!r}")
    if facts.get("complete") is not True:
        raise RepairBlocked("broker_facts complete is not true")
    captured_at = _parse_time(facts.get("captured_at"), "broker_facts.captured_at")
    age = (now.astimezone(timezone.utc) - captured_at).total_seconds()
    if age < -30:
        raise RepairBlocked("broker-facts capture timestamp is in the future")
    if age > max_age_seconds:
        raise RepairBlocked(
            f"broker-facts evidence is stale ({age:.1f}s old; maximum {max_age_seconds}s)"
        )
    if facts.get("execution_evidence_mode") != EXPECTED_MODE:
        raise RepairBlocked(
            "broker-facts execution_evidence_mode is not CUMULATIVE_ORDER_SNAPSHOTS"
        )
    scope = facts.get("execution_evidence_scope")
    if not isinstance(scope, list) or "CURRENT_ORDER_SNAPSHOTS" not in scope:
        raise RepairBlocked("broker-facts execution_evidence_scope lacks CURRENT_ORDER_SNAPSHOTS")
    open_orders = facts.get("open_orders")
    positions = facts.get("positions")
    if not isinstance(open_orders, list) or not isinstance(positions, list):
        raise RepairBlocked("broker-facts must contain complete positions and open_orders lists")
    if open_orders:
        raise RepairBlocked("fresh broker evidence contains open orders")
    fills = facts.get("fills")
    if not isinstance(fills, list):
        raise RepairBlocked("broker-facts must contain a fills list")
    expected_by_order = {str(item["external_order_id"]): item for item in TARGETS}
    observed_ids = [str(item.get("external_order_id", "")) for item in fills if isinstance(item, Mapping)]
    if len(observed_ids) != len(set(observed_ids)):
        raise RepairBlocked("broker evidence contains duplicate external order fill identities")
    if set(observed_ids) != set(expected_by_order):
        raise RepairBlocked(
            "broker evidence fill identities do not exactly match the fixed repair scope: "
            f"expected {sorted(expected_by_order)}, observed {sorted(observed_ids)}"
        )
    normalized_fills: dict[str, dict[str, Any]] = {}
    for raw_fill in fills:
        if not isinstance(raw_fill, Mapping):
            raise RepairBlocked("broker evidence contains a malformed fill row")
        external_id = str(raw_fill.get("external_order_id", ""))
        target = expected_by_order[external_id]
        for field in ("external_order_id", "account_id", "instrument_id", "quantity", "price", "filled_at", "evidence_mode", "evidence_reference"):
            if field not in raw_fill:
                raise RepairBlocked(f"broker evidence fill {external_id} is missing {field}")
        if raw_fill.get("account_id") != EXPECTED_ACCOUNT_ID:
            raise RepairBlocked(f"broker evidence fill {external_id} account mismatch")
        if raw_fill.get("instrument_id") != target["instrument_id"]:
            raise RepairBlocked(f"broker evidence fill {external_id} instrument mismatch")
        if raw_fill.get("evidence_mode") != EXPECTED_MODE:
            raise RepairBlocked(f"broker evidence fill {external_id} evidence_mode mismatch")
        if raw_fill.get("evidence_reference") != target["evidence_reference"]:
            raise RepairBlocked(f"broker evidence fill {external_id} evidence_reference mismatch")
        if not _same_decimal(raw_fill.get("quantity"), target["quantity"], f"broker fill {external_id} quantity"):
            raise RepairBlocked(f"broker evidence fill {external_id} quantity mismatch")
        if not _same_decimal(raw_fill.get("price"), target["price"], f"broker fill {external_id} price"):
            raise RepairBlocked(f"broker evidence fill {external_id} price mismatch")
        if _parse_time(raw_fill.get("filled_at"), f"broker fill {external_id} filled_at") != _parse_time(target["filled_at"], "expected filled_at"):
            raise RepairBlocked(f"broker evidence fill {external_id} filled_at mismatch")
        metadata = raw_fill.get("metadata")
        if not isinstance(metadata, Mapping):
            raise RepairBlocked(f"broker evidence fill {external_id} metadata/provenance is missing")
        if metadata.get("evidence_reference") != target["evidence_reference"]:
            raise RepairBlocked(f"broker evidence fill {external_id} metadata evidence_reference mismatch")
        if metadata.get("evidence_scope") != "CURRENT_ORDER_SNAPSHOTS":
            raise RepairBlocked(f"broker evidence fill {external_id} metadata evidence_scope mismatch")
        if metadata.get("synthetic") is not True or metadata.get("source") != "order_list_query":
            raise RepairBlocked(f"broker evidence fill {external_id} metadata provenance mismatch")
        raw_order = metadata.get("raw")
        if not isinstance(raw_order, Mapping):
            raise RepairBlocked(f"broker evidence fill {external_id} raw provenance is missing")
        expected_raw = {
            "order_id": external_id,
            "code": target["raw_code"],
            "trd_side": target["side"],
            "order_status": "FILLED_ALL",
        }
        for field, expected in expected_raw.items():
            if str(raw_order.get(field)) != str(expected):
                raise RepairBlocked(f"broker evidence fill {external_id} raw {field} mismatch")
        for field, expected in (("dealt_qty", target["quantity"]), ("dealt_avg_price", target["price"])):
            if not _same_decimal(raw_order.get(field), expected, f"broker raw {field}"):
                raise RepairBlocked(f"broker evidence fill {external_id} raw {field} mismatch")
        normalized_fills[external_id] = dict(raw_fill)
    market_state = payload.get("market_state")
    if not isinstance(market_state, Mapping) or market_state.get("complete") is not True:
        raise RepairBlocked("evidence does not contain complete market-state facts")
    rth = market_state.get("rth")
    if not isinstance(rth, Mapping) or rth.get("observed") is not True:
        raise RepairBlocked("evidence does not prove current US RTH")
    return {
        "path": str(evidence_path),
        "captured_at": captured_at.isoformat(),
        "age_seconds": age,
        "fills": normalized_fills,
        "account_id": EXPECTED_ACCOUNT_ID,
        "execution_evidence_mode": EXPECTED_MODE,
        "expected_nonflat_entry_blocker_accepted": expected_nonflat_entry_blocker_accepted,
    }


def _identity(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "filename": path.name,
        "size": stat.st_size,
        "sha256": _file_sha256(path),
    }


def _validate_db_path(path: Path) -> None:
    if path.name != EXPECTED_DB_FILENAME:
        raise RepairBlocked(
            f"refusing non-canonical database filename {path.name!r}; expected {EXPECTED_DB_FILENAME!r}"
        )
    if not path.exists() or not path.is_file():
        raise RepairBlocked(f"database does not exist: {path}")


def _backup_database(db_path: Path, backup_path: Path) -> None:
    if backup_path.exists():
        raise RepairBlocked(f"refusing to overwrite existing backup: {backup_path}")
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    # Use a separate read connection.  Calling ``backup`` on the connection
    # that owns the BEGIN IMMEDIATE transaction can wait forever on its own
    # reserved writer lock on rollback-journal databases.
    source = _open_readonly(db_path)
    destination = sqlite3.connect(str(backup_path), timeout=0.0)
    try:
        source.backup(destination)
        destination.commit()
    finally:
        source.close()
        destination.close()


def _semantic_change_only(
    before: Mapping[str, Any], after: Mapping[str, Any], targets: tuple[Mapping[str, Any], ...]
) -> bool:
    before_copy = json.loads(_canonical_json(before))
    after_copy = json.loads(_canonical_json(after))
    expected_by_id = {str(item["fill_id"]): item for item in targets}
    before_fills = before_copy.get("core_fills", {}).get("rows", [])
    after_fills = after_copy.get("core_fills", {}).get("rows", [])
    columns = before_copy.get("core_fills", {}).get("columns", [])
    if before_copy.keys() != after_copy.keys() or columns != after_copy.get("core_fills", {}).get("columns", []):
        return False
    for table in before_copy:
        if table != "core_fills" and before_copy[table] != after_copy[table]:
            return False
    try:
        id_index = columns.index("id")
        mode_index = columns.index("evidence_mode")
    except ValueError:
        return False
    before_by_id = {str(row[id_index]): row for row in before_fills}
    after_by_id = {str(row[id_index]): row for row in after_fills}
    if set(before_by_id) != set(after_by_id):
        return False
    changed_ids: set[str] = set()
    for fill_id, before_row in before_by_id.items():
        after_row = after_by_id[fill_id]
        if before_row == after_row:
            continue
        if fill_id not in expected_by_id:
            return False
        changed = [
            index
            for index, (left, right) in enumerate(zip(before_row, after_row, strict=True))
            if left != right
        ]
        if (
            changed != [mode_index]
            or before_row[mode_index] != OLD_MODE
            or after_row[mode_index] != EXPECTED_MODE
        ):
            return False
        changed_ids.add(fill_id)
    return changed_ids == set(expected_by_id)


def _report_base(db_path: Path, evidence_path: Path, max_age_seconds: int) -> dict[str, Any]:
    try:
        db_identity = _identity(db_path)
    except OSError:
        db_identity = {
            "path": str(db_path.resolve()),
            "filename": db_path.name,
            "size": None,
            "sha256": None,
        }
    return {
        "tool": "repair_stage6_oct7_fill_evidence_mode",
        "mode": "DRY_RUN",
        "status": "BLOCKED",
        "execution_compatibility": "stage6-execution-v4",
        "db": db_identity,
        "evidence_path": str(evidence_path.resolve()),
        "evidence_max_age_seconds": max_age_seconds,
        "fixed_scope": {
            "account_id": EXPECTED_ACCOUNT_ID,
            "source_intent_id": EXPECTED_SOURCE_INTENT_ID,
            "book_id": EXPECTED_BOOK_ID,
            "targets": [dict(item) for item in TARGETS],
        },
        "writes": {"database": False, "backup": False, "updates": 0},
        "broker_mutations": {"submit": 0, "cancel": 0, "replace": 0, "recover": 0},
        "errors": [],
    }


def _dry_run(db_path: Path, evidence_path: Path, max_age_seconds: int) -> tuple[int, dict[str, Any]]:
    report = _report_base(db_path, evidence_path, max_age_seconds)
    try:
        _validate_db_path(db_path)
        payload = json.loads(evidence_path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise RepairBlocked("evidence document must be a JSON object")
        evidence_sha = _file_sha256(evidence_path)
        report["evidence_sha256"] = evidence_sha
        report["evidence"] = _validate_evidence(
            payload,
            evidence_path=evidence_path,
            max_age_seconds=max_age_seconds,
            now=datetime.now(timezone.utc),
        )
        with _open_readonly(db_path) as connection:
            before = _database_snapshot(connection)
            report["before_snapshot_digest"] = _snapshot_digest(before)
            local_rows = []
            for target in TARGETS:
                local_rows.append(_validate_local_target(connection, target))
            report["local_targets"] = local_rows
            activity = _activity_gate(connection, EXPECTED_ACCOUNT_ID)
            report["activity_gate"] = activity
            if activity["blocked"]:
                raise RepairBlocked("; ".join(activity["blockers"]))
        report["would_update"] = [
            {"fill_id": item["fill_id"], "from": OLD_MODE, "to": EXPECTED_MODE}
            for item in TARGETS
        ]
        report["status"] = "READY"
        return 0, report
    except (OSError, sqlite3.Error, RepairBlocked, json.JSONDecodeError) as exc:
        report["errors"].append(str(exc))
        return 2, report


def _apply(db_path: Path, evidence_path: Path, max_age_seconds: int, confirm: str | None, report_path: Path | None) -> tuple[int, dict[str, Any]]:
    if confirm != CONFIRMATION:
        return 2, {
            "tool": "repair_stage6_oct7_fill_evidence_mode",
            "mode": "APPLY",
            "status": "BLOCKED",
            "error": f"exact confirmation required: {CONFIRMATION}",
            "writes": {"database": False, "backup": False, "updates": 0},
        }
    if report_path is None:
        return 2, {
            "tool": "repair_stage6_oct7_fill_evidence_mode",
            "mode": "APPLY",
            "status": "BLOCKED",
            "error": "--report is required with --apply",
            "writes": {"database": False, "backup": False, "updates": 0},
        }
    code, dry_report = _dry_run(db_path, evidence_path, max_age_seconds)
    if code != 0:
        dry_report["mode"] = "APPLY"
        return code, dry_report
    report = dict(dry_report)
    report["mode"] = "APPLY"
    backup_path = db_path.with_name(
        f"{db_path.stem}-evidence-mode-repair-backup-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.db"
    )
    report["backup_path"] = str(backup_path.resolve())
    report["writes"] = {"database": False, "backup": False, "updates": 0}
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(str(db_path), timeout=0.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 0")
        try:
            connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise RepairBlocked(f"database writer lock unavailable; active writer may exist: {exc}") from exc
        # The lock is acquired before backup so a concurrent writer cannot
        # change the audited rows between preflight and the consistent backup.
        _backup_database(db_path, backup_path)
        before = _database_snapshot(connection)
        for target in TARGETS:
            _validate_local_target(connection, target)
        activity = _activity_gate(connection, EXPECTED_ACCOUNT_ID)
        if activity["blocked"]:
            raise RepairBlocked("; ".join(activity["blockers"]))
        rowcounts = []
        for target in TARGETS:
            cursor = connection.execute(
                "UPDATE core_fills SET evidence_mode = ? WHERE id = ? AND evidence_mode = ?",
                (EXPECTED_MODE, target["fill_id"], OLD_MODE),
            )
            rowcounts.append(cursor.rowcount)
            if cursor.rowcount != 1:
                raise RepairBlocked(f"conditional update affected {cursor.rowcount} rows for {target['fill_id']}")
        after = _database_snapshot(connection)
        if not _semantic_change_only(before, after, TARGETS):
            raise RepairBlocked("transaction snapshot shows a change outside the two target evidence_mode cells")
        connection.commit()
        report["before_snapshot_digest"] = _snapshot_digest(before)
        report["after_snapshot_digest"] = _snapshot_digest(after)
        report["rowcounts"] = rowcounts
        report["writes"] = {"database": True, "backup": True, "updates": 2}
        report["status"] = "APPLIED"
        report["verification"] = {
            "only_target_evidence_mode_changed": True,
            "target_ids": [item["fill_id"] for item in TARGETS],
            "old_mode": OLD_MODE,
            "new_mode": EXPECTED_MODE,
        }
        return 0, report
    except (OSError, sqlite3.Error, RepairBlocked) as exc:
        if connection is not None:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
        report["errors"] = [str(exc)]
        report["status"] = "BLOCKED"
        return 2, report
    finally:
        if connection is not None:
            connection.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help=f"fixed database filename {EXPECTED_DB_FILENAME}")
    parser.add_argument("--evidence", type=Path, required=True, help="independent broker-preflight JSON document")
    parser.add_argument("--apply", action="store_true", help="apply exactly the two audited evidence_mode updates")
    parser.add_argument("--confirm", help="must exactly equal the fixed audited confirmation phrase")
    parser.add_argument("--report", type=Path, help="external JSON audit report path (required with --apply)")
    parser.add_argument(
        "--max-evidence-age-seconds",
        type=int,
        default=DEFAULT_MAX_EVIDENCE_AGE_SECONDS,
        help=f"freshness bound for broker evidence (default {DEFAULT_MAX_EVIDENCE_AGE_SECONDS}; max 86400)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.max_evidence_age_seconds <= 0 or args.max_evidence_age_seconds > 86400:
        payload = {"status": "BLOCKED", "error": "--max-evidence-age-seconds must be between 1 and 86400"}
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 2
    if args.apply:
        code, payload = _apply(args.db, args.evidence, args.max_evidence_age_seconds, args.confirm, args.report)
    else:
        code, payload = _dry_run(args.db, args.evidence, args.max_evidence_age_seconds)
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
