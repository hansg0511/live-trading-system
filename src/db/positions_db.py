"""
src/db/positions_db.py

Shared SQLite ledger for stat-arb execution state.
Orders and fills are shared across strategies; positions_stat_arb stores
confirmed stat-arb live state only; trades stores shared closed trade summaries.
"""

from contextlib import contextmanager
from pathlib import Path
import sqlite3


DB_PATH = "data/trading.db"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")


@contextmanager
def get_conn(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: str = DB_PATH, schema_path: str | Path = SCHEMA_PATH):
    with open(schema_path) as f:
        schema = f.read()
    with get_conn(db_path) as conn:
        conn.executescript(schema)


def get_open_position(pair: str, db_path: str = DB_PATH):
    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM positions_stat_arb WHERE pair = ? AND status = 'open'",
            (pair,),
        ).fetchone()
        return dict(row) if row else None


def get_all_open_positions(db_path: str = DB_PATH):
    with get_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM positions_stat_arb WHERE status = 'open'"
        ).fetchall()
        return [dict(r) for r in rows]


def _reverse_side(side: str) -> str:
    return "BUY" if side == "SELL" else "SELL"


def _infer_entry_sides(entry_zscore: float, entry_hedge_ratio: float) -> tuple[str, str]:
    long_spread = entry_zscore < 0
    independent_side = "SELL" if entry_hedge_ratio >= 0 else "BUY"
    if not long_spread:
        independent_side = _reverse_side(independent_side)
    dependent_side = "BUY" if long_spread else "SELL"
    return independent_side, dependent_side


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
    db_path: str = DB_PATH,
) -> int:
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO orders
                (strategy_id, strategy_identifier, position_id, symbol, leg, leg_type,
                 side, intended_price, intended_quantity_raw, submitted_quantity,
                 submitted_time, status, broker, broker_order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                strategy_id,
                strategy_identifier,
                position_id,
                symbol,
                leg,
                leg_type,
                side,
                intended_price,
                intended_quantity_raw,
                submitted_quantity,
                submitted_time,
                status,
                broker,
                broker_order_id,
            ),
        )
        return cur.lastrowid


def record_order_fill(
    order_id: int,
    fill_price: float,
    fill_time: str,
    fill_quantity: float,
    intended_price: float | None = None,
    db_path: str = DB_PATH,
) -> None:
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT intended_price, side FROM orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            raise ValueError(f"No order with id={order_id}")
        price_basis = intended_price if intended_price is not None else float(row["intended_price"])
        side = row["side"]
        slippage = fill_price - price_basis if side == "BUY" else price_basis - fill_price
        conn.execute(
            """
            UPDATE orders
            SET fill_price = ?, fill_time = ?, fill_quantity = ?, slippage = ?, status = 'filled'
            WHERE id = ?
            """,
            (fill_price, fill_time, fill_quantity, slippage, order_id),
        )


def update_order_status(order_id: int, status: str, db_path: str = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            "UPDATE orders SET status = ? WHERE id = ?",
            (status, order_id),
        )


def get_pending_orders(position_id: int | None = None, db_path: str = DB_PATH) -> list[dict]:
    with get_conn(db_path) as conn:
        if position_id is not None:
            rows = conn.execute(
                "SELECT * FROM orders WHERE position_id = ? AND status IN ('pending', 'submitted')",
                (position_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM orders WHERE status IN ('pending', 'submitted')"
            ).fetchall()
        return [dict(r) for r in rows]


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
    db_path: str = DB_PATH,
    **legacy_kwargs,
) -> int:
    """
    Inserts a confirmed open position. If fill details are provided, they are
    written to fills as entry executions.
    """
    identifier = strategy_identifier or pair
    if entry_side1 is None or entry_side2 is None:
        entry_side1, entry_side2 = _infer_entry_sides(entry_zscore, entry_hedge_ratio)

    if entry_leg1_price is None:
        entry_leg1_price = legacy_kwargs.get("latest_price_s1")
    if entry_leg2_price is None:
        entry_leg2_price = legacy_kwargs.get("latest_price_s2")
    if executed_size1 is None:
        executed_size1 = legacy_kwargs.get("target_size1") or legacy_kwargs.get("submitted_quantity1")
    if executed_size2 is None:
        executed_size2 = legacy_kwargs.get("target_size2") or legacy_kwargs.get("submitted_quantity2")
    if intended_size1 is None:
        intended_size1 = executed_size1
    if intended_size2 is None:
        intended_size2 = executed_size2
    if entry_leg1_price is None or entry_leg2_price is None:
        raise ValueError("entry_leg1_price and entry_leg2_price are required to open a position")
    if executed_size1 is None or executed_size2 is None:
        raise ValueError("executed_size1 and executed_size2 are required to open a position")

    with get_conn(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO positions_stat_arb
                (strategy_id, strategy_identifier, pair, ticker1, ticker2,
                 entry_hedge_ratio, entry_alpha, entry_residual_mean,
                 entry_residual_std, entry_zscore, entry_side1, entry_side2,
                 entry_leg1_price, entry_leg2_price,
                 executed_size1, executed_size2, intended_size1, intended_size2,
                 entry_date, status, broker)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
            """,
            (
                strategy_id,
                identifier,
                pair,
                ticker1,
                ticker2,
                entry_hedge_ratio,
                entry_alpha,
                entry_residual_mean,
                entry_residual_std,
                entry_zscore,
                entry_side1,
                entry_side2,
                entry_leg1_price,
                entry_leg2_price,
                executed_size1,
                executed_size2,
                intended_size1,
                intended_size2,
                entry_date,
                broker,
            ),
        )
        position_id = cur.lastrowid

        if order_id1 is not None and fill_price1 is not None and fill_time1 is not None:
            record_order_fill(order_id1, fill_price1, fill_time1, float(executed_size1), intended_price1, db_path)
        if order_id2 is not None and fill_price2 is not None and fill_time2 is not None:
            record_order_fill(order_id2, fill_price2, fill_time2, float(executed_size2), intended_price2, db_path)

        return position_id


def link_order_to_position(order_id: int, position_id: int, db_path: str = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute(
            "UPDATE orders SET position_id = ? WHERE id = ?",
            (position_id, order_id),
        )


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
    db_path: str = DB_PATH,
    **legacy_kwargs,
) -> int:
    with get_conn(db_path) as conn:
        pos = conn.execute(
            "SELECT * FROM positions_stat_arb WHERE id = ?",
            (position_id,),
        ).fetchone()
        if pos is None:
            raise ValueError(f"No position with id={position_id}")
        if pos["status"] != "open":
            raise ValueError(f"Position {position_id} is already '{pos['status']}'")

        conn.execute(
            "UPDATE positions_stat_arb SET status = 'closed' WHERE id = ?",
            (position_id,),
        )

        entry_side1 = pos["entry_side1"]
        entry_side2 = pos["entry_side2"]
        if exit_side1 is None:
            exit_side1 = _reverse_side(entry_side1)
        if exit_side2 is None:
            exit_side2 = _reverse_side(entry_side2)
        if exit_price1 is None:
            exit_price1 = legacy_kwargs.get("exit_price1") or legacy_kwargs.get("exit_price")
        if exit_price2 is None:
            exit_price2 = legacy_kwargs.get("exit_price2") or legacy_kwargs.get("exit_price")
        if exit_price1 is None or exit_price2 is None:
            raise ValueError("exit_price1 and exit_price2 are required to close a position")
        size1 = float(pos["executed_size1"])
        size2 = float(pos["executed_size2"])
        entry_price1 = float(pos["entry_leg1_price"])
        entry_price2 = float(pos["entry_leg2_price"])

        pnl_leg1 = (exit_price1 - entry_price1) * size1 if entry_side1 == "BUY" else (entry_price1 - exit_price1) * size1
        pnl_leg2 = (exit_price2 - entry_price2) * size2 if entry_side2 == "BUY" else (entry_price2 - exit_price2) * size2
        realized_pnl = pnl_leg1 + pnl_leg2

        cur = conn.execute(
            """
            INSERT INTO trades
                (position_id, strategy_id, strategy_identifier, entry_date, exit_date,
                 entry_price1, entry_price2, exit_price1, exit_price2,
                 entry_zscore, exit_zscore, exit_reason, realized_pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position_id,
                pos["strategy_id"],
                pos["strategy_identifier"],
                pos["entry_date"],
                exit_date,
                entry_price1,
                entry_price2,
                exit_price1,
                exit_price2,
                pos["entry_zscore"],
                exit_zscore,
                exit_reason,
                realized_pnl,
            ),
        )
        trade_id = cur.lastrowid

        if order_id1 is not None and intended_price1 is not None and fill_time1 is not None:
            record_order_fill(order_id1, exit_price1, fill_time1, size1, intended_price1, db_path)
        if order_id2 is not None and intended_price2 is not None and fill_time2 is not None:
            record_order_fill(order_id2, exit_price2, fill_time2, size2, intended_price2, db_path)
        return trade_id
