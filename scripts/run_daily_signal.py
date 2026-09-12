"""
scripts/run_daily_signal.py

Daily stat-arb signal script. Intended to be triggered once via cron
at 15:55 America/New_York. Sleeps until 15:59 to submit orders,
approximating "cheat on close" execution.
"""

import argparse
import time
from datetime import datetime, time as dtime
from pathlib import Path
import sys

import numpy as np
import pytz
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.models import OrderStatus, TimeFrame
from src.data.yfinance.provider import YFinanceDataProvider
from src.strategies.stat_arb.signal import compute_rolling_ols_signal, compute_zscore, estimate_ar1
from src.db.positions_db import (
    init_db,
    get_open_position,
    get_pending_orders,
    record_order,
    record_order_fill,
    open_position,
    close_position,
    link_order_to_position,
    update_order_status,
)

try:
    from src.brokers.moomoo.adapter import MooMooAdapter
    MooMooAdapterAvailable = True
except ImportError:
    MooMooAdapterAvailable = False

# Stores entry signal metadata keyed by pair, used by _open_entry_positions()
# after fills are confirmed during reconciliation.
_pending_entry_meta: dict[str, dict] = {}

# Stores exit signal metadata keyed by pair, used by _close_exit_positions()
# after fills are confirmed during reconciliation.
_pending_exit_meta: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Config — locked parameters
# ---------------------------------------------------------------------------

Z_ENTRY_THRESHOLD = 2.2
Z_EXIT_THRESHOLD = 1.0
Z_STOP_THRESHOLD = 4.5
HEDGE_RATIO_THRESHOLD = 0.8
MAX_HOLDING_DAYS = 15
EXECUTION_TIME = dtime(15, 59)
NY_TZ = "America/New_York"
PAIR_SELECTION_RESULTS_PATH = PROJECT_ROOT / "pair_selection_results.csv"
PAIR_TRADE_NOTIONAL = 10000.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_tickers_from_pairs(pairs_series):
    tickers = set()
    for pair in pairs_series:
        parts = pair.split('-')
        if len(parts) == 2:
            tickers.add(parts[0])
            tickers.add(parts[1])
    return list(tickers)

def hedge_ratio_guard(current_hr: float, csv_hr: float, threshold: float = HEDGE_RATIO_THRESHOLD) -> bool:
    """
    Guard compares the LIVE rolling hedge ratio against the pair-selection
    CSV value (hedge_ratio_csv).

    threshold = maximum allowed proportional drift, symmetric in both directions.
    E.g. threshold=0.8 means current_hr can range from 0.2x to 1.8x csv_hr
    (i.e. drift up to 80% below or above csv_hr) before the guard fails.

    NOTE: if csv_hr is negative, (1 - threshold) and (1 + threshold) multipliers
    still work correctly since the comparison is against the signed csv_hr directly —
    but a sign flip in current_hr will naturally fail this band too. See
    hedge_ratio_sign_flipped() for treating that as a distinct, more serious case.
    """
    if csv_hr == 0:
        return True
    
    drift = abs(current_hr - csv_hr) / abs(csv_hr)
    return drift <= threshold


def compute_frozen_zscore(latest_price_s1: float, latest_price_s2: float,
                           entry_hr: float, entry_alpha: float,
                           entry_resid_mean: float, entry_resid_std: float) -> float:
    """
    Recomputes today's residual/Z-score using the FROZEN entry reference
    (alpha, HR, residual mean, residual std locked at trade entry) rather
    than freshly re-estimated rolling values. This is what governs exit/stop/hold
    decisions for an already-open position — using fresh rolling HR here
    instead would silently change what the position is being measured against
    mid-trade.

    resid = ln(s2) - (alpha + beta * ln(s1)), matching the log-space
    vectorized OLS. Epsilon matches compute_zscore's 1e-10 convention.
    """
    resid_today = np.log(latest_price_s2) - (entry_alpha + entry_hr * np.log(latest_price_s1))
    return (resid_today - entry_resid_mean) / (entry_resid_std + 1e-10)


def days_held(entry_date_str: str, today: datetime) -> int:
    entry_date = datetime.strptime(entry_date_str, "%Y-%m-%d")
    return (today.date() - entry_date.date()).days


def sleep_until(target_time: dtime, tz: str = NY_TZ):
    tz_obj = pytz.timezone(tz)
    while True:
        now = datetime.now(tz_obj)
        if now.time() >= target_time:
            break
        time.sleep(30)
    print(f"Reached {target_time} {tz}, proceeding.")


def compute_pair_order_plan(price1: float, price2: float, hedge_ratio: float, zscore: float):
    """
    Translate a pair signal into two leg sizes.
    price1 / ticker1 = independent leg (x)
    price2 / ticker2 = dependent leg (y)
    size2 is the dependent-leg base; size1 follows the requested hedge-ratio sizing rule.
    """
    if price1 <= 0 or price2 <= 0:
        raise ValueError("Prices must be positive to compute order quantities.")

    long_spread = zscore < 0
    raw_size2 = PAIR_TRADE_NOTIONAL / price2
    raw_size1 = abs(raw_size2 * hedge_ratio * (price2 / price1))
    size2 = max(int(raw_size2), 1)
    size1 = max(int(raw_size1), 1)
    independent_side = "SELL" if hedge_ratio >= 0 else "BUY"
    inverse_independent_side = "BUY" if independent_side == "SELL" else "SELL"

    if long_spread:
        return {
            "ticker1_side": independent_side,
            "ticker2_side": "BUY",
            "ticker1_qty": size1,
            "ticker2_qty": size2,
            "ticker1_intended_qty": raw_size1,
            "ticker2_intended_qty": raw_size2,
        }
    return {
        "ticker1_side": inverse_independent_side,
        "ticker2_side": "SELL",
        "ticker1_qty": size1,
        "ticker2_qty": size2,
        "ticker1_intended_qty": raw_size1,
        "ticker2_intended_qty": raw_size2,
    }


def reverse_order_side(side: str) -> str:
    return "BUY" if side == "SELL" else "SELL"


def get_entry_order_sides(entry_zscore: float, entry_hedge_ratio: float) -> tuple[str, str]:
    """
    Returns (independent_side, dependent_side) for the entry trade.
    """
    long_spread = entry_zscore < 0
    independent_side = "SELL" if entry_hedge_ratio >= 0 else "BUY"
    if not long_spread:
        independent_side = reverse_order_side(independent_side)
    dependent_side = "BUY" if long_spread else "SELL"
    return independent_side, dependent_side


def submit_leg(broker: "MooMooAdapter", symbol: str, side: str, quantity: float, price: float = 0.0):
    from src.core.models import Order, OrderSide, OrderType

    broker_symbol = f"US.{symbol}"
    order = Order(
        symbol=broker_symbol,
        quantity=float(quantity),
        side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
        order_type=OrderType.MARKET,
        price=float(price),
    )
    order_id, create_time = broker.place_order(order)
    print(f"Submitted {side} {quantity:.4f} {symbol} @ {price:.4f} -> {order_id}")
    return order_id, float(price), create_time


def leg_pnl(entry_side: str, entry_price: float, exit_price: float, quantity: float) -> float:
    if entry_side == "BUY":
        return (exit_price - entry_price) * quantity
    if entry_side == "SELL":
        return (entry_price - exit_price) * quantity
    raise ValueError(f"Unknown side: {entry_side}")


def position_unrealized_pnl(open_pos: dict, current_price1: float, current_price2: float) -> float:
    entry_independent_side = open_pos["entry_side1"]
    entry_dependent_side = open_pos["entry_side2"]
    entry_leg1_price = float(open_pos.get("entry_leg1_price") or current_price1)
    entry_leg2_price = float(open_pos.get("entry_leg2_price") or current_price2)
    size1 = float(open_pos["executed_size1"])
    size2 = float(open_pos["executed_size2"])
    return (
        leg_pnl(entry_independent_side, entry_leg1_price, current_price1, size1)
        + leg_pnl(entry_dependent_side, entry_leg2_price, current_price2, size2)
    )


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def reconcile_orders(broker: "MooMooAdapter", timeout: int = 10, interval: int = 5):
    """
    Poll the broker for fill status of all pending/submitted orders.
    Updates DB with actual fill prices, quantities, and slippage.
    """
    deadline = time.time() + timeout
    pending = get_pending_orders()

    if not pending:
        print("No pending orders to reconcile.")
        return

    print(f"Reconciling {len(pending)} order(s) via push + poll (timeout={timeout}s)...")

    while pending and time.time() < deadline:
        for order_dict in list(pending):
            oid = order_dict["id"]
            broker_oid = order_dict.get("broker_order_id")
            if not broker_oid:
                print(f"  Order {oid}: no broker_order_id, marking as orphan.")
                pending.remove(order_dict)
                continue

            status = broker.get_order_status(broker_oid)
            if status == OrderStatus.FILLED:
                fills = broker.get_order_fills(broker_oid)
                if fills:
                    total_qty = sum(float(f["qty"]) for f in fills)
                    avg_price = sum(float(f["price"]) * float(f["qty"]) for f in fills) / total_qty if total_qty > 0 else float(order_dict["intended_price"])
                    last_time = max(f["create_time"] for f in fills)
                    record_order_fill(oid, avg_price, str(last_time), total_qty, float(order_dict["intended_price"]))
                    print(f"  Order {oid} (broker={broker_oid}): FILLED {total_qty} @ {avg_price:.4f}")
                else:
                    update_order_status(oid, "filled")
                    print(f"  Order {oid} (broker={broker_oid}): FILLED (no fill details)")
                pending.remove(order_dict)
            elif status in (OrderStatus.CANCELLED, OrderStatus.REJECTED):
                update_order_status(oid, status.value.lower())
                print(f"  Order {oid} (broker={broker_oid}): {status.value}")
                pending.remove(order_dict)

        if pending:
            time.sleep(interval)

    if pending:
        print(f"WARNING: {len(pending)} order(s) still pending after {timeout}s:")
        for o in pending:
            print(f"  Order {o['id']} (broker={o.get('broker_order_id')})")


# ---------------------------------------------------------------------------
# Post-reconciliation: open positions for filled entry pairs
# ---------------------------------------------------------------------------

def _open_entry_positions(today_str: str):
    """
    After reconciliation, scan for filled entry orders that aren't yet linked
    to a position. For each pair with both legs filled, open the position
    using actual fill data from the orders table and the stored signal metadata
    from _pending_entry_meta.
    """
    from src.db.positions_db import get_conn as _get_conn

    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM orders
            WHERE leg_type = 'entry'
              AND status = 'filled'
              AND position_id IS NULL
            ORDER BY strategy_identifier, leg
            """
        ).fetchall()

    filled = [dict(r) for r in rows]
    if not filled:
        return

    by_pair: dict[str, list[dict]] = {}
    for o in filled:
        by_pair.setdefault(o["strategy_identifier"], []).append(o)

    for pair, orders in by_pair.items():
        if len(orders) != 2:
            print(f"  _open_entry_positions: {pair} has {len(orders)} filled leg(s), need 2. Skipping.")
            continue

        meta = _pending_entry_meta.get(pair)
        if meta is None:
            print(f"  _open_entry_positions: {pair} has no signal metadata. Skipping.")
            continue

        leg1 = next(o for o in orders if o["leg"] == "ticker1")
        leg2 = next(o for o in orders if o["leg"] == "ticker2")

        position_id = open_position(
            pair=pair,
            ticker1=leg1["symbol"].removeprefix("US."),
            ticker2=leg2["symbol"].removeprefix("US."),
            entry_hedge_ratio=meta["entry_hedge_ratio"],
            entry_alpha=meta["entry_alpha"],
            entry_residual_mean=meta["entry_residual_mean"],
            entry_residual_std=meta["entry_residual_std"],
            entry_zscore=meta["entry_zscore"],
            entry_date=today_str,
            broker="moomoo",
            entry_side1=leg1["side"],
            entry_side2=leg2["side"],
            entry_leg1_price=leg1["fill_price"],
            entry_leg2_price=leg2["fill_price"],
            executed_size1=leg1["fill_quantity"],
            executed_size2=leg2["fill_quantity"],
            intended_size1=leg1["intended_quantity_raw"],
            intended_size2=leg2["intended_quantity_raw"],
        )
        link_order_to_position(leg1["id"], position_id)
        link_order_to_position(leg2["id"], position_id)
        print(f"  {pair}: position opened (id={position_id}) from fills "
              f"(leg1={leg1['fill_price']}, leg2={leg2['fill_price']})")

    _pending_entry_meta.clear()


# ---------------------------------------------------------------------------
# Post-reconciliation: close positions for filled exit pairs
# ---------------------------------------------------------------------------

def _close_exit_positions(today_str: str):
    """After reconciliation, close positions whose exit legs are both filled."""
    from src.db.positions_db import get_conn as _get_conn

    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT p.id, p.pair, p.status,
                   p.executed_size1, p.executed_size2,
                   p.entry_leg1_price, p.entry_leg2_price,
                   p.entry_side1, p.entry_side2
            FROM positions_stat_arb p
            JOIN orders o ON o.position_id = p.id
            WHERE p.status = 'open'
              AND o.leg_type = 'exit'
              AND o.status = 'filled'
            """
        ).fetchall()

    open_positions = [dict(r) for r in rows]
    if not open_positions:
        return

    for pos in open_positions:
        position_id = pos["id"]
        pair = pos["pair"]

        with _get_conn() as conn:
            exit_orders = conn.execute(
                """
                SELECT * FROM orders
                WHERE position_id = ? AND leg_type = 'exit' AND status = 'filled'
                """,
                (position_id,),
            ).fetchall()

        exit_orders = [dict(r) for r in exit_orders]
        if len(exit_orders) != 2:
            print(f"  _close_exit_positions: {pair} (pos #{position_id}) has "
                  f"{len(exit_orders)} filled exit leg(s), need 2. Skipping.")
            continue

        leg1 = next(o for o in exit_orders if o["leg"] == "ticker1")
        leg2 = next(o for o in exit_orders if o["leg"] == "ticker2")

        meta = _pending_exit_meta.get(pair, {})
        exit_side1 = meta.get("exit_side1") or ("BUY" if pos["entry_side1"] == "SELL" else "SELL")
        exit_side2 = meta.get("exit_side2") or ("BUY" if pos["entry_side2"] == "SELL" else "SELL")

        close_position(
            position_id=position_id,
            exit_date=today_str,
            exit_reason=meta.get("exit_reason", "unknown"),
            exit_zscore=meta.get("exit_zscore", 0.0),
            exit_side1=exit_side1,
            exit_side2=exit_side2,
            exit_price1=leg1["fill_price"],
            exit_price2=leg2["fill_price"],
            order_id1=leg1["id"],
            order_id2=leg2["id"],
        )
        print(f"  {pair}: position closed (id={position_id}) from fills "
              f"(leg1={leg1['fill_price']}, leg2={leg2['fill_price']})")

    _pending_exit_meta.clear()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Daily stat-arb signal execution")
    parser.add_argument("--signal-only", action="store_true", help="Compute signals and print decisions, do not place orders")
    parser.add_argument("--force-entry", type=str, metavar="PAIR", help="Force an entry on the given pair (e.g. AAPL-MSFT)")
    parser.add_argument("--force-exit", type=str, metavar="PAIR", help="Force close an open position for the given pair")
    parser.add_argument("--no-sleep", action="store_true", help="Skip sleep_until(15:59 ET), execute immediately")
    parser.add_argument("--no-reconcile", action="store_true", help="Skip reconciliation after order placement")
    args = parser.parse_args()

    init_db()  # no-op if trading.db + tables already exist

    provider = YFinanceDataProvider()
    provider.connect()

    df = pd.read_csv(str(PAIR_SELECTION_RESULTS_PATH), dtype={"symbol": str})

    if args.force_entry:
        pair = args.force_entry
        if pair not in df['pair'].values:
            print(f"Error: pair '{pair}' not found in {PAIR_SELECTION_RESULTS_PATH}")
            return
        open_pos = get_open_position(pair)
        if open_pos is not None:
            print(f"Error: pair '{pair}' already has an open position (id={open_pos['id']}). Skipping.")
            return
        top_5_pairs = [pair]
        print(f"Force-entry pair: {pair}")
    else:
        top_5_pairs = df.nsmallest(5, 'cointegration_pvalue_log')['pair'].tolist()
        print(f"Top 5 ranking pairs: {top_5_pairs}")

    tickers_for_top_pairs = extract_tickers_from_pairs(top_5_pairs)
    print(f"Tickers for top pairs: {tickers_for_top_pairs}")

    data = provider.get_historical_candlesticks(
        symbol=tickers_for_top_pairs,
        start=datetime.today() - pd.Timedelta(days=150),
        end=datetime.today(),
        timeframe=TimeFrame.DAY
    )

    today = datetime.today()
    today_str = today.strftime("%Y-%m-%d")

    entries_to_execute = []
    exits_to_execute = []

    for pair in top_5_pairs:
        # pair format is always independent-dependent
        independent_ticker, dependent_ticker = pair.split('-')
        print(f"\n--- Pair: {pair} ---")

        pair_info = df[df['pair'] == pair].iloc[0]
        hedge_ratio_csv = pair_info['hedge_ratio_log']
        half_life_log = pair_info['half_life_log']

        s1 = data['Close'][independent_ticker]
        s2 = data['Close'][dependent_ticker]

        available_days = len(s1)
        lookback = min(90, available_days - 10)

        s1_log, s2_log = np.log(s1), np.log(s2)
        residuals_df = compute_rolling_ols_signal(s1_log, s2_log, lookback=lookback, estimate_ar1_fn=estimate_ar1)
        zscore_lookback = max(int(half_life_log), 10)
        rolling_zscore = compute_zscore(residuals_df['residual'], lookback=zscore_lookback)

        latest_hr = residuals_df['hedge_ratio'].iloc[-1]
        latest_intercept = residuals_df['intercept'].iloc[-1]
        latest_price_s1 = s1.iloc[-1]
        latest_price_s2 = s2.iloc[-1]

        print(f"Hedge ratio (CSV): {hedge_ratio_csv:.4f}")
        print(f"Latest rolling hedge ratio: {latest_hr:.4f}")

        # --- Guard: always checked against CSV HR, regardless of open/flat ---
        guard_ok = hedge_ratio_guard(latest_hr, hedge_ratio_csv)
        if not guard_ok:
            print(f"{pair}: hedge ratio guard FAILED "
                  f"(drifted from {hedge_ratio_csv:.4f} to {latest_hr:.4f}).")

        open_pos = get_open_position(pair)

        # =====================================================================
        # CASE 1: pair already has an open position -> evaluate EXIT
        # =====================================================================
        if open_pos is not None:
            frozen_z = compute_frozen_zscore(
                latest_price_s1, latest_price_s2,
                open_pos["entry_hedge_ratio"],
                open_pos["entry_alpha"],
                open_pos["entry_residual_mean"],
                open_pos["entry_residual_std"],
            )
            held = days_held(open_pos["entry_date"], today)
            print(f"{pair}: OPEN position (entry {open_pos['entry_date']}, "
                  f"held {held}d). Frozen z-score: {frozen_z:.4f}")

            exit_reason = None
            if args.force_exit and args.force_exit == pair:
                exit_reason = "force_exit"
            elif abs(frozen_z) >= Z_STOP_THRESHOLD:
                exit_reason = "z_stop"
            elif abs(frozen_z) <= Z_EXIT_THRESHOLD:
                exit_reason = "z_exit"
            elif held >= MAX_HOLDING_DAYS and position_unrealized_pnl(open_pos, latest_price_s1, latest_price_s2) < 0:
                exit_reason = "max_hold"
            elif not guard_ok:
                # Guard failure on an open position -> force exit, don't just skip.
                # A skipped guard check with no exit path would leave a position
                # open against a hedge ratio the system no longer trusts.
                exit_reason = "hr_guard_breach"

            if exit_reason:
                exits_to_execute.append({
                "position_id": open_pos["id"],
                "pair": pair,
                    "ticker1": independent_ticker,
                    "ticker2": dependent_ticker,
                    "exit_reason": exit_reason,
                    "exit_zscore": frozen_z,
                    "latest_price_s1": latest_price_s1,
                    "latest_price_s2": latest_price_s2,
                    "open_pos": open_pos,
                "entry_price": None,   # filled in at execution from actual fills
                "exit_price": None,
            })
                _pending_exit_meta[pair] = {
                    "exit_reason": exit_reason,
                    "exit_zscore": frozen_z,
                    "exit_side1": reverse_order_side(open_pos["entry_side1"]),
                    "exit_side2": reverse_order_side(open_pos["entry_side2"]),
                }
                print(f"{pair}: EXIT triggered ({exit_reason})")
            else:
                print(f"{pair}: holding, no exit condition met.")

            continue  # a pair with an open position is never also a new entry

        # =====================================================================
        # CASE 2: pair is flat -> evaluate ENTRY
        # =====================================================================
        if not guard_ok and not args.force_entry:
            print(f"{pair}: guard failed, skipping entry.")
            continue

        latest_rolling_z = rolling_zscore.iloc[-1]
        resid_mean_window = residuals_df['residual'].iloc[-zscore_lookback:].mean()
        resid_std_window = residuals_df['residual'].iloc[-zscore_lookback:].std()

        print(f"Latest rolling z-score: {latest_rolling_z:.4f}")

        if args.force_entry or abs(latest_rolling_z) > Z_ENTRY_THRESHOLD:
            entries_to_execute.append({
                "pair": pair,
                "ticker1": independent_ticker,
                "ticker2": dependent_ticker,
                "entry_zscore": latest_rolling_z,
                "entry_hedge_ratio": latest_hr,          # frozen HR = rolling HR at entry moment
                "entry_alpha": latest_intercept,         # frozen alpha = rolling intercept at entry moment
                "entry_residual_mean": resid_mean_window, # frozen mean = window mean at entry
                "entry_residual_std": resid_std_window,   # frozen std = window std at entry
                "latest_price_s1": latest_price_s1,
                "latest_price_s2": latest_price_s2,
            })
            _pending_entry_meta[pair] = {
                "entry_zscore": latest_rolling_z,
                "entry_hedge_ratio": latest_hr,
                "entry_alpha": latest_intercept,
                "entry_residual_mean": resid_mean_window,
                "entry_residual_std": resid_std_window,
            }
            force_note = " (force-entry)" if args.force_entry else ""
            print(f"{pair}: ENTRY triggered{force_note} "
                  f"(|z|={abs(latest_rolling_z):.4f}{'' if args.force_entry else f' > {Z_ENTRY_THRESHOLD}'})")

    print(f"\n{len(entries_to_execute)} entr{'y' if len(entries_to_execute)==1 else 'ies'} queued: "
          f"{[e['pair'] for e in entries_to_execute]}")
    print(f"{len(exits_to_execute)} exit(s) queued: "
          f"{[e['pair'] for e in exits_to_execute]}")

    if args.signal_only:
        print("Signal dry run complete. No orders placed.")
        return

    # --- Wait until execution time, then place orders ---
    if entries_to_execute or exits_to_execute:
        if args.force_entry or args.no_sleep:
            print("--no-sleep: executing immediately." if args.no_sleep else "--force-entry: executing immediately.")
        else:
            sleep_until(EXECUTION_TIME)

        if not MooMooAdapterAvailable:
            raise ImportError("MooMooAdapter is unavailable; moomoo-api could not be imported.")

        broker = MooMooAdapter()
        broker.connect()

        for exit_sig in exits_to_execute:
            open_pos = exit_sig.get("open_pos")
            if open_pos is None:
                open_pos = get_open_position(exit_sig["pair"])
            if open_pos is None:
                print(f"{exit_sig['pair']}: position disappeared before exit execution, skipping.")
                continue

            exit_price1 = float(exit_sig["latest_price_s1"])
            exit_price2 = float(exit_sig["latest_price_s2"])
            executed_size1 = float(open_pos["executed_size1"])
            executed_size2 = float(open_pos["executed_size2"])

            print(f"Placing EXIT order for {exit_sig['pair']} (reason={exit_sig['exit_reason']})")
            entry_independent_side = open_pos["entry_side1"]
            entry_dependent_side = open_pos["entry_side2"]
            exit_independent_side = reverse_order_side(entry_independent_side)
            exit_dependent_side = reverse_order_side(entry_dependent_side)

            leg1_broker_order_id, _, leg1_create_time = submit_leg(
                broker,
                exit_sig["ticker1"],
                exit_independent_side,
                executed_size1,
                exit_price1,
            )
            leg2_broker_order_id, _, leg2_create_time = submit_leg(
                broker,
                exit_sig["ticker2"],
                exit_dependent_side,
                executed_size2,
                exit_price2,
            )

            exit_order1_id = record_order(
                strategy_id="stat_arb",
                strategy_identifier=exit_sig["pair"],
                symbol=exit_sig["ticker1"],
                leg="ticker1",
                leg_type="exit",
                side=exit_independent_side,
                intended_price=exit_price1,
                intended_quantity_raw=executed_size1,
                submitted_quantity=executed_size1,
                submitted_time=leg1_create_time,
                broker="moomoo",
                position_id=exit_sig["position_id"],
                broker_order_id=leg1_broker_order_id,
                status="submitted",
            )
            exit_order2_id = record_order(
                strategy_id="stat_arb",
                strategy_identifier=exit_sig["pair"],
                symbol=exit_sig["ticker2"],
                leg="ticker2",
                leg_type="exit",
                side=exit_dependent_side,
                intended_price=exit_price2,
                intended_quantity_raw=executed_size2,
                submitted_quantity=executed_size2,
                submitted_time=leg2_create_time,
                broker="moomoo",
                position_id=exit_sig["position_id"],
                broker_order_id=leg2_broker_order_id,
                status="submitted",
            )

            print(
                f"{exit_sig['pair']}: exit orders submitted "
                f"(legs {leg1_broker_order_id}, {leg2_broker_order_id})"
            )

        for entry_sig in entries_to_execute:
            entry_price1 = float(entry_sig["latest_price_s1"])
            entry_price2 = float(entry_sig["latest_price_s2"])
            plan = compute_pair_order_plan(
                price1=entry_price1,
                price2=entry_price2,
                hedge_ratio=entry_sig["entry_hedge_ratio"],
                zscore=entry_sig["entry_zscore"],
            )

            print(f"Placing ENTRY order for {entry_sig['pair']}")
            leg1_broker_order_id, leg1_entry_price, leg1_create_time = submit_leg(
                broker, entry_sig["ticker1"], plan["ticker1_side"], plan["ticker1_qty"], entry_price1
            )
            leg2_broker_order_id, leg2_entry_price, leg2_create_time = submit_leg(
                broker, entry_sig["ticker2"], plan["ticker2_side"], plan["ticker2_qty"], entry_price2
            )

            entry_order1_id = record_order(
                strategy_id="stat_arb",
                strategy_identifier=entry_sig["pair"],
                symbol=entry_sig["ticker1"],
                leg="ticker1",
                leg_type="entry",
                side=plan["ticker1_side"],
                intended_price=entry_price1,
                intended_quantity_raw=plan["ticker1_intended_qty"],
                submitted_quantity=plan["ticker1_qty"],
                submitted_time=leg1_create_time,
                broker="moomoo",
                broker_order_id=leg1_broker_order_id,
                status="submitted",
            )
            entry_order2_id = record_order(
                strategy_id="stat_arb",
                strategy_identifier=entry_sig["pair"],
                symbol=entry_sig["ticker2"],
                leg="ticker2",
                leg_type="entry",
                side=plan["ticker2_side"],
                intended_price=entry_price2,
                intended_quantity_raw=plan["ticker2_intended_qty"],
                submitted_quantity=plan["ticker2_qty"],
                submitted_time=leg2_create_time,
                broker="moomoo",
                broker_order_id=leg2_broker_order_id,
                status="submitted",
            )

            print(
                f"{entry_sig['pair']}: entry orders submitted "
                f"(legs {leg1_broker_order_id}, {leg2_broker_order_id})"
            )

        if not args.no_reconcile:
            broker.start_push()
            reconcile_orders(broker)
            _open_entry_positions(today_str)
            _close_exit_positions(today_str)
            broker.disconnect()
    else:
        print("No entries or exits triggered today. Nothing to execute.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}")
        sys.exit(1)
