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

from src.core.models import TimeFrame
from src.data.yfinance.provider import YFinanceDataProvider
from src.strategies.stat_arb.signal import compute_rolling_ols_signal, compute_zscore, estimate_ar1
from src.db.positions_db import init_db, get_open_position, find_active_operation
from src.execution import ExecutionConfig, ExecutionEngine, ExecutionSafetyError

try:
    from src.brokers.moomoo.adapter import MooMooAdapter
    MooMooAdapterAvailable = True
except ImportError:
    MooMooAdapterAvailable = False

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
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Daily stat-arb signal execution")
    parser.add_argument("--signal-only", action="store_true", help="Compute signals and print decisions, do not place orders")
    parser.add_argument("--force-entry", type=str, metavar="PAIR", help="Force an entry on the given pair (e.g. AAPL-MSFT)")
    parser.add_argument("--force-exit", type=str, metavar="PAIR", help="Force close an open position for the given pair")
    parser.add_argument("--no-sleep", action="store_true", help="Skip sleep_until(15:59 ET), execute immediately")
    parser.add_argument("--no-reconcile", action="store_true", help="Skip post-submission polling in SIMULATE (startup reconciliation is never skipped)")
    parser.add_argument("--trd-env", choices=("SIMULATE", "REAL"), help="Trading environment; defaults to FUTU_TRD_ENV or SIMULATE")
    parser.add_argument("--acc-id", type=int, help="Explicit trading account ID (required for REAL)")
    parser.add_argument("--expected-real-account-id", type=int, help="Account ID that must exactly match the broker in REAL")
    parser.add_argument("--enable-live-trading", action="store_true", help="Explicitly arm REAL trading; does not bypass other checks")
    parser.add_argument("--state-db", type=str, help="Explicit state database path")
    args = parser.parse_args()

    config = ExecutionConfig.from_env()
    overrides = {}
    if args.trd_env:
        overrides["trd_env"] = args.trd_env
    if args.acc_id is not None:
        overrides["account_id"] = args.acc_id
    if args.expected_real_account_id is not None:
        overrides["expected_real_account_id"] = args.expected_real_account_id
    if args.enable_live_trading:
        overrides["enable_live_trading"] = True
    if args.state_db:
        overrides["state_db_path"] = args.state_db
    if overrides:
        config = config.with_overrides(**overrides)
    config.validate()
    print(config.startup_summary())
    init_db(config.db_path, trd_env=config.trd_env)

    provider = YFinanceDataProvider()
    provider.connect()

    df = pd.read_csv(str(PAIR_SELECTION_RESULTS_PATH), dtype={"symbol": str})

    if args.force_entry:
        pair = args.force_entry
        if pair not in df['pair'].values:
            print(f"Error: pair '{pair}' not found in {PAIR_SELECTION_RESULTS_PATH}")
            return
        open_pos = get_open_position(pair, config.db_path)
        if open_pos is not None:
            print(f"Error: pair '{pair}' already has an open position (id={open_pos['id']}). Skipping.")
            return
        pending_entry = find_active_operation(pair, "entry", config.db_path)
        if pending_entry is not None:
            print(f"Error: pair '{pair}' already has an active entry operation (id={pending_entry['operation_id']}, status={pending_entry['status']}). Skipping.")
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

        open_pos = get_open_position(pair, config.db_path)

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
                print(f"{pair}: EXIT triggered ({exit_reason})")
            else:
                print(f"{pair}: holding, no exit condition met.")

            continue  # a pair with an open position is never also a new entry

        # =====================================================================
        # CASE 2: pair is flat -> evaluate ENTRY
        # =====================================================================
        pending_entry = find_active_operation(pair, "entry", config.db_path)
        if pending_entry is not None:
            print(f"{pair}: active entry operation {pending_entry['operation_id']} ({pending_entry['status']}); skipping duplicate entry.")
            continue
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

    # --- Wait until execution time, then use the durable two-leg engine ---
    if entries_to_execute or exits_to_execute:
        if args.force_entry or args.no_sleep:
            print("--no-sleep: executing immediately." if args.no_sleep else "--force-entry: executing immediately.")
        else:
            sleep_until(EXECUTION_TIME)

        if not MooMooAdapterAvailable:
            raise ImportError("MooMooAdapter is unavailable; moomoo-api could not be imported.")

        broker = MooMooAdapter(
            market=config.market,
            trd_env=config.trd_env,
            acc_id=config.account_id,
            expected_account_id=config.expected_real_account_id,
            enable_live_trading=config.enable_live_trading,
            security_firm=config.security_firm,
            db_path=str(config.db_path),
        )
        broker.connect()
        engine = ExecutionEngine(broker, config=config, db_path=config.db_path)
        # Keep the timestamp attached to the actual bar used for the signal.
        # REAL mode therefore fails closed when the daily source is stale;
        # it must not be made to look fresh by stamping it with wall-clock time.
        signal_timestamp = data.index[-1] if len(data.index) else None
        try:
            with engine.run_lock():
                # Reconciliation is mandatory before any new order.  In REAL,
                # unresolved discrepancies abort the run before submission.
                startup = engine.startup_reconcile()
                if config.is_real and not startup.ready:
                    raise ExecutionSafetyError("REAL execution blocked by startup reconciliation")
                try:
                    broker.start_push()
                except Exception as exc:
                    print(f"Warning: push subscription unavailable; polling remains authoritative: {exc}")

                for exit_sig in exits_to_execute:
                    open_pos = exit_sig.get("open_pos") or get_open_position(exit_sig["pair"], config.db_path)
                    if open_pos is None:
                        print(f"{exit_sig['pair']}: position disappeared before exit execution, skipping.")
                        continue
                    exit_sig = dict(exit_sig)
                    exit_sig["open_pos"] = open_pos
                    print(f"Submitting EXIT intent for {exit_sig['pair']} (reason={exit_sig['exit_reason']})")
                    result = engine.execute_exit(
                        exit_sig,
                        signal_timestamp=signal_timestamp,
                        operation_date=today_str,
                    )
                    print(f"{exit_sig['pair']}: exit operation {result['operation_id']} status={result['status']}")

                for entry_sig in entries_to_execute:
                    plan = compute_pair_order_plan(
                        price1=float(entry_sig["latest_price_s1"]),
                        price2=float(entry_sig["latest_price_s2"]),
                        hedge_ratio=entry_sig["entry_hedge_ratio"],
                        zscore=entry_sig["entry_zscore"],
                    )
                    print(f"Submitting ENTRY intent for {entry_sig['pair']}")
                    result = engine.execute_entry(
                        entry_sig,
                        plan,
                        signal_timestamp=signal_timestamp,
                        operation_date=today_str,
                    )
                    print(f"{entry_sig['pair']}: entry operation {result['operation_id']} status={result['status']}")

                if args.no_reconcile and config.is_real:
                    print("--no-reconcile cannot disable REAL reconciliation; running the mandatory poll.")
                final = engine.reconcile(timeout=0 if args.no_reconcile and not config.is_real else 10)
                if final.issues:
                    print(f"Reconciliation requires attention: {len(final.issues)} issue(s) recorded.")
        finally:
            broker.disconnect()
    else:
        print("No entries or exits triggered today. Nothing to execute.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}")
        sys.exit(1)
