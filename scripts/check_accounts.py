"""
Query all moomoo accounts across all markets: balances, positions, and recent orders.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import moomoo as moo
from src.brokers.moomoo.adapter import MooMooAdapter

# Market labels for display
MARKET_LABELS = {
    moo.TrdMarket.US: "US",
    moo.TrdMarket.HK: "HK",
    moo.TrdMarket.SG: "SG",
    moo.TrdMarket.MY: "MY",
}

def list_accounts_for_market(market) -> list[dict]:
    """Return accounts accessible under the given market filter."""
    ctx = moo.OpenSecTradeContext(
        host="127.0.0.1", port=11111, is_encrypt=False,
        filter_trdmarket=market,
    )
    ret, data = ctx.get_acc_list()
    ctx.close()
    if ret != 0 or data is None:
        return []
    return data.to_dict("records")

# Collect accounts across all markets
all_accounts: dict[str, dict] = {}  # key = f"{trd_env}_{acc_id}"
for market in MARKET_LABELS:
    for acct in list_accounts_for_market(market):
        key = f"{acct['trd_env']}_{acct['acc_id']}"
        if key not in all_accounts:
            entry = dict(acct)
            entry["_markets_seen"] = set()
            all_accounts[key] = entry
        all_accounts[key]["_markets_seen"].add(MARKET_LABELS[market])

# Also get the default US-context accounts (already covered above, but ensure we have them)
print("=== All Accounts ===\n")
for key, acct in all_accounts.items():
    trd_env = acct["trd_env"]
    acc_id = acct["acc_id"]
    acc_type = acct["acc_type"]
    sim_type = acct.get("sim_acc_type", "") or "N/A"
    comp_name = acct.get("competition_acc_name", "") or "N/A"
    markets = ", ".join(sorted(acct["_markets_seen"]))

    print(f"  {trd_env:<10} acc_id={acc_id:<22} type={acc_type:<16} markets={markets}")
    if sim_type != "N/A" and sim_type:
        print(f"             sim_type={sim_type}  comp_name={comp_name}")
    print()

# --- For each account, query balance + positions ---
import pandas as pd

for key, acct in all_accounts.items():
    trd_env = str(acct["trd_env"])
    acc_id = acct["acc_id"]
    markets = sorted(acct["_markets_seen"])

    # Default to US if available, otherwise first market
    primary_market = "US" if "US" in markets else markets[0]

    label = f"{trd_env} (acc_id={acc_id}, market={primary_market})"

    broker = MooMooAdapter(market=primary_market, trd_env=trd_env, acc_id=int(acc_id))
    broker.connect()

    try:
        bal = broker.get_account_balance()
        print(f"=== {label} Balance ===")
        print(f"  Cash:        ${bal.cash:>12.2f}")
        print(f"  Equity:      ${bal.equity:>12.2f}")
        print(f"  Buying Power:${bal.buying_power:>12.2f}")

        positions = broker.get_positions()
        print(f"\n  Positions ({len(positions)}):")
        if positions:
            for p in positions:
                mkt = f"{p.current_price:>10.4f}" if p.current_price is not None else "N/A"
                pnl = f"{p.pnl:>+10.2f}" if p.pnl is not None else "N/A"
                print(f"    {p.symbol:<12} qty={p.quantity:>8.2f} "
                      f"cost={p.average_price:>10.4f} mkt={mkt} "
                      f"pnl={pnl}")
        else:
            print("    (none)")

        ret2, odata = broker.trade_context.order_list_query(
            trd_env=broker.trd_env, acc_id=broker._acc_id or 0, refresh_cache=True,
        )
        if ret2 == 0 and odata is not None and len(odata):
            print(f"\n  Recent orders ({len(odata)}):")
            for _, o in odata.iterrows():
                filled = float(o["dealt_qty"])
                fp = float(o["dealt_avg_price"])
                print(f"    #{o['order_id']:<10} {o['code']:<12} {o['trd_side']:<6} "
                      f"qty={float(o['qty']):>8.2f} status={o['order_status']:<16} "
                      f"filled={filled:>8.2f} @ {fp:>8.4f} ({o['create_time'][:10]})")
        else:
            print("\n  Recent orders: (none)")
    except Exception as e:
        print(f"=== {label} Error: {e}")

    print()
    broker.disconnect()

# --- Local DB ---
from src.db.positions_db import get_conn
with get_conn() as conn:
    rows = conn.execute(
        "SELECT id, strategy_identifier, symbol, side, leg_type, status, "
        "  broker_order_id, fill_price, fill_quantity, position_id, broker "
        "FROM orders ORDER BY id"
    ).fetchall()
if rows:
    print(f"=== DB Orders ({len(rows)}) ===")
    for r in rows:
        fill = ""
        if r["fill_price"]:
            fill = f" fill={r['fill_price']} x {r['fill_quantity']}"
        print(f"  #{r['id']}  {r['strategy_identifier']:<14} {r['symbol']:<10} "
              f"{r['side']:<5} {r['leg_type']:<6} status={r['status']:<10} "
              f"broker={r['broker'] or ''} {fill}")
else:
    print("=== DB Orders (empty) ===")

with get_conn() as conn:
    rows = conn.execute("SELECT * FROM positions_stat_arb ORDER BY id").fetchall()
if rows:
    print(f"\n=== DB Positions ({len(rows)}) ===")
    for r in rows:
        print(f"  #{r['id']}  {r['pair']:<14} status={r['status']:<8} "
              f"broker={r['broker']:<8} z={r['entry_zscore']:<+8.4f}")
else:
    print("\n=== DB Positions (empty) ===")
