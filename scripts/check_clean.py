import sqlite3
from datetime import datetime

DB = "data/trading.db"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

rows = conn.execute("SELECT * FROM orders ORDER BY id").fetchall()
print("=== Orders ===")
for r in rows:
    d = dict(r)
    print(f"  #{d['id']}  {d['symbol']:<10} {d['side']:<5} {d['leg_type']:<6} {d['leg']:<8} "
          f"status={d['status']:<10} broker={d['broker']:<8} "
          f"broker_id={d['broker_order_id'] or '':<12} "
          f"pos_id={d['position_id'] or '':<4} "
          f"intended={d['intended_price']:<8} "
          f"fill_price={d['fill_price'] or '':<8} fill_qty={d['fill_quantity'] or ''}")
print()

rows = conn.execute("SELECT * FROM positions_stat_arb ORDER BY id").fetchall()
print("=== Positions ===")
for r in rows:
    d = dict(r)
    print(f"  #{d['id']}  {d['pair']:<14} status={d['status']:<8} broker={d['broker']:<8} "
          f"date={d['entry_date']:<12} "
          f"z={d['entry_zscore']:<+8.4f} "
          f"hr={d['entry_hedge_ratio']:<8.4f} "
          f"alpha={d['entry_alpha']:<10.6f}")
    print(f"       leg1 {d['ticker1']:<6} side={d['entry_side1']:<5} "
          f"price={d['entry_leg1_price']:<10.4f} size={d['executed_size1']:<10}")
    print(f"       leg2 {d['ticker2']:<6} side={d['entry_side2']:<5} "
          f"price={d['entry_leg2_price']:<10.4f} size={d['executed_size2']:<10}")
print()

# Summary
open_pos = [r for r in rows if r["status"] == "open"]
pending = [r for r in conn.execute("SELECT * FROM orders WHERE status IN ('pending','submitted')").fetchall()]
filled = [r for r in conn.execute("SELECT * FROM orders WHERE status = 'filled'").fetchall()]
print(f"Summary: {len(open_pos)} open position(s), "
      f"{len(pending)} pending order(s), {len(filled)} filled order(s)")

conn.close()
