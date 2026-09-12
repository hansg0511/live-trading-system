"""
Test script: place a single BUY order, verify DB writes and reconciliation.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.core.models import Order, OrderSide, OrderType
from src.brokers.moomoo.adapter import MooMooAdapter
from src.db.positions_db import (
    init_db, record_order, get_conn,
)

init_db()

from datetime import datetime
today_str = datetime.today().strftime("%Y-%m-%d")

broker = MooMooAdapter(market="US")
broker.connect()

# --- Place a single BUY order for EOG ---
price = 130.0
qty = 10

order = Order(
    symbol="US.EOG",
    quantity=float(qty),
    side=OrderSide.BUY,
    order_type=OrderType.LIMIT,
    price=float(price),
)
broker_order_id = broker.place_order(order)
print(f"Placed BUY US.EOG qty={qty} @ {price} -> broker_order_id={broker_order_id}")

# --- Place a second BUY for SLB ---
order2 = Order(
    symbol="US.SLB",
    quantity=10,
    side=OrderSide.BUY,
    order_type=OrderType.LIMIT,
    price=50.0,
)
broker_order_id2 = broker.place_order(order2)
print(f"Placed BUY US.SLB qty=10 @ 50 -> broker_order_id={broker_order_id2}")

# --- Record orders without position_id (will be linked post-fill) ---
db_order_id = record_order(
    strategy_id="stat_arb",
    strategy_identifier="EOG-SLB",
    symbol="US.EOG",
    leg="ticker1",
    leg_type="entry",
    side="BUY",
    intended_price=price,
    intended_quantity_raw=float(qty),
    submitted_quantity=float(qty),
    submitted_time=today_str,
    broker="moomoo",
    broker_order_id=broker_order_id,
    status="submitted",
)
print(f"DB order id={db_order_id} (EOG leg)")

db_order_id2 = record_order(
    strategy_id="stat_arb",
    strategy_identifier="EOG-SLB",
    symbol="US.SLB",
    leg="ticker2",
    leg_type="entry",
    side="BUY",
    intended_price=50.0,
    intended_quantity_raw=10.0,
    submitted_quantity=10.0,
    submitted_time=today_str,
    broker="moomoo",
    broker_order_id=broker_order_id2,
    status="submitted",
)
print(f"DB order id={db_order_id2} (SLB leg)")

# --- Reconciliation: polls for fills and records them ---
print("\n--- Reconciliation ---")
from scripts.run_daily_signal import reconcile_orders
reconcile_orders(broker, timeout=60)

# --- Verify ---
with get_conn() as conn:
    rows = conn.execute("SELECT id, status, fill_price, fill_quantity, position_id, broker_order_id FROM orders WHERE strategy_identifier = 'EOG-SLB'").fetchall()
    print(f"\nOrder status after reconciliation:")
    for r in rows:
        print(f"  id={r['id']} broker={r['broker_order_id']} status={r['status']} fill_price={r['fill_price']} fill_qty={r['fill_quantity']} position_id={r['position_id']}")

    pos = conn.execute("SELECT * FROM positions_stat_arb WHERE pair = 'EOG-SLB'").fetchone()
    if pos:
        print(f"\nPosition created: id={pos['id']} status={pos['status']} leg1={pos['entry_leg1_price']} leg2={pos['entry_leg2_price']}")
    else:
        print("\nNo position created yet (orders may not have filled).")

broker.disconnect()
print("\nDone.")
