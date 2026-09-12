# Stat-Arb Dry Run Testing & Reconciliation Plan

## Context

The stat-arb pairs trading system (`scripts/run_daily_signal.py`) has been through two bug-fix sweeps but has never been executed end-to-end. We need to validate three layers independently before trusting it with live execution:

1. **Signal computation** — data download, rolling OLS, z-scores, guards, entry/exit logic
2. **Order placement + DB** — broker adapter, `record_order`, `open_position`, `close_position`
3. **Trade reconciliation** — after orders fill, update DB with actual fill prices/quantities (currently missing entirely)

---

## Phase 1: Signal Dry Run (`--signal-only`)

**Goal:** Run all signal computation without sleeping or placing orders. Print what *would* happen.

### Changes to `scripts/run_daily_signal.py`

Add `argparse` at the top of `main()`:

```
--signal-only    Compute signals for all pairs, print entry/exit decisions, exit
--force-entry PAIR  Force an entry on a specific pair (e.g. "AAPL-MSFT"), skip signal check
--no-sleep       Skip sleep_until(15:59), execute immediately
```

When `--signal-only`:
- Run data download + signal computation for all top-5 pairs (unchanged)
- Print a summary table: pair, latest z-score, HR, guard status, entry/exit decision, reason
- Exit before `sleep_until` — no broker connection, no orders

**Validates:** yfinance download, `compute_rolling_ols_signal`, `compute_zscore`, `estimate_ar1`, `hedge_ratio_guard`, `compute_frozen_zscore`, `compute_pair_order_plan`, entry/exit threshold logic.

---

## Phase 2: Forced Entry (`--force-entry PAIR`)

**Goal:** Place a real entry order on the paper account for a specific pair, testing the full DB + broker pipeline.

### Changes to `scripts/run_daily_signal.py`

When `--force-entry PAIR`:
- Look up the pair in `pair_selection_results.csv` (error if not found)
- Download data for just that pair's two tickers (150 days)
- Compute rolling OLS signal to get current HR, alpha, residual mean/std
- **Skip z-score threshold check** — force entry regardless of signal strength
- Skip `sleep_until` (implied, no `--no-sleep` needed)
- Execute the entry flow: `record_order` (x2) → `submit_leg` (x2) → `open_position`
- If pair already has an open position → warn and exit

**Validates:** `compute_pair_order_plan`, `record_order`, `submit_leg`, `MooMooAdapter.place_order`, `open_position`, DB writes.

### Position ID backfill (bug fix)

Currently, entry orders are recorded with `position_id = NULL` because `record_order` runs before `open_position`. After `open_position` returns the new `position_id`:

- Add `update_order_position_id(order_id, position_id)` to `positions_db.py`
- Call it for both `entry_order1_id` and `entry_order2_id` after `open_position`

```python
# positions_db.py
def update_order_position_id(order_id: int, position_id: int, db_path: str = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute("UPDATE orders SET position_id = ? WHERE id = ?", (position_id, order_id))
```

---

## Phase 3: Full Reconciliation

**Goal:** After placing orders, poll the broker for actual fills and update the DB with real fill prices, quantities, and slippage.

### 3a. Add `get_order_fills()` to `src/brokers/moomoo/adapter.py`

```python
def get_order_fills(self, order_id: str) -> list[dict]:
    """Retrieve fill records for a specific order from the broker."""
    self._require_connected()
    ret, data = self.trade_context.deal_list_query(trd_env=self.trd_env)
    if ret != 0 or data is None:
        return []
    if isinstance(data, pd.DataFrame):
        filtered = data[data['order_id'].astype(str) == str(order_id)]
        return filtered.to_dict('records')
    return []
```

Returns list of dicts with keys: `deal_id`, `order_id`, `code`, `price`, `qty`, `deal_time`, etc.

### 3b. Add reconciliation helpers to `src/db/positions_db.py`

```python
def get_pending_orders(position_id: int | None = None, db_path: str = DB_PATH) -> list[dict]:
    """Get all orders with status in ('pending', 'submitted')."""
    with get_conn(db_path) as conn:
        if position_id is not None:
            rows = conn.execute(
                "SELECT * FROM orders WHERE position_id = ? AND status IN ('pending', 'submitted')",
                (position_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM orders WHERE status IN ('pending', 'submitted')"
            ).fetchall()
        return [dict(r) for r in rows]

def update_order_status(order_id: int, status: str, db_path: str = DB_PATH) -> None:
    """Update order status (for cancelled/rejected orders)."""
    with get_conn(db_path) as conn:
        conn.execute("UPDATE orders SET status = ? WHERE id = ?", (status, order_id))
```

### 3c. Add `reconcile_orders()` to `scripts/run_daily_signal.py`

After placing all orders (both entries and exits), run a reconciliation loop:

```python
def reconcile_orders(broker, order_ids: list[int], timeout: int = 120, interval: int = 5):
    """Poll broker for order fills and update DB."""
    from src.db.positions_db import record_order_fill, update_order_status
    
    deadline = time.time() + timeout
    pending = set(order_ids)
    
    while pending and time.time() < deadline:
        for oid in list(pending):
            # Look up the broker_order_id from the orders table
            status = broker.get_order_status(broker_order_id)
            if status == OrderStatus.FILLED:
                fills = broker.get_order_fills(broker_order_id)
                if fills:
                    total_qty = sum(f['qty'] for f in fills)
                    avg_price = sum(f['price'] * f['qty'] for f in fills) / total_qty
                    last_time = max(f['deal_time'] for f in fills)
                    record_order_fill(oid, avg_price, last_time, total_qty, intended_price)
                pending.discard(oid)
            elif status in (OrderStatus.CANCELLED, OrderStatus.REJECTED):
                update_order_status(oid, status.value.lower())
                pending.discard(oid)
        if pending:
            time.sleep(interval)
    
    if pending:
        print(f"WARNING: {len(pending)} orders not filled within {timeout}s: {pending}")
```

### 3d. Integration into execution flow

After the entry/exit execution blocks, collect all `order_id`s and call `reconcile_orders()`:

```python
all_order_ids = []
# ... during execution, collect order_ids from record_order calls ...
reconcile_orders(broker, all_order_ids)
```

This replaces the current behavior of passing intended prices as fill prices.

---

## Files to Modify

| File | Changes |
|------|---------|
| `scripts/run_daily_signal.py` | Add argparse, `--signal-only`, `--force-entry`, `--no-sleep`, reconciliation loop, position_id backfill |
| `src/brokers/moomoo/adapter.py` | Add `get_order_fills()` method |
| `src/db/positions_db.py` | Add `update_order_position_id()`, `get_pending_orders()`, `update_order_status()` |

## Execution Order

1. **Phase 3a + 3b** — adapter + DB helpers (no behavior change, just new functions)
2. **Phase 1** — `--signal-only` flag (test signal computation)
3. **Phase 2** — `--force-entry` flag + position_id backfill (test DB + broker)
4. **Phase 3c + 3d** — reconciliation loop (test fill tracking)

## Verification

1. `python scripts/run_daily_signal.py --signal-only` — should print signal decisions for top 5 pairs without errors
2. `python scripts/run_daily_signal.py --force-entry <PAIR>` — should place 2 leg orders on paper account, record in DB, and reconcile fills
3. After forced entry: `python scripts/run_daily_signal.py --signal-only` — should show the pair as "OPEN position"
4. Check DB: `sqlite3 data/trading.db "SELECT * FROM orders WHERE position_id IS NOT NULL"` — entry orders should have position_id linked
5. Check DB: `sqlite3 data/trading.db "SELECT * FROM orders WHERE status = 'filled'"` — orders should have real fill prices
