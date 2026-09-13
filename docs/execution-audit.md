# Existing execution lifecycle audit

This audit records the execution path that existed before the execution-safety
hardening work on branch `execution-safety-hardening`.

## Entry

1. `scripts/run_daily_signal.py` computes the signal and sizing plan in memory.
2. Signal metadata is stored only in the module-level `_pending_entry_meta` map.
3. `submit_leg()` calls `MooMooAdapter.place_order()` for ticker 1. No durable
   local operation or order-intent row exists yet.
4. The runner calls `submit_leg()` for ticker 2. If the process stops between
   either call, the broker can have exposure with no local record.
5. Only after both broker calls return does the runner insert two rows in
   `orders` with broker order IDs and `submitted` status.
6. Reconciliation polls the broker. A completed order is written through
   `record_order_fill()`, which overwrites a single aggregate fill column and
   marks the order `filled` even when the aggregate quantity is partial.
7. `_open_entry_positions()` requires two filled rows and the in-memory signal
   metadata. It then inserts `positions_stat_arb` as `open`.

Consequences: order intent was not durable before submission; fill callbacks
could be counted twice or overwrite prior fills; signal metadata disappeared on
restart; and one-leg submission/fill failures could leave broker exposure
untracked.

## Exit

1. The runner finds an open `positions_stat_arb` row and computes the frozen
   exit signal in memory.
2. Exit metadata is stored only in `_pending_exit_meta`.
3. It submits both exit legs to the broker before inserting either local order
   row. A crash or second invocation can therefore duplicate or orphan exits.
4. After both calls, it inserts two `orders` rows linked to the position.
5. Reconciliation polls and records fills using the same single-fill-column
   behavior as entry.
6. `_close_exit_positions()` closes the position and writes a `trades` row only
   when two exit rows happen to be marked filled.

## State that existed only in RAM

`_pending_entry_meta` and `_pending_exit_meta` were the sole source for hedge
ratio, alpha, residual statistics, signal z-score, exit reason, and exit
z-score. Broker order IDs existed only after both submissions completed and
were not correlated with a durable operation ID.

## Crash/failure points

- Before leg 1 submission: no local intent exists.
- After leg 1 broker acceptance and before leg 2: one-leg exposure may be
  present with no local record.
- After either broker call and before the subsequent local insert: the broker
  order cannot be reconstructed reliably from local state.
- During fills: a missed callback leaves stale state; a replayed callback can
  overwrite or double-count quantities.
- Before `_open_entry_positions()`/`_close_exit_positions()`: restart loses
  the in-memory metadata required to create the position or trade summary.

The hardened lifecycle persists an operation and both leg intents first,
records broker IDs immediately after each submission, accumulates idempotent
fill events, and reconciles broker state before admitting new live orders.
