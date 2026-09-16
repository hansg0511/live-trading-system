# Generic Moomoo Execution Roadmap

## Purpose

This is the staged implementation plan for moving from the smoke-tested,
legacy Moomoo pair-execution path to a generic, broker-neutral trading
infrastructure. Work advances one stage at a time: complete the stage, run
its checks, review the evidence, then begin the next stage.

The current SIM smoke test validates the legacy `MooMooAdapter` and
`ExecutionEngine` path. It does not yet prove the generic OMS against OpenD.

## Current checkpoint: passed

The supervised SIM smoke test has demonstrated:

- OpenD connectivity and SIM-account selection;
- Moomoo order submission and fill recovery;
- entry and exit reconciliation to a flat broker account;
- OpenD order-query throttling and transient-query recovery; and
- legacy smoke-harness safety gates.

It does not provide atomic multi-leg execution: the two equity legs are
separate broker orders and can fill at different times. The generic execution
supervisor must therefore manage partial-fill states explicitly.

## Stage 0 — Consolidate the baseline (complete)

The `generic-trading-core` branch carries the generic domain/OMS baseline and
the later smoke-hardened legacy revision (`360e55c`, equivalent to the
source-branch revision `ab47c82`). The generic-core documentation distinguishes
the original legacy baseline from the current smoke-passed revision, and the
legacy smoke test remains a separate regression artifact. The completed
checkpoint is tagged `generic-stage0-complete-20260915`.

**Completed evidence:** the branch was clean when the checkpoint was created,
the full offline test suite passed, and the documented baseline matched the
code. This stage made no broker calls. Later maintenance commits do not alter
the tagged checkpoint.

## Stage 1 — Build the generic Moomoo bridge (complete)

`src/brokers/moomoo/generic_adapter.py` implements the Moomoo adapter for the
interfaces in `src/trading_core/ports.py`. It translates generic accounts,
instrument mappings, snapshots, order attempts, fills, cancellations,
replacements, and broker errors to and from OpenD. It is SIM-only, requires an
explicit two-way internal-instrument-to-Moomoo-symbol resolver, and fails
closed if a broker symbol cannot be mapped. The pure OpenD response helpers
live in `src/brokers/moomoo/common.py`, shared with the legacy adapter, while
the generic core remains independent of Moomoo and pair-specific code.

Execution uses the broker-neutral `ExecutionSession` selected on the generic
`ExecutionPolicy` and propagated through each `BrokerSubmitRequest`. The
default is `REGULAR`; `EXTENDED` means US pre/post-market and retains the
existing OpenD `fill_outside_rth=True` mapping with limit orders only. The
legacy `allow_extended_hours=True` field remains an alias for `EXTENDED`.
`OVERNIGHT` is distinct: on Moomoo it is a US LIMIT-only request mapped to
native `Session.OVERNIGHT`, without `fill_outside_rth`.

**Completed evidence:** offline fake-OpenD adapter-contract tests cover
account identity, balances, positions, orders, fills, submissions,
cancellations, replacements, error responses, and mapping failures. No OpenD
connection or broker order was made during Stage 1. The next broker call is
the separately approved Stage 2 generic supervised SIM smoke.

## Stage 2 — Generic supervised SIM smoke (complete)

`scripts/generic_sim_smoke_test.py` is the separate generic-OMS harness. It
uses its own generic-core SQLite database, not the legacy smoke database or
the ordinary trading database. It blocks an account with pre-existing broker
positions/open orders, and it never cancels or closes anything it did not
create. The harness is implemented and offline-tested, and the supervised
generic SIM round trip is complete:

- SIM account `5077333` completed the generic RTH entry: AAPL BUY 1 (Moomoo
  order `3408387`) and MSFT SELL-short 1 (Moomoo order `3408388`).
- The generic exit completed both legs: AAPL SELL 1 (Moomoo order `3408464`)
  and MSFT BUY-to-cover 1 (Moomoo order `3408465`).
- Final broker state was flat, with no open orders and no reconciliation
  issues. Both actions used the generic harness against SIM only; no REAL
  orders were sent.

Workflow:

```powershell
# Read-only: creates/verifies the isolated generic-core configuration.
python scripts/generic_sim_smoke_test.py preflight `
  --state-db data/generic-sim-smoke.db --acc-id <sim-account-id>

# During regular hours, omit limit prices for a one-share market-order smoke.
python scripts/generic_sim_smoke_test.py enter `
  --state-db data/generic-sim-smoke.db --acc-id <sim-account-id> --submit

# For US pre/post-market, provide marketable limit prices and select the
# explicit extended session. Repeat with current exit prices later.
python scripts/generic_sim_smoke_test.py enter `
  --state-db data/generic-sim-smoke.db --acc-id <sim-account-id> `
  --limit-price1 <AAPL-buy-limit> --limit-price2 <MSFT-sell-limit> `
  --session EXTENDED --submit

python scripts/generic_sim_smoke_test.py exit `
  --state-db data/generic-sim-smoke.db --acc-id <sim-account-id> `
  --limit-price1 <AAPL-sell-limit> --limit-price2 <MSFT-buy-limit> `
  --session EXTENDED --submit

# For US overnight, both legs must be explicit LIMIT orders. The four
# placeholders are operator-supplied prices; the harness never invents them.
python scripts/generic_sim_smoke_test.py enter `
  --state-db data/generic-sim-smoke.db --acc-id <sim-account-id> `
  --limit-price1 <AAPL-buy-overnight-limit> `
  --limit-price2 <MSFT-sell-overnight-limit> `
  --session OVERNIGHT --submit

python scripts/generic_sim_smoke_test.py exit `
  --state-db data/generic-sim-smoke.db --acc-id <sim-account-id> `
  --limit-price1 <AAPL-sell-overnight-limit> `
  --limit-price2 <MSFT-buy-overnight-limit> `
  --session OVERNIGHT --submit
```

`status` is read-only. It reports the durable entry/exit intents from the
isolated database and may run normal broker-fact recovery for an incomplete
intent. A fully filled and reconciled entry is eligible for exit whether its
intent is `COMPLETED` (the normal post-submit acknowledgement) or remains
`FILLED` after restart recovery interrupted before that acknowledgement. The
exit is blocked unless every entry leg is terminally filled for its requested
quantity and broker positions exactly match that entry; partial, unknown, or
reconciliation-required states never qualify.

The smoke harness uses a versioned, deterministic intent identity. Its `v2`
SHA-256 digest covers the stage, ordered symbols, sides, quantities, order type
and limit prices, plus the effective execution session and policy flags. A
same-payload retry therefore reuses the same durable intent, while a materially
different request (for example, the earlier overnight LIMIT attempt versus a
regular-session MARKET attempt) receives a new identity. Existing fixed-format
or earlier-session intents are retained for audit and are never overwritten.

The Moomoo SIM API currently rejects native overnight submissions with
`Paper trading does not support overnight trading sessions`. This is a broker
limitation, not permission to fall back to regular or pre/post hours. The
generic OMS records that response as a durable terminal rejection when it has
no broker order ID or fill evidence, does not submit later sequential legs,
and recovery repairs the earlier smoke intent by resolving only the exact
fill-query reconciliation issue created by that rejected attempt. Ambiguous
transport failures, broker order IDs, and any fill evidence remain
reconciliation-required.

Moomoo SIM also currently rejects `deal_list_query` with `Paper trading does
not support deal data`. When that exact unsupported-history response occurs,
the generic Moomoo adapter may synthesize one order-level fill fact only from
an order query row that is terminal `FILLED_ALL`, has positive dealt quantity
equal to the submitted quantity, a positive `dealt_avg_price`, and a
parseable timestamp. Partial, unfilled, malformed, or otherwise ambiguous
rows remain reconciliation-required. Recovery resolves the related
account-level fill-query issue only after every leg of that exact intent has
durable terminal fill evidence; it does not infer fills from status alone.

**Completed:** the generic OMS, rather than the legacy engine, completed a
supervised SIM round trip. Regular US market hours are the default test window;
an explicit `EXTENDED` or `OVERNIGHT` session may instead be used for US limit
orders with operator-supplied prices. Either path requires explicit operator
approval before any SIM orders are submitted.

## Stage 3 — Partial-fill recovery

Implement the generic runtime behavior behind its partial-fill policies:
polling and push ingestion, restart recovery, timeout handling, stale-order
handling, and clear operator actions. The initial policy stays conservative:
wait, reconcile, and surface exposure instead of blindly retrying.

**Done when:** simulated delayed fills, one-leg fills, and process restarts
are durably recovered and visibly actionable.

## Stage 4 — Shared portfolio and allocations

Connect broker-account exposure to generic virtual strategy books. Add
shared-capital and account-wide risk controls. Every broker position must be
explicitly attributed to a strategy/book or visible as an `UNKNOWN` residual;
unrelated positions must never silently appear as pairs-strategy exposure.

**Done when:** independent strategies can share an account without
pair-specific size fields or hidden exposure.

## Stage 5 — Two-sleeve stat-arb integration

Represent each pairs configuration as an independent sleeve producing
generic entry and exit intents. Both sleeves use the same account-level
allocator and risk limits, while retaining separate strategy configuration,
signals, and ownership.

**Done when:** two configured sleeves can independently emit, allocate, and
reconcile intents.

## Stage 6 — Combined-book SIM pilot

Run live signals in SIM at deliberately small size. Exercise normal
entries/exits, simultaneous sleeve signals, restart recovery, delayed fills,
and unrelated broker positions.

**Done when:** multiple sessions complete with clean reconciliation and no
manual database repair.

## Stage 7 — Operational readiness

Add the service lifecycle and operating controls required for a durable
system: scheduler, audit/status views, monitoring and alerts, runbooks,
deployment controls, and a separately armed REAL-trading path.

**Done when:** the system can be monitored and safely intervened in before
any limited REAL rollout is considered.

## Execution order

Proceed strictly in order: Stage 0, then Stage 1, and so on. Do not begin a
supervised broker stage until the previous stage's tests and review gate have
passed.
