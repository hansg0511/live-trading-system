# Generic trading core boundary (Stages 1–3)

This document is the boundary contract for the generic trading-core work. It
does not replace the existing stat-arbitrage execution path, and it does not
claim that the generic OMS has been proven against OpenD or any live broker.

## Dependency direction

The intended dependency graph is deliberately one-way:

```text
strategies  ───────►  trading_core  ◄───────  adapters
```

`src/trading_core/` owns provider-neutral domain objects, policies, lifecycle
rules, and reconciliation decisions. Strategy code translates a strategy's
signal into a generic intent. An adapter translates generic commands and
broker responses at the edge. The core must never import a strategy package,
market-data provider, broker adapter, or provider SDK.

The reverse edge is forbidden as well: an adapter or translator may depend on
the core contracts, but the core must not reach into a translator to discover
how a broker works. This keeps a new strategy or broker replaceable without
changing the domain state machine.

The existing `src/execution/`, `src/db/`, and
`src/brokers/moomoo/adapter.py` modules are the hardened legacy path. They are
not evidence that the generic core is complete; they remain outside the new
dependency boundary until an explicit translator is introduced.

## Provider-neutral domain vocabulary

The generic layer should use stable value objects rather than strings whose
meaning is supplied by a particular broker.

- An **account reference** identifies the execution scope: environment,
  venue/market, broker account identity, and (where required) firm or account
  class. Account identity is part of every command and broker fact. A missing
  or conflicting account reference is an admission failure, not an implicit
  account selection.
- An **instrument** identifies one tradable asset at one venue and asset
  class. It carries canonical symbol identity and the quantity/price rules
  needed by policy. It is not a pair member and it must not encode a
  stat-arb ticker convention.
- An **execution intent** is the logical user/strategy request. It has an
  immutable intent ID, account reference, operation kind, requested legs,
  policy snapshot, and idempotency key.
- A **leg** is one requested signed instrument delta within an intent. A leg
  may be BUY, SELL, or a normalized signed quantity; it is not a broker order.
- A **broker-order attempt** is one submission attempt for a leg. A single leg
  may have zero, one, or several attempts after timeout, rejection, or
  reconciliation. Broker order IDs belong to attempts, not to the intent or
  the leg itself.
- A **fill** is immutable execution evidence. It is additive and keyed by the
  broker's deal ID when available, otherwise by a deterministic fingerprint
  scoped to the attempt. Replaying a fill must not change cumulative quantity.

The minimum durable relationship is therefore:

```text
intent 1 ──► N legs 1 ──► N broker-order attempts 1 ──► N fills
```

This is intentionally more general than the legacy two-leg assumption. A
retry creates a new attempt under the same intent/leg; it must never create a
second logical intent merely because the first broker response was lost.

## Books and allocations

The system has two different kinds of ownership and they must not be merged.

1. **Broker account book** — the broker's current signed net quantities and
   broker-reported orders/fills. This is the source of truth for actual
   exposure.
2. **Virtual strategy books** — strategy allocations claiming ownership of
   some account exposure. A strategy allocation is a ledger claim, not a
   broker position query.

Allocations are account-scoped and instrument-scoped. They may be split among
strategies, operations, portfolios, or risk buckets. A pair label is not an
ownership boundary. If the broker has a signed quantity that cannot be
explained by the sum of virtual allocations, the remainder is an **UNKNOWN
residual**. It must remain visible and must block new or destructive execution
until reconciled explicitly. Missing callbacks, missing local rows, or an
empty order query must never be interpreted as a flat account.

When a virtual allocation is closed, the allocation is reduced only by broker
fill evidence. A local intent reaching a terminal state does not, by itself,
prove that account exposure is gone.

## Execution policy and risk

Execution policy is a pure decision boundary around the durable intent:

```text
signal/command
  -> normalize account + instruments
  -> obtain broker/account facts
  -> project signed account-wide risk
  -> approve or reject
  -> persist intent and legs
  -> submit attempts through an adapter
  -> ingest broker facts/events/fills
  -> reconcile broker truth and virtual ownership
```

Policy may reject for stale data, invalid instruments, unavailable session
state, account mismatch, insufficient buying power/margin, exposure limits,
operation limits, or unresolved reconciliation. The policy must not call a
provider SDK or infer broker facts from strategy metadata.

Risk projection is account-wide and signed. It starts with current broker
positions, applies remaining signed working-order quantities and the proposed
intent deltas, then evaluates that projection against gross exposure, net exposure,
margin/buying-power, concentration, and pending-operation limits. It must
handle multiple strategies trading the same instrument and opposing signed
quantities. Virtual allocations explain ownership but are not added again to
broker exposure. Strategy-level notional is not a substitute for the account
projection.

## Broker facts, events, and fills

Adapters expose normalized facts and events to the core:

- account identity and account status;
- instrument validity and market/session state;
- signed broker positions;
- open/recent broker orders;
- order status facts;
- deal/fill facts with broker IDs, quantity, price, and timestamps.

Push notifications are an optimization. Polling or a complete broker snapshot
must be able to reconstruct state after a restart. Every event handler is
idempotent. A broker status of `filled` without quantity evidence is not a
fill. A broker order accepted without a durable attempt ID is ambiguous and
enters reconciliation; it is never silently retried.

## Sticky reconciliation

Reconciliation issues are durable facts, not transient log messages. An issue
has a stable category/entity key, details, severity, and open/resolved state.
Examples include:

- broker position with no virtual owner;
- local allocation missing at the broker;
- signed quantity mismatch;
- unknown broker order or ambiguous order match;
- missing broker ID after a possible submission;
- partial multi-leg intent;
- failed account/position/order/fill query;
- unresolved UNKNOWN residual.

An issue remains open across restarts and across polling cycles. It may be
resolved only by an explicit operator action or by a complete, successful
snapshot that proves the issue is absent. A failed or partial query must not
clear an older block. REAL mutation remains blocked while any relevant issue
is open; SIM smoke stages surface the same condition before submission.

## Legacy smoke boundary and current broker evidence

The original supervised smoke harness is a tagged, legacy broker-boundary
artifact. These files remain the historical audit baseline and are preserved
byte-for-byte at the tag below:

| Artifact | Baseline tag | Git blob |
| --- | --- | --- |
| `scripts/sim_smoke_test.py` | `broker-boundary-smoke-ready-20260913` | `9745ac57f654900128316d9d54a51705dc487cf9` |
| `tests/test_sim_smoke.py` | `broker-boundary-smoke-ready-20260913` | `25041eb2ed78cd6cc2a583d1d13109dbb724a945` |

The working branch contains a later smoke-hardened revision in commit
`360e55c`. It adds the tightly scoped SIM external-order allowlist, OpenD
order-query throttling protection, transient query-failure recovery, and
regression tests. The revised legacy harness was then used for a supervised
SIM entry/exit round trip that reconciled the broker flat. This is evidence
for the legacy Moomoo boundary only; it is not evidence that the generic OMS
has reached a broker.

The legacy smoke path remains strategy-independent but is still wired to the
existing `MooMooAdapter` + `ExecutionEngine` two-leg implementation. It is an
integration boundary, not a generic-core consumer. Do not import it from the
core, change it to exercise the new OMS, or use it as proof that generic
multi-leg allocation works. A generic smoke test must remain a separate,
explicit artifact with its own baseline.

`smoke-ready` refers only to the original tagged code checkpoint and its
offline tests. `smoke-passed` now refers to the later supervised SIM run
recorded by the smoke-hardened revision; it requires observing broker
acknowledgement, fills, exit, and flat reconciliation. Neither designation
proves generic-OMS broker integration.

## Migration status

| Area | Status | Boundary statement |
| --- | --- | --- |
| Existing durable pair engine, SQLite ledger, fill idempotency, and Moomoo adapter | Implemented | Hardened two-leg path; remains legacy/pair-specific. |
| Deterministic safety tests and legacy SIM smoke tests | Tested | Fake-broker coverage plus a supervised SIM broker round trip. |
| `src/trading_core/` provider-neutral domain boundary | Implemented and unit-tested | Domain, persistence, state machine, OMS, signed risk projection, and reconciliation primitives are SDK-free and strategy-free. |
| Stat-arb intent translator | Implemented and unit-tested | Produces generic two-leg intents equivalent to representative legacy plans; it does not route them to a broker. |
| Broker adapter translator into the generic port | Planned for roadmap Stage 1 | Keep the proven legacy path intact until a separate generic adapter boundary is implemented and supervised. |
| Account books, virtual allocations, attempts, and UNKNOWN residual ledger | Implemented at contract/persistence depth | Allocations are updated from fill evidence and residual comparison is tested; production snapshot orchestration and operator workflows remain later work. |
| Generic OMS against a real OpenD/broker account | Planned | Must be separately supervised and proven; generic OMS is currently fake-tested, not broker-proven. |

The generic layer is complete only when its durable intent/leg/attempt/fill
model, account-wide signed risk projection, allocation ownership, and sticky
reconciliation have deterministic tests and a separate supervised broker
smoke. Passing the legacy smoke test is necessary evidence for the legacy
adapter boundary but is not evidence that generic Moomoo integration is
complete.
