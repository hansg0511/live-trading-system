# Generic Moomoo Execution Roadmap

## Purpose

This is the staged implementation plan for moving from the smoke-tested,
legacy Moomoo pair-execution path to a generic, broker-neutral trading
infrastructure. Work advances one stage at a time: complete the stage, run
its checks, review the evidence, then begin the next stage.

The completed SIM smoke test validates the generic OMS and Moomoo bridge
against OpenD; the legacy `MooMooAdapter`/`ExecutionEngine` path remains a
separate regression artifact.

## Current checkpoint: Stage 7 offline preparation in progress; Stage 6 is the gate

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

## Stage 3 — Partial-fill and restart recovery (complete — offline)

The generic OMS now provides a broker-neutral recovery supervisor in
`src/trading_core/oms.py`. It polls durable active intents and broker-order
attempts after process restart, matches broker truth by external/client order
identity, and applies normalized `BrokerFill` facts idempotently. A
`BrokerOrderEvent` route is available for push-compatible adapters, but the
public route treats caller-supplied fills and status fields as advisory. It
performs adapter-authoritative order and fill lookups; only the OMS-instance
private adapter bridge can pass already-authenticated normalized push facts.
Polling remains the source of truth and no push transport is required. The
repository persists order-event dedupe, dedicated OMS evidence fingerprints,
and operator recovery actions in `core_broker_order_events` and
`core_recovery_actions`.

The supervisor uses an injected UTC clock and policy-controlled
`timeout_seconds`/`stale_order_seconds` thresholds. Working orders that are
stale or timed out, delayed/partial legs, ambiguous matches, missing broker
evidence, and broker-query failures remain fail-closed and create durable
reconciliation issues. Recovery actions record observed positions, remaining
quantities, stale/timeout flags, occurrence counts, and allowed next steps
(`poll_again`/fresh broker facts, explicit reconciliation, or operator
review). Snapshot identity is checked against the persisted account, leg
instrument, side, submitted quantity, and broker/client order identity before
any lifecycle transition; cumulative snapshot/event quantities must be
supported by durable fills. Positive fills reported with WORKING,
REJECTED, or CANCELLED facts are handled explicitly, and mixed terminal legs
remain actionable reconciliation while uniform clean rejection/cancellation
can terminate safely. Poll fill-recording failures, conflicting event dedupe
facts, external-ID conflicts, and unsupported terminal evidence are persisted
as issues/actions rather than escaping as raw errors. No blind retry, replace,
hedge, or position inference is performed. Duplicate normalized events/fills
are idempotent; late conflicting events remain reconciliation-required.

Partial residuals receive the same stale/timeout treatment as working orders,
and cancellation results are recorded with durable action resolution or an
open ambiguity action. The only implemented partial-fill policy is explicit
`WAIT`; unsupported policy values are rejected. Stage 2-era idempotency hashes
remain compatible for identical requests after the stale-order field was
added, while material payload changes still conflict.

The supported Stage 3 execution-policy surface is deliberately narrow:
`SEQUENTIAL` legging, `WAIT` partial-fill handling, `HOLD_AND_RECONCILE` or
`CANCEL_WORKING_LEGS` failure handling, one attempt, and no native atomicity
or required capability claims. `PARALLEL`, `BEST_EFFORT`, partial-fill modes
other than `WAIT`, `UNWIND_FILLED_LEGS`, multiple attempts,
`require_native_atomicity=True`, and non-empty `required_capabilities` are
rejected during policy construction; separate legs are never submitted when
native atomicity is requested.

Focused offline coverage exercises one leg filled while another works,
delayed completion, restart polling without resubmission, snapshot identity
and quantity/account/client identity mismatches, bounded terminal restart
discovery, positive submission/cancellation deal payloads, positive terminal
fills, mixed and clean terminal legs, poll/event fill conflicts with separate
ownership, legitimate partial push events, stale and timed-out partial
residuals, missing/ambiguous broker evidence, duplicate/late updates, legacy
hash compatibility, policy guards, sticky completion blockers, cancellation
outcomes, durable operator status, aged terminal-history safety blocks,
one-attempt fill ownership, malformed submit/cancel payloads, late fill-only
events, post-submit exception quarantine, broker-order timestamps, and
conflicting account aliases, terminal zero-fill contradictions from events and
snapshots, late fill-history evidence after local cancellation/rejection,
changed-evidence event replays, wrong-account event results, public fill API
terminal guards, and push fill ingestion with multiple attempts. The focused
Stage 3 hardening additionally covers explicit ambiguous-cancellation
rejection, direct aged-history recovery guards, account-wide multi-attempt
quarantine, unknown raw terminal fill metadata, exact WORKING/full-fill event
replay no-ops, and repository-level account/blocker/attempt validation. The
focused Stage 3 suite has 276 passing tests; the full offline suite has 356
passing tests and `compileall` is clean.

**Current status:** the broker-neutral runtime and offline recovery tests are
implemented. Terminal attempts outside the automatic restart window now leave
a durable `TERMINAL_HISTORY_EXPIRED` issue/action until an explicit,
exact-order-history verification resolves it; an aged row never becomes safe
merely by falling out of the polling window. The runtime quarantines persisted
multi-attempt legs, requires complete durable fills for the specific broker
attempt before accepting `FILLED`, and preserves each conflicting event's
ownership. Negative, non-finite, or nonnumeric raw fill quantity/price/time
fields are treated as possible exposure. Fill-only events after local terminal
state and every post-submit exception remain explicit reconciliation blockers.
When supplied, the adapter's broker order timestamp drives stale age; otherwise
the durable attempt timestamp is used, never a per-query capture time. Poll
snapshots, push events, and fills validate all supplied account aliases before
state or allocation changes. Terminal CANCELLED/REJECTED facts with zero
cumulative quantity are checked against incoming and later history fills before
any allocation; contradictions create sticky reconciliation issues/actions.
Event dedupe persists a normalized fill-evidence fingerprint, so an exact
replay is idempotent while changed fill evidence cannot bypass terminal checks;
new fingerprints exclude local receive/query time, while legacy-row replay
comparison remains strict about the historical receive timestamp.
Multiple broker attempts remain quarantined on polling, push, public-fill, and
completion paths, including when the unsupported leg belongs to a sibling
intent on the same account. The repository refuses direct fills while account
issues/actions are open, for disabled accounts or non-canonical account IDs,
and for any leg with more than one broker attempt; the OMS uses an explicit
validated internal path only after its full broker-evidence checks. The public
`apply_fill` compatibility API now derives the persisted account and validates
ownership, terminal state, attempt count, and account aliases before recording.
The clean terminal classifier now recursively inspects the complete normalized
and raw provider evidence envelope, including sibling response branches. Fill
facts carry an exact external-order/evidence reference and are checked at both
the OMS and repository boundaries. Broker event and order timestamps are
validated against durable submit evidence with a documented five-second clock
tolerance; stale terminal facts become reconciliation evidence. Replay
fingerprints include event identity, broker event time, account aliases,
no-fill assertions, cumulative quantities, and all fill evidence, with legacy
rows accepted only when every available field agrees. Moomoo cancellation
responses must echo the requested order and canonical account aliases, and
conflicts are ambiguous rather than clean. Persisted execution policies are
reconstructed and revalidated on every status/recovery/completion route;
invalid or unsupported rows become durable blockers. Same-account fills claimed
by sibling managed intents are account-level reconciliation blockers rather than
silently ignored.

Stage 3 is **complete for the broker-neutral offline implementation and
independent audit**. The final submit boundary requires callable
`get_authoritative_account_facts()`; Moomoo freshly queries positions, open
orders, and fills while ordinary display reads may retain bounded caching.
Missing capabilities or incomplete fresh facts remain durable blockers.

This is an offline completion only: it does not claim live OpenD partial-fill
or push-stream validation. A provider must adapt its own push stream to
`BrokerOrderEvent`/`BrokerFill`; polling remains authoritative.

The normalized submit/cancel contract is explicit: a missing cumulative-fill
value is `None` (unknown), not zero. A clean rejection requires durable
`no_submit_asserted` and `no_fill_asserted` provider assertions, an
authoritative zero cumulative quantity, no broker order ID, and no fill-shaped
raw evidence. A clean cancellation likewise requires an authoritative zero
quantity, `no_fill_asserted`, and `ambiguous is False`; missing, malformed,
unknown, or provider-specific fill-shaped response data remains
reconciliation-required. Terminal order snapshots and push events use the
same explicit no-fill assertion and raw-metadata checks before clean
REJECTED/CANCELLED promotion. The OMS validates
the persisted canonical account (internal ID, broker, environment, external
account ID, and enabled state) at every submit, poll, event, status, fill, and
completion boundary. Open hard recovery actions and reconciliation issues are
account-wide blockers, and account-scoped fills that cannot be attributed to a
managed attempt create an unknown-exposure action rather than leaving the
account safe to submit.

The eighth hardening pass removes the remaining ordinary public trust-boundary
shortcuts. Fill persistence now requires an instance-scoped validated broker
evidence capability plus exact internal-account, external-order, and evidence
identity; the legacy public `apply_fill` compatibility path is fail-closed and
cannot allocate from a caller-fabricated `Fill`. Provenance and provider
evidence walkers recurse through mappings and sequences, while nested raw
fill-shaped data cannot be hidden behind reserved internal fingerprint keys.
Fill timestamps and snapshot capture timestamps are checked against durable
submit/order time with the five-second tolerance. New submissions first pass
an account-wide broker-fact gate that blocks unknown positions, open orders, or
fills. Moomoo cancel normalization validates every returned row and all
account/order aliases. Pre-Stage-3 event and fill rows retain exact replay
compatibility, but changed evidence conflicts durably. Repository issue/action
resolution requires the private instance-scoped resolution capability; direct
ordinary calls cannot clear blockers. Hostile code with deliberate Python
introspection is outside this in-process encapsulation boundary; no normal
public API can fabricate an allocation or clear a safety blocker.

The ninth hardening pass kept provider evidence separate from OMS replay
fingerprints: legacy fingerprint-shaped provider keys were inspected rather
than overwritten. The tenth pass moves new fingerprints fully out of provider
metadata into dedicated event columns, while legacy replay remains exact and
collision-safe. Account-wide submission safety now requires one explicit,
complete normalized position/open-order/fill snapshot; missing, unsupported,
incomplete, or ambiguous facts and `UNKNOWN` broker-order facts create durable
`BROKER_FACT_UNAVAILABLE`/account-blocker actions instead of being treated as
empty results. Reverse reconciliation blocks a new submit when any nonzero
managed allocation is absent or has a different signed broker quantity, while
`UNKNOWN`/unowned allocation rows are never netted as managed exposure and
remain sticky account blockers. Fill identity is checked by both broker order
plus dedupe key and broker order plus external fill ID, with an additive lookup
index for legacy databases. Startup and recovery routes audit legacy duplicate
external-fill identities and persist per-intent reconciliation actions before
any lifecycle promotion. Moomoo submit normalization validates every returned
row, the requested account aliases, client/order identity, and mapped symbol;
extra, foreign, or conflicting rows are ambiguous and never reduced to the
first row.

The tenth hardening pass makes the public event boundary advisory for both
status and fill claims: an adapter-authoritative order snapshot is required
before public status can affect lifecycle, and caller-provided status,
cumulative quantities, and no-fill assertions are never promoted directly.
Account-wide broker facts now use an explicit `BrokerFactSnapshot.complete`
contract, so a successful true-empty result is safe but an unsupported,
partial, or ambiguous query is a durable `BROKER_FACT_UNAVAILABLE` blocker.
Unknown/unowned allocation rows are excluded from managed net exposure and
block the account, and startup/migration audits quarantine legacy duplicate
`(broker_order_id, external_fill_id)` rows with per-intent actions. New event
fingerprints are stored in dedicated columns rather than provider metadata;
old metadata-shaped fingerprints are used only for exact legacy replay and
never cause provider evidence to be dropped.

The eleventh hardening pass makes response-shape certainty explicit at the
Moomoo boundary. `as_records()` now recognizes an actual empty table as a
successful empty result, but null, malformed, unsupported, or mixed-type rows
raise a distinguished adapter error; consequently positions, open orders, or
fills from a malformed response produce an incomplete account-fact snapshot
and a durable safety blocker rather than a false flat account. The evidence
walker no longer skips any OMS-looking envelope or fingerprint-shaped key, so
provider metadata cannot hide nested fill/status evidence behind a valid-looking
hash. New fill and event evidence fingerprints use immutable broker execution
time (`filled_at`) and exclude local query/receive time; repeated Moomoo deal
queries therefore match across restart. Pre-migration replay remains strict:
legacy rows must match their stored receive timestamp and every available
historical field, while new or changed fill evidence creates a durable event
conflict before any allocation. Focused Stage 3 coverage now includes malformed
position/order/fill responses, recognized true-empty account facts, stable
repeated fill queries, valid-looking envelope collisions, and legacy rows that
receive new fill evidence; Stage 3 remains in progress pending review.

The twelfth hardening pass canonicalizes recognized Moomoo DataFrame/table
responses recursively into primitive rows before they enter normalized facts
or raw command payloads. Opaque table/SDK values are rejected or treated as
possible evidence, never as empty/no-fill data; account/provenance walkers also
reject opaque nested metadata. DataFrame cancellation responses therefore
retain deal and account evidence for the generic OMS, while genuine empty
DataFrames remain valid empty facts. Replay no-op compatibility now retains
the normalized source account identity alongside each persisted fill and
requires account identity and evidence-reference equality before any same-key
event can return early. The focused Stage 3 suite has 173 passing tests; the
complete suite has 296 passing tests and `compileall` is clean. Stage 3 remains
in progress pending review.

The thirteenth hardening pass closes the remaining opaque-provider gaps in the
generic core. Positive fill/deal scanners now treat every non-primitive nested
provider value as possible evidence, so cancellation/rejection no-fill claims
cannot resolve while an opaque value remains. `BrokerFactSnapshot.metadata` is
explicitly diagnostic-only: it is recursively checked for primitive values,
canonical account aliases, and execution-shaped fields before an empty
account-fact result can pass the submission gate. Opaque, foreign-account, and
execution-evidence metadata create a durable account-fact blocker; ordinary
primitive diagnostics remain supported. Stage 3 remains in progress pending
review.

The fourteenth hardening pass extends the same fail-closed rule to nested order
lifecycle evidence, including order rows hidden under arbitrary response
envelopes. Adapter-authenticated Moomoo order snapshots now carry explicit
provenance, so a terminal `CANCELLED_ALL`/`REJECTED` row with matching order
identity, submitted quantity, and authoritative zero `dealt_qty` can support a
clean no-fill result without allowing generic caller metadata to do so. Moomoo
order queries collapse only exact duplicate rows and reject conflicting status,
account/instrument, quantity, or fill-economics rows. Both the adapter and OMS
now enforce strict status/quantity consistency (`PARTIALLY_FILLED` requires a
strict partial, `FILLED` requires the exact submitted quantity, and terminal
reject/fail/cancel states cannot claim a complete fill). Focused Stage 3
coverage is 173 passing tests; Stage 3 remains in progress pending review.

The fifteenth hardening pass recognizes plain broker lifecycle claims such as
`status=FILLED` and malformed/non-empty order containers in every generic
no-submit/no-fill evidence path, while retaining only clearly benign primitive
diagnostics such as `status=OK`. Moomoo's source-bound zero-fill exception now
rejects nested lifecycle/order claims and contradictory alias values before
selecting any field; duplicate rows retain a complete canonical raw-evidence
signature, and nested deal IDs/quantities must agree with aggregate facts.
Working orders reporting zero cumulative quantity require an authoritative
no-fill assertion at the account-wide pre-submit gate. Unsupported Moomoo deal
history is surfaced as explicit account-fact uncertainty even when conservative
filled-order fallback evidence is available. Startup and recovery audits now
quarantine legacy duplicate provider-order claims across intents/accounts with
sticky account issues and per-intent operator actions. Focused Stage 3
coverage is 189 passing tests; Stage 3 remains in progress pending review.

The sixteenth hardening pass makes legacy duplicate provider-order claims an
enforcement barrier at every fill boundary, including normalized polling,
validated event ingestion, the compatibility API, and repository persistence;
the current matching row is not an exemption and no allocation is recorded
while a sibling claim remains. Moomoo position rows now validate every
populated code/symbol/ticker, quantity, and direction alias before resolving
or discarding a row, so contradictory flat-looking records become incomplete
account facts. Normalized broker fills carry canonical instrument provenance;
OMS and repository checks compare it with the intended leg and recursively
reject foreign or conflicting symbol aliases before persistence. Adapter
command and snapshot no-fill authority is bound to the expected order,
submitted quantity, instrument, and all sibling provider payloads; zero,
unknown, foreign, or contradictory facts remain reconciliation-required.
Focused Stage 3 coverage is 199 passing tests; the complete offline suite has
322 passing tests and `compileall` is clean. Stage 3 remains in progress
pending independent review.

The seventeenth hardening pass centralizes recursive provider-payload
coercion: JSON-looking strings are parsed and walked, while malformed or
opaque values are ambiguity rather than proof of no fill, no order, or clean
account facts. Public/advisory event handling now authenticates a capability-
bound snapshot through the same complete no-fill and sibling-evidence checks
used by polling, with case/format-insensitive `raw`, `authoritative-snapshot`,
and `response` aliases. Account-wide broker facts group rows by canonical
external order ID, allow only exact duplicates, and durably block
contradictory duplicate status/quantity/instrument/fill evidence. Focused
Stage 3 coverage is 206 passing tests; the complete offline suite has 329
passing tests and `compileall` is clean. Stage 3 remains in progress pending
independent review.

The eighteenth hardening pass makes provider-key normalization tokenize
camelCase and acronym-bearing aliases (`accountId`, `instrumentId`,
`externalOrderId`, and `brokerOrderID`) before applying the shared
case/kebab/snake normalization. Account-fact and broker-fill validators now
therefore inspect those aliases recursively instead of treating them as
unrecognized diagnostics. Command authority validation also enumerates every
normalized response alias (`response`, `Response`, `response-payload`, and
equivalent formatting) and rejects duplicate or contradictory branches; a
positive dealt/fill claim in any sibling cannot be hidden by a clean response.
Focused Stage 3 coverage is 214 passing tests; the complete offline suite has
337 passing tests and `compileall` is clean. Stage 3 remains in progress
pending independent review.

The nineteenth hardening pass centralizes execution-identity evidence across
the generic provider-payload boundary and the Moomoo raw-response adapter.
Nonempty `trade_id`/`tradeId`/`tradeID`, `execution_id`/`executionId`,
`deal_id`, `fill_id`, and equivalent stable execution references are now
material evidence even when `dealt_qty` is zero. Snapshot, event, submit,
reject, and cancel authority paths therefore remain reconciliation-required
instead of treating a zero quantity plus an execution identity as clean
no-fill. Focused Stage 3 coverage is 231 passing tests; the complete offline
suite has 354 passing tests and `compileall` is clean. Stage 3 remains in
progress pending independent review.

The twentieth hardening pass adds an explicit `get_authoritative_account_facts`
adapter contract for safety-critical submission gates. The generic OMS prefers
that fresh reader, while the Moomoo implementation also makes
`get_account_facts` itself fresh: positions, open orders, and deal history are
queried from OpenD with cache refresh requested, and the bounded local order
cache is bypassed for account-fact recovery (including the conservative filled-
order fallback). Ordinary display order reads retain their bounded cache, but
cached rows can no longer hide a newly arrived broker/manual order from the
submit safety gate; query failures remain incomplete/blocking. Focused Stage 3
coverage is 233 passing tests; the complete offline suite has 356 passing
tests and `compileall` is clean. This was the penultimate hardening pass;
final Stage 3 closure is recorded below.

The twenty-first hardening pass makes the fresh-facts capability mandatory at
the generic OMS submit boundary. `get_account_facts()` is now explicitly an
ordinary-read/display API and is never a fallback for order-safety decisions;
an adapter without callable `get_authoritative_account_facts()` receives a
durable `BROKER_FACT_UNAVAILABLE` blocker and cannot submit. Offline adapter
fakes now declare the authoritative path explicitly, with coverage proving a
cached-only adapter sends no order while a conforming authoritative adapter is
used. Focused generic OMS/adapter/recovery coverage is 276 passing tests; the
complete offline suite remains 356 passing tests and `compileall` is clean.
Stage 3 is complete for the broker-neutral offline runtime. No live broker
partial-fill validation is implied; Stage 4 is next.

## Stage 4 — Shared portfolio and allocations (complete — offline)

Connect broker-account exposure to generic virtual strategy books. Add
shared-capital and account-wide risk controls. Every broker position must be
explicitly attributed to a strategy/book or visible as an `UNKNOWN` residual;
unrelated positions must never silently appear as pairs-strategy exposure.

**Done when:** independent strategies can share an account without
pair-specific size fields or hidden exposure.

The generic core now uses the broker-neutral `Book` and `BookAllocation`
configuration model. A book allocation binds a declared book and strategy to
one account and one explicit limit basis (`risk_budget`, `capital_amount`, or
`capital_fraction`). The schema migration/indexes, repository getters, and
restart-safe ownership queries persist books, allocations, intent ownership,
broker-order claims, fills through their owning intent, and position ledger
rows without pair-specific fields.

When an account has declared book allocations, new risk-bearing intents must
name an enabled book with a matching active strategy allocation. Existing
managed position rows without a valid book, external/unknown rows, unknown
book requests, and broker positions/orders/fills that cannot be attributed to
one known book remain visible and create sticky account reconciliation actions;
they are never netted into another book. The fresh Stage 3 broker-fact gate
continues to reject unowned broker facts before the book gate runs.

The OMS projects signed managed positions plus active intent reservations per
book, applies each book's bound, and applies an account-wide aggregate bound.
Fractional allocations require an explicit account capacity (for example
`allocation_capacity`); an account may optionally provide a tighter aggregate
capacity such as `account_capacity` or `aggregate_risk_budget`. Capacity-unit
mixing and missing limits fail closed. The read-only `book_risk_status()` API
exposes per-book exposure, limit, remaining capacity, declarations, and
unknown allocations for restart/operator views.

Focused Stage 4 tests cover two independent books sharing one account,
per-book and aggregate-capacity rejection before any adapter submission,
unknown allocation/requested-book blockers, mixed/incompatible capacity-base
rejection, persistence/restart of the book declarations and sticky
reconciliation state, and idempotent migration of pre-Stage 4 schemas. The
focused file has 7 passing tests; the complete offline suite has 363 passing
tests and `compileall` is clean. No broker/OpenD call or live validation was
performed.

Initialization now additively migrates missing `book_id` columns on legacy
intent and position-allocation tables, creates the Stage 4 lookup indexes only
after those columns exist, and records migration version 4. Re-running
initialization is safe. Account aggregation fails closed when declared books
use mixed limit bases or explicitly incompatible exposure units; it records a
`book_capacity_configuration` blocker rather than skipping the aggregate
guard.

Stage 5 is implemented separately from the generic OMS: pair mechanics remain
strategy-owned, while the generic books, allocator, and intent records remain
broker-neutral. There is no optimizer, UI/service, multi-broker, or SIM pilot
work in this stage.

## Stage 5 — Two-sleeve stat-arb integration (complete — offline)

Represent each pairs configuration as an independent sleeve producing
generic entry and exit intents. Both sleeves use the same account-level
allocator and risk limits, while retaining separate strategy configuration,
signals, and ownership.

**Done when:** two configured sleeves can independently emit, allocate, and
reconcile intents.

Stage 5 is complete for the broker-neutral offline runtime. The new
strategy-owned `PairSleeve` contract gives each sleeve a stable identity,
exactly one declared Stage 4 book, and separate pair configuration. A
`NormalizedPairTarget` carries signed two-leg exposure and a stable cycle ID;
the sleeve translates it into a book-attributed generic entry or exit intent.
Repeated evaluation of the same cycle uses the same idempotency key, while a
later cycle creates a new intent. Pair mechanics remain outside the generic
OMS and repository.

The Stage 5 allocation coordinator provides a versioned, timestamped,
provenance-bearing Clean40-style update. It validates the complete sleeve to
book mapping, rejects mixed capacity units and invalid account totals, and
atomically expires/replaces only the affected Stage 4 book-cap declarations.
Its effective-time policy is monotonic per account/book: a new version may be
scheduled at the latest declaration time or later, but an earlier future
version is rejected atomically. This prevents overlapping future declarations
from inflating capacity. Existing position ownership and intents are not
changed and no implicit liquidation is performed. Allocation declarations and
generic intents remain restart-persistent through the existing repository.

Two configured example sleeves (A and B) and an example 50/50 allocation
update show where future Clean40/pairs signals plug into the normalized target
interface. Focused Stage 5 coverage has 6 passing tests; the complete offline
suite has 369 passing tests and `compileall` is clean. No broker/OpenD call,
market-data call, production signal generation, or live validation was
performed.

Stage 6 is the separate combined-book SIM pilot.  The offline readiness
harness is now implemented in `src/strategies/stat_arb/stage6_pilot.py`; the
actual broker pilot remains outstanding.  It is still gated on supervised
validation of simultaneous sleeve signals, restart recovery, delayed fills,
and unrelated broker positions.

## Stage 6 — Combined-book SIM pilot

The bounded readiness harness accepts an explicit `Stage6PilotSpec` containing
one enabled SIM account, exactly two already persisted Stage 5 sleeves/books,
their versioned Clean40 allocation update, and one normalized target per
sleeve.  Sleeve instrument IDs and provider mappings/symbols are configuration
inputs; the harness never invents securities, quantities, prices, or signals.
It validates ownership, allocation coverage/units, signal pair identity, and
the account's durable book/reconciliation state.  It builds both generic
intents before dispatch and assigns a deterministic `stage6-run-*` correlation
ID.  Armed runs persist that correlation in the generic intent metadata;
dry-runs expose the complete intent/run mapping in the operator report without
creating active intents or contacting a broker.

For a target with `action: "EXIT"`, `signed_quantities` are the current
signed quantities being closed, in the configured instrument order.  They are
not the order delta and must not be pre-negated: `PairSleeve.to_intent()`
performs the one and only sign reversal needed to create the closing legs.  The
runner rejects an EXIT target that differs from the durable per-book exposure
even in dry-run mode, and the SIM-submit gate repeats the comparison against
the same fresh authoritative broker positions.  A flat book therefore cannot
pass a non-zero EXIT configuration, and a target that would open rather than
close a position is stopped before any submission.

The default runner mode is `DRY_RUN`.  The only submission mode is an
explicit `SIM_SUBMIT` arm; there is no LIVE mode and the runner rejects a
non-SIM account before constructing a dispatch.  SIM preflight requires a
fresh, complete authoritative account-facts snapshot, no open broker orders,
no unknown/unattributed positions or fills, no open reconciliation/action
blockers, and compatible declared book capacity.  Every actual submit still
passes through the existing GenericOMS safety gate.  `recover(account)` is an
explicit read-only restart/poll hook; the harness does not retry, cancel,
hedge, or repair exposure automatically.

Offline preparation:

```python
from src.strategies.stat_arb.stage6_pilot import (
    Stage6PilotRunner,
    Stage6PilotSpec,
    Stage6RunMode,
)

spec = Stage6PilotSpec(
    account=explicit_sim_account,
    sleeves=(sleeve_a, sleeve_b),
    allocation_update=explicit_clean40_update,
    targets=(normalized_target_a, normalized_target_b),
)
report = Stage6PilotRunner(repository, oms).run(spec)  # DRY_RUN by default
print(report.operator_text())
```

An offline declarative boundary is also available at
`src/strategies/stat_arb/stage6_config.py` with the local command
`scripts/generic_stage6_pilot.py`.  The checked-in
`configs/stage6-pilot.template.json` is intentionally non-runnable: every
account, pair, mapping, size, signal, and timestamp placeholder must be
replaced by the operator.  The commands are deliberately asymmetric:

```powershell
# Pure validation; does not initialize a database or contact OpenD.
python scripts/generic_stage6_pilot.py validate `
  --config configs/stage6-pilot.json

# Repository preparation and complete two-intent report; never contacts a broker.
python scripts/generic_stage6_pilot.py dry-run `
  --config configs/stage6-pilot.json

# Explicit SIM compatibility checkpoint; read-only provider facts plus a
# transactional import of the verified bounded legacy order ledger.  This
# creates no order and requires the operator's explicit flat-state confirmation.
python scripts/generic_stage6_pilot.py baseline `
  --config configs/stage6-pilot.json `
  --legacy-db data/generic-sim-smoke.db `
  --legacy-label legacy-smoke `
  --verify-flat

# Only after offline review and a fresh RTH gate.  The phrase must exactly
# match the run correlation printed by validate/dry-run.
python scripts/generic_stage6_pilot.py sim-submit `
  --config configs/stage6-pilot.json `
  --arm-sim `
  --confirm "ARM STAGE6 SIM <stage6-run-id>"
```

The SIM command rejects REAL/LIVE accounts, missing `RTH_ONLY` handoff
policy, non-REGULAR sessions, extended-hours flags, missing arm, or a
confirmation phrase that does not exactly contain the deterministic run ID.
It constructs the existing SIM-only Moomoo adapter but submits only through
`Stage6PilotRunner` → `GenericOMS`; the CLI has no direct order path.  Before
arming, the operator must complete this checklist: replace all placeholders;
persist/verify the canonical SIM account and two enabled books/strategy/sleeves;
verify every Moomoo symbol mapping and approved tiny signed quantity; review
the allocation version/effective time/provenance and signal cycle IDs; run
`validate` then `dry-run`; confirm OpenD, account, fresh broker facts, regular
market state, and no broker/local blockers immediately before `sim-submit`.
This is configuration/readiness tooling only and does not constitute Stage 6
pilot evidence or completion.

Moomoo SIM currently rejects the deal-history endpoint.  The adapter therefore
advertises `CUMULATIVE_ORDER_SNAPSHOTS` with bounded current/historical order
coverage, never fabricates an external deal ID, and only derives fill evidence
for terminal fully filled orders whose cumulative dealt quantity, average
price, order identity, account, instrument, side, and provider timestamp all
match.  Partial, delayed, malformed, conflicting, or unknown cumulative facts
remain blocked.  Before the first SIM submit, the explicit `baseline` command
must freshly verify the account is flat and import the exact known legacy
claims into a retired `legacy-smoke` strategy/book; the persisted baseline is
then required to clear the bounded history-gap blocker.  Those imported rows
are not attributed to either new Stage 6 sleeve and the source database is
read-only.  Re-running the exact baseline is idempotent; changed or unmatched
source/account/order evidence is rejected.

After offline review, a separately approved RTH SIM run uses:

```python
report = Stage6PilotRunner(repository, oms).run(
    spec,
    mode=Stage6RunMode.SIM_SUBMIT,
)
```

Before that command, the operator must supply and verify the SIM account
identity, two persisted enabled books and sleeve configurations, broker
instrument mappings, explicit normalized entry/exit targets and quantities,
allocation version/effective time/provenance, approved small sizing, market
session/limit-price policy where applicable, and the current OpenD/SIM
preflight facts.  RTH execution must use the documented Moomoo interpreter and
environment; no extended/overnight pricing is inferred by this harness.

The offline scenario driver covers normal two-sleeve preparation, one-book
capacity rejection, duplicate/simultaneous signal gating, explicit restart
recovery invocation, delayed/partial open-order representation, unrelated
broker-position blocking, and SIM-only arm invariants.  These tests do not
prove Moomoo fills, push delivery, market-session behavior, or live
reconciliation.

**Bounded SIM progress (2026-10-01 UTC):** the approved generic Stage 6
attempt reached OpenD and account `5077333` in RTH.  The runner submitted only
the Book A AAPL BUY 1 leg (`3438891`, filled at `329.848`), then stopped before
MSFT, SPY, or QQQ were submitted.  The verified AAPL exposure was subsequently
flattened through GenericOMS (`3438909`, SELL 1 filled at `329.77`), and the
fresh final account snapshot was flat with no open orders or open reconciliation
issues/actions.  Stage 6 is still incomplete because the four-leg combined
pilot did not complete; Stage 7 remains in progress.

The generic core now exposes a proof-gated compensated-partial recovery for
that specific lifecycle shape: it can close an entry only after a fresh flat
account snapshot, a complete historical-order window from intent creation
through the current time, exact attempt-scoped fill evidence, and a linked
compensating exit.  The earlier offline-only pass had not yet obtained that
proof; the read-only proof and local closures are recorded below.  This is not
Stage 6 completion evidence.

**Proof-gated historical cleanup (2026-10-02 UTC):** a fresh read-only SIM
query for account `5077333` returned a complete flat account snapshot with no
open orders, plus complete cumulative historical-order coverage for the
current AAPL/MSFT round trip.  The current entry
`stage5-intent-c8fd47657f3c767fdee49056` and its compensating exit
`stage6-compensating-exit-445a7b5cf8029dc1c9be7394` were safely terminalized
locally as `COMPLETED`; orders `3440582`, `3440583`, `3440681`, and `3440682`
remain fully auditable and no broker mutation was performed by the cleanup.
The two older compensated-partial records were then proven against the same
fresh account/history coverage and closed locally: source entries
`stage5-intent-ecb04165af52fdba0803fd0c` and
`stage5-intent-ded757a2cdd5b338ac85f6a4` are `CANCELLED`, while their exact
compensating exits are `COMPLETED`.  The never-submitted Book B retry intent
`stage5-intent-b6a8c4bb98cbe72783309c55` is also `CANCELLED` under the
fresh-flat/no-local-submission proof.  These are local, auditable lifecycle
closures only; no broker mutation was performed.  The ownership path excludes
only verified flat historical claims from active capacity; unknown, active, or
unresolved claims remain blockers.  Stage 6's four-leg pilot and Stage 7
completion gates remain incomplete.

**Bounded SIM retry (2026-10-02 UTC):** a fresh RTH/SIM run used new A/B
cycle and signal IDs and passed the offline configuration checks, full test
suite (`449 passed`), account identity, fresh flat preflight, and the exact
zero-dealt `SUBMITTING` acknowledgement path.  The generic runner submitted
only Book A before its account-wide safety gate stopped the batch: AAPL BUY 1
order `3440582` filled at `331.01`, and MSFT SELL/short 1 order `3440583`
filled at `516.224`.  Book B SPY/QQQ was not submitted.  Fresh broker facts
then showed AAPL +1, MSFT -1, no open orders, while generic local recovery
kept the run in reconciliation-required status because sibling-fill and
legacy-book ownership blockers were present.  No manual or direct-SDK
flattening was performed; the approved Stage 6 four-leg round trip remains
incomplete and the account must be flattened through GenericOMS before any
new submission.

**Bounded SIM outcome (2026-10-02 UTC, not a pass):** a later controlled run
filled the four entry-direction orders `3441507`, `3441508`, `3441510`, and
`3441511`.  Its first EXIT configuration was incorrectly pre-negated and
therefore filled same-direction orders `3441521`, `3441522`, `3441524`, and
`3441525`; this is a Stage 6 execution defect, not completion evidence.  The
operator then used the verified current signed exposure basis `[+2, -2]` for
each pair and flattened through GenericOMS: `3441532` SELL 2 AAPL at `333.66`,
`3441533` BUY 2 MSFT at `515.09`, `3441535` SELL 2 SPY at `769.21`, and
`3441536` BUY 2 QQQ at `749.05`.  A fresh complete SIM snapshot captured at
`2026-10-03T04:44:39Z` reported no positions and no open orders.  Stage 6
remains **not passed** because the wrong-side exits were sent; no overnight
submission, cancellation, replacement, or retry is permitted.

**Local ledger readiness cleanup (2026-10-03 UTC, no new order):** a fresh
read-only SIM account-facts snapshot at `2026-10-03T05:06:10Z` was complete,
flat, and had no open orders.  The old internal rejection
`stage6-intent-final-abort-aapl-exit-20261001` was verified through the
GenericOMS definite-no-submit path: its provider assertion is `accepted=false`,
`REJECTED`, no external order ID, authoritative cumulative fill `0`, no fill
rows, and no submission evidence.  Its exact
`TERMINAL_HISTORY_EXPIRED` issue/action rows are now `RESOLVED`; no unrelated
issue or action was changed.  The age guard now preserves this narrow
no-submit outcome while still blocking an aged ambiguous rejection (covered
by targeted regression tests).  A bounded read-only historical-order query
through `2026-10-03T05:13:05Z` was complete and contained no row matching the
old attempt's internal or external identity.

The current controlled run remains auditable but Stage 6 is not passed: the two
entry intents, both same-direction erroneous EXIT intents, and both corrective
EXIT intents are durably filled.  The fresh flat account and proof-gated
aggregate closure establish lifecycle completeness only; the wrong-side exits
were actually submitted, so this is not approval or readiness evidence for a
new pilot.  Stage 7 local status is `DRY_RUN_ONLY` with zero open issues,
actions, alerts, or unfinished intents.  The configured Stage 6 dry-run still
contains historical EXIT targets and safely stops on target-vs-durable-exposure
mismatch after closure; no fresh ENTRY dry-run was run from that EXIT
configuration.

**Verified aggregate lifecycle cleanup (2026-10-03 UTC, no new order):** a
fresh complete SIM account-facts snapshot at `2026-10-03T05:25:44Z` again
reported no positions and no open orders, with 20 cumulative order-snapshot
fills.  The proof-gated aggregate resolver then closed Book A and Book B
separately, naming all six participating intents, twelve exact
attempt/order identities, twelve durable fills, quantities, prices,
account/book/instrument provenance, and per-symbol signed totals (all zero).
The same-direction EXIT fills remain in the durable ledger and are recorded as
an incident in two `VERIFIED_AGGREGATE_ROUNDTRIP_CLOSED` audit events; no
unrelated intent, allocation, issue, or action was changed.  All six named
intents are now `COMPLETED`; the fresh Stage 7 status remains `DRY_RUN_ONLY`
with zero alerts, unfinished intents, open reconciliation issues, and open
recovery actions.  Stage 6 remains **not passed** because the wrong-side exits
were actually submitted.

**Next-session ENTRY readiness (2026-10-03 UTC, no submit):** the separate
`configs/stage6-pilot-sim-next-entry-20261005.json` keeps the existing SIM
account `moomoo:sim:5077333`, the two existing 50/50 books, RTH-only policy,
and explicitly unique next-cycle IDs for AAPL/MSFT and SPY/QQQ ENTER targets
of `[1, -1]`.  Configuration validation passed with run ID
`stage6-run-39494cd4793fed63c9d4d116`.  A fresh read-only Moomoo snapshot at
`2026-10-03T05:39:54Z` was complete, had no positions or open orders, and
contained 20 cumulative order-snapshot fills.  The repository-supported dry
run exited `0` with `preflight_passed=true`, no stop reasons, two
`PLANNED_NOT_SUBMITTED` intents, and `broker_facts.queried=false`; the DB still
has zero open issues/actions and zero persisted intents or broker orders for
the new cycle.  The run keeps `RTH_ONLY`/`REGULAR` and `allow_extended_hours:
false`; UTC was Saturday and outside RTH, so no SIM submission was attempted.
This is ENTRY configuration/preflight evidence only, not a Stage 6 pass or
broker-validation claim.  The historical EXIT configuration remains unchanged
as incident evidence.

**Bounded SIM lifecycle evidence (2026-10-05 UTC; current-cycle acceptance
passed):** during RTH, the repository-supported configuration
`configs/stage6-pilot-sim-next-entry-20261005.json` submitted exactly one
two-book SIM cycle through GenericOMS.  Book A filled AAPL BUY 1 at `335.225`
(`3443862`) and MSFT SELL 1 at `531.74` (`3443863`); Book B filled SPY BUY 1
at `770.76` (`3443867`) and QQQ SELL 1 at `753.06` (`3443868`).  The required
fresh preflight at `2026-10-05T13:44:18Z` was complete, flat, had no open
orders, and observed the regular-RTH `AFTERNOON` market state for all four
symbols.  After both entry intents were durably filled, a fresh interpreter
process invoked the read-only `Stage6PilotRunner.recover(account)` path at
`2026-10-05T13:46:40Z`; it preserved both intent IDs/statuses, observed AAPL
`+1`/MSFT `-1` and SPY `+1`/QQQ `-1`, and created no duplicate broker attempt.
Each entry leg retained exactly one broker order and one complete fill.  The
separate current-exposure EXIT configuration then produced
the expected opposite-side orders: Book A AAPL SELL 1 at `334.62`
(`3443910`) and MSFT BUY 1 at `529.34` (`3443911`), and Book B SPY SELL 1 at
`770.60` (`3443921`) and QQQ BUY 1 at `751.95` (`3443922`).

The exit runner stopped fail-closed after Book B was initially `PARTIALLY_FILLED`
while the single QQQ order was still `WORKING`; no duplicate was submitted.
Bounded GenericOMS recovery later recorded that same order as filled and the
proof-gated round-trip resolver terminalized only the two named book pairs
after fresh flat account facts and complete bounded historical order evidence.
The final authoritative SIM snapshot at `2026-10-05T13:56:00Z` was complete,
flat, and had no open orders (eight exact fills observed); both durable book
risk exposures were `0.0` with `10.0` remaining capacity.  Local Stage 7 status was
`STOPPED`/`DRY_RUN_ONLY` with zero alerts, unfinished intents, open
reconciliation issues, or open recovery actions.  This is auditable SIM
lifecycle evidence and supervised current-cycle acceptance, not approval for
REAL trading or autonomous paper trading.  The earlier wrong-direction EXIT
incident remains retained as an immutable incident record and was not undone.
The overall Stage 6 roadmap gate remains **in progress** until the documented
multiple-session clean-reconciliation criterion is satisfied; that status is
not a rejection of this distinct Oct 5 cycle.

Run live signals in SIM at deliberately small size only after that preparation.
Exercise normal entries/exits, simultaneous sleeve signals, restart recovery,
delayed fills, and unrelated broker positions.

**Done when:** multiple sessions complete with clean reconciliation and no
manual database repair.

## Stage 7 — Operational readiness (offline preparation in progress)

Stage 7 preparation is deliberately local and generic.  It does not add a
dashboard, deployment platform, external alert provider, automatic signal
generation, or a REAL-trading path.  Stage 6's supervised SIM pilot remains a
completion gate for operational readiness; this work may be reviewed before
that pilot, but it must not be treated as evidence that the pilot ran.

Implemented offline in `src/trading_core/operations.py` and
`scripts/generic_operational_cli.py`:

- `OperationalConfig` loads explicit account/book/sleeve/pilot references,
  defaults to `DRY_RUN`, requires `sim_arm: true` for `SIM_ARMED`, accepts SIM
  accounts only, and structurally rejects REAL/LIVE modes and accounts.
- `OperationalStatusReporter` reuses the generic repository/OMS read APIs to
  show account identity, book allocation/exposure/headroom, intents, broker
  orders, fills, open recovery actions, reconciliation blockers, pilot-run
  correlations, and local alerts.  It does not create a second ledger or
  query a broker.
- `OperationalService` provides bounded `start`, `stop`, `once`, and finite
  `run(ticks=...)` operations.  Dry-run ticks are status-only.  SIM-armed
  ticks require an explicitly wired `GenericOMS` and only call its polling
  recovery route; they never generate, submit, cancel, replace, hedge, or
  retry orders.  Lifecycle transitions and recovery/pilot actions persist a
  correlated durable intent event before mutation/action and a terminal event
  afterward; missing terminal audit evidence is surfaced as an audit failure.
  The only dispatch escape hatch is a separately explicit, `confirm_sim=True`
  handoff to the Stage 6 SIM runner.
- `core_operational_events` is an additive durable local audit table.  Events
  have a structured envelope and recursively redact common secret fields both
  in the operations layer and at the repository persistence boundary.
  A book-risk read failure emits a `BOOK_RISK_STATUS_ERROR` critical local
  alert.  `CollectingAlertSink` is a local alert interface/stub; no external
  notification service is configured.

Operator runbook (offline):

```powershell
# Read-only status; dry-run is implicit when no config is supplied.
python scripts/generic_operational_cli.py status `
  --state-db data/generic-sim-smoke.db --account-id <SIM_ACCOUNT> --json

# Start one local dry-run lifecycle and execute one status-only tick.
python scripts/generic_operational_cli.py once `
  --state-db data/generic-sim-smoke.db --account-id <SIM_ACCOUNT> --json
```

An explicit SIM configuration must name the persisted SIM account, enabled
books, sleeve IDs, and pilot references, and must set both
`"mode": "SIM_ARMED"` and `"sim_arm": true`.  The local CLI does not wire a
broker adapter, so a SIM-armed CLI tick stops with a visible recovery-capability
blocker until the separately supervised Stage 6 runner supplies one.  A
reconciliation blocker, unknown/manual broker exposure, or failed recovery
tick is a stop condition: preserve the durable event, inspect the displayed
allowed next steps, and do not manually edit the database or place a legacy
order.  Restart by re-running the read-only status command; the operational
event history remains in the generic state database for the incident record.

There is intentionally no REAL command, config value, or arm path in this
stage.  Stage 7 remains **in progress** pending Stage 6 SIM completion and a
future review of scheduler/deployment/monitoring controls.

**Done when:** after Stage 6 has completed its supervised SIM evidence, the
system can be monitored and safely intervened in before any separately
authorized production design is considered.

## Execution order

Proceed strictly in order: Stage 0, then Stage 1, and so on. Do not begin a
supervised broker stage until the previous stage's tests and review gate have
passed.
