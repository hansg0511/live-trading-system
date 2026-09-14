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

## Stage 0 — Consolidate the baseline

Carry the smoke-tested commit `ab47c82` onto `generic-trading-core`. Update
the generic-core documentation so it distinguishes the original legacy
baseline from the current smoke-passed revision. Keep the legacy smoke test
as a separate regression artifact.

**Done when:** the generic branch is clean, its full test suite passes, and
the documented baseline matches the code. This stage makes no broker calls.

## Stage 1 — Build the generic Moomoo bridge

Implement a Moomoo adapter for the interfaces in
`src/trading_core/ports.py`. The adapter must translate generic accounts,
instruments, snapshots, order attempts, fills, cancellations, replacements,
and broker errors to and from OpenD. Reuse the proven account-safety and
rate-limit behavior, without allowing the generic core to import legacy
pair-specific code.

**Done when:** fake-broker and adapter-contract tests pass. This stage is
SIM-only and submits no broker orders.

## Stage 2 — Generic supervised SIM smoke

Create a separate generic-OMS smoke harness with its own isolated state
database and preflight. It will submit one deliberately small generic
two-leg intent, persist attempts, fills, and allocations, exit the position,
and reconcile the broker flat.

**Done when:** the generic OMS, rather than the legacy engine, completes a
supervised SIM round trip. This stage requires US market hours and explicit
operator approval before any SIM orders are submitted.

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
