# live-trading-system

This repository contains the stat-arbitrage signal runner and a durable,
fail-closed execution layer for Moomoo/OpenD. The execution work is on the
`execution-safety-hardening` branch. The statistical signal, pair selection,
thresholds, and research code are intentionally unchanged.

This is an execution-safety foundation, not a claim that the strategy is
production-ready, risk-free, or suitable for unattended capital.

## Execution architecture

Every entry or exit is a durable `pair_operations` record with two
`operation_legs` and two compatibility `orders` rows. Intent is committed to
SQLite before the first broker call. Each leg then moves through explicit
states such as `created`, `submitting`, `submitted`, `partially_filled`, and
`filled`; a pair becomes `open` (entry) or `closed` (exit) only after both
requested quantities have fill evidence.

The lifecycle is:

```text
signal
  -> execution/data checks
  -> centralized risk admission
  -> persist operation + both leg intents
  -> submit leg 1, persist broker ID
  -> submit leg 2, persist broker ID
  -> apply idempotent cumulative fills
  -> startup/poll reconciliation
  -> create/close the normal position record only when both legs are full
```

`fill_events` stores individual deal IDs (or a deterministic fallback key),
so replayed callbacks do not double-count. A partial leg remains partial with
requested, cumulative, remaining, and average-fill quantities visible in the
database. If leg 1 submits and leg 2 fails, the operation is
`requires_reconciliation`; the runner records the exposure and attempts a
conservative cancellation of leg 1 only when it is not already filled. It does
not invent automatic hedging or pretend the pair is flat.

The pre-change lifecycle and crash points are recorded in
[`docs/execution-audit.md`](/C:/Users/hansg/OneDrive/Desktop/CodingMaster/MooMoo/docs/execution-audit.md).

## SIMULATE and REAL

`SIMULATE` is the default. It uses the Moomoo simulated environment, still
creates the durable ledger, runs reconciliation, and exercises the risk gate.
Use it for all development and fake-broker tests.

REAL is deliberately multi-step and fail-closed. Before considering it, make
sure OpenD is running and manually unlock trading in the OpenD GUI. This code
does not unlock trading through the SDK and never logs passwords or secrets.

Required REAL conditions:

1. Set `FUTU_TRD_ENV=REAL` (or pass `--trd-env REAL`).
2. Set the exact expected account ID in `EXPECTED_REAL_ACCOUNT_ID` (or pass
   `--expected-real-account-id`).
3. Set the separate arming flag `ENABLE_LIVE_TRADING=true` (or pass
   `--enable-live-trading`).
4. Set `FUTU_ACC_ID`/`--acc-id` to the same account ID when an explicit account
   selection is desired; a REAL account is never auto-selected.
5. Use an existing, known state database. REAL mode refuses to create a new
   database at a typo'd path.
6. The broker account identity, current positions/orders, market/session state,
   symbol validity, signal timestamp, and risk/account queries must all pass.
   Any unresolved reconciliation issue blocks new REAL operations.

Example PowerShell setup (replace the placeholder with the verified account
ID; do not commit these values):

```powershell
$env:FUTU_TRD_ENV = "REAL"
$env:EXPECTED_REAL_ACCOUNT_ID = "<verified-account-id>"
$env:FUTU_ACC_ID = "<verified-account-id>"
$env:ENABLE_LIVE_TRADING = "true"
$env:TRADING_STATE_DB = "C:\TradingState\live-trading.db"
python scripts/run_daily_signal.py --no-sleep
```

The startup summary prints the environment, account ID, live-arming state,
gross cap, pair cap, and account/margin caps. `--force-entry` can bypass only
the strategy signal threshold; it cannot bypass environment arming,
idempotency, reconciliation, freshness, symbol/market checks, or risk limits.
The existing daily source is end-of-day data, so the default
`DATA_MAX_AGE_SECONDS=900` will correctly reject a stale REAL signal. Do not
relax that threshold without first validating the timestamp and execution
price source.

## Risk gate

Before either entry leg is submitted, one centralized admission function
checks account equity, buying power, current initial-margin usage, estimated
incremental margin, gross exposure, proposed pair exposure, open-pair count,
pending operations, and unresolved reconciliation. Gross exposure and broker
margin are separate checks. Conservative defaults are:

| Setting | Default | Environment variable |
| --- | ---: | --- |
| Account utilization cap | 25% | `MAX_ACCOUNT_UTILIZATION` |
| Margin utilization cap | 50% | `MAX_MARGIN_UTILIZATION` |
| Gross exposure cap | 50,000 | `MAX_GROSS_EXPOSURE` |
| Pair exposure cap | 10,000 | `MAX_PAIR_EXPOSURE` |
| Open-pair cap | 5 | `MAX_OPEN_PAIRS` |
| Pending-operation/order cap | 5 operations (10 leg intents) | `MAX_PENDING_OPERATIONS` |
| Estimated margin rate | 1.0 | `ESTIMATED_MARGIN_RATE` |
| Data age limit | 900 seconds | `DATA_MAX_AGE_SECONDS` |

SIMULATE reports warnings where a broker field is unavailable when it is safe
to continue; REAL rejects missing required account/risk data.

## Reconciliation and restarts

Reconciliation runs at startup and before every new operation. It retrieves
broker positions, open/recent orders, order statuses, and fills where the
adapter provides them. It correlates orders using the persisted operation
remark/broker ID, then compares broker positions and local open positions.
Discrepancies are kept in `reconciliation_issues`; they are never discarded.
Broker state is authoritative for actual executed exposure. A missing broker
ID, ambiguous order match, partial pair, broker-only position, local-only
position, quantity mismatch, duplicate local position, or failed state query
requires attention. REAL entries and exits remain blocked while an unresolved
issue exists. Resolve an issue only after comparing the broker account and the
ledger manually; the code does not assume that a missing callback means flat.

After a process restart, the operation ID, signal metadata, intended sizes,
broker IDs, and fill evidence are reconstructed from SQLite. A stale runner
lock fails visibly; remove the lock file only after verifying the recorded PID
and timestamp are stale.

The default database is resolved relative to the project root at
`data/trading.db`, not the process launch directory. Set `TRADING_STATE_DB` or
pass `--state-db` for an explicit location. Live SQLite files, WAL/SHM files,
logs, credentials, session transcripts, IDE state, and Python caches are
ignored by Git.

## Tests

The safety tests use deterministic fakes only; they never contact Moomoo or a
real account:

```powershell
python -m pytest -q
```

Before REAL capital is considered, verify OpenD/account permissions, the
broker API version, current market/symbol behavior, state-database backups,
manual reconciliation procedures, and a supervised SIMULATE-to-REAL rollout.

## Supervised SIM two-leg smoke test

[`scripts/sim_smoke_test.py`](scripts/sim_smoke_test.py) is a separate,
strategy-independent harness for proving the OpenD connection and the
existing durable two-leg execution path. It is hard-coded to `SIMULATE`,
requires explicit US symbols and an isolated `--state-db`, and never cancels
or closes positions it did not create. `preflight` and `status` are
non-submitting stages; only `enter` and `exit` accept `--submit`.

Run preflight first. It discovers the account list, selects exactly one active
non-MASTER US margin/`STOCK_AND_OPTION` SIM account, validates symbols, reads
fresh account/position/order state, obtains Moomoo/OpenD reference prices, and
requires the US regular session (`09:30`–`16:00` America/New_York):

```powershell
python scripts/sim_smoke_test.py preflight `
  --state-db data/sim-smoke.db `
  --symbol1 US.AAPL --symbol2 US.MSFT
```

If preflight is ready during regular US hours, use the account ID it prints
explicitly for the mutating stages:

```powershell
python scripts/sim_smoke_test.py enter `
  --state-db data/sim-smoke.db --acc-id <sim-account-id> `
  --symbol1 US.AAPL --symbol2 US.MSFT `
  --quantity1 1 --quantity2 1 --gross-cap 1000 --submit

python scripts/sim_smoke_test.py status `
  --state-db data/sim-smoke.db `
  --symbol1 US.AAPL --symbol2 US.MSFT

python scripts/sim_smoke_test.py exit `
  --state-db data/sim-smoke.db --acc-id <sim-account-id> `
  --symbol1 US.AAPL --symbol2 US.MSFT --submit
```

The entry must reconcile to two broker order IDs, two full fills, and one
local open pair before the exit is attempted. `status` can be run after a
process restart; it refreshes broker positions, orders, and fills and reports
any unresolved reconciliation issue. The exit must end with both legs filled,
the local pair closed, and the target symbols flat at the broker. Existing
account positions or open orders block a new smoke entry and are left alone.

The repository's installed SDK is currently available in the `moomoo`
environment. If SDK logging cannot write its normal user directory on
Windows, redirect it to the ignored workspace directory before running the
smoke harness:

```powershell
$env:APPDATA = (Join-Path (Get-Location) '.tmp_appdata')
$env:PYTHONPATH = 'C:\Users\<user>\AppData\Roaming\Python\Python314\site-packages'
& 'D:\anaconda3\envs\moomoo\python.exe' scripts/sim_smoke_test.py preflight `
  --state-db data/sim-smoke.db --symbol1 US.AAPL --symbol2 US.MSFT
```

Do not run the smoke harness against `data/trading.db`, do not pass
`FUTU_TRD_ENV=REAL`, and do not use SDK trade-unlock calls.
