PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS core_schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL,
    description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core_accounts (
    id TEXT PRIMARY KEY,
    broker TEXT NOT NULL,
    environment TEXT NOT NULL CHECK (environment IN ('SIM', 'LIVE')),
    external_account_id TEXT NOT NULL,
    base_currency TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (broker, environment, external_account_id)
);

CREATE TABLE IF NOT EXISTS core_instruments (
    id TEXT PRIMARY KEY,
    asset_class TEXT NOT NULL,
    symbol TEXT NOT NULL,
    venue TEXT NOT NULL,
    currency TEXT NOT NULL,
    multiplier TEXT NOT NULL,
    tick_size TEXT NOT NULL,
    lot_size TEXT NOT NULL,
    expiry TEXT,
    strike TEXT,
    option_right TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (asset_class, symbol, venue, expiry, strike, option_right)
);

CREATE TABLE IF NOT EXISTS core_instrument_mappings (
    id TEXT PRIMARY KEY,
    instrument_id TEXT NOT NULL REFERENCES core_instruments(id) ON DELETE RESTRICT,
    provider TEXT NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('BROKER', 'MARKET_DATA')),
    external_symbol TEXT NOT NULL,
    external_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (provider, purpose, external_symbol),
    UNIQUE (instrument_id, provider, purpose)
);

CREATE TABLE IF NOT EXISTS core_strategies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    strategy_type TEXT NOT NULL,
    version TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    config_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core_books (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core_book_allocations (
    id TEXT PRIMARY KEY,
    book_id TEXT NOT NULL REFERENCES core_books(id) ON DELETE RESTRICT,
    strategy_id TEXT NOT NULL REFERENCES core_strategies(id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    capital_fraction TEXT,
    capital_amount TEXT,
    risk_budget TEXT,
    effective_at TEXT NOT NULL,
    expires_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core_order_intents (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    strategy_id TEXT NOT NULL REFERENCES core_strategies(id) ON DELETE RESTRICT,
    book_id TEXT REFERENCES core_books(id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    source_signal_id TEXT,
    execution_policy_json TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (account_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS core_order_legs (
    id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES core_order_intents(id) ON DELETE RESTRICT,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    instrument_id TEXT NOT NULL REFERENCES core_instruments(id) ON DELETE RESTRICT,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity TEXT NOT NULL,
    quantity_unit TEXT NOT NULL,
    order_type TEXT NOT NULL,
    limit_price TEXT,
    stop_price TEXT,
    time_in_force TEXT,
    status TEXT NOT NULL,
    cumulative_filled_quantity TEXT NOT NULL DEFAULT '0',
    average_fill_price TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (intent_id, sequence)
);

CREATE TABLE IF NOT EXISTS core_broker_orders (
    id TEXT PRIMARY KEY,
    order_leg_id TEXT NOT NULL REFERENCES core_order_legs(id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    broker TEXT NOT NULL,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    external_order_id TEXT,
    client_order_id TEXT,
    status TEXT NOT NULL,
    submitted_quantity TEXT NOT NULL,
    submitted_at TEXT,
    updated_at TEXT NOT NULL,
    replaces_broker_order_id TEXT REFERENCES core_broker_orders(id) ON DELETE RESTRICT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (order_leg_id, attempt_number),
    UNIQUE (account_id, external_order_id),
    UNIQUE (id, order_leg_id)
);

CREATE TABLE IF NOT EXISTS core_broker_order_events (
    id TEXT PRIMARY KEY,
    broker_order_id TEXT NOT NULL REFERENCES core_broker_orders(id) ON DELETE RESTRICT,
    dedupe_key TEXT NOT NULL,
    external_event_id TEXT,
    event_type TEXT NOT NULL,
    broker_status TEXT,
    event_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    oms_fill_fingerprint TEXT,
    oms_event_fingerprint TEXT,
    UNIQUE (broker_order_id, dedupe_key)
);

CREATE TABLE IF NOT EXISTS core_fills (
    id TEXT PRIMARY KEY,
    broker_order_id TEXT NOT NULL REFERENCES core_broker_orders(id) ON DELETE RESTRICT,
    order_leg_id TEXT NOT NULL REFERENCES core_order_legs(id) ON DELETE RESTRICT,
    external_fill_id TEXT,
    dedupe_key TEXT NOT NULL,
    quantity TEXT NOT NULL,
    price TEXT NOT NULL,
    fee TEXT,
    fee_currency TEXT,
    filled_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    evidence_mode TEXT NOT NULL DEFAULT 'INDIVIDUAL_DEALS',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (broker_order_id, dedupe_key),
    FOREIGN KEY (broker_order_id, order_leg_id)
        REFERENCES core_broker_orders(id, order_leg_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS core_broker_snapshots (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    captured_at TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS core_account_balance_snapshots (
    id TEXT PRIMARY KEY,
    broker_snapshot_id TEXT NOT NULL REFERENCES core_broker_snapshots(id) ON DELETE RESTRICT,
    currency TEXT NOT NULL,
    cash TEXT,
    buying_power TEXT,
    equity TEXT,
    initial_margin TEXT,
    maintenance_margin TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS core_position_snapshots (
    id TEXT PRIMARY KEY,
    broker_snapshot_id TEXT NOT NULL REFERENCES core_broker_snapshots(id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    instrument_id TEXT NOT NULL REFERENCES core_instruments(id) ON DELETE RESTRICT,
    signed_quantity TEXT NOT NULL,
    average_price TEXT,
    captured_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS core_broker_order_snapshots (
    id TEXT PRIMARY KEY,
    broker_snapshot_id TEXT NOT NULL REFERENCES core_broker_snapshots(id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    instrument_id TEXT NOT NULL REFERENCES core_instruments(id) ON DELETE RESTRICT,
    external_order_id TEXT NOT NULL,
    client_order_id TEXT,
    side TEXT NOT NULL,
    quantity TEXT NOT NULL,
    filled_quantity TEXT NOT NULL,
    status TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS core_position_allocations (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    instrument_id TEXT NOT NULL REFERENCES core_instruments(id) ON DELETE RESTRICT,
    strategy_id TEXT REFERENCES core_strategies(id) ON DELETE RESTRICT,
    book_id TEXT REFERENCES core_books(id) ON DELETE RESTRICT,
    ownership_class TEXT NOT NULL CHECK (ownership_class IN ('MANAGED', 'EXTERNAL', 'UNKNOWN')),
    signed_quantity TEXT NOT NULL,
    source_intent_id TEXT REFERENCES core_order_intents(id) ON DELETE RESTRICT,
    updated_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS core_risk_decisions (
    id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES core_order_intents(id) ON DELETE RESTRICT,
    approved INTEGER NOT NULL CHECK (approved IN (0, 1)),
    reason TEXT NOT NULL,
    checks_json TEXT NOT NULL DEFAULT '{}',
    evaluated_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS core_reconciliation_runs (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    broker_snapshot_id TEXT REFERENCES core_broker_snapshots(id) ON DELETE RESTRICT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS core_reconciliation_issues (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES core_reconciliation_runs(id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    issue_key TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    category TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    sticky INTEGER NOT NULL DEFAULT 1 CHECK (sticky IN (0, 1)),
    details_json TEXT NOT NULL DEFAULT '{}',
    detected_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    resolved_at TEXT,
    UNIQUE (account_id, issue_key)
);

CREATE TABLE IF NOT EXISTS core_recovery_actions (
    id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL REFERENCES core_order_intents(id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    action_key TEXT NOT NULL,
    state TEXT NOT NULL,
    summary TEXT NOT NULL,
    observed_positions_json TEXT NOT NULL DEFAULT '{}',
    remaining_quantities_json TEXT NOT NULL DEFAULT '{}',
    stale INTEGER NOT NULL DEFAULT 0 CHECK (stale IN (0, 1)),
    timed_out INTEGER NOT NULL DEFAULT 0 CHECK (timed_out IN (0, 1)),
    allowed_next_steps_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    resolved_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (account_id, intent_id, action_key)
);

-- Stage 7 local operational audit trail.  This is deliberately separate from
-- the trading ledger: it records operator/service decisions and never stores
-- credentials or provider secrets.  Existing databases receive this table
-- through the normal idempotent schema initialization path.
CREATE TABLE IF NOT EXISTS core_operational_events (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL,
    mode TEXT NOT NULL,
    outcome TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    summary TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}'
);

-- A verified, bounded account-facts checkpoint.  This is not a claim that
-- the provider exposes complete historical deal data; it records the exact
-- fresh position/order observation and the order-evidence coverage that an
-- operator explicitly reconciled before a SIM submission.
CREATE TABLE IF NOT EXISTS core_execution_evidence_baselines (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    captured_at TEXT NOT NULL,
    evidence_mode TEXT NOT NULL,
    coverage_json TEXT NOT NULL DEFAULT '[]',
    source_ledger_fingerprint TEXT NOT NULL,
    source_order_ids_json TEXT NOT NULL DEFAULT '[]',
    position_fingerprint TEXT NOT NULL,
    open_order_fingerprint TEXT NOT NULL,
    verified_flat INTEGER NOT NULL CHECK (verified_flat IN (0, 1)),
    status TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (account_id, source_ledger_fingerprint)
);

-- Stage 6 validation evidence is separate from the execution ledger.  The
-- phase rows retain the operator's preflight/recovery/final observations;
-- the session row is an immutable deterministic result derived from those
-- observations.  Neither table creates an execution or broker-order path.
CREATE TABLE IF NOT EXISTS core_stage6_validation_observations (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    phase TEXT NOT NULL CHECK (phase IN ('PREFLIGHT', 'RECOVERY', 'FINAL')),
    captured_at TEXT NOT NULL,
    process_id TEXT,
    fresh_process INTEGER NOT NULL DEFAULT 0 CHECK (fresh_process IN (0, 1)),
    evidence_json TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (id, evidence_hash)
);

CREATE TABLE IF NOT EXISTS core_stage6_validation_sessions (
    session_id TEXT PRIMARY KEY,
    us_trading_date TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    execution_compatibility TEXT NOT NULL,
    account_id TEXT NOT NULL REFERENCES core_accounts(id) ON DELETE RESTRICT,
    environment TEXT NOT NULL CHECK (environment = 'SIM'),
    result TEXT NOT NULL CHECK (result IN ('CLEAN_PASS', 'FAILED', 'INVALID')),
    qualified INTEGER NOT NULL CHECK (qualified IN (0, 1)),
    counted_for_completion INTEGER NOT NULL CHECK (counted_for_completion IN (0, 1)),
    evidence_class TEXT NOT NULL CHECK (evidence_class IN ('DURABLE', 'LEGACY_VERIFIED_EVIDENCE')),
    expected_entry_order_count INTEGER NOT NULL CHECK (expected_entry_order_count >= 0),
    actual_entry_order_count INTEGER NOT NULL CHECK (actual_entry_order_count >= 0),
    expected_exit_order_count INTEGER NOT NULL CHECK (expected_exit_order_count >= 0),
    actual_exit_order_count INTEGER NOT NULL CHECK (actual_exit_order_count >= 0),
    duplicate_attempt_count INTEGER NOT NULL CHECK (duplicate_attempt_count >= 0),
    run_ids_json TEXT NOT NULL DEFAULT '[]',
    entry_intent_ids_json TEXT NOT NULL DEFAULT '[]',
    exit_intent_ids_json TEXT NOT NULL DEFAULT '[]',
    failure_reasons_json TEXT NOT NULL DEFAULT '[]',
    audit_refs_json TEXT NOT NULL DEFAULT '[]',
    preflight_json TEXT NOT NULL DEFAULT '{}',
    recovery_json TEXT NOT NULL DEFAULT '{}',
    final_json TEXT NOT NULL DEFAULT '{}',
    evidence_json TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- A US trading date can contribute at most one clean session to the Stage 6
-- completion series.  Failed/invalid/legacy rows remain fully retained.
CREATE UNIQUE INDEX IF NOT EXISTS uq_core_stage6_clean_date
    ON core_stage6_validation_sessions(us_trading_date)
    WHERE result = 'CLEAN_PASS' AND qualified = 1;

CREATE INDEX IF NOT EXISTS idx_core_intents_status
    ON core_order_intents(account_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_core_legs_status
    ON core_order_legs(intent_id, status, sequence);
CREATE INDEX IF NOT EXISTS idx_core_broker_orders_leg
    ON core_broker_orders(order_leg_id, attempt_number);
CREATE INDEX IF NOT EXISTS idx_core_broker_orders_client
    ON core_broker_orders(account_id, client_order_id);
CREATE INDEX IF NOT EXISTS idx_core_events_received
    ON core_broker_order_events(broker_order_id, received_at);
CREATE INDEX IF NOT EXISTS idx_core_fills_leg
    ON core_fills(order_leg_id, filled_at);
-- Additive lookup for the second fill identity.  Enforcement remains in the
-- repository so pre-Stage-3 databases containing historical duplicates can
-- initialize safely without a destructive migration.
CREATE INDEX IF NOT EXISTS idx_core_fills_broker_external_fill
    ON core_fills(broker_order_id, external_fill_id);
CREATE INDEX IF NOT EXISTS idx_core_snapshots_account
    ON core_broker_snapshots(account_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_core_positions_snapshot
    ON core_position_snapshots(account_id, instrument_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_core_allocations_position
    ON core_position_allocations(account_id, instrument_id, ownership_class);
CREATE INDEX IF NOT EXISTS idx_core_reconciliation_open
    ON core_reconciliation_issues(account_id, status, severity, last_seen_at);
CREATE INDEX IF NOT EXISTS idx_core_recovery_actions_open
    ON core_recovery_actions(account_id, status, last_seen_at);
CREATE INDEX IF NOT EXISTS idx_core_operational_events_account
    ON core_operational_events(account_id, occurred_at, id);
CREATE INDEX IF NOT EXISTS idx_core_execution_baselines_account
    ON core_execution_evidence_baselines(account_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_core_stage6_validation_observations_session
    ON core_stage6_validation_observations(session_id, captured_at, id);
CREATE INDEX IF NOT EXISTS idx_core_stage6_validation_sessions_account
    ON core_stage6_validation_sessions(account_id, us_trading_date, completed_at);

INSERT OR IGNORE INTO core_schema_migrations(version, applied_at, description)
VALUES (1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'Initial broker-neutral trading core schema');

INSERT OR IGNORE INTO core_schema_migrations(version, applied_at, description)
VALUES (2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'Durable generic recovery actions');

INSERT OR IGNORE INTO core_schema_migrations(version, applied_at, description)
VALUES (3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'Dedicated broker-event evidence fingerprints');

INSERT OR IGNORE INTO core_schema_migrations(version, applied_at, description)
VALUES (9, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'Durable Stage 6 supervised SIM validation evidence');
