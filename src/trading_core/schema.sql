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
CREATE INDEX IF NOT EXISTS idx_core_snapshots_account
    ON core_broker_snapshots(account_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_core_positions_snapshot
    ON core_position_snapshots(account_id, instrument_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_core_allocations_position
    ON core_position_allocations(account_id, instrument_id, ownership_class);
CREATE INDEX IF NOT EXISTS idx_core_reconciliation_open
    ON core_reconciliation_issues(account_id, status, severity, last_seen_at);

INSERT OR IGNORE INTO core_schema_migrations(version, applied_at, description)
VALUES (1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 'Initial broker-neutral trading core schema');
