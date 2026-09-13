-- Durable stat-arb execution ledger.
-- Broker state is authoritative for executed positions; this database stores
-- intent, correlation, and the evidence needed to reconcile it after restart.

CREATE TABLE IF NOT EXISTS positions_stat_arb (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL DEFAULT 'stat_arb',
    strategy_identifier TEXT NOT NULL,
    pair TEXT NOT NULL,
    ticker1 TEXT NOT NULL,
    ticker2 TEXT NOT NULL,
    entry_hedge_ratio REAL NOT NULL,
    entry_alpha REAL NOT NULL,
    entry_residual_mean REAL NOT NULL,
    entry_residual_std REAL NOT NULL,
    entry_zscore REAL NOT NULL,
    executed_size1 REAL NOT NULL,
    executed_size2 REAL NOT NULL,
    intended_size1 REAL NOT NULL,
    intended_size2 REAL NOT NULL,
    entry_side1 TEXT NOT NULL,
    entry_side2 TEXT NOT NULL,
    entry_leg1_price REAL NOT NULL,
    entry_leg2_price REAL NOT NULL,
    entry_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    broker TEXT NOT NULL,
    entry_operation_id TEXT,
    exit_operation_id TEXT,
    environment TEXT NOT NULL DEFAULT 'SIMULATE',
    account_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_positions_stat_arb_status
    ON positions_stat_arb(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_stat_arb_open_pair
    ON positions_stat_arb(pair)
    WHERE status = 'open';

-- A pair operation is the durable two-leg intent/state machine.
CREATE TABLE IF NOT EXISTS pair_operations (
    operation_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    strategy_identifier TEXT NOT NULL,
    pair TEXT NOT NULL,
    operation_type TEXT NOT NULL CHECK (operation_type IN ('entry', 'exit')),
    environment TEXT NOT NULL CHECK (environment IN ('SIMULATE', 'REAL')),
    account_id TEXT,
    ticker1 TEXT NOT NULL,
    ticker2 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'created',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pair_operations_pair_status
    ON pair_operations(pair, status);
CREATE INDEX IF NOT EXISTS idx_pair_operations_active
    ON pair_operations(status)
    WHERE status NOT IN ('closed', 'failed');

CREATE TABLE IF NOT EXISTS operation_legs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL,
    leg TEXT NOT NULL CHECK (leg IN ('ticker1', 'ticker2')),
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    intended_price REAL NOT NULL,
    intended_quantity_raw REAL NOT NULL,
    requested_quantity REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'created',
    broker_order_status TEXT,
    broker_order_id TEXT,
    local_order_id INTEGER,
    position_id INTEGER,
    submitted_at TEXT,
    cumulative_filled_quantity REAL NOT NULL DEFAULT 0,
    remaining_quantity REAL NOT NULL,
    average_fill_price REAL,
    last_fill_at TEXT,
    last_error TEXT,
    FOREIGN KEY (operation_id) REFERENCES pair_operations(operation_id),
    FOREIGN KEY (local_order_id) REFERENCES orders(id),
    FOREIGN KEY (position_id) REFERENCES positions_stat_arb(id),
    UNIQUE (operation_id, leg)
);

CREATE INDEX IF NOT EXISTS idx_operation_legs_broker_order
    ON operation_legs(broker_order_id);
CREATE INDEX IF NOT EXISTS idx_operation_legs_status
    ON operation_legs(status);

-- Each broker deal is applied exactly once. event_key is a broker deal ID
-- where available, or a deterministic fallback fingerprint.
CREATE TABLE IF NOT EXISTS fill_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_leg_id INTEGER NOT NULL,
    broker_order_id TEXT,
    broker_fill_id TEXT,
    event_key TEXT NOT NULL UNIQUE,
    fill_time TEXT NOT NULL,
    quantity REAL NOT NULL CHECK (quantity >= 0),
    price REAL NOT NULL,
    broker_status TEXT,
    raw_payload_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (operation_leg_id) REFERENCES operation_legs(id)
);

CREATE INDEX IF NOT EXISTS idx_fill_events_leg
    ON fill_events(operation_leg_id);

-- Shared broker-facing compatibility order book. New execution code writes a
-- row before submission and mirrors cumulative fill state into these columns.
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    strategy_identifier TEXT NOT NULL,
    position_id INTEGER,
    symbol TEXT NOT NULL,
    leg TEXT NOT NULL,
    leg_type TEXT NOT NULL,
    side TEXT NOT NULL,
    intended_price REAL NOT NULL,
    intended_quantity_raw REAL NOT NULL,
    submitted_quantity REAL NOT NULL,
    submitted_time TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    broker TEXT NOT NULL,
    broker_order_id TEXT,
    fill_price REAL,
    fill_time TEXT,
    fill_quantity REAL,
    slippage REAL,
    operation_id TEXT,
    requested_quantity REAL,
    cumulative_filled_quantity REAL NOT NULL DEFAULT 0,
    remaining_quantity REAL,
    average_fill_price REAL,
    broker_order_status TEXT,
    environment TEXT,
    account_id TEXT,
    idempotency_key TEXT,
    FOREIGN KEY (position_id) REFERENCES positions_stat_arb(id),
    FOREIGN KEY (operation_id) REFERENCES pair_operations(operation_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_strategy_id ON orders(strategy_id);
CREATE INDEX IF NOT EXISTS idx_orders_position_id ON orders(position_id);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);

CREATE TABLE IF NOT EXISTS reconciliation_issues (
    issue_id TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'high',
    entity_key TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reconciliation_issues_open
    ON reconciliation_issues(status, severity);

CREATE TABLE IF NOT EXISTS system_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id INTEGER NOT NULL,
    strategy_id TEXT NOT NULL,
    strategy_identifier TEXT NOT NULL,
    entry_date TEXT NOT NULL,
    exit_date TEXT NOT NULL,
    entry_price1 REAL NOT NULL,
    entry_price2 REAL NOT NULL,
    exit_price1 REAL NOT NULL,
    exit_price2 REAL NOT NULL,
    entry_zscore REAL NOT NULL,
    exit_zscore REAL,
    exit_reason TEXT NOT NULL,
    realized_pnl REAL NOT NULL,
    exit_operation_id TEXT,
    FOREIGN KEY (position_id) REFERENCES positions_stat_arb(id)
);

CREATE INDEX IF NOT EXISTS idx_trades_strategy_id ON trades(strategy_id);
CREATE INDEX IF NOT EXISTS idx_trades_position_id ON trades(position_id);

CREATE TABLE IF NOT EXISTS account_snapshots (
    date TEXT PRIMARY KEY,
    cash REAL NOT NULL,
    equity REAL NOT NULL,
    buying_power REAL NOT NULL
);
