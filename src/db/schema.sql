-- Stat-arb positions: never deleted, marked closed via status + linked to trades via FK
CREATE TABLE IF NOT EXISTS positions_stat_arb (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL DEFAULT 'stat_arb',
    strategy_identifier TEXT NOT NULL,     -- pair name / strategy-local identifier
    pair TEXT NOT NULL,                    -- e.g. "AAPL-MSFT"
    ticker1 TEXT NOT NULL,
    ticker2 TEXT NOT NULL,
    entry_hedge_ratio REAL NOT NULL,
    entry_alpha REAL NOT NULL,
    entry_residual_mean REAL NOT NULL,
    entry_residual_std REAL NOT NULL,
    entry_zscore REAL NOT NULL,
    executed_size1 REAL NOT NULL,          -- rounded integer shares filled
    executed_size2 REAL NOT NULL,          -- rounded integer shares filled
    intended_size1 REAL NOT NULL,          -- fractional intended size before rounding
    intended_size2 REAL NOT NULL,          -- fractional intended size before rounding
    entry_side1 TEXT NOT NULL,             -- BUY | SELL
    entry_side2 TEXT NOT NULL,             -- BUY | SELL
    entry_leg1_price REAL NOT NULL,
    entry_leg2_price REAL NOT NULL,
    entry_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',   -- 'open' | 'closed'
    broker TEXT NOT NULL                   -- 'moomoo' | 'interactive_brokers' | etc.
);

CREATE INDEX IF NOT EXISTS idx_positions_stat_arb_status
    ON positions_stat_arb(status);

CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_stat_arb_open_pair
    ON positions_stat_arb(pair)
    WHERE status = 'open';   -- enforces: a pair can only have ONE open position at a time

-- Shared broker-facing order book across all strategies
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    strategy_identifier TEXT NOT NULL,     -- pair / underlying / strategy-local identifier
    position_id INTEGER,
    symbol TEXT NOT NULL,
    leg TEXT NOT NULL,                     -- 'ticker1' / 'ticker2' / single-leg symbol
    leg_type TEXT NOT NULL,                -- 'entry' | 'exit'
    side TEXT NOT NULL,                    -- 'BUY' | 'SELL'
    intended_price REAL NOT NULL,
    intended_quantity_raw REAL NOT NULL,
    submitted_quantity REAL NOT NULL,
    submitted_time TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending' | 'submitted' | 'filled' | 'cancelled' | 'rejected'
    broker TEXT NOT NULL,                  -- 'moomoo' | 'interactive_brokers' | etc.
    broker_order_id TEXT,
    fill_price REAL,
    fill_time TEXT,
    fill_quantity REAL,
    slippage REAL,
    FOREIGN KEY (position_id) REFERENCES positions_stat_arb(id)
);

CREATE INDEX IF NOT EXISTS idx_orders_strategy_id ON orders(strategy_id);
CREATE INDEX IF NOT EXISTS idx_orders_position_id ON orders(position_id);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);

-- Shared closed-trade log across all strategies
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
    exit_reason TEXT NOT NULL,             -- 'z_exit' | 'z_stop' | 'max_hold'
    realized_pnl REAL NOT NULL,
    FOREIGN KEY (position_id) REFERENCES positions_stat_arb(id)
);

CREATE INDEX IF NOT EXISTS idx_trades_strategy_id ON trades(strategy_id);
CREATE INDEX IF NOT EXISTS idx_trades_position_id ON trades(position_id);

-- Daily equity curve, account-level (no strategy_id)
CREATE TABLE IF NOT EXISTS account_snapshots (
    date TEXT PRIMARY KEY,
    cash REAL NOT NULL,
    equity REAL NOT NULL,
    buying_power REAL NOT NULL
);
