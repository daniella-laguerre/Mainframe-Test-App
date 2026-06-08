-- ============================================================
-- Financial Trading Platform - Database Schema
-- ============================================================

-- Instruments / Securities
CREATE TABLE instruments (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    isin VARCHAR(12),
    cusip VARCHAR(9),
    name VARCHAR(200),
    asset_class VARCHAR(50) NOT NULL, -- equity, fx, crypto, derivatives, fixed_income
    exchange VARCHAR(50),
    currency VARCHAR(3) DEFAULT 'USD',
    lot_size DECIMAL(18,8) DEFAULT 1,
    tick_size DECIMAL(18,8) DEFAULT 0.01,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Clients / Accounts
CREATE TABLE clients (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200),
    account_type VARCHAR(50), -- institutional, retail, proprietary, market_maker
    tier VARCHAR(20), -- platinum, gold, silver, bronze
    region VARCHAR(50),
    risk_limit DECIMAL(18,2),
    margin_requirement DECIMAL(5,4) DEFAULT 0.1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Orders
CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    parent_order_id VARCHAR(50),
    client_id VARCHAR(50) NOT NULL,
    instrument_id INTEGER REFERENCES instruments(id),
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(4) NOT NULL, -- buy, sell
    order_type VARCHAR(20) NOT NULL, -- market, limit, stop, stop_limit, ioc, fok
    quantity DECIMAL(18,8) NOT NULL,
    limit_price DECIMAL(18,8),
    stop_price DECIMAL(18,8),
    filled_quantity DECIMAL(18,8) DEFAULT 0,
    avg_fill_price DECIMAL(18,8),
    status VARCHAR(20) DEFAULT 'new', -- new, partially_filled, filled, cancelled, rejected, expired
    time_in_force VARCHAR(10) DEFAULT 'day', -- day, gtc, ioc, fok, gtd
    venue VARCHAR(50), -- NYSE, NASDAQ, CME, dark_pool, internal_crossing
    algo_strategy VARCHAR(50), -- VWAP, TWAP, POV, Sniper, Iceberg, DMA
    broker VARCHAR(50),
    counterparty VARCHAR(50),
    slippage_bps DECIMAL(10,4),
    commission DECIMAL(18,4),
    fees DECIMAL(18,4),
    reject_reason TEXT,
    fix_cl_ord_id VARCHAR(50),
    fix_orig_cl_ord_id VARCHAR(50),
    correlation_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    submitted_at TIMESTAMP,
    acknowledged_at TIMESTAMP,
    first_fill_at TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX idx_orders_client ON orders(client_id);
CREATE INDEX idx_orders_symbol ON orders(symbol);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created ON orders(created_at);
CREATE INDEX idx_orders_parent ON orders(parent_order_id);

-- Trades / Executions
CREATE TABLE trades (
    id BIGSERIAL PRIMARY KEY,
    trade_id VARCHAR(50) NOT NULL UNIQUE,
    order_id VARCHAR(50) NOT NULL,
    client_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(4) NOT NULL,
    quantity DECIMAL(18,8) NOT NULL,
    price DECIMAL(18,8) NOT NULL,
    execution_venue VARCHAR(50),
    liquidity_flag VARCHAR(10), -- maker, taker, auction
    commission DECIMAL(18,4),
    fees DECIMAL(18,4),
    clearing_house VARCHAR(50),
    settlement_date DATE,
    slippage_bps DECIMAL(10,4),
    market_impact_bps DECIMAL(10,4),
    arrival_price DECIMAL(18,8),
    benchmark_price DECIMAL(18,8),
    is_reported BOOLEAN DEFAULT FALSE,
    fix_exec_id VARCHAR(50),
    correlation_id VARCHAR(50),
    executed_at TIMESTAMP DEFAULT NOW(),
    -- nanosecond precision stored as bigint
    executed_at_nanos BIGINT
);

CREATE INDEX idx_trades_order ON trades(order_id);
CREATE INDEX idx_trades_client ON trades(client_id);
CREATE INDEX idx_trades_symbol ON trades(symbol);
CREATE INDEX idx_trades_executed ON trades(executed_at);

-- Positions
CREATE TABLE positions (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    quantity DECIMAL(18,8) DEFAULT 0,
    avg_cost DECIMAL(18,8) DEFAULT 0,
    market_value DECIMAL(18,2) DEFAULT 0,
    unrealized_pnl DECIMAL(18,2) DEFAULT 0,
    realized_pnl DECIMAL(18,2) DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(client_id, symbol)
);

CREATE INDEX idx_positions_client ON positions(client_id);

-- Risk Snapshots
CREATE TABLE risk_snapshots (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(50),
    symbol VARCHAR(20),
    asset_class VARCHAR(50),
    var_95 DECIMAL(18,4),
    var_99 DECIMAL(18,4),
    expected_shortfall DECIMAL(18,4),
    delta DECIMAL(18,8),
    gamma DECIMAL(18,8),
    vega DECIMAL(18,8),
    theta DECIMAL(18,8),
    rho DECIMAL(18,8),
    exposure DECIMAL(18,2),
    concentration_pct DECIMAL(5,4),
    margin_used DECIMAL(18,2),
    margin_available DECIMAL(18,2),
    counterparty_exposure DECIMAL(18,2),
    snapshot_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_risk_client ON risk_snapshots(client_id);
CREATE INDEX idx_risk_snapshot ON risk_snapshots(snapshot_at);

-- Order Audit Trail (every state transition)
CREATE TABLE order_audit_trail (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL, -- new, acknowledged, partial_fill, fill, cancel, reject, amend
    previous_status VARCHAR(20),
    new_status VARCHAR(20),
    quantity DECIMAL(18,8),
    price DECIMAL(18,8),
    venue VARCHAR(50),
    fix_message TEXT,
    regulatory_timestamp TIMESTAMP, -- MiFID II / SEC Rule 613 CAT
    source_system VARCHAR(50),
    operator_id VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_audit_order ON order_audit_trail(order_id);
CREATE INDEX idx_audit_created ON order_audit_trail(created_at);

-- Compliance Alerts
CREATE TABLE compliance_alerts (
    id BIGSERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL, -- spoofing, layering, wash_trade, fat_finger, concentration
    severity VARCHAR(20) NOT NULL, -- critical, high, medium, low
    client_id VARCHAR(50),
    order_id VARCHAR(50),
    symbol VARCHAR(20),
    description TEXT,
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_by VARCHAR(50),
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Market Data Snapshots (for analytics)
CREATE TABLE market_data_snapshots (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    bid_price DECIMAL(18,8),
    ask_price DECIMAL(18,8),
    bid_size DECIMAL(18,8),
    ask_size DECIMAL(18,8),
    last_price DECIMAL(18,8),
    last_size DECIMAL(18,8),
    vwap DECIMAL(18,8),
    twap DECIMAL(18,8),
    volume DECIMAL(18,2),
    open_price DECIMAL(18,8),
    high_price DECIMAL(18,8),
    low_price DECIMAL(18,8),
    spread_bps DECIMAL(10,4),
    realized_vol DECIMAL(10,6),
    implied_vol DECIMAL(10,6),
    book_imbalance DECIMAL(10,6),
    quote_updates_per_sec INTEGER,
    snapshot_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_mktdata_symbol ON market_data_snapshots(symbol);
CREATE INDEX idx_mktdata_snapshot ON market_data_snapshots(snapshot_at);

-- Batch Job Runs
CREATE TABLE batch_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50), -- reconciliation, eod_risk, settlement, etl
    status VARCHAR(20) DEFAULT 'running', -- running, completed, failed, partial
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    mismatches_found INTEGER DEFAULT 0,
    missing_trades INTEGER DEFAULT 0,
    settlement_mismatches INTEGER DEFAULT 0,
    data_drift_detected BOOLEAN DEFAULT FALSE,
    duration_seconds DECIMAL(10,2),
    error_message TEXT,
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

-- Business KPIs (aggregated)
CREATE TABLE business_kpis (
    id BIGSERIAL PRIMARY KEY,
    period_start TIMESTAMP NOT NULL,
    period_end TIMESTAMP NOT NULL,
    total_volume DECIMAL(18,2),
    total_trades INTEGER,
    total_orders INTEGER,
    fill_rate DECIMAL(5,4),
    avg_slippage_bps DECIMAL(10,4),
    reject_rate DECIMAL(5,4),
    cancel_replace_ratio DECIMAL(5,4),
    total_commission DECIMAL(18,2),
    total_pnl DECIMAL(18,2),
    revenue_per_minute DECIMAL(18,4),
    cost_per_trade DECIMAL(18,4),
    system_availability DECIMAL(5,4),
    avg_latency_ms DECIMAL(10,2),
    p99_latency_ms DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Seed instruments
INSERT INTO instruments (symbol, isin, cusip, name, asset_class, exchange, currency, tick_size) VALUES
('AAPL', 'US0378331005', '037833100', 'Apple Inc.', 'equity', 'NASDAQ', 'USD', 0.01),
('MSFT', 'US5949181045', '594918104', 'Microsoft Corp.', 'equity', 'NASDAQ', 'USD', 0.01),
('GOOGL', 'US02079K3059', '02079K305', 'Alphabet Inc.', 'equity', 'NASDAQ', 'USD', 0.01),
('AMZN', 'US0231351067', '023135106', 'Amazon.com Inc.', 'equity', 'NASDAQ', 'USD', 0.01),
('TSLA', 'US88160R1014', '88160R101', 'Tesla Inc.', 'equity', 'NASDAQ', 'USD', 0.01),
('JPM', 'US46625H1005', '46625H100', 'JPMorgan Chase', 'equity', 'NYSE', 'USD', 0.01),
('GS', 'US38141G1040', '38141G104', 'Goldman Sachs', 'equity', 'NYSE', 'USD', 0.01),
('MS', 'US6174464486', '617446448', 'Morgan Stanley', 'equity', 'NYSE', 'USD', 0.01),
('BAC', 'US0605051046', '060505104', 'Bank of America', 'equity', 'NYSE', 'USD', 0.01),
('C', 'US1729674242', '172967424', 'Citigroup Inc.', 'equity', 'NYSE', 'USD', 0.01),
('NVDA', 'US67066G1040', '67066G104', 'NVIDIA Corp.', 'equity', 'NASDAQ', 'USD', 0.01),
('META', 'US30303M1027', '30303M102', 'Meta Platforms', 'equity', 'NASDAQ', 'USD', 0.01),
('BRK.B', 'US0846707026', '084670702', 'Berkshire Hathaway B', 'equity', 'NYSE', 'USD', 0.01),
('V', 'US92826C8394', '92826C839', 'Visa Inc.', 'equity', 'NYSE', 'USD', 0.01),
('WMT', 'US9311421039', '931142103', 'Walmart Inc.', 'equity', 'NYSE', 'USD', 0.01),
('EUR/USD', NULL, NULL, 'Euro/US Dollar', 'fx', 'CME', 'USD', 0.0001),
('GBP/USD', NULL, NULL, 'British Pound/US Dollar', 'fx', 'CME', 'USD', 0.0001),
('USD/JPY', NULL, NULL, 'US Dollar/Japanese Yen', 'fx', 'CME', 'JPY', 0.01),
('BTC-USD', NULL, NULL, 'Bitcoin/US Dollar', 'crypto', 'COINBASE', 'USD', 0.01),
('ETH-USD', NULL, NULL, 'Ethereum/US Dollar', 'crypto', 'COINBASE', 'USD', 0.01),
('ES', NULL, NULL, 'E-mini S&P 500 Future', 'derivatives', 'CME', 'USD', 0.25),
('NQ', NULL, NULL, 'E-mini NASDAQ 100 Future', 'derivatives', 'CME', 'USD', 0.25),
('CL', NULL, NULL, 'Crude Oil Future', 'derivatives', 'NYMEX', 'USD', 0.01),
('GC', NULL, NULL, 'Gold Future', 'derivatives', 'COMEX', 'USD', 0.10),
('SPY', 'US78462F1030', '78462F103', 'SPDR S&P 500 ETF', 'equity', 'NYSE', 'USD', 0.01);

-- Seed clients
INSERT INTO clients (client_id, name, account_type, tier, region, risk_limit, margin_requirement) VALUES
('INST-001', 'Bridgewater Associates', 'institutional', 'platinum', 'US-EAST', 500000000, 0.05),
('INST-002', 'Renaissance Technologies', 'institutional', 'platinum', 'US-EAST', 1000000000, 0.05),
('INST-003', 'Citadel Securities', 'market_maker', 'platinum', 'US-EAST', 2000000000, 0.03),
('INST-004', 'Two Sigma Investments', 'institutional', 'platinum', 'US-EAST', 750000000, 0.05),
('INST-005', 'DE Shaw & Co', 'institutional', 'gold', 'US-EAST', 300000000, 0.08),
('INST-006', 'Man Group', 'institutional', 'gold', 'EMEA', 200000000, 0.08),
('INST-007', 'Millennium Management', 'institutional', 'gold', 'US-EAST', 400000000, 0.08),
('INST-008', 'Point72 Asset Management', 'institutional', 'gold', 'US-EAST', 250000000, 0.08),
('HF-001', 'Alpha Capital Partners', 'institutional', 'silver', 'US-WEST', 50000000, 0.10),
('HF-002', 'Quantum Trading LLC', 'institutional', 'silver', 'APAC', 30000000, 0.10),
('HF-003', 'Nordic Arbitrage Fund', 'institutional', 'silver', 'EMEA', 25000000, 0.10),
('PROP-001', 'Internal Prop Desk A', 'proprietary', 'platinum', 'US-EAST', 100000000, 0.05),
('PROP-002', 'Internal Prop Desk B', 'proprietary', 'gold', 'EMEA', 50000000, 0.05),
('RET-001', 'John Smith', 'retail', 'bronze', 'US-EAST', 100000, 0.25),
('RET-002', 'Jane Doe', 'retail', 'bronze', 'US-WEST', 250000, 0.25),
('RET-003', 'Bob Johnson', 'retail', 'silver', 'EMEA', 500000, 0.15),
('MM-001', 'Virtu Financial', 'market_maker', 'platinum', 'US-EAST', 3000000000, 0.02),
('MM-002', 'Flow Traders', 'market_maker', 'gold', 'EMEA', 500000000, 0.03),
('ALGO-001', 'Systematic Alpha Fund', 'institutional', 'gold', 'APAC', 150000000, 0.08),
('ALGO-002', 'High Frequency Strategies', 'institutional', 'platinum', 'US-EAST', 800000000, 0.05);
