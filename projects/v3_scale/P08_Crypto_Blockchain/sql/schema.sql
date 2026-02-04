-- P08 Crypto & Blockchain Analytics - Star Schema DDL
-- Author: Mboya Jeffers
-- Database: PostgreSQL
-- Pattern: Kimball Dimensional Model

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Crypto Asset Dimension
CREATE TABLE IF NOT EXISTS dim_asset (
    asset_key           VARCHAR(16) PRIMARY KEY,
    asset_id            VARCHAR(100) NOT NULL UNIQUE,  -- CoinGecko ID
    symbol              VARCHAR(20) NOT NULL,
    name                VARCHAR(255) NOT NULL,
    category            VARCHAR(50),  -- cryptocurrency, stablecoin, defi, etc.
    platform            VARCHAR(100),  -- Native blockchain
    contract_address    VARCHAR(255),  -- For tokens
    genesis_date        DATE,
    description         TEXT,
    market_cap_rank     INTEGER,
    coingecko_rank      INTEGER,
    is_active           BOOLEAN DEFAULT TRUE,
    effective_date      DATE NOT NULL,
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_asset_id ON dim_asset(asset_id);
CREATE INDEX idx_dim_asset_symbol ON dim_asset(symbol);
CREATE INDEX idx_dim_asset_category ON dim_asset(category);
CREATE INDEX idx_dim_asset_rank ON dim_asset(market_cap_rank);

-- Exchange Dimension
CREATE TABLE IF NOT EXISTS dim_exchange (
    exchange_key        VARCHAR(16) PRIMARY KEY,
    exchange_id         VARCHAR(100) NOT NULL UNIQUE,
    exchange_name       VARCHAR(255) NOT NULL,
    country             VARCHAR(100),
    year_established    INTEGER,
    trust_score         INTEGER,  -- CoinGecko trust score
    trust_score_rank    INTEGER,
    trade_volume_24h_btc DECIMAL(20,8),
    url                 VARCHAR(500),
    is_centralized      BOOLEAN DEFAULT TRUE,
    has_trading_incentive BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_exchange_id ON dim_exchange(exchange_id);
CREATE INDEX idx_dim_exchange_name ON dim_exchange(exchange_name);
CREATE INDEX idx_dim_exchange_rank ON dim_exchange(trust_score_rank);

-- Blockchain/Chain Dimension
CREATE TABLE IF NOT EXISTS dim_chain (
    chain_key           VARCHAR(16) PRIMARY KEY,
    chain_id            VARCHAR(50) NOT NULL UNIQUE,
    chain_name          VARCHAR(100) NOT NULL,
    native_asset        VARCHAR(20),
    consensus_mechanism VARCHAR(50),  -- PoW, PoS, DPoS, etc.
    block_time_seconds  INTEGER,
    tps_capacity        INTEGER,  -- Transactions per second
    launch_date         DATE,
    is_evm_compatible   BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_chain_id ON dim_chain(chain_id);
CREATE INDEX idx_dim_chain_name ON dim_chain(chain_name);

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key            VARCHAR(16) PRIMARY KEY,
    full_date           DATE NOT NULL UNIQUE,
    year                INTEGER NOT NULL,
    month               INTEGER NOT NULL,
    day                 INTEGER NOT NULL,
    quarter             INTEGER NOT NULL,
    week_of_year        INTEGER NOT NULL,
    day_of_week         INTEGER NOT NULL,
    day_name            VARCHAR(10) NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_full ON dim_date(full_date);
CREATE INDEX idx_dim_date_year ON dim_date(year);

-- ============================================
-- FACT TABLES
-- ============================================

-- Price/Market Data Fact Table
CREATE TABLE IF NOT EXISTS fact_prices (
    price_key           VARCHAR(32) PRIMARY KEY,
    asset_key           VARCHAR(16) NOT NULL REFERENCES dim_asset(asset_key),
    date_key            VARCHAR(16) NOT NULL REFERENCES dim_date(date_key),
    asset_id            VARCHAR(100) NOT NULL,

    -- Price metrics (in USD)
    price_usd           DECIMAL(30,10),
    price_btc           DECIMAL(30,18),
    price_eth           DECIMAL(30,18),

    -- OHLC (daily)
    open_price          DECIMAL(30,10),
    high_price          DECIMAL(30,10),
    low_price           DECIMAL(30,10),
    close_price         DECIMAL(30,10),

    -- Volume
    volume_24h          DECIMAL(30,2),
    volume_change_24h   DECIMAL(10,2),

    -- Market metrics
    market_cap          DECIMAL(30,2),
    market_cap_rank     INTEGER,
    fully_diluted_valuation DECIMAL(30,2),

    -- Supply metrics
    circulating_supply  DECIMAL(30,2),
    total_supply        DECIMAL(30,2),
    max_supply          DECIMAL(30,2),

    -- Price changes
    price_change_24h    DECIMAL(30,10),
    price_change_pct_24h DECIMAL(10,4),
    price_change_pct_7d  DECIMAL(10,4),
    price_change_pct_30d DECIMAL(10,4),

    -- Volatility
    volatility_30d      DECIMAL(10,4),
    volatility_90d      DECIMAL(10,4),

    -- All-time metrics
    ath_price           DECIMAL(30,10),
    ath_date            DATE,
    ath_change_pct      DECIMAL(10,4),
    atl_price           DECIMAL(30,10),
    atl_date            DATE,
    atl_change_pct      DECIMAL(10,4),

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_prices_asset ON fact_prices(asset_key);
CREATE INDEX idx_fact_prices_date ON fact_prices(date_key);
CREATE INDEX idx_fact_prices_market_cap ON fact_prices(market_cap DESC);
CREATE INDEX idx_fact_prices_volume ON fact_prices(volume_24h DESC);

-- Historical OHLCV Fact Table (for time series)
CREATE TABLE IF NOT EXISTS fact_ohlcv (
    ohlcv_key           VARCHAR(32) PRIMARY KEY,
    asset_key           VARCHAR(16) NOT NULL REFERENCES dim_asset(asset_key),
    date_key            VARCHAR(16) NOT NULL REFERENCES dim_date(date_key),

    -- OHLCV data
    open_price          DECIMAL(30,10) NOT NULL,
    high_price          DECIMAL(30,10) NOT NULL,
    low_price           DECIMAL(30,10) NOT NULL,
    close_price         DECIMAL(30,10) NOT NULL,
    volume              DECIMAL(30,2) NOT NULL,
    market_cap          DECIMAL(30,2),

    -- Calculated metrics
    daily_return        DECIMAL(10,6),
    daily_volatility    DECIMAL(10,6),
    price_range_pct     DECIMAL(10,4),  -- (high-low)/open

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_ohlcv_asset ON fact_ohlcv(asset_key);
CREATE INDEX idx_fact_ohlcv_date ON fact_ohlcv(date_key);

-- Global Market Metrics Fact Table
CREATE TABLE IF NOT EXISTS fact_global_metrics (
    metric_key          VARCHAR(32) PRIMARY KEY,
    date_key            VARCHAR(16) NOT NULL REFERENCES dim_date(date_key),
    snapshot_date       DATE NOT NULL,

    -- Total market
    total_market_cap    DECIMAL(30,2),
    total_volume_24h    DECIMAL(30,2),
    total_cryptocurrencies INTEGER,
    active_cryptocurrencies INTEGER,

    -- Dominance
    btc_dominance       DECIMAL(10,4),
    eth_dominance       DECIMAL(10,4),
    top10_dominance     DECIMAL(10,4),

    -- Market changes
    market_cap_change_24h DECIMAL(10,4),

    -- DeFi metrics
    defi_total_tvl      DECIMAL(30,2),
    defi_volume_24h     DECIMAL(30,2),
    defi_dominance      DECIMAL(10,4),

    -- Stablecoin metrics
    stablecoin_total_supply DECIMAL(30,2),

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_global_date ON fact_global_metrics(date_key);
CREATE INDEX idx_fact_global_snapshot ON fact_global_metrics(snapshot_date);

-- ============================================
-- ANALYTICS VIEWS
-- ============================================

-- Top Cryptocurrencies by Market Cap
CREATE OR REPLACE VIEW v_top_by_market_cap AS
SELECT
    a.symbol,
    a.name,
    a.category,
    p.price_usd,
    p.market_cap,
    p.market_cap_rank,
    p.volume_24h,
    p.price_change_pct_24h,
    p.circulating_supply
FROM fact_prices p
JOIN dim_asset a ON p.asset_key = a.asset_key
WHERE a.is_current = TRUE
ORDER BY p.market_cap DESC
LIMIT 100;

-- Top Gainers (24h)
CREATE OR REPLACE VIEW v_top_gainers_24h AS
SELECT
    a.symbol,
    a.name,
    p.price_usd,
    p.price_change_pct_24h,
    p.volume_24h,
    p.market_cap
FROM fact_prices p
JOIN dim_asset a ON p.asset_key = a.asset_key
WHERE a.is_current = TRUE
  AND p.market_cap > 10000000  -- Min $10M market cap
ORDER BY p.price_change_pct_24h DESC
LIMIT 50;

-- Top Losers (24h)
CREATE OR REPLACE VIEW v_top_losers_24h AS
SELECT
    a.symbol,
    a.name,
    p.price_usd,
    p.price_change_pct_24h,
    p.volume_24h,
    p.market_cap
FROM fact_prices p
JOIN dim_asset a ON p.asset_key = a.asset_key
WHERE a.is_current = TRUE
  AND p.market_cap > 10000000
ORDER BY p.price_change_pct_24h ASC
LIMIT 50;

-- Volatility Leaders
CREATE OR REPLACE VIEW v_high_volatility AS
SELECT
    a.symbol,
    a.name,
    p.price_usd,
    p.volatility_30d,
    p.volatility_90d,
    p.volume_24h,
    p.market_cap
FROM fact_prices p
JOIN dim_asset a ON p.asset_key = a.asset_key
WHERE a.is_current = TRUE
  AND p.volatility_30d IS NOT NULL
ORDER BY p.volatility_30d DESC
LIMIT 50;

-- Market Dominance Trend
CREATE OR REPLACE VIEW v_dominance_trend AS
SELECT
    d.full_date,
    g.btc_dominance,
    g.eth_dominance,
    g.top10_dominance,
    g.total_market_cap,
    g.defi_dominance
FROM fact_global_metrics g
JOIN dim_date d ON g.date_key = d.date_key
ORDER BY d.full_date DESC
LIMIT 365;

-- ============================================
-- COMMENTS
-- ============================================

COMMENT ON TABLE dim_asset IS 'Cryptocurrency asset master data - SCD Type 2 enabled';
COMMENT ON TABLE fact_prices IS 'Daily price and market metrics snapshot';
COMMENT ON TABLE fact_ohlcv IS 'Historical OHLCV time series data';
COMMENT ON TABLE fact_global_metrics IS 'Global crypto market metrics';
COMMENT ON COLUMN fact_prices.volatility_30d IS 'Standard deviation of daily returns over 30 days';
COMMENT ON COLUMN fact_global_metrics.btc_dominance IS 'Bitcoin market cap as percentage of total market';
