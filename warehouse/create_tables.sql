-- REAL-TIME CRYPTO DATA PLATFORM — Database Schema
-- File: warehouse/create_tables.sql

-- Drop tables if they exist (safe to re-run)
DROP TABLE IF EXISTS crypto_events CASCADE;
DROP TABLE IF EXISTS pipeline_logs CASCADE;
DROP TABLE IF EXISTS ml_predictions CASCADE;

-- TABLE 1: crypto_events
-- Stores live cryptocurrency prices from CoinGecko API
CREATE TABLE crypto_events (
    id                  SERIAL PRIMARY KEY,
    coin_id             VARCHAR(50) NOT NULL,
    coin_name           VARCHAR(100),
    symbol              VARCHAR(20),
    price_usd           DECIMAL(20,8),
    price_change_24h    DECIMAL(10,4),
    price_change_pct_24h DECIMAL(10,4),
    market_cap          DECIMAL(25,2),
    volume_24h          DECIMAL(25,2),
    high_24h            DECIMAL(20,8),
    low_24h             DECIMAL(20,8),
    fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at        TIMESTAMP,
    source              VARCHAR(50) DEFAULT 'coingecko'
);

-- TABLE 2: pipeline_logs
-- Tracks every pipeline run — success, failure, duration
CREATE TABLE pipeline_logs (
    id                  SERIAL PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL,
    task_name           VARCHAR(100) NOT NULL,
    status              VARCHAR(20) CHECK (status IN ('running', 'success', 'failed')),
    started_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at         TIMESTAMP,
    duration_seconds    DECIMAL(10,2),
    rows_processed      INTEGER DEFAULT 0,
    error_message       TEXT,
    dag_run_id          VARCHAR(200)
);

-- TABLE 3: ml_predictions
-- Stores anomaly detection results from ML model
CREATE TABLE ml_predictions (
    id                  SERIAL PRIMARY KEY,
    coin_id             VARCHAR(50),
    price_usd           DECIMAL(20,8),
    prediction          VARCHAR(50),
    confidence          DECIMAL(5,4),
    is_anomaly          BOOLEAN DEFAULT FALSE,
    anomaly_type        VARCHAR(100),
    model_version       VARCHAR(50),
    predicted_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- INDEXES — speeds up dashboard queries
CREATE INDEX idx_crypto_coin_id      ON crypto_events(coin_id);
CREATE INDEX idx_crypto_fetched_at   ON crypto_events(fetched_at);
CREATE INDEX idx_crypto_symbol       ON crypto_events(symbol);
CREATE INDEX idx_pipeline_status     ON pipeline_logs(status);
CREATE INDEX idx_pipeline_name       ON pipeline_logs(pipeline_name);
CREATE INDEX idx_predictions_anomaly ON ml_predictions(is_anomaly);
CREATE INDEX idx_predictions_coin    ON ml_predictions(coin_id);