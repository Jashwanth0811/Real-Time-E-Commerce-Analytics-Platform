-- ============================================================
-- E-Commerce Data Warehouse - Schema Initialization
-- ============================================================

-- Create schemas for medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS airflow_meta;

-- ─── Bronze Layer: Raw Ingestion Tables ─────────────────────

CREATE TABLE IF NOT EXISTS bronze.raw_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(64),
    event_type      VARCHAR(32),
    user_id         VARCHAR(64),
    session_id      VARCHAR(64),
    product_id      VARCHAR(64),
    category        VARCHAR(64),
    price           NUMERIC(10,2),
    quantity        INTEGER,
    page_url        TEXT,
    referrer        TEXT,
    device_type     VARCHAR(32),
    country         VARCHAR(64),
    city            VARCHAR(64),
    ts              TIMESTAMP,
    raw_payload     JSONB,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bronze.raw_orders (
    id              BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(64),
    user_id         VARCHAR(64),
    status          VARCHAR(32),
    total_amount    NUMERIC(12,2),
    discount_pct    NUMERIC(5,2),
    payment_method  VARCHAR(32),
    items           JSONB,
    shipping_addr   JSONB,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    raw_payload     JSONB,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- ─── Silver Layer: Cleaned & Conformed ──────────────────────

CREATE TABLE IF NOT EXISTS silver.events (
    event_id        VARCHAR(64) PRIMARY KEY,
    event_type      VARCHAR(32) NOT NULL,
    user_id         VARCHAR(64) NOT NULL,
    session_id      VARCHAR(64),
    product_id      VARCHAR(64),
    category        VARCHAR(64),
    price           NUMERIC(10,2),
    quantity        INTEGER,
    device_type     VARCHAR(32),
    country         VARCHAR(64),
    city            VARCHAR(64),
    event_date      DATE NOT NULL,
    event_hour      INTEGER,
    ts              TIMESTAMP NOT NULL,
    processed_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id        VARCHAR(64) PRIMARY KEY,
    user_id         VARCHAR(64) NOT NULL,
    status          VARCHAR(32) NOT NULL,
    total_amount    NUMERIC(12,2),
    discount_pct    NUMERIC(5,2),
    payment_method  VARCHAR(32),
    item_count      INTEGER,
    order_date      DATE NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP,
    processed_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.users (
    user_id         VARCHAR(64) PRIMARY KEY,
    first_seen      TIMESTAMP,
    last_seen       TIMESTAMP,
    total_sessions  INTEGER DEFAULT 0,
    country         VARCHAR(64),
    city            VARCHAR(64),
    device_type     VARCHAR(32),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ─── Gold Layer: Business Aggregates ────────────────────────

CREATE TABLE IF NOT EXISTS gold.daily_revenue (
    report_date         DATE PRIMARY KEY,
    total_orders        INTEGER DEFAULT 0,
    completed_orders    INTEGER DEFAULT 0,
    gross_revenue       NUMERIC(14,2) DEFAULT 0,
    net_revenue         NUMERIC(14,2) DEFAULT 0,
    avg_order_value     NUMERIC(10,2) DEFAULT 0,
    unique_buyers       INTEGER DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.product_performance (
    report_date         DATE,
    product_id          VARCHAR(64),
    category            VARCHAR(64),
    views               INTEGER DEFAULT 0,
    add_to_cart         INTEGER DEFAULT 0,
    purchases           INTEGER DEFAULT 0,
    revenue             NUMERIC(12,2) DEFAULT 0,
    conversion_rate     NUMERIC(6,4) DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_date, product_id)
);

CREATE TABLE IF NOT EXISTS gold.user_funnel (
    report_date         DATE,
    stage               VARCHAR(32),
    users               INTEGER DEFAULT 0,
    drop_off_rate       NUMERIC(6,4) DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_date, stage)
);

CREATE TABLE IF NOT EXISTS gold.category_summary (
    report_date         DATE,
    category            VARCHAR(64),
    total_views         INTEGER DEFAULT 0,
    total_purchases     INTEGER DEFAULT 0,
    total_revenue       NUMERIC(12,2) DEFAULT 0,
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (report_date, category)
);

-- ─── Indexes ─────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_bronze_events_ts ON bronze.raw_events(ts);
CREATE INDEX IF NOT EXISTS idx_bronze_orders_created ON bronze.raw_orders(created_at);
CREATE INDEX IF NOT EXISTS idx_silver_events_date ON silver.events(event_date);
CREATE INDEX IF NOT EXISTS idx_silver_events_user ON silver.events(user_id);
CREATE INDEX IF NOT EXISTS idx_silver_orders_date ON silver.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_gold_revenue_date ON gold.daily_revenue(report_date);

-- ─── Pipeline run log ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS airflow_meta.pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    dag_id          VARCHAR(128),
    run_id          VARCHAR(128),
    stage           VARCHAR(64),
    records_in      INTEGER,
    records_out     INTEGER,
    status          VARCHAR(32),
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    notes           TEXT
);

-- ─── Grant permissions ───────────────────────────────────────

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO warehouse;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO warehouse;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO warehouse;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA airflow_meta TO warehouse;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO warehouse;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA silver TO warehouse;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold TO warehouse;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA airflow_meta TO warehouse;

SELECT 'Database initialized successfully' AS status;
