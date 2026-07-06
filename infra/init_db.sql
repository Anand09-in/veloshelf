-- VeloShelf — Postgres schema (Phase 2)
-- Run once before starting the Flink job:
--   docker-compose exec postgres psql -U veloshelf -d veloshelf -f /docker-entrypoint-initdb.d/init_db.sql
-- Or via make: make initdb

-- ---------------------------------------------------------------------------
-- Windowed features
-- Latest feature snapshot per (store_id, sku_id).
-- Upserted by the Flink job on every 1-min tumbling window close.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS windowed_features (
    store_id        TEXT        NOT NULL,
    sku_id          TEXT        NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    order_rate      FLOAT       NOT NULL DEFAULT 0,   -- orders / minute
    depletion_vel   FLOAT       NOT NULL DEFAULT 0,   -- units depleted / minute
    demand_momentum FLOAT       NOT NULL DEFAULT 1,   -- short/long rate ratio
    on_hand_est     INT         NOT NULL DEFAULT 0,   -- estimated on-hand after window
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (store_id, sku_id)
);

-- ---------------------------------------------------------------------------
-- Alerts
-- Inserted when a rule threshold is breached.
-- resolved=TRUE when the metric drops back below threshold.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alerts (
    alert_id     TEXT        PRIMARY KEY,
    alert_type   TEXT        NOT NULL CHECK (alert_type IN ('stockout_risk', 'surge')),
    store_id     TEXT        NOT NULL,
    sku_id       TEXT        NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metric_value FLOAT       NOT NULL,
    threshold    FLOAT       NOT NULL,
    resolved     BOOLEAN     NOT NULL DEFAULT FALSE
);

-- ---------------------------------------------------------------------------
-- Indexes for dashboard query patterns
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_features_store  ON windowed_features (store_id);
CREATE INDEX IF NOT EXISTS idx_features_sku    ON windowed_features (sku_id);
CREATE INDEX IF NOT EXISTS idx_alerts_active   ON alerts (resolved, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_store    ON alerts (store_id, resolved);