-- Realtime Data Sources table
-- Version: 028
-- Description: Table for managing realtime data sources (ts_proxy, tushare_ws, sina, eastmoney) with priority and health.

CREATE TABLE IF NOT EXISTS realtime_sources (
    id                      VARCHAR(64) PRIMARY KEY,
    name                    VARCHAR(128) NOT NULL,
    type                    VARCHAR(32) NOT NULL,
    config                  TEXT,
    priority                INTEGER NOT NULL DEFAULT 1,
    is_primary              INTEGER NOT NULL DEFAULT 0,
    health_check_on_startup INTEGER NOT NULL DEFAULT 0,
    enabled                 INTEGER NOT NULL DEFAULT 1,
    last_health_status      VARCHAR(32),
    last_health_at          TIMESTAMP,
    last_health_error       TEXT,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_realtime_sources_type ON realtime_sources(type);
CREATE INDEX IF NOT EXISTS idx_realtime_sources_enabled_priority ON realtime_sources(enabled, priority);
