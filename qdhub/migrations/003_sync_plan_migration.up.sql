-- SyncPlan Migration
-- Version: 003
-- Description: Migrate from SyncJob to SyncPlan model
--   - Create sync_plan table (aggregate root)
--   - Create sync_task table (aggregate internal entity)
--   - Modify sync_execution table to reference sync_plan
--   - Drop legacy sync_jobs table
-- 
-- Note: param_dependencies column was added to api_metadata in 001_init_schema.up.sql
-- For existing databases that don't have this column, manually run:
--   ALTER TABLE api_metadata ADD COLUMN param_dependencies TEXT;

-- ==================== Create sync_plan table ====================

CREATE TABLE IF NOT EXISTS sync_plan (
    id               VARCHAR(64) PRIMARY KEY,
    name             VARCHAR(128) NOT NULL,
    description      TEXT,
    data_source_id   VARCHAR(64) NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    data_store_id    VARCHAR(64) REFERENCES quant_data_stores(id),
    selected_apis    TEXT NOT NULL,    -- JSON: ["api1", "api2"]
    resolved_apis    TEXT,             -- JSON: ["api1", "api2", "dep_api"]
    execution_graph  TEXT,             -- JSON: ExecutionGraph struct
    cron_expression  VARCHAR(128),
    status           VARCHAR(32) DEFAULT 'draft',  -- draft/resolved/enabled/disabled/running
    last_executed_at TIMESTAMP,
    next_execute_at  TIMESTAMP,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sync_plan_data_source ON sync_plan(data_source_id);
CREATE INDEX IF NOT EXISTS idx_sync_plan_status ON sync_plan(status);

-- ==================== Create sync_task table ====================

CREATE TABLE IF NOT EXISTS sync_task (
    id             VARCHAR(64) PRIMARY KEY,
    sync_plan_id   VARCHAR(64) NOT NULL REFERENCES sync_plan(id) ON DELETE CASCADE,
    api_name       VARCHAR(128) NOT NULL,
    sync_mode      VARCHAR(32) NOT NULL,     -- direct/template
    params         TEXT,                      -- JSON: static params
    param_mappings TEXT,                      -- JSON: [{param_name, source_task, source_field, ...}]
    dependencies   TEXT,                      -- JSON: ["TaskName1", "TaskName2"]
    level          INTEGER DEFAULT 0,         -- execution level (0 = no dependencies)
    sort_order     INTEGER DEFAULT 0,         -- order within same level
    sync_frequency INTEGER DEFAULT 0,         -- sync frequency in nanoseconds (0 = always sync)
    last_synced_at TIMESTAMP,                 -- last successful sync time
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sync_task_plan ON sync_task(sync_plan_id);
CREATE INDEX IF NOT EXISTS idx_sync_task_level ON sync_task(level, sort_order);

-- ==================== Create new sync_execution table ====================
-- Note: We create a new table instead of altering the old one for data safety

CREATE TABLE IF NOT EXISTS sync_execution (
    id               VARCHAR(64) PRIMARY KEY,
    sync_plan_id     VARCHAR(64) NOT NULL REFERENCES sync_plan(id) ON DELETE CASCADE,
    workflow_inst_id VARCHAR(64) REFERENCES workflow_instance(id),
    status           VARCHAR(32) NOT NULL,    -- pending/running/success/failed/cancelled
    started_at       TIMESTAMP NOT NULL,
    finished_at      TIMESTAMP,
    record_count     INTEGER DEFAULT 0,
    error_message    TEXT,
    execute_params   TEXT,                     -- JSON: ExecuteParams struct
    synced_apis      TEXT,                     -- JSON: ["api1", "api2"]
    skipped_apis     TEXT                      -- JSON: ["api3", "api4"]
);

CREATE INDEX IF NOT EXISTS idx_sync_execution_plan ON sync_execution(sync_plan_id);
CREATE INDEX IF NOT EXISTS idx_sync_execution_status ON sync_execution(status);

-- ==================== Drop legacy tables ====================
-- Note: We keep these at the end so data can be migrated if needed

DROP TABLE IF EXISTS sync_executions;  -- old table with plural name
DROP TABLE IF EXISTS sync_jobs;
