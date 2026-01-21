-- SyncPlan Migration Rollback
-- Version: 003
-- Description: Rollback SyncPlan migration

-- ==================== Recreate legacy sync_jobs table ====================

CREATE TABLE IF NOT EXISTS sync_jobs (
    id              VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(128) NOT NULL,
    description     TEXT,
    api_meta_id     VARCHAR(64) NOT NULL REFERENCES api_metadata(id),
    data_store_id   VARCHAR(64) NOT NULL REFERENCES quant_data_stores(id),
    workflow_def_id VARCHAR(64) REFERENCES workflow_definition(id),
    mode            VARCHAR(32) NOT NULL,
    cron_expression VARCHAR(128),
    params          TEXT,
    param_rules     TEXT,
    status          VARCHAR(32) DEFAULT 'disabled',
    last_run_at     TIMESTAMP,
    next_run_at     TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_api_meta ON sync_jobs(api_meta_id);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_data_store ON sync_jobs(data_store_id);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON sync_jobs(status);

-- ==================== Recreate legacy sync_executions table ====================

CREATE TABLE IF NOT EXISTS sync_executions (
    id               VARCHAR(64) PRIMARY KEY,
    sync_job_id      VARCHAR(64) NOT NULL REFERENCES sync_jobs(id),
    workflow_inst_id VARCHAR(64) REFERENCES workflow_instance(id),
    status           VARCHAR(32) NOT NULL,
    started_at       TIMESTAMP NOT NULL,
    finished_at      TIMESTAMP,
    record_count     INTEGER DEFAULT 0,
    error_message    TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_executions_job ON sync_executions(sync_job_id);
CREATE INDEX IF NOT EXISTS idx_sync_executions_status ON sync_executions(status);

-- ==================== Drop new tables ====================

DROP TABLE IF EXISTS sync_execution;
DROP TABLE IF EXISTS sync_task;
DROP TABLE IF EXISTS sync_plan;

-- ==================== Remove param_dependencies from api_metadata ====================
-- SQLite doesn't support DROP COLUMN directly, would need table recreation
-- For simplicity, we leave the column (it won't affect the old code)
