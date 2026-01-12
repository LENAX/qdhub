-- QDHub Initial Schema Migration
-- Version: 001
-- Description: Create core tables for QDHub

-- Data Sources table
CREATE TABLE IF NOT EXISTS data_sources (
    id          VARCHAR(64) PRIMARY KEY,
    name        VARCHAR(128) NOT NULL,
    description TEXT,
    base_url    VARCHAR(512),
    doc_url     VARCHAR(512),
    status      VARCHAR(32) DEFAULT 'active',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- API Categories table
CREATE TABLE IF NOT EXISTS api_categories (
    id             VARCHAR(64) PRIMARY KEY,
    data_source_id VARCHAR(64) NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    name           VARCHAR(128) NOT NULL,
    description    TEXT,
    parent_id      VARCHAR(64) REFERENCES api_categories(id),
    sort_order     INTEGER DEFAULT 0,
    doc_path       VARCHAR(512),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_api_categories_data_source ON api_categories(data_source_id);
CREATE INDEX IF NOT EXISTS idx_api_categories_parent ON api_categories(parent_id);

-- API Metadata table
CREATE TABLE IF NOT EXISTS api_metadata (
    id              VARCHAR(64) PRIMARY KEY,
    data_source_id  VARCHAR(64) NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    category_id     VARCHAR(64) REFERENCES api_categories(id),
    name            VARCHAR(128) NOT NULL,
    display_name    VARCHAR(256),
    description     TEXT,
    endpoint        VARCHAR(512),
    request_params  TEXT,  -- JSON
    response_fields TEXT,  -- JSON
    rate_limit      TEXT,  -- JSON
    permission      VARCHAR(64),
    status          VARCHAR(32) DEFAULT 'active',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(data_source_id, name)
);

CREATE INDEX IF NOT EXISTS idx_api_metadata_data_source ON api_metadata(data_source_id);
CREATE INDEX IF NOT EXISTS idx_api_metadata_category ON api_metadata(category_id);

-- Tokens table
CREATE TABLE IF NOT EXISTS tokens (
    id             VARCHAR(64) PRIMARY KEY,
    data_source_id VARCHAR(64) NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    token_value    TEXT NOT NULL,
    expires_at     TIMESTAMP,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(data_source_id)
);

-- Quant Data Stores table
CREATE TABLE IF NOT EXISTS quant_data_stores (
    id           VARCHAR(64) PRIMARY KEY,
    name         VARCHAR(128) NOT NULL,
    type         VARCHAR(32) NOT NULL,
    dsn          TEXT,
    storage_path VARCHAR(512),
    status       VARCHAR(32) DEFAULT 'active',
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table Schemas table
CREATE TABLE IF NOT EXISTS table_schemas (
    id             VARCHAR(64) PRIMARY KEY,
    data_store_id  VARCHAR(64) NOT NULL REFERENCES quant_data_stores(id) ON DELETE CASCADE,
    api_meta_id    VARCHAR(64) NOT NULL REFERENCES api_metadata(id),
    table_name     VARCHAR(128) NOT NULL,
    columns        TEXT NOT NULL,  -- JSON
    primary_keys   TEXT,           -- JSON
    indexes        TEXT,           -- JSON
    status         VARCHAR(32) DEFAULT 'pending',
    error_message  TEXT,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(data_store_id, table_name)
);

CREATE INDEX IF NOT EXISTS idx_table_schemas_data_store ON table_schemas(data_store_id);
CREATE INDEX IF NOT EXISTS idx_table_schemas_api_meta ON table_schemas(api_meta_id);

-- Data Type Mapping Rules table
CREATE TABLE IF NOT EXISTS data_type_mapping_rules (
    id               VARCHAR(64) PRIMARY KEY,
    data_source_type VARCHAR(32) NOT NULL,
    source_type      VARCHAR(64) NOT NULL,
    target_db_type   VARCHAR(32) NOT NULL,
    target_type      VARCHAR(64) NOT NULL,
    field_pattern    VARCHAR(256),
    priority         INTEGER DEFAULT 0,
    is_default       INTEGER DEFAULT 0,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(data_source_type, source_type, target_db_type, field_pattern)
);

CREATE INDEX IF NOT EXISTS idx_mapping_rules_lookup ON data_type_mapping_rules(data_source_type, target_db_type, priority DESC);

-- Workflow Definitions table
CREATE TABLE IF NOT EXISTS workflow_definitions (
    id              VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(128) NOT NULL UNIQUE,
    description     TEXT,
    category        VARCHAR(32) NOT NULL,
    definition_yaml TEXT NOT NULL,
    version         INTEGER DEFAULT 1,
    status          VARCHAR(32) DEFAULT 'enabled',
    is_system       INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sync Jobs table
CREATE TABLE IF NOT EXISTS sync_jobs (
    id              VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(128) NOT NULL,
    description     TEXT,
    api_meta_id     VARCHAR(64) NOT NULL REFERENCES api_metadata(id),
    data_store_id   VARCHAR(64) NOT NULL REFERENCES quant_data_stores(id),
    workflow_def_id VARCHAR(64) REFERENCES workflow_definitions(id),
    mode            VARCHAR(32) NOT NULL,
    cron_expression VARCHAR(128),
    params          TEXT,  -- JSON
    param_rules     TEXT,  -- JSON
    status          VARCHAR(32) DEFAULT 'disabled',
    last_run_at     TIMESTAMP,
    next_run_at     TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sync_jobs_api_meta ON sync_jobs(api_meta_id);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_data_store ON sync_jobs(data_store_id);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON sync_jobs(status);

-- Workflow Instances table
CREATE TABLE IF NOT EXISTS workflow_instances (
    id                  VARCHAR(64) PRIMARY KEY,
    workflow_def_id     VARCHAR(64) NOT NULL REFERENCES workflow_definitions(id),
    engine_instance_id  VARCHAR(64),
    trigger_type        VARCHAR(32) NOT NULL,
    trigger_params      TEXT,  -- JSON
    status              VARCHAR(32) NOT NULL,
    progress            REAL DEFAULT 0,
    started_at          TIMESTAMP NOT NULL,
    finished_at         TIMESTAMP,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_workflow_instances_def ON workflow_instances(workflow_def_id);
CREATE INDEX IF NOT EXISTS idx_workflow_instances_status ON workflow_instances(status);

-- Sync Executions table
CREATE TABLE IF NOT EXISTS sync_executions (
    id               VARCHAR(64) PRIMARY KEY,
    sync_job_id      VARCHAR(64) NOT NULL REFERENCES sync_jobs(id),
    workflow_inst_id VARCHAR(64) REFERENCES workflow_instances(id),
    status           VARCHAR(32) NOT NULL,
    started_at       TIMESTAMP NOT NULL,
    finished_at      TIMESTAMP,
    record_count     INTEGER DEFAULT 0,
    error_message    TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_executions_job ON sync_executions(sync_job_id);
CREATE INDEX IF NOT EXISTS idx_sync_executions_status ON sync_executions(status);

-- Task Instances table
CREATE TABLE IF NOT EXISTS task_instances (
    id                VARCHAR(64) PRIMARY KEY,
    workflow_inst_id  VARCHAR(64) NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    task_name         VARCHAR(128) NOT NULL,
    status            VARCHAR(32) NOT NULL,
    started_at        TIMESTAMP,
    finished_at       TIMESTAMP,
    retry_count       INTEGER DEFAULT 0,
    error_message     TEXT
);

CREATE INDEX IF NOT EXISTS idx_task_instances_wf ON task_instances(workflow_inst_id);
