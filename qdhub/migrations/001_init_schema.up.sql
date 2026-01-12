-- QDHub Initial Schema Migration (Merged with Task Engine)
-- Version: 001
-- Description: Create core tables for QDHub, using Task Engine table structure with QDHub-specific extensions
--
-- IMPORTANT NOTES:
-- 1. This migration creates tables that are SHARED between Task Engine and QDHub
-- 2. Task Engine tables use singular names (workflow_definition, workflow_instance, task_instance)
-- 3. QDHub-specific fields are added to Task Engine tables:
--    - workflow_definition: category, definition_yaml, version, is_system, updated_at
--    - workflow_instance: engine_instance_id, trigger_type, trigger_params, progress
-- 4. Task Engine's initSchema() uses CREATE TABLE IF NOT EXISTS, so it won't conflict with this migration
-- 5. Ensure this migration runs BEFORE Task Engine initializes its schema for best compatibility

-- ==================== QDHub Specific Tables ====================

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
    description  TEXT,
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
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

-- Sync Jobs table
CREATE TABLE IF NOT EXISTS sync_jobs (
    id              VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(128) NOT NULL,
    description     TEXT,
    api_meta_id     VARCHAR(64) NOT NULL REFERENCES api_metadata(id),
    data_store_id   VARCHAR(64) NOT NULL REFERENCES quant_data_stores(id),
    workflow_def_id VARCHAR(64) REFERENCES workflow_definition(id),
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

-- Sync Executions table
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

-- ==================== Task Engine Tables (with QDHub extensions) ====================

-- Workflow定义表 (Task Engine structure + QDHub extensions)
-- This table is shared between Task Engine and QDHub
-- Task Engine's initSchema uses CREATE TABLE IF NOT EXISTS, so it won't conflict
CREATE TABLE IF NOT EXISTS workflow_definition (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    params TEXT,  -- JSON格式存储参数
    dependencies TEXT,  -- JSON格式存储依赖关系
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'ENABLED',
    sub_task_error_tolerance REAL NOT NULL DEFAULT 0.0,
    transactional INTEGER NOT NULL DEFAULT 0,
    transaction_mode TEXT DEFAULT '',
    max_concurrent_task INTEGER NOT NULL DEFAULT 10,
    cron_expr TEXT DEFAULT '',
    cron_enabled INTEGER NOT NULL DEFAULT 0,
    -- QDHub specific fields (added to base Task Engine structure)
    category TEXT,  -- QDHub workflow category (metadata/sync/custom)
    definition_yaml TEXT,  -- QDHub workflow definition YAML
    version INTEGER DEFAULT 1,  -- QDHub workflow version
    is_system INTEGER DEFAULT 0,  -- QDHub system workflow flag
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP  -- QDHub update timestamp
);

CREATE INDEX IF NOT EXISTS idx_workflow_definition_name ON workflow_definition(name);
CREATE INDEX IF NOT EXISTS idx_workflow_definition_status ON workflow_definition(status);

-- Task定义表（存储Workflow中的Task定义，与task_instance运行时实例区分）
CREATE TABLE IF NOT EXISTS task_definition (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    job_func_id TEXT,
    job_func_name TEXT,
    compensation_func_id TEXT,
    compensation_func_name TEXT,
    params TEXT,  -- JSON格式存储参数
    timeout_seconds INTEGER DEFAULT 30,
    retry_count INTEGER DEFAULT 0,
    dependencies TEXT,  -- JSON数组，存储依赖的Task名称
    required_params TEXT,  -- JSON数组，必需参数列表
    result_mapping TEXT,  -- JSON对象，结果映射规则
    status_handlers TEXT,  -- JSON对象，状态处理器映射
    is_template INTEGER DEFAULT 0,
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_id) REFERENCES workflow_definition(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_task_definition_workflow_id ON task_definition(workflow_id);

-- WorkflowInstance表 (Task Engine structure + QDHub extensions)
-- This table is shared between Task Engine and QDHub
-- Task Engine's initSchema uses CREATE TABLE IF NOT EXISTS, so it won't conflict
CREATE TABLE IF NOT EXISTS workflow_instance (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    status TEXT NOT NULL,  -- Ready/Running/Paused/Terminated/Success/Failed
    start_time DATETIME,
    end_time DATETIME,
    breakpoint TEXT,  -- JSON格式存储断点数据
    error_message TEXT,
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- QDHub specific fields (added to base Task Engine structure)
    engine_instance_id TEXT,  -- QDHub: Task Engine instance ID (same as id for compatibility)
    trigger_type TEXT,  -- QDHub: trigger type (manual/cron/event)
    trigger_params TEXT,  -- QDHub: trigger parameters (JSON)
    progress REAL DEFAULT 0,  -- QDHub: workflow progress (0-100)
    FOREIGN KEY (workflow_id) REFERENCES workflow_definition(id)
);

CREATE INDEX IF NOT EXISTS idx_workflow_instance_workflow_id ON workflow_instance(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_instance_status ON workflow_instance(status);

-- Task实例表 (Task Engine structure)
CREATE TABLE IF NOT EXISTS task_instance (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    workflow_instance_id TEXT NOT NULL,
    job_func_id TEXT,
    job_func_name TEXT,
    compensation_func_id TEXT,
    compensation_func_name TEXT,
    params TEXT,  -- JSON格式存储参数
    status TEXT NOT NULL,  -- Pending/Running/Success/Failed/TimeoutFailed
    timeout_seconds INTEGER DEFAULT 30,
    retry_count INTEGER DEFAULT 0,
    start_time DATETIME,
    end_time DATETIME,
    error_msg TEXT,
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workflow_instance_id) REFERENCES workflow_instance(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_task_instance_workflow_instance_id ON task_instance(workflow_instance_id);
CREATE INDEX IF NOT EXISTS idx_task_instance_status ON task_instance(status);

-- Job函数元数据表 (Task Engine)
CREATE TABLE IF NOT EXISTS job_function_meta (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    code_path TEXT,  -- 函数加载路径
    hash TEXT,  -- 函数二进制哈希
    param_types TEXT,  -- JSON格式存储参数类型列表
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_job_function_meta_name ON job_function_meta(name);

-- Task Handler元数据表 (Task Engine)
CREATE TABLE IF NOT EXISTS task_handler_meta (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_task_handler_meta_name ON task_handler_meta(name);
