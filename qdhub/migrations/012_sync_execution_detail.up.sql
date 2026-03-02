-- 同步执行明细：按任务记录每个 API 的同步行数、状态、错误信息，用于统计与错误排查
CREATE TABLE IF NOT EXISTS sync_execution_detail (
    id            VARCHAR(64) PRIMARY KEY,
    execution_id  VARCHAR(64) NOT NULL REFERENCES sync_execution(id) ON DELETE CASCADE,
    task_id       VARCHAR(64) NOT NULL,
    api_name      VARCHAR(128) NOT NULL,
    record_count  INTEGER DEFAULT 0,
    status        VARCHAR(32) NOT NULL,   -- success/failed
    error_message TEXT,
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sync_execution_detail_exec ON sync_execution_detail(execution_id);
CREATE INDEX IF NOT EXISTS idx_sync_execution_detail_api ON sync_execution_detail(execution_id, api_name);
