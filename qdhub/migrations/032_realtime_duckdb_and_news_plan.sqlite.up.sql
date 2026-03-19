-- 实时库与新闻实时 SyncPlan
-- Version: 032
-- Description:
--   1) 新增专用于实时行情与新闻的 QuantDataStore（realtime_duckdb），与主历史库解耦，避免新闻/行情同步拖垮主库。
--   2) 新增新闻实时 SyncPlan（Realtime_news），plan_mode=news_realtime，写入 realtime_duckdb，cron 每 5 分钟执行一次。

-- 1) 实时库：独立 DuckDB 文件
INSERT OR IGNORE INTO quant_data_stores (id, name, description, type, dsn, storage_path, status, created_at, updated_at)
VALUES (
    'realtime-duckdb-0000-4000-8000-000000000001',
    'realtime_duckdb',
    '实时行情与新闻专用 DuckDB，与主历史库分离',
    'duckdb',
    '',
    './data/realtime_ticks.duckdb',
    'active',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);

-- 2) 新闻实时 SyncPlan：使用 realtime_duckdb，每 5 分钟拉取
INSERT OR IGNORE INTO sync_plan (
    id,
    name,
    description,
    data_source_id,
    data_store_id,
    plan_mode,
    selected_apis,
    resolved_apis,
    execution_graph,
    cron_expression,
    schedule_start_cron,
    schedule_end_cron,
    schedule_pause_start_cron,
    schedule_pause_end_cron,
    pull_interval_seconds,
    default_execute_params,
    incremental_mode,
    last_successful_end_date,
    incremental_start_date_api,
    incremental_start_date_column,
    status,
    last_executed_at,
    next_execute_at,
    created_at,
    updated_at
)
SELECT
    'realtime-news' AS id,
    'Realtime_news' AS name,
    '新闻快讯实时同步：按 news_sync_checkpoint 增量拉取，写入实时库，每 5 分钟更新' AS description,
    ds.id AS data_source_id,
    'realtime-duckdb-0000-4000-8000-000000000001' AS data_store_id,
    'realtime' AS plan_mode,
    '[]' AS selected_apis,
    NULL AS resolved_apis,
    NULL AS execution_graph,
    NULL AS cron_expression,
    NULL AS schedule_start_cron,
    NULL AS schedule_end_cron,
    NULL AS schedule_pause_start_cron,
    NULL AS schedule_pause_end_cron,
    0 AS pull_interval_seconds,
    '{}' AS default_execute_params,
    1 AS incremental_mode,
    NULL AS last_successful_end_date,
    NULL AS incremental_start_date_api,
    NULL AS incremental_start_date_column,
    'enabled' AS status,
    NULL AS last_executed_at,
    NULL AS next_execute_at,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM data_sources ds
WHERE LOWER(ds.name) = 'tushare'
  AND NOT EXISTS (SELECT 1 FROM sync_plan WHERE id = 'realtime-news')
LIMIT 1;

-- 若无 Tushare 数据源则插入一条 data_store 占位、计划不插入（依赖用户先配置 Tushare）
-- 上面 INSERT SELECT 已用 NOT EXISTS 避免重复；若没有 data_sources 则 0 行插入，realtime-news 计划需后续由初始化或用户创建。
