-- 标准实时 SyncPlan：ts_proxy、sina、tushare_ws 各一条，统一写入 realtime_duckdb
-- Version: 033
-- Description:
--   主源（ts_proxy）配置交易时间窗 9:30-11:30、13:00-15:00，由 ReconcileRunningWindow 自动启停；
--   灾备源（sina、tushare_ws）不配 schedule 窗口，仅人工 connect 或故障切换时启动。

-- Realtime_ts_proxy：ts_realtime_mkt_tick（通过 ts_proxy 转发时由配置选择 adapter）
INSERT OR IGNORE INTO sync_plan (
    id, name, description, data_source_id, data_store_id, plan_mode,
    selected_apis, resolved_apis, execution_graph, cron_expression,
    schedule_start_cron, schedule_end_cron, schedule_pause_start_cron, schedule_pause_end_cron,
    pull_interval_seconds, default_execute_params, incremental_mode,
    last_successful_end_date, incremental_start_date_api, incremental_start_date_column,
    status, last_executed_at, next_execute_at, created_at, updated_at
)
SELECT
    'realtime-ts-forward' AS id,
    'Realtime_ts_proxy' AS name,
    'ts_proxy 全市场行情：通过 ts_proxy 或直连，写实时库；交易时段 9:30-11:30、13:00-15:00 自动启停' AS description,
    ds.id AS data_source_id,
    'realtime-duckdb-0000-4000-8000-000000000001' AS data_store_id,
    'realtime' AS plan_mode,
    '["ts_realtime_mkt_tick"]' AS selected_apis,
    '["ts_realtime_mkt_tick"]' AS resolved_apis,
    NULL AS execution_graph,
    NULL AS cron_expression,
    '0 30 9 * * 1-5' AS schedule_start_cron,
    '0 0 15 * * 1-5' AS schedule_end_cron,
    '0 30 11 * * 1-5' AS schedule_pause_start_cron,
    '0 0 13 * * 1-5' AS schedule_pause_end_cron,
    3 AS pull_interval_seconds,
    '{}' AS default_execute_params,
    0 AS incremental_mode,
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
  AND NOT EXISTS (SELECT 1 FROM sync_plan WHERE id = 'realtime-ts-forward')
LIMIT 1;

-- Realtime_sina_quote：realtime_quote，灾备源不配 schedule
INSERT OR IGNORE INTO sync_plan (
    id, name, description, data_source_id, data_store_id, plan_mode,
    selected_apis, resolved_apis, execution_graph, cron_expression,
    schedule_start_cron, schedule_end_cron, schedule_pause_start_cron, schedule_pause_end_cron,
    pull_interval_seconds, default_execute_params, incremental_mode,
    last_successful_end_date, incremental_start_date_api, incremental_start_date_column,
    status, last_executed_at, next_execute_at, created_at, updated_at
)
SELECT
    'realtime-sina-quote' AS id,
    'Realtime_sina_quote' AS name,
    '新浪实时行情，写实时库；灾备源，仅 connect 或故障切换时启动' AS description,
    ds.id AS data_source_id,
    'realtime-duckdb-0000-4000-8000-000000000001' AS data_store_id,
    'realtime' AS plan_mode,
    '["realtime_quote"]' AS selected_apis,
    '["realtime_quote"]' AS resolved_apis,
    NULL AS execution_graph,
    NULL AS cron_expression,
    NULL AS schedule_start_cron,
    NULL AS schedule_end_cron,
    NULL AS schedule_pause_start_cron,
    NULL AS schedule_pause_end_cron,
    60 AS pull_interval_seconds,
    '{}' AS default_execute_params,
    0 AS incremental_mode,
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
  AND NOT EXISTS (SELECT 1 FROM sync_plan WHERE id = 'realtime-sina-quote')
LIMIT 1;


-- sync_task：每个计划一条任务，否则 ExecuteSyncPlan 报 "no tasks found"
INSERT OR IGNORE INTO sync_task (id, sync_plan_id, api_name, sync_mode, params, param_mappings, dependencies, level, sort_order, sync_frequency, last_synced_at, created_at)
SELECT 'realtime-ts-forward-task' AS id, 'realtime-ts-forward' AS sync_plan_id, 'ts_realtime_mkt_tick' AS api_name, 'direct' AS sync_mode, NULL, NULL, NULL, 0, 0, 0, NULL, CURRENT_TIMESTAMP
WHERE EXISTS (SELECT 1 FROM sync_plan WHERE id = 'realtime-ts-forward') AND NOT EXISTS (SELECT 1 FROM sync_task WHERE sync_plan_id = 'realtime-ts-forward');

INSERT OR IGNORE INTO sync_task (id, sync_plan_id, api_name, sync_mode, params, param_mappings, dependencies, level, sort_order, sync_frequency, last_synced_at, created_at)
SELECT 'realtime-sina-quote-task' AS id, 'realtime-sina-quote' AS sync_plan_id, 'realtime_quote' AS api_name, 'direct' AS sync_mode, NULL, NULL, NULL, 0, 0, 0, NULL, CURRENT_TIMESTAMP
WHERE EXISTS (SELECT 1 FROM sync_plan WHERE id = 'realtime-sina-quote') AND NOT EXISTS (SELECT 1 FROM sync_task WHERE sync_plan_id = 'realtime-sina-quote');
