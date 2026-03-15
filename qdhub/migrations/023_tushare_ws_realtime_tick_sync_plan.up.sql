-- Tushare WS 全市场 tick 实时 SyncPlan 种子
-- Version: 023
-- Description: 插入 tushare WS 接口 ts_realtime_mkt_tick 的实时 sync plan，
--   运行时段：每周一至周五 9:15 - 15:00（与 021 的 api_metadata/api_sync_strategies 配套）。

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
    'tushare-ws-realtime-tick' AS id,
    'Tushare WS 全市场 Tick 实时' AS name,
    'tushare WS 全市场 tick 行情（HQ_STK_TICK），交易时段 9:15-15:00 自动启停' AS description,
    ds.id AS data_source_id,
    NULL AS data_store_id,
    'realtime' AS plan_mode,
    '["ts_realtime_mkt_tick"]' AS selected_apis,
    NULL AS resolved_apis,
    NULL AS execution_graph,
    NULL AS cron_expression,
    '0 15 9 * * 1-5' AS schedule_start_cron,
    '0 0 15 * * 1-5' AS schedule_end_cron,
    60 AS pull_interval_seconds,
    NULL AS default_execute_params,
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
  AND NOT EXISTS (SELECT 1 FROM sync_plan WHERE id = 'tushare-ws-realtime-tick');
