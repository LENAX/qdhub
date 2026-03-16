-- Tushare WS 全市场 tick 实时计划：补全 sync_task
-- Version: 026
-- Description: 迁移 023 只插入了 sync_plan，未插入 sync_task，导致执行时报 "no tasks found for plan"。
--   本迁移为计划 tushare-ws-realtime-tick 插入一条任务（ts_realtime_mkt_tick），仅当该计划下尚无任务时插入。

INSERT INTO sync_task (
    id,
    sync_plan_id,
    api_name,
    sync_mode,
    params,
    param_mappings,
    dependencies,
    level,
    sort_order,
    sync_frequency,
    last_synced_at,
    created_at
)
SELECT
    'tushare-ws-realtime-tick-ts_realtime_mkt_tick' AS id,
    'tushare-ws-realtime-tick' AS sync_plan_id,
    'ts_realtime_mkt_tick' AS api_name,
    'direct' AS sync_mode,
    NULL AS params,
    NULL AS param_mappings,
    NULL AS dependencies,
    0 AS level,
    0 AS sort_order,
    0 AS sync_frequency,
    NULL AS last_synced_at,
    CURRENT_TIMESTAMP AS created_at
WHERE EXISTS (SELECT 1 FROM sync_plan WHERE id = 'tushare-ws-realtime-tick')
  AND NOT EXISTS (SELECT 1 FROM sync_task WHERE sync_plan_id = 'tushare-ws-realtime-tick');
