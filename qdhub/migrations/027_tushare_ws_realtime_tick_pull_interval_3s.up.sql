-- 实时行情同步拉取间隔改为 3 秒（与 Level1 更新频率一致）
-- Version: 027
-- Description: tushare-ws-realtime-tick 计划 pull_interval_seconds 从 60 改为 3。

UPDATE sync_plan
SET pull_interval_seconds = 3,
    updated_at = CURRENT_TIMESTAMP
WHERE id = 'tushare-ws-realtime-tick';
