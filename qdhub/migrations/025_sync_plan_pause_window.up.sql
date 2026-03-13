-- 运行时段内午休暂停窗口
-- Version: 025
-- Description: 为 realtime 计划增加 schedule_pause_start_cron / schedule_pause_end_cron，
--   在此时段内视为“不在运行窗口”（自动停止）；用于 A 股午休 11:30-13:00。

ALTER TABLE sync_plan ADD COLUMN schedule_pause_start_cron VARCHAR(128) DEFAULT NULL;
ALTER TABLE sync_plan ADD COLUMN schedule_pause_end_cron VARCHAR(128) DEFAULT NULL;

-- Tushare WS 全市场 Tick 实时计划：午休 11:30 - 13:00 暂停
UPDATE sync_plan
SET schedule_pause_start_cron = '0 30 11 * * 1-5',
    schedule_pause_end_cron   = '0 0 13 * * 1-5'
WHERE id = 'tushare-ws-realtime-tick';
