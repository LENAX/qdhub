-- Revert 025: remove pause window columns (SQLite 3.35+ required for DROP COLUMN)
-- If your SQLite does not support DROP COLUMN, recreate table or leave columns unused.

-- SQLite 3.35.0+:
-- ALTER TABLE sync_plan DROP COLUMN schedule_pause_start_cron;
-- ALTER TABLE sync_plan DROP COLUMN schedule_pause_end_cron;

-- Clear values for rollback without schema change:
UPDATE sync_plan SET schedule_pause_start_cron = NULL, schedule_pause_end_cron = NULL WHERE id = 'tushare-ws-realtime-tick';
