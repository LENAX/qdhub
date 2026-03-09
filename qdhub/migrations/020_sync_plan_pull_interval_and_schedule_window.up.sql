-- SyncPlan: pull_interval_seconds (realtime Pull mode), schedule_start_cron, schedule_end_cron (running window)
-- Version: 020

ALTER TABLE sync_plan ADD COLUMN pull_interval_seconds INTEGER DEFAULT 60;
ALTER TABLE sync_plan ADD COLUMN schedule_start_cron VARCHAR(128) DEFAULT NULL;
ALTER TABLE sync_plan ADD COLUMN schedule_end_cron VARCHAR(128) DEFAULT NULL;
