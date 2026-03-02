-- SyncPlan Incremental Mode
-- Version: 010
-- Description: Add incremental_mode and last_successful_end_date for scheduled incremental sync.

ALTER TABLE sync_plan ADD COLUMN incremental_mode INTEGER DEFAULT 0;
ALTER TABLE sync_plan ADD COLUMN last_successful_end_date VARCHAR(16);
