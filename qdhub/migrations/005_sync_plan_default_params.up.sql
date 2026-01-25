-- SyncPlan Default Execute Params
-- Version: 005
-- Description: Add default_execute_params column to sync_plan for scheduled runs.

ALTER TABLE sync_plan ADD COLUMN default_execute_params TEXT;
