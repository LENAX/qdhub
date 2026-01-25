-- Rollback: remove default_execute_params from sync_plan

ALTER TABLE sync_plan DROP COLUMN default_execute_params;
