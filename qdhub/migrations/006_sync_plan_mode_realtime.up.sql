-- SyncPlan PlanMode for batch/realtime
-- Version: 006
-- Description: Add plan_mode column to sync_plan to distinguish batch and realtime modes.

ALTER TABLE sync_plan ADD COLUMN plan_mode VARCHAR(32) DEFAULT 'batch';

