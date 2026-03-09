-- Revert 020: remove pull_interval_seconds, schedule_start_cron, schedule_end_cron from sync_plan

-- SQLite does not support DROP COLUMN in older versions; leave columns for backward compatibility or use rebuild.
-- For SQLite 3.35+ (2021): ALTER TABLE sync_plan DROP COLUMN pull_interval_seconds; etc.
-- Omit down migration if not needed for rollback.
