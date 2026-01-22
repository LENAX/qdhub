-- API Sync Strategy Migration Rollback
-- Version: 004
-- Description: Drop api_sync_strategies table

DROP INDEX IF EXISTS idx_api_sync_strategies_api_name;
DROP INDEX IF EXISTS idx_api_sync_strategies_data_source;
DROP TABLE IF EXISTS api_sync_strategies;
