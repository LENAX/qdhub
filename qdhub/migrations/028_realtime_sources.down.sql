-- Rollback realtime_sources table
DROP INDEX IF EXISTS idx_realtime_sources_enabled_priority;
DROP INDEX IF EXISTS idx_realtime_sources_type;
DROP TABLE IF EXISTS realtime_sources;
