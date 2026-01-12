-- QDHub Schema Rollback
-- Version: 001
-- Description: Drop all core tables

DROP TABLE IF EXISTS task_instances;
DROP TABLE IF EXISTS sync_executions;
DROP TABLE IF EXISTS workflow_instances;
DROP TABLE IF EXISTS sync_jobs;
DROP TABLE IF EXISTS workflow_definitions;
DROP TABLE IF EXISTS data_type_mapping_rules;
DROP TABLE IF EXISTS table_schemas;
DROP TABLE IF EXISTS quant_data_stores;
DROP TABLE IF EXISTS tokens;
DROP TABLE IF EXISTS api_metadata;
DROP TABLE IF EXISTS api_categories;
DROP TABLE IF EXISTS data_sources;
