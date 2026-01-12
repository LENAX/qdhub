-- QDHub Schema Rollback (Merged with Task Engine)
-- Version: 001
-- Description: Drop all core tables

-- Drop QDHub specific tables first (due to foreign key constraints)
DROP TABLE IF EXISTS sync_executions;
DROP TABLE IF EXISTS sync_jobs;
DROP TABLE IF EXISTS data_type_mapping_rules;
DROP TABLE IF EXISTS table_schemas;
DROP TABLE IF EXISTS quant_data_stores;
DROP TABLE IF EXISTS tokens;
DROP TABLE IF EXISTS api_metadata;
DROP TABLE IF EXISTS api_categories;
DROP TABLE IF EXISTS data_sources;

-- Drop Task Engine tables
DROP TABLE IF EXISTS task_instance;
DROP TABLE IF EXISTS workflow_instance;
DROP TABLE IF EXISTS task_definition;
DROP TABLE IF EXISTS workflow_definition;
DROP TABLE IF EXISTS task_handler_meta;
DROP TABLE IF EXISTS job_function_meta;
