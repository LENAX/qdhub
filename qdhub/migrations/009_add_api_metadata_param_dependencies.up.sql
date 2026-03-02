-- api_metadata.param_dependencies
-- Version: 009
-- Description: Add param_dependencies column to api_metadata for databases created before 001 included this column.
-- Idempotent: use runMigrationFileOrIgnoreDuplicateColumn so "duplicate column" is ignored when 001 already created the table with this column.

ALTER TABLE api_metadata ADD COLUMN param_dependencies TEXT;
