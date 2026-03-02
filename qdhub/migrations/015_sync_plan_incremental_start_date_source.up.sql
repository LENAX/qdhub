-- SyncPlan Incremental Start Date Source
-- Version: 015
-- Description: Add incremental_start_date_api and incremental_start_date_column for optional "data latest date" from target DuckDB (MAX(column) on specified table).

ALTER TABLE sync_plan ADD COLUMN incremental_start_date_api VARCHAR(128);
ALTER TABLE sync_plan ADD COLUMN incremental_start_date_column VARCHAR(128);
