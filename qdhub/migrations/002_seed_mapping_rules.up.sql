-- Seed default data type mapping rules
-- Version: 002
-- Description: Insert default mapping rules for tushare data source to various target databases
--
-- NOTE: These are the default mapping rules. Users can add custom rules via the API.
-- This migration uses INSERT OR IGNORE for SQLite, ON CONFLICT for PostgreSQL.
-- For MySQL, use INSERT IGNORE (requires modifying the script or using a preprocessor).

-- Tushare -> DuckDB mappings
INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-str-duckdb', 'tushare', 'str', 'duckdb', 'VARCHAR', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-float-duckdb', 'tushare', 'float', 'duckdb', 'DOUBLE', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-int-duckdb', 'tushare', 'int', 'duckdb', 'BIGINT', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-datetime-duckdb', 'tushare', 'datetime', 'duckdb', 'TIMESTAMP', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-date-duckdb', 'tushare', 'date', 'duckdb', 'DATE', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Tushare -> ClickHouse mappings
INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-str-clickhouse', 'tushare', 'str', 'clickhouse', 'String', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-float-clickhouse', 'tushare', 'float', 'clickhouse', 'Float64', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-int-clickhouse', 'tushare', 'int', 'clickhouse', 'Int64', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-datetime-clickhouse', 'tushare', 'datetime', 'clickhouse', 'DateTime', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-date-clickhouse', 'tushare', 'date', 'clickhouse', 'Date', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Tushare -> PostgreSQL mappings
INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-str-postgres', 'tushare', 'str', 'postgres', 'TEXT', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-float-postgres', 'tushare', 'float', 'postgres', 'DOUBLE PRECISION', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-int-postgres', 'tushare', 'int', 'postgres', 'BIGINT', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-datetime-postgres', 'tushare', 'datetime', 'postgres', 'TIMESTAMP', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT OR IGNORE INTO data_type_mapping_rules (id, data_source_type, source_type, target_db_type, target_type, priority, is_default, created_at, updated_at)
VALUES ('rule-tushare-date-postgres', 'tushare', 'date', 'postgres', 'DATE', 100, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
