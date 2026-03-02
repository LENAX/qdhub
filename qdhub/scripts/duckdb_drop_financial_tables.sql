-- 针对 DuckDB 量化库（如 qdhub/data/qdhub.db）执行，仅删除三张财报表以便按新主键重建。
-- 执行后需在应用内对该三张表重新「建表」或重新同步，表会按元数据以复合主键
-- (ts_code, end_date, report_type, comp_type) 重建，避免多期财报被覆盖成一条。

-- 用法示例（在项目根目录）:
--   duckdb qdhub/data/qdhub.db < qdhub/scripts/duckdb_drop_financial_tables.sql

DROP TABLE IF EXISTS income;
DROP TABLE IF EXISTS balancesheet;
DROP TABLE IF EXISTS cashflow;
