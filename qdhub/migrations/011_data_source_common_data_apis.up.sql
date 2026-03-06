-- DataSource Common Data APIs
-- Version: 011
-- Description: Add common_data_apis column to data_sources for per-datasource common data API list (e.g. trade_cal, stock_basic, index_basic for tushare).

ALTER TABLE data_sources ADD COLUMN common_data_apis TEXT;
