-- Rollback: remove common_data_apis from data_sources

ALTER TABLE data_sources DROP COLUMN common_data_apis;
