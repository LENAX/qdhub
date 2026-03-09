-- Realtime API Sync Strategy columns and seed
-- Version: 019
-- Description: Add realtime-specific columns to api_sync_strategies and set values for the 6 realtime APIs.

-- Add columns (idempotent: skip if already present in SQLite we use IF NOT EXISTS only for tables; for columns we need to check)
-- SQLite does not support IF NOT EXISTS for ALTER COLUMN; use separate ALTERs (will fail silently or need one-time migration)
ALTER TABLE api_sync_strategies ADD COLUMN realtime_ts_code_chunk_size INTEGER DEFAULT 0;
ALTER TABLE api_sync_strategies ADD COLUMN realtime_ts_code_format VARCHAR(32) DEFAULT '';
ALTER TABLE api_sync_strategies ADD COLUMN iterate_params TEXT;

-- Update rt_min: preferred_param=ts_code, required_params=["freq"], dependencies=["FetchStockBasic"], fixed_params freq=1MIN, chunk_size=50, format=comma_separated
UPDATE api_sync_strategies SET
  preferred_param = 'ts_code',
  required_params = '["freq"]',
  dependencies = '["FetchStockBasic"]',
  fixed_params = '{"freq":"1MIN"}',
  realtime_ts_code_chunk_size = 50,
  realtime_ts_code_format = 'comma_separated'
WHERE data_source_id IN (SELECT id FROM data_sources WHERE LOWER(name) = 'tushare') AND api_name = 'rt_min';

-- Update realtime_quote: preferred_param=ts_code, dependencies=["FetchStockBasic"], iterate_params={"src":["sina"]}, chunk_size=50, format=comma_separated
UPDATE api_sync_strategies SET
  preferred_param = 'ts_code',
  required_params = COALESCE(required_params, '[]'),
  dependencies = '["FetchStockBasic"]',
  iterate_params = '{"src":["sina"]}',
  realtime_ts_code_chunk_size = 50,
  realtime_ts_code_format = 'comma_separated'
WHERE data_source_id IN (SELECT id FROM data_sources WHERE LOWER(name) = 'tushare') AND api_name = 'realtime_quote';

-- Update realtime_tick: preferred_param=ts_code, dependencies=["FetchStockBasic"], chunk_size=1, format=single
UPDATE api_sync_strategies SET
  preferred_param = 'ts_code',
  dependencies = '["FetchStockBasic"]',
  realtime_ts_code_chunk_size = 1,
  realtime_ts_code_format = 'single'
WHERE data_source_id IN (SELECT id FROM data_sources WHERE LOWER(name) = 'tushare') AND api_name = 'realtime_tick';

-- Update realtime_list: preferred_param=none, dependencies=[], iterate_params={"src":["sina"]}
UPDATE api_sync_strategies SET
  preferred_param = 'none',
  dependencies = '[]',
  iterate_params = '{"src":["sina"]}',
  realtime_ts_code_chunk_size = 0,
  realtime_ts_code_format = ''
WHERE data_source_id IN (SELECT id FROM data_sources WHERE LOWER(name) = 'tushare') AND api_name = 'realtime_list';

-- Insert rt_idx_min if not exists: preferred_param=ts_code, required_params=["freq"], dependencies=["FetchIndexBasic"], fixed_params freq=1MIN, chunk_size=20, format=comma_separated
INSERT OR IGNORE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, fixed_params, description, realtime_ts_code_chunk_size, realtime_ts_code_format, created_at, updated_at)
SELECT
  lower(hex(randomblob(16))),
  ds.id,
  'rt_idx_min',
  'ts_code',
  0,
  '["freq"]',
  '["FetchIndexBasic"]',
  '{"freq":"1MIN"}',
  '指数实时分钟 - ts_code 逗号分隔，freq=1MIN',
  20,
  'comma_separated',
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare'
AND NOT EXISTS (SELECT 1 FROM api_sync_strategies a WHERE a.data_source_id = ds.id AND a.api_name = 'rt_idx_min');
