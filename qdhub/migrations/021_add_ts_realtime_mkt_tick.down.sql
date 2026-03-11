-- rollback for 021_add_ts_realtime_mkt_tick

DELETE FROM api_sync_strategies
WHERE api_name = 'ts_realtime_mkt_tick'
  AND data_source_id IN (SELECT id FROM data_sources WHERE LOWER(name) = 'tushare');

DELETE FROM api_metadata
WHERE name = 'ts_realtime_mkt_tick'
  AND data_source_id IN (SELECT id FROM data_sources WHERE LOWER(name) = 'tushare');
