-- Revert 024: 取消 Tushare WS 全市场 Tick 实时计划与 Data Store 的关联

UPDATE sync_plan
SET data_store_id = NULL
WHERE id = 'tushare-ws-realtime-tick';
