-- 为 Tushare WS 全市场 Tick 实时计划关联 Data Store
-- Version: 024
-- Description: 若系统中已有 quant_data_stores，将 sync_plan(id=tushare-ws-realtime-tick) 的 data_store_id
--   设为第一个 data store（按 created_at），以便执行时能解析 target_db_path。若无任何 data store 则不变，需用户在 UI 编辑计划关联。

UPDATE sync_plan
SET data_store_id = (
    SELECT id FROM quant_data_stores ORDER BY created_at ASC LIMIT 1
)
WHERE id = 'tushare-ws-realtime-tick'
  AND (data_store_id IS NULL OR data_store_id = '')
  AND EXISTS (SELECT 1 FROM quant_data_stores LIMIT 1);
