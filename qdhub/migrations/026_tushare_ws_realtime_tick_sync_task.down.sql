-- Revert 026: remove sync_task(s) for tushare-ws-realtime-tick added by this migration

DELETE FROM sync_task WHERE sync_plan_id = 'tushare-ws-realtime-tick' AND id = 'tushare-ws-realtime-tick-ts_realtime_mkt_tick';
