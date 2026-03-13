-- Revert 023: remove tushare WS realtime tick sync plan seed

DELETE FROM sync_plan WHERE id = 'tushare-ws-realtime-tick';
