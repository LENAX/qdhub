-- Revert 027: restore pull_interval_seconds to 60 for tushare-ws-realtime-tick

UPDATE sync_plan
SET pull_interval_seconds = 60,
    updated_at = CURRENT_TIMESTAMP
WHERE id = 'tushare-ws-realtime-tick';
