-- Seed default realtime data sources (SQLite)
-- Version: 029
-- Description: Insert four default realtime sources. Priority: ts_proxy=1, sina=2, dc=3, tushare_ws=4. tushare_ws enabled=0 (HK cannot reach mainland).

INSERT OR IGNORE INTO realtime_sources (id, name, type, config, priority, is_primary, health_check_on_startup, enabled, created_at, updated_at)
VALUES
    ('aaaaaaaa-0001-4000-8000-000000000001', 'Tushare 转发', 'tushare_forward', '{"ws_url":"","rsa_public_key_path":"/root/.key/public.pem"}', 1, 1, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('aaaaaaaa-0002-4000-8000-000000000002', '新浪', 'sina', '{}', 2, 1, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('aaaaaaaa-0003-4000-8000-000000000003', '东财', 'eastmoney', '{}', 3, 0, 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('aaaaaaaa-0004-4000-8000-000000000004', 'Tushare 直连', 'tushare_ws', '{"endpoint":"wss://ws.tushare.pro/listening","token":""}', 4, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
