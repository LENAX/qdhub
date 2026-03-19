-- Fix and normalize default realtime_sources seed for SQLite
-- Version: 030
-- Description:
-- 1) Rename \"Tushare 转发\" display name to a concise ts_proxy-style name (keep type=tushare_proxy).
-- 2) Ensure three core realtime sources exist with expected flags:
--      - ts_proxy  (type=tushare_proxy) : is_primary=1, enabled=1,  health_check_on_startup=1
--      - sina        (type=sina)            : is_primary=1, enabled=1,  health_check_on_startup=0
--      - tushare_ws  (type=tushare_ws)      : is_primary=0, enabled=0,  health_check_on_startup=0
--    (eastmoney remains as an optional non-primary source if present)
-- 3) Reset tushare_proxy config to minimal JSON without hardcoded /root paths;
--    actual ws_url & rsa_public_key_path are provided via application config/env.

-- 1) Rename existing tushare_proxy row display name (if present).
UPDATE realtime_sources
SET name = 'ts_proxy'
WHERE type = 'tushare_proxy';

-- 2) Ensure core rows exist with expected flags (INSERT OR IGNORE keeps existing ids if already present).
INSERT OR IGNORE INTO realtime_sources (
    id, name, type, config, priority, is_primary, health_check_on_startup, enabled,
    last_health_status, last_health_at, last_health_error, created_at, updated_at
)
VALUES
    (
      'aaaaaaaa-0001-4000-8000-000000000001',
      'ts_proxy',
      'tushare_proxy',
      '{\"ws_url\":\"\",\"rsa_public_key_path\":\"\"}',
      1,
      1,
      1,
      1,
      NULL,
      NULL,
      '',
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP
    ),
    (
      'aaaaaaaa-0002-4000-8000-000000000002',
      '新浪',
      'sina',
      '{}',
      2,
      1,
      0,
      1,
      NULL,
      NULL,
      '',
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP
    );

-- 3) Normalize existing core rows flags & config to the expected defaults.
UPDATE realtime_sources
SET
    name                    = 'ts_proxy',
    config                  = '{\"ws_url\":\"\",\"rsa_public_key_path\":\"\"}',
    priority                = 1,
    is_primary              = 1,
    health_check_on_startup = 1,
    enabled                 = 1
WHERE id = 'aaaaaaaa-0001-4000-8000-000000000001';

UPDATE realtime_sources
SET
    name                    = '新浪',
    config                  = '{}',
    priority                = 2,
    is_primary              = 1,
    health_check_on_startup = 0,
    enabled                 = 1
WHERE id = 'aaaaaaaa-0002-4000-8000-000000000002';
