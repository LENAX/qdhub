-- API Sync Strategy Time Window Config for news / major_news / cctv_news
-- Version: 018
-- Description: Seed time window related configs into api_sync_strategies.fixed_params
--              so that the new time-window strategies are visible at DB level.

-- news: 快讯，按自然日（D）+ src 维度拆分，时间窗口策略仅用于内部实现，这里记录到 fixed_params.time_window 方便查看。
UPDATE api_sync_strategies
SET
    fixed_params = json_set(
        COALESCE(NULLIF(fixed_params, ''), '{}'),
        '$.time_window',
        json('{"enabled":true,"freq":"D","date_param_key":"","use_trade_calendar":false}')
    ),
    updated_at = CURRENT_TIMESTAMP
WHERE api_name = 'news'
  AND data_source_id IN (
      SELECT id FROM data_sources WHERE LOWER(name) = 'tushare'
  );

-- major_news: 通讯，按 3 小时窗口 + src 维度拆分。
UPDATE api_sync_strategies
SET
    fixed_params = json_set(
        COALESCE(NULLIF(fixed_params, ''), '{}'),
        '$.time_window',
        json('{"enabled":true,"freq":"3H","date_param_key":"","use_trade_calendar":false}')
    ),
    updated_at = CURRENT_TIMESTAMP
WHERE api_name = 'major_news'
  AND data_source_id IN (
      SELECT id FROM data_sources WHERE LOWER(name) = 'tushare'
  );

-- cctv_news: 新闻联播，按自然日逐日，同步参数为 date=YYYYMMDD。
-- 若不存在策略记录，则插入一条；若已存在，仅补充 fixed_params.time_window。
INSERT INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, fixed_params, fixed_param_keys, description, created_at, updated_at)
SELECT
    hex(randomblob(16))                          AS id,
    ds.id                                        AS data_source_id,
    'cctv_news'                                  AS api_name,
    'none'                                       AS preferred_param,
    0                                            AS support_date_range,
    json('[]')                                   AS required_params,
    json('[]')                                   AS dependencies,
    json('{"time_window":{"enabled":true,"freq":"D","date_param_key":"date","use_trade_calendar":false}}') AS fixed_params,
    json('[]')                                   AS fixed_param_keys,
    '新闻联播：按自然日逐日，使用 date=YYYYMMDD 参数'                                    AS description,
    CURRENT_TIMESTAMP                            AS created_at,
    CURRENT_TIMESTAMP                            AS updated_at
FROM data_sources ds
WHERE LOWER(ds.name) = 'tushare'
  AND NOT EXISTS (
      SELECT 1 FROM api_sync_strategies s
      WHERE s.data_source_id = ds.id AND s.api_name = 'cctv_news'
  );

-- 如果已经存在 cctv_news 策略，则仅补充 fixed_params.time_window。
UPDATE api_sync_strategies
SET
    fixed_params = json_set(
        COALESCE(NULLIF(fixed_params, ''), '{}'),
        '$.time_window',
        json('{"enabled":true,"freq":"D","date_param_key":"date","use_trade_calendar":false}')
    ),
    preferred_param = 'none',
    support_date_range = 0,
    updated_at = CURRENT_TIMESTAMP
WHERE api_name = 'cctv_news'
  AND data_source_id IN (
      SELECT id FROM data_sources WHERE LOWER(name) = 'tushare'
  );

