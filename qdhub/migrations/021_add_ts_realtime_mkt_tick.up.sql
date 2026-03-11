-- Add tushare ws realtime tick api metadata and strategy
-- Version: 021

INSERT OR IGNORE INTO api_metadata (
    id, data_source_id, category_id, name, display_name, description, endpoint,
    request_params, response_fields, rate_limit, permission, param_dependencies, status, created_at, updated_at
)
SELECT
    lower(hex(randomblob(16))) AS id,
    ds.id AS data_source_id,
    COALESCE(
        (SELECT c.id FROM api_categories c WHERE c.data_source_id = ds.id AND (c.name LIKE '%实时%' OR c.name LIKE '%行情%') LIMIT 1),
        (SELECT c.id FROM api_categories c WHERE c.data_source_id = ds.id ORDER BY c.sort_order ASC, c.created_at ASC LIMIT 1)
    ) AS category_id,
    'ts_realtime_mkt_tick' AS name,
    'tushare全市场数据' AS display_name,
    'tushare WS 全市场 tick 行情（HQ_STK_TICK）' AS description,
    'tushare_ws_realtime_tick' AS endpoint,
    '[{"name":"topic","type":"string","required":false,"default":"HQ_STK_TICK"},{"name":"codes","type":"array[string]","required":false,"default":["3*.SZ","0*.SZ","6*.SH"]}]' AS request_params,
    '[' ||
      '{"name":"code","type":"string","description":"证券代码","is_primary":true},' ||
      '{"name":"ts_code","type":"string","description":"证券代码（标准化字段）","is_primary":true},' ||
      '{"name":"name","type":"string","description":"证券名称"},' ||
      '{"name":"trade_time","type":"datetime","description":"交易时间","is_primary":true},' ||
      '{"name":"pre_price","type":"double","description":"昨收价"},' ||
      '{"name":"price","type":"double","description":"现价"},' ||
      '{"name":"open","type":"double","description":"开盘价"},' ||
      '{"name":"high","type":"double","description":"最高价"},' ||
      '{"name":"low","type":"double","description":"最低价"},' ||
      '{"name":"close","type":"double","description":"收盘价"},' ||
      '{"name":"open_int","type":"double","description":"持仓量（股票通常为空）"},' ||
      '{"name":"volume","type":"double","description":"成交量"},' ||
      '{"name":"amount","type":"double","description":"成交额"},' ||
      '{"name":"num","type":"double","description":"笔数"},' ||
      '{"name":"ask_price1","type":"double","description":"委卖价1"},' ||
      '{"name":"ask_volume1","type":"double","description":"委卖量1"},' ||
      '{"name":"bid_price1","type":"double","description":"委买价1"},' ||
      '{"name":"bid_volume1","type":"double","description":"委买量1"},' ||
      '{"name":"ask_price2","type":"double","description":"委卖价2"},' ||
      '{"name":"ask_volume2","type":"double","description":"委卖量2"},' ||
      '{"name":"bid_price2","type":"double","description":"委买价2"},' ||
      '{"name":"bid_volume2","type":"double","description":"委买量2"},' ||
      '{"name":"ask_price3","type":"double","description":"委卖价3"},' ||
      '{"name":"ask_volume3","type":"double","description":"委卖量3"},' ||
      '{"name":"bid_price3","type":"double","description":"委买价3"},' ||
      '{"name":"bid_volume3","type":"double","description":"委买量3"},' ||
      '{"name":"ask_price4","type":"double","description":"委卖价4"},' ||
      '{"name":"ask_volume4","type":"double","description":"委卖量4"},' ||
      '{"name":"bid_price4","type":"double","description":"委买价4"},' ||
      '{"name":"bid_volume4","type":"double","description":"委买量4"},' ||
      '{"name":"ask_price5","type":"double","description":"委卖价5"},' ||
      '{"name":"ask_volume5","type":"double","description":"委卖量5"},' ||
      '{"name":"bid_price5","type":"double","description":"委买价5"},' ||
      '{"name":"bid_volume5","type":"double","description":"委买量5"}' ||
    ']' AS response_fields,
    '{"limit":"根据 tushare 账户权限"}' AS rate_limit,
    'realtime' AS permission,
    '[]' AS param_dependencies,
    'active' AS status,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM data_sources ds
WHERE LOWER(ds.name) = 'tushare'
  AND NOT EXISTS (
      SELECT 1 FROM api_metadata am
      WHERE am.data_source_id = ds.id AND am.name = 'ts_realtime_mkt_tick'
  );

UPDATE api_metadata
SET
    display_name = 'tushare全市场数据',
    description = 'tushare WS 全市场 tick 行情（HQ_STK_TICK）',
    endpoint = 'tushare_ws_realtime_tick',
    request_params = '[{"name":"topic","type":"string","required":false,"default":"HQ_STK_TICK"},{"name":"codes","type":"array[string]","required":false,"default":["3*.SZ","0*.SZ","6*.SH"]}]',
    response_fields = '[' ||
      '{"name":"code","type":"string","description":"证券代码","is_primary":true},' ||
      '{"name":"ts_code","type":"string","description":"证券代码（标准化字段）","is_primary":true},' ||
      '{"name":"name","type":"string","description":"证券名称"},' ||
      '{"name":"trade_time","type":"datetime","description":"交易时间","is_primary":true},' ||
      '{"name":"pre_price","type":"double","description":"昨收价"},' ||
      '{"name":"price","type":"double","description":"现价"},' ||
      '{"name":"open","type":"double","description":"开盘价"},' ||
      '{"name":"high","type":"double","description":"最高价"},' ||
      '{"name":"low","type":"double","description":"最低价"},' ||
      '{"name":"close","type":"double","description":"收盘价"},' ||
      '{"name":"open_int","type":"double","description":"持仓量（股票通常为空）"},' ||
      '{"name":"volume","type":"double","description":"成交量"},' ||
      '{"name":"amount","type":"double","description":"成交额"},' ||
      '{"name":"num","type":"double","description":"笔数"},' ||
      '{"name":"ask_price1","type":"double","description":"委卖价1"},' ||
      '{"name":"ask_volume1","type":"double","description":"委卖量1"},' ||
      '{"name":"bid_price1","type":"double","description":"委买价1"},' ||
      '{"name":"bid_volume1","type":"double","description":"委买量1"},' ||
      '{"name":"ask_price2","type":"double","description":"委卖价2"},' ||
      '{"name":"ask_volume2","type":"double","description":"委卖量2"},' ||
      '{"name":"bid_price2","type":"double","description":"委买价2"},' ||
      '{"name":"bid_volume2","type":"double","description":"委买量2"},' ||
      '{"name":"ask_price3","type":"double","description":"委卖价3"},' ||
      '{"name":"ask_volume3","type":"double","description":"委卖量3"},' ||
      '{"name":"bid_price3","type":"double","description":"委买价3"},' ||
      '{"name":"bid_volume3","type":"double","description":"委买量3"},' ||
      '{"name":"ask_price4","type":"double","description":"委卖价4"},' ||
      '{"name":"ask_volume4","type":"double","description":"委卖量4"},' ||
      '{"name":"bid_price4","type":"double","description":"委买价4"},' ||
      '{"name":"bid_volume4","type":"double","description":"委买量4"},' ||
      '{"name":"ask_price5","type":"double","description":"委卖价5"},' ||
      '{"name":"ask_volume5","type":"double","description":"委卖量5"},' ||
      '{"name":"bid_price5","type":"double","description":"委买价5"},' ||
      '{"name":"bid_volume5","type":"double","description":"委买量5"}' ||
    ']',
    updated_at = CURRENT_TIMESTAMP
WHERE name = 'ts_realtime_mkt_tick'
  AND data_source_id IN (SELECT id FROM data_sources WHERE LOWER(name) = 'tushare');

INSERT OR IGNORE INTO api_sync_strategies (
    id, data_source_id, api_name, preferred_param, support_date_range, required_params,
    dependencies, fixed_params, fixed_param_keys, description,
    realtime_ts_code_chunk_size, realtime_ts_code_format, iterate_params, created_at, updated_at
)
SELECT
    lower(hex(randomblob(16))) AS id,
    ds.id AS data_source_id,
    'ts_realtime_mkt_tick' AS api_name,
    'none' AS preferred_param,
    0 AS support_date_range,
    '[]' AS required_params,
    '[]' AS dependencies,
    '{"topic":"HQ_STK_TICK","codes":["3*.SZ","0*.SZ","6*.SH"]}' AS fixed_params,
    '["topic","codes"]' AS fixed_param_keys,
    'tushare ws 全市场 tick 实时行情（可通过 fixed_params 配置 topic/codes）' AS description,
    0 AS realtime_ts_code_chunk_size,
    '' AS realtime_ts_code_format,
    NULL AS iterate_params,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM data_sources ds
WHERE LOWER(ds.name) = 'tushare'
  AND NOT EXISTS (
      SELECT 1 FROM api_sync_strategies s
      WHERE s.data_source_id = ds.id AND s.api_name = 'ts_realtime_mkt_tick'
  );
