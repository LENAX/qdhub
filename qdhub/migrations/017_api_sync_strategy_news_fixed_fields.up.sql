-- API Sync Strategy Fixed Fields for news/major_news
-- Version: 017
-- Description: Seed fixed_params and fixed_param_keys for Tushare news & major_news
--              so that fields are explicitly requested (including content).

-- news 快讯：对齐官方 WebClient 请求字段
UPDATE api_sync_strategies
SET
    fixed_params = '{"fields":"datetime,content,title,channels,score"}',
    fixed_param_keys = '["fields"]',
    updated_at = CURRENT_TIMESTAMP
WHERE api_name = 'news'
  AND data_source_id IN (
      SELECT id FROM data_sources WHERE LOWER(name) = 'tushare'
  );

-- major_news 通讯：对齐官方 WebClient 请求字段
UPDATE api_sync_strategies
SET
    fixed_params = '{"fields":"title,pub_time,src,url,content"}',
    fixed_param_keys = '["fields"]',
    updated_at = CURRENT_TIMESTAMP
WHERE api_name = 'major_news'
  AND data_source_id IN (
      SELECT id FROM data_sources WHERE LOWER(name) = 'tushare'
  );

