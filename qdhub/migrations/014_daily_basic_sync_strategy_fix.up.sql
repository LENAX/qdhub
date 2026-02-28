-- daily_basic 改为按 trade_date 逐日同步：Tushare 单次最多 6000 条，用 start_date/end_date 大范围只拿到约一天数据
UPDATE api_sync_strategies SET support_date_range = 0 WHERE api_name = 'daily_basic';
