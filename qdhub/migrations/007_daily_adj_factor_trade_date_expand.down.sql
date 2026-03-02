-- Rollback: 恢复 daily/adj_factor 的 support_date_range 为 1

UPDATE api_sync_strategies
SET support_date_range = 1,
    updated_at = CURRENT_TIMESTAMP,
    description = '日线 - trade_date 或 start_date+end_date'
WHERE api_name = 'daily';

UPDATE api_sync_strategies
SET support_date_range = 1,
    updated_at = CURRENT_TIMESTAMP,
    description = '复权因子 - trade_date 或 start_date+end_date'
WHERE api_name = 'adj_factor';
