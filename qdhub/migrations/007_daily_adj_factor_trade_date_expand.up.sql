-- Daily/AdjFactor 按 trade_date 扩展日期范围
-- Version: 007
-- Description: 对 daily、adj_factor 将 support_date_range 设为 0，使 date range 通过 trade_cal 的 cal_date 截取为多个 trade_date 逐日拉取（传入单日 trade_date 仅能获取一天数据）

UPDATE api_sync_strategies
SET support_date_range = 0,
    updated_at = CURRENT_TIMESTAMP,
    description = '日线 - 按 trade_date 从 trade_cal 截取 [start_date,end_date] 内交易日逐日拉取'
WHERE api_name = 'daily';

UPDATE api_sync_strategies
SET support_date_range = 0,
    updated_at = CURRENT_TIMESTAMP,
    description = '复权因子 - 按 trade_date 从 trade_cal 截取日期范围逐日拉取'
WHERE api_name = 'adj_factor';
