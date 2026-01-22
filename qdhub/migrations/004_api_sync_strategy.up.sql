-- API Sync Strategy Migration
-- Version: 004
-- Description: Create api_sync_strategies table for managing API synchronization configurations
--
-- This table stores sync strategies that define how each API should be synchronized:
-- - preferred_param: "none" (direct query), "trade_date" (by date), "ts_code" (by stock code)
-- - support_date_range: whether API supports start_date/end_date range queries
-- - required_params: additional required parameters (JSON array)
-- - dependencies: upstream task dependencies (JSON array)

-- API Sync Strategies table
CREATE TABLE IF NOT EXISTS api_sync_strategies (
    id               VARCHAR(64) PRIMARY KEY,
    data_source_id   VARCHAR(64) NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    api_name         VARCHAR(128) NOT NULL,
    preferred_param  VARCHAR(32) NOT NULL DEFAULT 'ts_code',  -- none/trade_date/ts_code
    support_date_range INTEGER DEFAULT 0,  -- 0: false, 1: true
    required_params  TEXT,  -- JSON array of required parameter names
    dependencies     TEXT,  -- JSON array of dependency task names
    description      TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(data_source_id, api_name)
);

CREATE INDEX IF NOT EXISTS idx_api_sync_strategies_data_source ON api_sync_strategies(data_source_id);
CREATE INDEX IF NOT EXISTS idx_api_sync_strategies_api_name ON api_sync_strategies(api_name);

-- ==================== Seed Data for Tushare API Sync Strategies ====================
-- Note: data_source_id will be resolved at runtime or by application initialization
-- For now, we use a placeholder that should be replaced with actual Tushare data source ID

-- Insert sync strategies for Tushare APIs
-- The data_source_id should be set to the actual Tushare data source ID after it's created

-- Helper: Create a temporary table to hold strategies, then insert with proper data_source_id
-- This approach allows the migration to work independently of the actual data source ID

-- ========== 无必填参数（直接查询）==========
INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'trade_cal',
    'none',
    1,
    NULL,
    NULL,
    '交易日历 - 可选参数: exchange, start_date, end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stock_basic',
    'none',
    0,
    NULL,
    NULL,
    '股票基础信息 - 可选参数: list_status, exchange 等'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'namechange',
    'none',
    1,
    NULL,
    NULL,
    '股票曾用名 - 可选参数: ts_code, start_date, end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'index_basic',
    'none',
    0,
    '["market"]',
    NULL,
    '指数基本信息 - 必填: market (SSE/SZSE/...)'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hs_const',
    'none',
    0,
    '["hs_type"]',
    NULL,
    '沪深港通成分 - 必填: hs_type (SH/SZ)'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_limit',
    'none',
    1,
    NULL,
    NULL,
    '涨跌停价格 - 可选参数: ts_code, trade_date, start_date, end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

-- ========== 支持 trade_date（按日期查询全市场）==========
INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '日线 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'weekly',
    'trade_date',
    0,
    NULL,
    '["FetchTradeCal"]',
    '周线 - 只支持 ts_code+trade_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'monthly',
    'trade_date',
    0,
    NULL,
    '["FetchTradeCal"]',
    '月线 - 只支持 ts_code+trade_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'daily_basic',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '每日指标 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'adj_factor',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '复权因子 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'top_list',
    'trade_date',
    0,
    NULL,
    '["FetchTradeCal"]',
    '龙虎榜 - trade_date 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'top_inst',
    'trade_date',
    0,
    NULL,
    '["FetchTradeCal"]',
    '龙虎榜机构 - trade_date 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'margin',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '融资融券汇总 - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'margin_detail',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '融资融券明细 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'block_trade',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '大宗交易 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'index_daily',
    'ts_code',
    1,
    '["ts_code"]',
    '["FetchTradeCal"]',
    '指数日线 - ts_code 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'index_weight',
    'ts_code',
    0,
    '["index_code"]',
    '["FetchTradeCal"]',
    '指数权重 - index_code 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

-- ========== 资金流向 API ==========
INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'moneyflow_hsgt',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '沪深港通资金流向 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'moneyflow_ind_ths',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '同花顺行业资金流向 - trade_date 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'moneyflow_cnt_ths',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '同花顺概念资金流向 - trade_date 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'moneyflow_mkt_dc',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '东财大盘资金流向 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'moneyflow_ind_dc',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '东财板块资金流向 - trade_date 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'moneyflow',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    '个股资金流向 - ts_code 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'moneyflow_ths',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    '同花顺个股资金流向 - ts_code 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'moneyflow_dc',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    '东财个股资金流向 - ts_code 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

-- ========== 龙虎榜相关 API ==========
INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hsgt_top10',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '沪深股通十大成交 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ggt_top10',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '港股通十大成交 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'limit_list_d',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '每日涨跌停榜单 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

-- ========== 同花顺概念板块 API ==========
INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ths_index',
    'none',
    0,
    NULL,
    NULL,
    '同花顺板块指数 - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ths_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '同花顺板块行情 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ths_member',
    'none',
    0,
    '["ts_code"]',
    NULL,
    '同花顺概念成分 - ts_code（板块代码）必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

-- ========== 开盘啦题材数据 API ==========
INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'kpl_list',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '开盘啦榜单 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'kpl_concept',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    '开盘啦概念题材列表 - trade_date 或 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'kpl_concept_cons',
    'none',
    0,
    '["ts_code"]',
    NULL,
    '开盘啦概念成分 - ts_code（概念代码）必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

-- ========== 必须提供 ts_code（按股票拆分）==========
INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'income',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    '利润表 - ts_code 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'balancesheet',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    '资产负债表 - ts_code 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cashflow',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    '现金流量表 - ts_code 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fina_indicator',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    '财务指标 - ts_code 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fina_mainbz',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    '主营业务构成 - ts_code 必填'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';
INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'anns_d',
    'none',
    1,
    NULL,
    NULL,
    'anns_d - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bak_basic',
    'none',
    0,
    NULL,
    NULL,
    'bak_basic - 支持 trade_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bse_mapping',
    'none',
    0,
    NULL,
    NULL,
    'bse_mapping - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cb_basic',
    'none',
    0,
    NULL,
    NULL,
    'cb_basic - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cb_call',
    'none',
    1,
    NULL,
    NULL,
    'cb_call - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cb_issue',
    'none',
    1,
    NULL,
    NULL,
    'cb_issue - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ci_index_member',
    'none',
    0,
    NULL,
    NULL,
    'ci_index_member - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cn_cpi',
    'none',
    0,
    NULL,
    NULL,
    'cn_cpi - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cn_gdp',
    'none',
    0,
    NULL,
    NULL,
    'cn_gdp - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cn_m',
    'none',
    0,
    NULL,
    NULL,
    'cn_m - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cn_pmi',
    'none',
    0,
    NULL,
    NULL,
    'cn_pmi - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cn_ppi',
    'none',
    0,
    NULL,
    NULL,
    'cn_ppi - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'dc_hot',
    'none',
    0,
    NULL,
    NULL,
    'dc_hot - 支持 trade_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'dc_member',
    'none',
    0,
    NULL,
    NULL,
    'dc_member - 支持 trade_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'disclosure_date',
    'none',
    0,
    NULL,
    NULL,
    'disclosure_date - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'dividend',
    'none',
    0,
    NULL,
    NULL,
    'dividend - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'eco_cal',
    'none',
    1,
    NULL,
    NULL,
    'eco_cal - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'etf_basic',
    'none',
    0,
    NULL,
    NULL,
    'etf_basic - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'etf_index',
    'none',
    0,
    NULL,
    NULL,
    'etf_index - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'film_record',
    'none',
    1,
    NULL,
    NULL,
    'film_record - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'forecast',
    'none',
    1,
    NULL,
    NULL,
    'forecast - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_basic',
    'none',
    0,
    NULL,
    NULL,
    'fund_basic - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_company',
    'none',
    0,
    NULL,
    NULL,
    'fund_company - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_div',
    'none',
    0,
    NULL,
    NULL,
    'fund_div - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_manager',
    'none',
    0,
    NULL,
    NULL,
    'fund_manager - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_nav',
    'none',
    1,
    NULL,
    NULL,
    'fund_nav - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_portfolio',
    'none',
    1,
    NULL,
    NULL,
    'fund_portfolio - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_sales_ratio',
    'none',
    0,
    NULL,
    NULL,
    'fund_sales_ratio - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_sales_vol',
    'none',
    0,
    NULL,
    NULL,
    'fund_sales_vol - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fut_basic',
    'none',
    0,
    '["exchange"]',
    NULL,
    'fut_basic - 必填: exchange'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fut_weekly_detail',
    'none',
    0,
    NULL,
    NULL,
    'fut_weekly_detail - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fut_weekly_monthly',
    'none',
    1,
    '["freq"]',
    NULL,
    'fut_weekly_monthly - 必填: freq | 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fx_obasic',
    'none',
    0,
    NULL,
    NULL,
    'fx_obasic - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ggt_monthly',
    'none',
    0,
    NULL,
    NULL,
    'ggt_monthly - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'gz_index',
    'none',
    1,
    NULL,
    NULL,
    'gz_index - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hibor',
    'none',
    1,
    NULL,
    NULL,
    'hibor - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_basic',
    'none',
    0,
    NULL,
    NULL,
    'hk_basic - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_tradecal',
    'none',
    1,
    NULL,
    NULL,
    'hk_tradecal - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hm_list',
    'none',
    0,
    NULL,
    NULL,
    'hm_list - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'index_classify',
    'none',
    0,
    NULL,
    NULL,
    'index_classify - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'index_member_all',
    'none',
    0,
    NULL,
    NULL,
    'index_member_all - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'libor',
    'none',
    1,
    NULL,
    NULL,
    'libor - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'major_news',
    'none',
    1,
    NULL,
    NULL,
    'major_news - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'new_share',
    'none',
    1,
    NULL,
    NULL,
    'new_share - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'news',
    'none',
    1,
    '["start_date", "end_date", "src"]',
    NULL,
    'news - 必填: start_date, end_date, src | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'npr',
    'none',
    1,
    NULL,
    NULL,
    'npr - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'opt_basic',
    'none',
    0,
    NULL,
    NULL,
    'opt_basic - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'pledge_stat',
    'none',
    0,
    NULL,
    NULL,
    'pledge_stat - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'realtime_list',
    'none',
    0,
    NULL,
    NULL,
    'realtime_list - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'realtime_quote',
    'none',
    0,
    NULL,
    NULL,
    'realtime_quote - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'realtime_tick',
    'none',
    0,
    NULL,
    NULL,
    'realtime_tick - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'report_rc',
    'none',
    1,
    NULL,
    NULL,
    'report_rc - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'repurchase',
    'none',
    1,
    NULL,
    NULL,
    'repurchase - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'sf_month',
    'none',
    0,
    NULL,
    NULL,
    'sf_month - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'sge_basic',
    'none',
    0,
    NULL,
    NULL,
    'sge_basic - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'share_float',
    'none',
    1,
    NULL,
    NULL,
    'share_float - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'shibor',
    'none',
    1,
    NULL,
    NULL,
    'shibor - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'shibor_lpr',
    'none',
    1,
    NULL,
    NULL,
    'shibor_lpr - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'shibor_quote',
    'none',
    1,
    NULL,
    NULL,
    'shibor_quote - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_account',
    'none',
    1,
    NULL,
    NULL,
    'stk_account - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_account_old',
    'none',
    1,
    NULL,
    NULL,
    'stk_account_old - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_holdernumber',
    'none',
    1,
    NULL,
    NULL,
    'stk_holdernumber - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_holdertrade',
    'none',
    1,
    NULL,
    NULL,
    'stk_holdertrade - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_managers',
    'none',
    1,
    NULL,
    NULL,
    'stk_managers - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_week_month_adj',
    'none',
    1,
    '["freq"]',
    NULL,
    'stk_week_month_adj - 必填: freq | 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_weekly_monthly',
    'none',
    1,
    '["freq"]',
    NULL,
    'stk_weekly_monthly - 必填: freq | 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stock_company',
    'none',
    0,
    NULL,
    NULL,
    'stock_company - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stock_hsgt',
    'none',
    1,
    '["type"]',
    NULL,
    'stock_hsgt - 必填: type | 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'tdx_index',
    'none',
    0,
    NULL,
    NULL,
    'tdx_index - 支持 trade_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'tdx_member',
    'none',
    0,
    NULL,
    NULL,
    'tdx_member - 支持 trade_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'teleplay_record',
    'none',
    1,
    NULL,
    NULL,
    'teleplay_record - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ths_hot',
    'none',
    0,
    NULL,
    NULL,
    'ths_hot - 支持 trade_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'tmt_twincome',
    'none',
    1,
    '["item"]',
    NULL,
    'tmt_twincome - 必填: item | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'tmt_twincomedetail',
    'none',
    1,
    NULL,
    NULL,
    'tmt_twincomedetail - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_basic',
    'none',
    0,
    NULL,
    NULL,
    'us_basic - 无必填参数'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_tbr',
    'none',
    1,
    NULL,
    NULL,
    'us_tbr - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_tltr',
    'none',
    1,
    NULL,
    NULL,
    'us_tltr - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_tradecal',
    'none',
    1,
    NULL,
    NULL,
    'us_tradecal - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_trltr',
    'none',
    1,
    NULL,
    NULL,
    'us_trltr - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_trycr',
    'none',
    1,
    NULL,
    NULL,
    'us_trycr - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_tycr',
    'none',
    1,
    NULL,
    NULL,
    'us_tycr - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'wz_index',
    'none',
    1,
    NULL,
    NULL,
    'wz_index - 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';



-- ========== preferred_param: trade_date ==========
INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bak_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'bak_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bc_bestotcqt',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'bc_bestotcqt - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bc_otcqt',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'bc_otcqt - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bo_cinema',
    'trade_date',
    0,
    '["date"]',
    '["FetchTradeCal"]',
    'bo_cinema - 必填: date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bo_daily',
    'trade_date',
    0,
    '["date"]',
    '["FetchTradeCal"]',
    'bo_daily - 必填: date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bo_monthly',
    'trade_date',
    0,
    '["date"]',
    '["FetchTradeCal"]',
    'bo_monthly - 必填: date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bo_weekly',
    'trade_date',
    0,
    '["date"]',
    '["FetchTradeCal"]',
    'bo_weekly - 必填: date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bond_blk',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'bond_blk - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'bond_blk_detail',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'bond_blk_detail - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'broker_recommend',
    'trade_date',
    0,
    '["month"]',
    '["FetchTradeCal"]',
    'broker_recommend - 必填: month'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cb_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'cb_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cb_factor_pro',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'cb_factor_pro - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ccass_hold',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'ccass_hold - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ccass_hold_detail',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'ccass_hold_detail - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cctv_news',
    'trade_date',
    0,
    '["date"]',
    '["FetchTradeCal"]',
    'cctv_news - 必填: date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ci_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'ci_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'daily_info',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'daily_info - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'dc_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'dc_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'dc_index',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'dc_index - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'etf_share_size',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'etf_share_size - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ft_limit',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'ft_limit - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_adj',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'fund_adj - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'fund_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_factor_pro',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'fund_factor_pro - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fund_share',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'fund_share - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fut_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'fut_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fut_holding',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'fut_holding - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fut_mapping',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'fut_mapping - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fut_settle',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'fut_settle - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fut_wsr',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'fut_wsr - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fx_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'fx_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ggt_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'ggt_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_adjfactor',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'hk_adjfactor - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'hk_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_daily_adj',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'hk_daily_adj - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_hold',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'hk_hold - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hm_detail',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'hm_detail - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'idx_factor_pro',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'idx_factor_pro - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'index_dailybasic',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'index_dailybasic - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'index_global',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'index_global - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'index_monthly',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'index_monthly - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'index_weekly',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'index_weekly - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'irm_qa_sh',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'irm_qa_sh - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'irm_qa_sz',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'irm_qa_sz - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'limit_cpt_list',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'limit_cpt_list - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'limit_list_ths',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'limit_list_ths - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'limit_step',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'limit_step - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'margin_secs',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'margin_secs - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'opt_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'opt_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'repo_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'repo_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'sge_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'sge_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'slb_len',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'slb_len - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'slb_len_mm',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'slb_len_mm - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'slb_sec',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'slb_sec - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'slb_sec_detail',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'slb_sec_detail - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_ah_comparison',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'stk_ah_comparison - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_auction',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'stk_auction - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_auction_c',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'stk_auction_c - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_auction_o',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'stk_auction_o - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_factor_pro',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'stk_factor_pro - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_nineturn',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'stk_nineturn - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_premarket',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'stk_premarket - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_surv',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'stk_surv - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stock_st',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'stock_st - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'suspend_d',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'suspend_d - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'sw_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'sw_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'sz_daily_info',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'sz_daily_info - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'tdx_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'tdx_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_adjfactor',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'us_adjfactor - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_daily',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'us_daily - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_daily_adj',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'us_daily_adj - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'yc_cb',
    'trade_date',
    1,
    NULL,
    '["FetchTradeCal"]',
    'yc_cb - 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';



-- ========== preferred_param: ts_code ==========
INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cb_price_chg',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    'cb_price_chg - 必填: ts_code'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cb_rate',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    'cb_rate - 必填: ts_code'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cb_share',
    'ts_code',
    1,
    '["ann_date"]',
    '["FetchStockBasic"]',
    'cb_share - 必填: ts_code, ann_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cyq_chips',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'cyq_chips - 必填: ts_code | 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'cyq_perf',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'cyq_perf - 必填: ts_code | 支持 trade_date | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'express',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'express - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'fina_audit',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'fina_audit - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'ft_mins',
    'ts_code',
    1,
    '["freq"]',
    '["FetchStockBasic"]',
    'ft_mins - 必填: ts_code, freq | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_balancesheet',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'hk_balancesheet - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_cashflow',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'hk_cashflow - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_fina_indicator',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'hk_fina_indicator - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_income',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'hk_income - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'hk_mins',
    'ts_code',
    1,
    '["freq"]',
    '["FetchStockBasic"]',
    'hk_mins - 必填: ts_code, freq | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'opt_mins',
    'ts_code',
    1,
    '["freq"]',
    '["FetchStockBasic"]',
    'opt_mins - 必填: ts_code, freq | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'pledge_detail',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    'pledge_detail - 必填: ts_code'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'rt_etf_k',
    'ts_code',
    0,
    '["topic"]',
    '["FetchStockBasic"]',
    'rt_etf_k - 必填: ts_code, topic'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'rt_fut_min',
    'ts_code',
    0,
    '["freq"]',
    '["FetchStockBasic"]',
    'rt_fut_min - 必填: ts_code, freq'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'rt_hk_k',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    'rt_hk_k - 必填: ts_code'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'rt_idx_k',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    'rt_idx_k - 必填: ts_code'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'rt_k',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    'rt_k - 必填: ts_code'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'rt_min',
    'ts_code',
    0,
    '["freq"]',
    '["FetchStockBasic"]',
    'rt_min - 必填: freq, ts_code'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_mins',
    'ts_code',
    1,
    '["freq"]',
    '["FetchStockBasic"]',
    'stk_mins - 必填: ts_code, freq | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'stk_rewards',
    'ts_code',
    0,
    NULL,
    '["FetchStockBasic"]',
    'stk_rewards - 必填: ts_code'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'top10_floatholders',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'top10_floatholders - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'top10_holders',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'top10_holders - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_balancesheet',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'us_balancesheet - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_cashflow',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'us_cashflow - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_fina_indicator',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'us_fina_indicator - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';


INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    'us_income',
    'ts_code',
    1,
    NULL,
    '["FetchStockBasic"]',
    'us_income - 必填: ts_code | 支持 start_date+end_date'
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';



-- 共生成 183 个策略配置
