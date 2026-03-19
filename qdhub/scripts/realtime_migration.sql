-- Realtime DuckDB 建表脚本（realtime_ticks.duckdb / realtime_duckdb）
-- 用途：为 ts_proxy、sina、news 实时数据建表；幂等，可重复执行。
--
-- 执行方式（在 qdhub 项目根目录）：
--   duckdb data/realtime_ticks.duckdb < scripts/realtime_migration.sql
-- 或先创建库：
--   mkdir -p data && duckdb data/realtime_ticks.duckdb -c "$(cat scripts/realtime_migration.sql)"

-- 1) sina 实时行情
CREATE TABLE IF NOT EXISTS realtime_quote(
    "name" VARCHAR,
    ts_code VARCHAR,
    date VARCHAR,
    "time" VARCHAR,
    open DOUBLE,
    pre_close DOUBLE,
    price DOUBLE,
    high DOUBLE,
    low DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    volume BIGINT,
    amount DOUBLE,
    b1_v DOUBLE,
    b1_p DOUBLE,
    b2_v DOUBLE,
    b2_p DOUBLE,
    b3_v DOUBLE,
    b3_p DOUBLE,
    b4_v DOUBLE,
    b4_p DOUBLE,
    b5_v DOUBLE,
    b5_p DOUBLE,
    a1_v DOUBLE,
    a1_p DOUBLE,
    a2_v DOUBLE,
    a2_p DOUBLE,
    a3_v DOUBLE,
    a3_p DOUBLE,
    a4_v DOUBLE,
    a4_p DOUBLE,
    a5_v DOUBLE,
    a5_p DOUBLE,
    PRIMARY KEY(ts_code, date, "time")
);
CREATE INDEX IF NOT EXISTS idx_realtime_quote_ts_code ON realtime_quote(ts_code);
CREATE INDEX IF NOT EXISTS idx_realtime_quote_date_time ON realtime_quote(date, "time");

-- 2) ts_proxy 实时行情（ts_realtime_mkt_tick 写入此表）
CREATE TABLE IF NOT EXISTS ts_proxy_realtime_quote(
    code VARCHAR,
    ts_code VARCHAR,
    "name" VARCHAR,
    trade_time TIMESTAMP,
    pre_price DOUBLE,
    price DOUBLE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    "close" DOUBLE,
    open_int DOUBLE,
    volume DOUBLE,
    amount DOUBLE,
    num DOUBLE,
    ask_price1 DOUBLE,
    ask_volume1 DOUBLE,
    bid_price1 DOUBLE,
    bid_volume1 DOUBLE,
    ask_price2 DOUBLE,
    ask_volume2 DOUBLE,
    bid_price2 DOUBLE,
    bid_volume2 DOUBLE,
    ask_price3 DOUBLE,
    ask_volume3 DOUBLE,
    bid_price3 DOUBLE,
    bid_volume3 DOUBLE,
    ask_price4 DOUBLE,
    ask_volume4 DOUBLE,
    bid_price4 DOUBLE,
    bid_volume4 DOUBLE,
    ask_price5 DOUBLE,
    ask_volume5 DOUBLE,
    bid_price5 DOUBLE,
    bid_volume5 DOUBLE,
    sync_batch_id VARCHAR,
    created_at TIMESTAMP,
    PRIMARY KEY (ts_code, trade_time)
);
CREATE INDEX IF NOT EXISTS idx_ts_proxy_realtime_quote_trade_time ON ts_proxy_realtime_quote(trade_time);
CREATE INDEX IF NOT EXISTS idx_ts_proxy_realtime_quote_code ON ts_proxy_realtime_quote(code);

-- 3) 新闻快讯（news_realtime_sync 写入此表）
CREATE TABLE IF NOT EXISTS news(
    datetime VARCHAR,
    "content" VARCHAR,
    title VARCHAR,
    channels VARCHAR,
    sync_batch_id VARCHAR,
    created_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_news_datetime ON news(datetime);
CREATE INDEX IF NOT EXISTS idx_news_channels ON news(channels);

-- 4) 新闻同步检查点（news_realtime_sync 读写此表；若工作流已跑过则已存在）
CREATE TABLE IF NOT EXISTS news_sync_checkpoint(
    api_name VARCHAR PRIMARY KEY,
    last_sync_datetime VARCHAR NOT NULL,
    last_sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_count INTEGER DEFAULT 0
);
