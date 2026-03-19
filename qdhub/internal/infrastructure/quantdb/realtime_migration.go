// Package quantdb: 启动时对 realtime DuckDB 执行建表 migration（幂等）。
package quantdb

import (
	"context"
	"strings"

	"qdhub/internal/domain/datastore"
)

// realtimeDuckDBStoreID 与 migration 032 中插入的 realtime_duckdb 存储 ID 一致。
const realtimeDuckDBStoreID = "realtime-duckdb-0000-4000-8000-000000000001"

// realtimeDDL 为 realtime_ticks.duckdb 建表语句（与 scripts/realtime_migration.sql 一致），逐条执行、幂等。
var realtimeDDL = []string{
	`CREATE TABLE IF NOT EXISTS realtime_quote(
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
	)`,
	`CREATE INDEX IF NOT EXISTS idx_realtime_quote_ts_code ON realtime_quote(ts_code)`,
	`CREATE INDEX IF NOT EXISTS idx_realtime_quote_date_time ON realtime_quote(date, "time")`,
	`CREATE TABLE IF NOT EXISTS ts_proxy_realtime_quote(
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
	)`,
	`CREATE INDEX IF NOT EXISTS idx_ts_proxy_realtime_quote_trade_time ON ts_proxy_realtime_quote(trade_time)`,
	`CREATE INDEX IF NOT EXISTS idx_ts_proxy_realtime_quote_code ON ts_proxy_realtime_quote(code)`,
	`CREATE TABLE IF NOT EXISTS news(
		datetime VARCHAR,
		"content" VARCHAR,
		title VARCHAR,
		channels VARCHAR,
		sync_batch_id VARCHAR,
		created_at TIMESTAMP
	)`,
	`CREATE INDEX IF NOT EXISTS idx_news_datetime ON news(datetime)`,
	`CREATE INDEX IF NOT EXISTS idx_news_channels ON news(channels)`,
	`CREATE TABLE IF NOT EXISTS news_sync_checkpoint(
		api_name VARCHAR PRIMARY KEY,
		last_sync_datetime VARCHAR NOT NULL,
		last_sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		record_count INTEGER DEFAULT 0
	)`,
}

// RunRealtimeDuckDBMigration 对已连接的 realtime QuantDB 执行建表 DDL，幂等。
func RunRealtimeDuckDBMigration(ctx context.Context, db datastore.QuantDB) error {
	for _, s := range realtimeDDL {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, err := db.Execute(ctx, s); err != nil {
			return err
		}
	}
	return nil
}
