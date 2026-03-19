// Package healthcheck: TradingDayProvider 用于按 trade_cal 表判断是否为交易日（排除节假日）.
package healthcheck

import (
	"context"
	"fmt"
	"strings"

	"qdhub/internal/domain/datastore"
)

// TradingDayProvider 判断某日期是否为交易日（如从 DuckDB trade_cal 的 is_open 查询）。可为 nil，nil 时回退到仅用星期判断。
type TradingDayProvider interface {
	IsTradingDay(ctx context.Context, date string) (bool, error)
}

// DuckDBTradingDayProvider 通过 QuantDB 查询 trade_cal 表：cal_date = date AND (is_open = 1 OR is_open = '1').
type DuckDBTradingDayProvider struct {
	DB datastore.QuantDB
}

// NewDuckDBTradingDayProvider 创建基于 DuckDB trade_cal 的交易日判断。db 可为 nil，IsTradingDay 将返回 false, nil。
func NewDuckDBTradingDayProvider(db datastore.QuantDB) TradingDayProvider {
	if db == nil {
		return nil
	}
	return &DuckDBTradingDayProvider{DB: db}
}

// IsTradingDay 查询 trade_cal 表，date 格式 YYYYMMDD。若无表或查询失败返回 false, err（调用方可按需回退到星期）。
func (p *DuckDBTradingDayProvider) IsTradingDay(ctx context.Context, date string) (bool, error) {
	if p == nil || p.DB == nil {
		return false, nil
	}
	// 兼容 cal_date 或 cal_trade 列名，以及 is_open 为 1 或 '1'
	sql := `SELECT 1 AS ok FROM trade_cal WHERE (is_open = 1 OR is_open = '1') AND cal_date = ? LIMIT 1`
	rows, err := p.DB.Query(ctx, sql, date)
	if err != nil {
		if hasColumnErr(err) {
			sql = `SELECT 1 AS ok FROM trade_cal WHERE (is_open = 1 OR is_open = '1') AND cal_trade = ? LIMIT 1`
			rows, err = p.DB.Query(ctx, sql, date)
		}
		if err != nil {
			return false, fmt.Errorf("trade_cal: %w", err)
		}
	}
	return len(rows) > 0, nil
}

func hasColumnErr(err error) bool {
	s := err.Error()
	return strings.Contains(s, "cal_date") || strings.Contains(s, "column") || strings.Contains(s, "CAL_DATE")
}
