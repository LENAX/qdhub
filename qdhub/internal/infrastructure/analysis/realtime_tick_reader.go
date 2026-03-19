package analysis

import (
	"context"
	"time"

	"qdhub/internal/domain/analysis"
	"qdhub/internal/domain/datastore"
)

var _ analysis.TickReader = (*RealtimeTickReader)(nil)

// RealtimeTickReader 从 realtime DuckDB 的 ts_realtime_mkt_tick 表读取分时 tick 数据。
type RealtimeTickReader struct {
	db datastore.QuantDB
}

func NewRealtimeTickReader(db datastore.QuantDB) *RealtimeTickReader {
	return &RealtimeTickReader{db: db}
}

const realtimeTickTable = "ts_realtime_mkt_tick"

func (r *RealtimeTickReader) GetRealtimeTicks(ctx context.Context, tsCode string, limit int) ([]analysis.TickRow, error) {
	ok, _ := r.db.TableExists(ctx, realtimeTickTable)
	if !ok {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	sql := `SELECT ts_code, name, trade_time, pre_price, price, open, high, low, close, volume, amount,
		ask_price1, ask_volume1, bid_price1, bid_volume1, ask_price2, ask_volume2, bid_price2, bid_volume2,
		ask_price3, ask_volume3, bid_price3, bid_volume3, ask_price4, ask_volume4, bid_price4, bid_volume4,
		ask_price5, ask_volume5, bid_price5, bid_volume5
		FROM ` + realtimeTickTable + ` WHERE ts_code = ? ORDER BY trade_time DESC LIMIT ?`
	rows, err := r.db.Query(ctx, sql, tsCode, limit)
	if err != nil {
		return nil, err
	}
	return mapRowsToTickRows(rows), nil
}

func (r *RealtimeTickReader) GetIntradayTicks(ctx context.Context, tsCode, tradeDate string) ([]analysis.TickRow, error) {
	ok, _ := r.db.TableExists(ctx, realtimeTickTable)
	if !ok || tsCode == "" {
		return nil, nil
	}
	if tradeDate == "" {
		tradeDate = time.Now().Format("20060102")
	}
	if len(tradeDate) != 8 {
		return nil, nil
	}
	start := tradeDate[:4] + "-" + tradeDate[4:6] + "-" + tradeDate[6:8] + " 00:00:00"
	end := tradeDate[:4] + "-" + tradeDate[4:6] + "-" + tradeDate[6:8] + " 23:59:59"
	sql := `SELECT ts_code, name, trade_time, pre_price, price, open, high, low, close, volume, amount,
		ask_price1, ask_volume1, bid_price1, bid_volume1, ask_price2, ask_volume2, bid_price2, bid_volume2,
		ask_price3, ask_volume3, bid_price3, bid_volume3, ask_price4, ask_volume4, bid_price4, bid_volume4,
		ask_price5, ask_volume5, bid_price5, bid_volume5
		FROM ` + realtimeTickTable + ` WHERE ts_code = ? AND trade_time >= CAST(? AS TIMESTAMP) AND trade_time <= CAST(? AS TIMESTAMP)
		ORDER BY trade_time ASC`
	rows, err := r.db.Query(ctx, sql, tsCode, start, end)
	if err != nil {
		return nil, err
	}
	return mapRowsToTickRows(rows), nil
}
