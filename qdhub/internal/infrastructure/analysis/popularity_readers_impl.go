package analysis

import (
	"context"
	"fmt"

	"qdhub/internal/domain/analysis"
)

// GetRank PopularityRankReader 实现
// 根据 req.Src 选择 ths_hot / dc_hot / kpl_list 表查询
func (r *Readers) GetRank(ctx context.Context, req analysis.PopularityRankRequest) ([]analysis.PopularityRank, error) {
	if req.Src == "" {
		req.Src = string(analysis.PopularityRankSrcTHS)
	}
	if !analysis.ValidPopularityRankSrc(req.Src) {
		return nil, fmt.Errorf("invalid popularity rank src %q", req.Src)
	}
	if req.Limit <= 0 {
		req.Limit = 50
	}

	switch analysis.PopularityRankSrc(req.Src) {
	case analysis.PopularityRankSrcTHS:
		return r.getThsHotRank(ctx, req)
	case analysis.PopularityRankSrcEastmoney:
		return r.getDcHotRank(ctx, req)
	case analysis.PopularityRankSrcKPL:
		return r.getKplListRank(ctx, req)
	}
	return nil, fmt.Errorf("unsupported src: %s", req.Src)
}

// getThsHotRank 同花顺人气榜 (ths_hot)
func (r *Readers) getThsHotRank(ctx context.Context, req analysis.PopularityRankRequest) ([]analysis.PopularityRank, error) {
	ok, _ := r.db.TableExists(ctx, "ths_hot")
	if !ok {
		return nil, fmt.Errorf("ths_hot 表不存在，请先同步同花顺人气榜数据")
	}

	tradeDate := req.TradeDate
	if tradeDate == "" {
		rows, err := r.db.Query(ctx, "SELECT MAX(trade_date) AS td FROM ths_hot")
		if err != nil || len(rows) == 0 {
			return nil, fmt.Errorf("无法获取 ths_hot 最新交易日: %w", err)
		}
		tradeDate = str(rows[0], "td")
	}

	sql := `
SELECT COALESCE(h.rank, 0) AS rank,
       h.ts_code,
       COALESCE(sb.name, h.name, h.ts_code) AS name,
       COALESCE(h.hot, h.vote, 0.0) AS score,
       0 AS change,
       COALESCE(d.pct_chg, h.pct_chg, 0.0) AS pct_chg,
       COALESCE(d.vol, 0.0) AS volume,
       COALESCE(db.turnover_rate, 0.0) AS turnover_rate,
       h.trade_date AS update_time
FROM ths_hot h
LEFT JOIN stock_basic sb ON sb.ts_code = h.ts_code
LEFT JOIN daily d ON d.ts_code = h.ts_code AND d.trade_date = h.trade_date
LEFT JOIN daily_basic db ON db.ts_code = h.ts_code AND db.trade_date = h.trade_date
WHERE h.trade_date = ?
ORDER BY h.rank ASC NULLS LAST
LIMIT ? OFFSET ?`
	rows, err := r.db.Query(ctx, sql, tradeDate, req.Limit, req.Offset)
	if err != nil {
		return nil, fmt.Errorf("ths_hot query: %w", err)
	}
	return rowsToPopularityRank(rows), nil
}

// getDcHotRank 东方财富人气榜 (dc_hot)
func (r *Readers) getDcHotRank(ctx context.Context, req analysis.PopularityRankRequest) ([]analysis.PopularityRank, error) {
	ok, _ := r.db.TableExists(ctx, "dc_hot")
	if !ok {
		return nil, fmt.Errorf("dc_hot 表不存在，请先同步东方财富人气榜数据")
	}

	tradeDate := req.TradeDate
	if tradeDate == "" {
		rows, err := r.db.Query(ctx, "SELECT MAX(trade_date) AS td FROM dc_hot")
		if err != nil || len(rows) == 0 {
			return nil, fmt.Errorf("无法获取 dc_hot 最新交易日: %w", err)
		}
		tradeDate = str(rows[0], "td")
	}

	sql := `
SELECT COALESCE(h.rank, 0) AS rank,
       h.ts_code,
       COALESCE(sb.name, h.name, h.ts_code) AS name,
       COALESCE(h.hot, h.heat, 0.0) AS score,
       0 AS change,
       COALESCE(d.pct_chg, h.pct_chg, 0.0) AS pct_chg,
       COALESCE(d.vol, h.volume, 0.0) AS volume,
       COALESCE(db.turnover_rate, h.turnover_rate, 0.0) AS turnover_rate,
       h.trade_date AS update_time
FROM dc_hot h
LEFT JOIN stock_basic sb ON sb.ts_code = h.ts_code
LEFT JOIN daily d ON d.ts_code = h.ts_code AND d.trade_date = h.trade_date
LEFT JOIN daily_basic db ON db.ts_code = h.ts_code AND db.trade_date = h.trade_date
WHERE h.trade_date = ?
ORDER BY h.rank ASC NULLS LAST
LIMIT ? OFFSET ?`
	rows, err := r.db.Query(ctx, sql, tradeDate, req.Limit, req.Offset)
	if err != nil {
		return nil, fmt.Errorf("dc_hot query: %w", err)
	}
	return rowsToPopularityRank(rows), nil
}

// getKplListRank 开盘啦榜单 (kpl_list)
func (r *Readers) getKplListRank(ctx context.Context, req analysis.PopularityRankRequest) ([]analysis.PopularityRank, error) {
	ok, _ := r.db.TableExists(ctx, "kpl_list")
	if !ok {
		return nil, fmt.Errorf("kpl_list 表不存在，请先同步开盘啦榜单数据")
	}

	tradeDate := req.TradeDate
	if tradeDate == "" {
		rows, err := r.db.Query(ctx, "SELECT MAX(trade_date) AS td FROM kpl_list")
		if err != nil || len(rows) == 0 {
			return nil, fmt.Errorf("无法获取 kpl_list 最新交易日: %w", err)
		}
		tradeDate = str(rows[0], "td")
	}

	sql := `
SELECT COALESCE(h.rank, ROW_NUMBER() OVER (ORDER BY COALESCE(h.hot, h.score, 0) DESC)) AS rank,
       h.ts_code,
       COALESCE(sb.name, h.name, h.ts_code) AS name,
       COALESCE(h.hot, h.score, 0.0) AS score,
       0 AS change,
       COALESCE(d.pct_chg, h.pct_chg, 0.0) AS pct_chg,
       COALESCE(d.vol, 0.0) AS volume,
       COALESCE(db.turnover_rate, 0.0) AS turnover_rate,
       h.trade_date AS update_time
FROM kpl_list h
LEFT JOIN stock_basic sb ON sb.ts_code = h.ts_code
LEFT JOIN daily d ON d.ts_code = h.ts_code AND d.trade_date = h.trade_date
LEFT JOIN daily_basic db ON db.ts_code = h.ts_code AND db.trade_date = h.trade_date
WHERE h.trade_date = ?
ORDER BY rank ASC
LIMIT ? OFFSET ?`
	rows, err := r.db.Query(ctx, sql, tradeDate, req.Limit, req.Offset)
	if err != nil {
		return nil, fmt.Errorf("kpl_list query: %w", err)
	}
	return rowsToPopularityRank(rows), nil
}

func rowsToPopularityRank(rows []map[string]any) []analysis.PopularityRank {
	out := make([]analysis.PopularityRank, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.PopularityRank{
			Rank:         int_(m, "rank"),
			TsCode:       str(m, "ts_code"),
			Name:         str(m, "name"),
			Score:        float(m, "score"),
			Change:       int_(m, "change"),
			PctChg:       float(m, "pct_chg"),
			Volume:       float(m, "volume"),
			TurnoverRate: float(m, "turnover_rate"),
			UpdateTime:   str(m, "update_time"),
		})
	}
	return out
}
