package analysis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"qdhub/internal/domain/analysis"
	"qdhub/internal/domain/datastore"
)

// Readers 实现 domain/analysis 的各类 Reader 与 CustomQueryExecutor，内部使用 QuantDB 执行 SQL
type Readers struct {
	db       datastore.QuantDB
	fallback FallbackProvider // 可选：本地无数据时从数据源（如 Tushare）兜底
}

// NewReaders 创建分析读实现，db 由上层注入（如 container 的 QuantDBAdapter）
func NewReaders(db datastore.QuantDB) *Readers {
	return &Readers{db: db}
}

// NewReadersWithFallback 创建带兜底的分析读实现：本地查不到数据时通过 fallback 从数据源拉取
func NewReadersWithFallback(db datastore.QuantDB, fallback FallbackProvider) *Readers {
	return &Readers{db: db, fallback: fallback}
}

// 因 Go 不允许同一类型上同名方法返回不同类型，以下接口由包装类型实现
type limitLadderReaderImpl struct{ *Readers }
type limitUpLadderReaderImpl struct{ *Readers }
type limitComparisonReaderImpl struct{ *Readers }
type limitUpComparisonReaderImpl struct{ *Readers }
type sectorLimitStatsReaderImpl struct{ *Readers }
type sectorLimitStocksReaderImpl struct{ *Readers }
type limitUpBySectorReaderImpl struct{ *Readers }
type FirstLimitUpReaderImpl struct{ *Readers }

// Ensure 编译期检查
var (
	_ analysis.KLineReader                 = (*Readers)(nil)
	_ analysis.LimitListReader             = (*Readers)(nil)
	_ analysis.ConceptRotationReader       = (*Readers)(nil)
	_ analysis.CustomQueryExecutor         = (*Readers)(nil)
	_ analysis.LimitStatsReader            = (*Readers)(nil)
	_ analysis.LimitStockListReader        = (*Readers)(nil)
	_ analysis.LimitLadderReader           = (*limitLadderReaderImpl)(nil)
	_ analysis.LimitComparisonReader       = (*limitComparisonReaderImpl)(nil)
	_ analysis.SectorLimitStatsReader      = (*sectorLimitStatsReaderImpl)(nil)
	_ analysis.SectorLimitStocksReader     = (*sectorLimitStocksReaderImpl)(nil)
	_ analysis.ConceptHeatReader           = (*Readers)(nil)
	_ analysis.ConceptStocksReader         = (*Readers)(nil)
	_ analysis.StockListReader             = (*stockListReaderImpl)(nil)
	_ analysis.IndexListReader             = (*indexListReaderImpl)(nil)
	_ analysis.ConceptListReader           = (*conceptListReaderImpl)(nil)
	_ analysis.DragonTigerReader           = (*dragonTigerReaderImpl)(nil)
	_ analysis.MoneyFlowReader             = (*Readers)(nil)
	_ analysis.PopularityRankReader        = (*Readers)(nil)
	_ analysis.NewsReader                  = (*newsReaderImpl)(nil)
	_ analysis.LimitUpListReader           = (*limitUpListReaderImpl)(nil)
	_ analysis.LimitUpLadderReader         = (*limitUpLadderReaderImpl)(nil)
	_ analysis.LimitUpComparisonReader     = (*limitUpComparisonReaderImpl)(nil)
	_ analysis.LimitUpBySectorReader       = (*limitUpBySectorReaderImpl)(nil)
	_ analysis.LimitUpStocksBySectorReader = (*Readers)(nil)
	_ analysis.FirstLimitUpReader          = (*FirstLimitUpReaderImpl)(nil)
	_ analysis.StockBasicReader            = (*Readers)(nil)
	_ analysis.FinancialIndicatorReader    = (*Readers)(nil)
	_ analysis.FinancialReportReader       = (*Readers)(nil)
	_ analysis.TradeCalendarReader         = (*Readers)(nil)
	_ analysis.StockSnapshotReader         = (*Readers)(nil)
	_ analysis.RealtimeTickReader          = (*Readers)(nil)
	_ analysis.IntradayTickReader          = (*Readers)(nil)
	_ analysis.IntradayKlineReader         = (*Readers)(nil)
	_ analysis.MoneyFlowConceptReader      = (*Readers)(nil)
)

// GetDailyWithAdjFactor 查询日线并关联复权因子，返回原始行供领域层复权计算。
// 复权因子缺失时用 ffill（该日之前最近一日的 adj_factor），避免除权前日被填成除权后因子导致前复权出现 -99% 异常；无前值则用 1.0。
func (r *Readers) GetDailyWithAdjFactor(ctx context.Context, tsCode, startDate, endDate string) ([]analysis.RawDailyRow, error) {
	sql := `
SELECT d.trade_date, COALESCE(s.name, '') AS name, d.open, d.high, d.low, d.close, d.vol, d.amount, d.pre_close, d.change, d.pct_chg,
       COALESCE(a.adj_factor,
                (SELECT a2.adj_factor FROM adj_factor a2 WHERE a2.ts_code = d.ts_code AND a2.trade_date < d.trade_date ORDER BY a2.trade_date DESC LIMIT 1),
                1.0) AS adj_factor
FROM daily d
LEFT JOIN adj_factor a ON d.ts_code = a.ts_code AND d.trade_date = a.trade_date
LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
WHERE d.ts_code = ? AND d.trade_date >= ? AND d.trade_date <= ?
ORDER BY d.trade_date`
	rows, err := r.db.Query(ctx, sql, tsCode, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("kline query: %w", err)
	}
	out := make([]analysis.RawDailyRow, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.RawDailyRow{
			TradeDate: str(m, "trade_date"),
			Name:      str(m, "name"),
			Open:      float(m, "open"),
			High:      float(m, "high"),
			Low:       float(m, "low"),
			Close:     float(m, "close"),
			Vol:       float(m, "vol"),
			Amount:    float(m, "amount"),
			PreClose:  float(m, "pre_close"),
			Change:    float(m, "change"),
			PctChg:    float(m, "pct_chg"),
			AdjFactor: float(m, "adj_factor"),
		})
	}
	// 兜底：本地无数据且配置了 fallback 时，从数据源（如 Tushare）拉取
	if len(out) == 0 && r.fallback != nil {
		fallbackRows, fallbackErr := r.fallback.FetchDaily(ctx, tsCode, startDate, endDate)
		if fallbackErr == nil && len(fallbackRows) > 0 {
			return fallbackRows, nil
		}
	}
	return out, nil
}

// GetLimitDatesBefore 返回某股票在 beforeDate 之前的所有涨停日期（倒序）
func (r *Readers) GetLimitDatesBefore(ctx context.Context, tsCode, beforeDate string) ([]string, error) {
	sql := `SELECT trade_date FROM limit_list_d WHERE ts_code = ? AND trade_date <= ? AND pct_chg >= 9.8 ORDER BY trade_date DESC`
	rows, err := r.db.Query(ctx, sql, tsCode, beforeDate)
	if err != nil {
		return nil, fmt.Errorf("limit list dates: %w", err)
	}
	dates := make([]string, 0, len(rows))
	for _, m := range rows {
		if d := str(m, "trade_date"); d != "" {
			dates = append(dates, d)
		}
	}
	return dates, nil
}

// GetRankedConcepts 题材轮动：按 rank_by 排序返回每日概念排名行
func (r *Readers) GetRankedConcepts(ctx context.Context, q analysis.ConceptRotationQuery) ([]analysis.ConceptRotationRow, error) {
	orderCol := "avg_pct_chg DESC"
	switch q.RankBy {
	case "limit_up_count":
		orderCol = "limit_up_count DESC, avg_pct_chg DESC"
	case "volume":
		orderCol = "total_volume DESC"
	case "net_inflow":
		orderCol = "net_inflow DESC"
	default:
		orderCol = "avg_pct_chg DESC"
	}
	sql := fmt.Sprintf(`
WITH concept_daily_stats AS (
    SELECT d.trade_date, cd.concept_code, c.name AS concept_name,
           COUNT(DISTINCT cd.ts_code) AS stock_count,
           AVG(d.pct_chg) AS avg_pct_chg,
           COUNT(DISTINCT CASE WHEN d.pct_chg >= 9.8
                                 AND NOT (TRIM(COALESCE(s.name, '')) LIKE 'ST%%' OR TRIM(COALESCE(s.name, '')) LIKE '*ST%%')
                               THEN d.ts_code END) AS limit_up_count,
           COALESCE(SUM(d.vol), 0) AS total_volume,
           COALESCE(SUM(mf.net_mf_amount), 0) AS net_inflow
    FROM daily d
    JOIN concept_detail cd ON d.ts_code = cd.ts_code
    JOIN concept c ON cd.concept_code = c.code
    LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
    LEFT JOIN moneyflow mf ON d.ts_code = mf.ts_code AND d.trade_date = mf.trade_date
    WHERE d.trade_date BETWEEN ? AND ?
    GROUP BY d.trade_date, cd.concept_code, c.name
),
ranked AS (
    SELECT trade_date, concept_code, concept_name, stock_count, avg_pct_chg, limit_up_count, total_volume, net_inflow,
           ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY %s) AS rn,
           CASE WHEN ? = 'pct_chg' THEN avg_pct_chg WHEN ? = 'limit_up_count' THEN limit_up_count WHEN ? = 'volume' THEN total_volume ELSE net_inflow END AS rank_value
    FROM concept_daily_stats
)
SELECT trade_date, concept_code, concept_name, rn AS rank, avg_pct_chg, limit_up_count, total_volume, net_inflow, stock_count, rank_value
FROM ranked
WHERE trade_date BETWEEN ? AND ?
ORDER BY trade_date, rn`, orderCol)
	args := []any{q.StartDate, q.EndDate, q.RankBy, q.RankBy, q.RankBy, q.StartDate, q.EndDate}
	if q.TopN > 0 {
		sql += " QUALIFY rn <= ?"
		args = append(args, q.TopN)
	}
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("concept rotation: %w", err)
	}
	out := make([]analysis.ConceptRotationRow, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.ConceptRotationRow{
			TradeDate:    str(m, "trade_date"),
			ConceptCode:  str(m, "concept_code"),
			ConceptName:  str(m, "concept_name"),
			Rank:         int_(m, "rank"),
			AvgPctChg:    float(m, "avg_pct_chg"),
			LimitUpCount: int_(m, "limit_up_count"),
			TotalVolume:  float(m, "total_volume"),
			NetInflow:    float(m, "net_inflow"),
			StockCount:   int_(m, "stock_count"),
			RankValue:    float(m, "rank_value"),
		})
	}
	return out, nil
}

// ExecuteReadOnlyQuery 执行只读 SQL，由调用方保证仅 SELECT 且限行/超时
func (r *Readers) ExecuteReadOnlyQuery(ctx context.Context, req analysis.CustomQueryRequest) (*analysis.CustomQueryResult, error) {
	norm := strings.TrimSpace(strings.ToUpper(req.SQL))
	if !strings.HasPrefix(norm, "SELECT") {
		return nil, fmt.Errorf("only read-only SELECT is allowed")
	}
	if req.MaxRows <= 0 {
		req.MaxRows = 10000
	}
	sql := req.SQL
	if !strings.Contains(strings.ToUpper(sql), "LIMIT") {
		sql = sql + " LIMIT " + strconv.Itoa(req.MaxRows)
	}
	rows, err := r.db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return &analysis.CustomQueryResult{Columns: nil, Rows: nil, RowCount: 0}, nil
	}
	cols := make([]string, 0, len(rows[0]))
	for k := range rows[0] {
		cols = append(cols, k)
	}
	rowSlice := make([][]any, 0, len(rows))
	for _, m := range rows {
		row := make([]any, len(cols))
		for i, c := range cols {
			row[i] = m[c]
		}
		rowSlice = append(rowSlice, row)
	}
	return &analysis.CustomQueryResult{Columns: cols, Rows: rowSlice, RowCount: len(rowSlice)}, nil
}

// GetLimitStats 涨跌停统计
// 数据以 limit_list_d 为准：U=涨停封板、Z=炸板、D=跌停，open_times 在 D 时为开板次数；
// 有 limit_list_ths 时仅用其 open_num 覆盖涨停封板/打开数，跌停仍以 limit_list_d 为准。
func (r *Readers) GetLimitStats(ctx context.Context, startDate, endDate string) ([]analysis.LimitStats, error) {
	ldOk, _ := r.db.TableExists(ctx, "limit_list_d")
	if !ldOk {
		return r.getLimitStatsFromDaily(ctx, startDate, endDate)
	}
	// 优先：limit_list_d + open_times 区分跌停封板/开板（Tushare 文档：跌停为开板次数）
	ldSQL := `
SELECT trade_date,
       COUNT(DISTINCT CASE WHEN "limit" = 'U'
                             AND NOT (TRIM(COALESCE(name, '')) LIKE 'ST%' OR TRIM(COALESCE(name, '')) LIKE '*ST%')
                           THEN ts_code END) AS limit_up_sealed,
       COUNT(DISTINCT CASE WHEN "limit" = 'Z'
                             AND NOT (TRIM(COALESCE(name, '')) LIKE 'ST%' OR TRIM(COALESCE(name, '')) LIKE '*ST%')
                           THEN ts_code END) AS limit_up_opened,
       COUNT(DISTINCT CASE WHEN "limit" IN ('U','Z')
                             AND NOT (TRIM(COALESCE(name, '')) LIKE 'ST%' OR TRIM(COALESCE(name, '')) LIKE '*ST%')
                           THEN ts_code END) AS limit_up_from_limit,
       COUNT(DISTINCT CASE WHEN pct_chg >= 9.8
                             AND NOT (TRIM(COALESCE(name, '')) LIKE 'ST%' OR TRIM(COALESCE(name, '')) LIKE '*ST%')
                           THEN ts_code END) AS limit_up_from_pct,
       COUNT(DISTINCT CASE WHEN "limit" = 'D' AND COALESCE(open_times, 0) = 0 THEN ts_code END) AS limit_down_sealed,
       COUNT(DISTINCT CASE WHEN "limit" = 'D' AND COALESCE(open_times, 0) > 0 THEN ts_code END) AS limit_down_opened,
       COUNT(DISTINCT CASE WHEN "limit" = 'D' THEN ts_code END) AS limit_down_from_limit,
       COUNT(DISTINCT CASE WHEN pct_chg <= -9.8 THEN ts_code END) AS limit_down_from_pct
FROM limit_list_d
WHERE trade_date BETWEEN ? AND ?
GROUP BY trade_date ORDER BY trade_date`
	ldRows, ldErr := r.db.Query(ctx, ldSQL, startDate, endDate)
	if ldErr != nil || len(ldRows) == 0 {
		// 仍以 limit_list_d 为准：若无 open_times 等导致上面 SQL 失败，用不依赖 open_times 的统计
		ldRows, ldErr = r.queryLimitStatsFromLimitListD(ctx, startDate, endDate)
	}
	if ldErr == nil && len(ldRows) > 0 {
		out := make([]analysis.LimitStats, 0, len(ldRows))
		for _, m := range ldRows {
			sealed := int_(m, "limit_up_sealed")
			opened := int_(m, "limit_up_opened")
			upFromLimit := int_(m, "limit_up_from_limit")
			upFromPct := int_(m, "limit_up_from_pct")
			total := sealed + opened
			if total < upFromPct {
				total = upFromPct
			}
			if total < upFromLimit {
				total = upFromLimit
			}
			if total > 0 && sealed == 0 && opened == 0 {
				sealed = total
			}
			dSealed := int_(m, "limit_down_sealed")
			dOpened := int_(m, "limit_down_opened")
			dFromLimit := int_(m, "limit_down_from_limit")
			dFromPct := int_(m, "limit_down_from_pct")
			dCount := dSealed + dOpened
			if dCount < dFromPct {
				dCount = dFromPct
			}
			if dCount < dFromLimit {
				dCount = dFromLimit
			}
			// 跌停：分钟/高频数据未稳定前不按 open_times 区分，有跌停则封板率 100%、打开数 0
			var dSealedVal, dOpenedVal int
			if dCount > 0 {
				dSealedVal = dCount
				dOpenedVal = 0
			}
			out = append(out, analysis.LimitStats{
				TradeDate:       str(m, "trade_date"),
				LimitUpCount:    total,
				LimitDownCount:  dCount,
				LimitUpSealed:   sealed,
				LimitUpOpened:   opened,
				LimitDownSealed: dSealedVal,
				LimitDownOpened: dOpenedVal,
			})
		}
		// 有 limit_list_ths 时用 open_num 覆盖更准确的封板/打开数（同样默认排除 ST/*ST）
		if thsOk, _ := r.db.TableExists(ctx, "limit_list_ths"); thsOk {
			thsSQL := `
SELECT trade_date,
       SUM(CASE WHEN COALESCE(open_num, 0) = 0
                  AND NOT (TRIM(COALESCE(name, '')) LIKE 'ST%' OR TRIM(COALESCE(name, '')) LIKE '*ST%')
                THEN 1 ELSE 0 END) AS limit_up_sealed,
       SUM(CASE WHEN COALESCE(open_num, 0) > 0
                  AND NOT (TRIM(COALESCE(name, '')) LIKE 'ST%' OR TRIM(COALESCE(name, '')) LIKE '*ST%')
                THEN 1 ELSE 0 END) AS limit_up_opened
FROM limit_list_ths
WHERE trade_date BETWEEN ? AND ?
GROUP BY trade_date`
			thsRows, thsErr := r.db.Query(ctx, thsSQL, startDate, endDate)
			if thsErr == nil {
				byDate := make(map[string]*struct{ sealed, opened int })
				for _, m := range thsRows {
					byDate[str(m, "trade_date")] = &struct{ sealed, opened int }{
						sealed: int_(m, "limit_up_sealed"),
						opened: int_(m, "limit_up_opened"),
					}
				}
				for i := range out {
					if v, ok := byDate[out[i].TradeDate]; ok {
						out[i].LimitUpSealed = v.sealed
						out[i].LimitUpOpened = v.opened
						out[i].LimitUpCount = v.sealed + v.opened
					}
				}
			}
		}
		return out, nil
	}
	return r.getLimitStatsFromDaily(ctx, startDate, endDate)
}

// queryLimitStatsFromLimitListD 仅用 limit_list_d 统计，跌停不区分 open_times（全部计为封板），用于无 open_times 列或首查失败时
func (r *Readers) queryLimitStatsFromLimitListD(ctx context.Context, startDate, endDate string) ([]map[string]any, error) {
	sql := `
SELECT trade_date,
       COUNT(DISTINCT CASE WHEN "limit" = 'U'
                             AND NOT (TRIM(COALESCE(name, '')) LIKE 'ST%' OR TRIM(COALESCE(name, '')) LIKE '*ST%')
                           THEN ts_code END) AS limit_up_sealed,
       COUNT(DISTINCT CASE WHEN "limit" = 'Z'
                             AND NOT (TRIM(COALESCE(name, '')) LIKE 'ST%' OR TRIM(COALESCE(name, '')) LIKE '*ST%')
                           THEN ts_code END) AS limit_up_opened,
       COUNT(DISTINCT CASE WHEN "limit" IN ('U','Z')
                             AND NOT (TRIM(COALESCE(name, '')) LIKE 'ST%' OR TRIM(COALESCE(name, '')) LIKE '*ST%')
                           THEN ts_code END) AS limit_up_from_limit,
       COUNT(DISTINCT CASE WHEN pct_chg >= 9.8
                             AND NOT (TRIM(COALESCE(name, '')) LIKE 'ST%' OR TRIM(COALESCE(name, '')) LIKE '*ST%')
                           THEN ts_code END) AS limit_up_from_pct,
       COUNT(DISTINCT CASE WHEN "limit" = 'D' THEN ts_code END) AS limit_down_sealed,
       0 AS limit_down_opened,
       COUNT(DISTINCT CASE WHEN "limit" = 'D' THEN ts_code END) AS limit_down_from_limit,
       COUNT(DISTINCT CASE WHEN pct_chg <= -9.8 THEN ts_code END) AS limit_down_from_pct
FROM limit_list_d
WHERE trade_date BETWEEN ? AND ?
GROUP BY trade_date ORDER BY trade_date`
	return r.db.Query(ctx, sql, startDate, endDate)
}

// getLimitStatsFromDaily 无 limit_list_d 时从 daily 表统计（无封板/打开细分，涨停/跌停均按封板率 100% 展示）
func (r *Readers) getLimitStatsFromDaily(ctx context.Context, startDate, endDate string) ([]analysis.LimitStats, error) {
	sbOk, _ := r.db.TableExists(ctx, "stock_basic")
	sql := ""
	if sbOk {
		sql = `
SELECT d.trade_date,
       COUNT(DISTINCT CASE WHEN d.pct_chg >= 9.8
                             AND NOT (TRIM(COALESCE(s.name, '')) LIKE 'ST%' OR TRIM(COALESCE(s.name, '')) LIKE '*ST%')
                           THEN d.ts_code END) AS limit_up_count,
       COUNT(DISTINCT CASE WHEN d.pct_chg <= -9.8 THEN d.ts_code END) AS limit_down_count,
       COUNT(DISTINCT CASE WHEN d.pct_chg > 0 THEN d.ts_code END) AS up_count,
       COUNT(DISTINCT CASE WHEN d.pct_chg < 0 THEN d.ts_code END) AS down_count,
       COUNT(DISTINCT CASE WHEN d.pct_chg = 0 THEN d.ts_code END) AS flat_count
FROM daily d
LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
WHERE d.trade_date BETWEEN ? AND ?
GROUP BY d.trade_date ORDER BY d.trade_date`
	} else {
		sql = `
SELECT trade_date,
       COUNT(DISTINCT CASE WHEN pct_chg >= 9.8 THEN ts_code END) AS limit_up_count,
       COUNT(DISTINCT CASE WHEN pct_chg <= -9.8 THEN ts_code END) AS limit_down_count,
       COUNT(DISTINCT CASE WHEN pct_chg > 0 THEN ts_code END) AS up_count,
       COUNT(DISTINCT CASE WHEN pct_chg < 0 THEN ts_code END) AS down_count,
       COUNT(DISTINCT CASE WHEN pct_chg = 0 THEN ts_code END) AS flat_count
FROM daily
WHERE trade_date BETWEEN ? AND ?
GROUP BY trade_date ORDER BY trade_date`
	}
	rows, err := r.db.Query(ctx, sql, startDate, endDate)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.LimitStats, 0, len(rows))
	for _, m := range rows {
		upCount := int_(m, "limit_up_count")
		downCount := int_(m, "limit_down_count")
		// 兜底路径无封板/打开细分：按同花顺口径，全部视为封板，打开数=0，封板率=100%
		out = append(out, analysis.LimitStats{
			TradeDate:       str(m, "trade_date"),
			LimitUpCount:    upCount,
			LimitDownCount:  downCount,
			LimitUpSealed:   upCount,
			LimitUpOpened:   0,
			LimitDownSealed: downCount,
			LimitDownOpened: 0,
			UpCount:         int_(m, "up_count"),
			DownCount:       int_(m, "down_count"),
			FlatCount:       int_(m, "flat_count"),
		})
	}
	return out, nil
}

// GetByDateAndType 涨跌停个股列表（limitType: up/down/z，对应 limit_list_d.limit 的 U/D/Z）
// 涨停时若有 limit_list_ths 表则以其为主表；否则用 limit_list_d 按 limit 字段筛选。
func (r *Readers) GetByDateAndType(ctx context.Context, tradeDate, limitType string) ([]analysis.LimitStock, error) {
	limitVal := strings.ToUpper(limitType)
	switch limitVal {
	case "UP":
		limitVal = "U"
	case "DOWN":
		limitVal = "D"
	case "Z":
		// 炸板
	default:
		if limitVal != "U" && limitVal != "D" && limitVal != "Z" {
			limitVal = "U"
		}
	}
	// 默认用 limit_list_d 查询，包含 open_times/last_time（炸板列表需要）
	sql := `
SELECT l.ts_code, COALESCE(s.name, l.name, '') AS name,
       COALESCE(l.first_time, '') AS limit_time,
       COALESCE(l.last_time, '') AS last_limit_time,
       '' AS limit_reason,
       l.close, l.pct_chg,
       COALESCE(l.turnover_ratio, 0) AS turnover_rate,
       COALESCE(l.amount, 0) AS amount,
       COALESCE(l.float_mv, 0) AS float_cap,
       COALESCE(l.open_times, 0) AS open_times,
       COALESCE(s.industry, l.industry, '') AS industry
FROM limit_list_d l
LEFT JOIN stock_basic s ON l.ts_code = s.ts_code
WHERE l.trade_date = ? AND l.limit = ?
ORDER BY l.first_time`
	args := []interface{}{tradeDate, limitVal}
	if limitType == "up" || limitType == "U" {
		if thsOk, _ := r.db.TableExists(ctx, "limit_list_ths"); thsOk {
			// 以 limit_list_ths 为主表，保证涨停原因来自 lu_desc；缺字段时用 limit_list_d 补
			sql = `
SELECT ths.ts_code, COALESCE(s.name, ths.name, l.name, '') AS name,
       COALESCE(ths.first_lu_time, l.first_time, '') AS limit_time,
       COALESCE(ths.last_lu_time, l.last_time, '') AS last_limit_time,
       COALESCE(NULLIF(TRIM(ths.lu_desc), ''), '') AS limit_reason,
       COALESCE(ths.price, l.close, 0) AS close,
       COALESCE(ths.pct_chg, l.pct_chg, 0) AS pct_chg,
       COALESCE(ths.turnover_rate, l.turnover_ratio, 0) AS turnover_rate,
       COALESCE(ths.turnover, l.amount, 0) AS amount,
       COALESCE(ths.free_float, l.float_mv, 0) AS float_cap,
       COALESCE(ths.open_num, l.open_times, 0) AS open_times,
       COALESCE(s.industry, l.industry, '') AS industry
FROM limit_list_ths ths
LEFT JOIN limit_list_d l ON l.ts_code = ths.ts_code AND l.trade_date = ths.trade_date AND l.limit = 'U'
LEFT JOIN stock_basic s ON s.ts_code = ths.ts_code
WHERE ths.trade_date = ?
ORDER BY COALESCE(ths.first_lu_time, l.first_time)`
			args = []interface{}{tradeDate}
		}
	}
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.LimitStock, 0, len(rows))
	for _, m := range rows {
		name := str(m, "name")
		if limitVal == "U" && isSTStock(name) {
			continue
		}
		out = append(out, analysis.LimitStock{
			TsCode:          str(m, "ts_code"),
			Name:            name,
			LimitTime:       str(m, "limit_time"),
			LastLimitTime:   str(m, "last_limit_time"),
			LimitReason:     str(m, "limit_reason"),
			OpenTimes:       int_(m, "open_times"),
			ConsecutiveDays: 0, FirstLimitDate: tradeDate,
			Close: float(m, "close"), PctChg: float(m, "pct_chg"),
			TurnoverRate: float(m, "turnover_rate"), Amount: float(m, "amount"),
			FloatCap: float(m, "float_cap"),
			Industry: str(m, "industry"), Concepts: nil,
		})
	}
	return out, nil
}

// GetLimitLadderByDate 内部实现，供 LimitLadderReader 包装使用。
// 天梯结构来自 limit_step；展示字段优先 limit_list_ths（同花顺，涨停原因等更详），其次 limit_list_d。
func (r *Readers) GetLimitLadderByDate(ctx context.Context, tradeDate string) (*analysis.LimitLadderStats, error) {
	if ok, _ := r.db.TableExists(ctx, "limit_step"); ok {
		// 优先：limit_step + limit_list_ths + limit_list_d + stock_basic（同花顺提供涨停原因、状态、封单等）
		if thsOk, _ := r.db.TableExists(ctx, "limit_list_ths"); thsOk {
			sqlThs := `
SELECT s.ts_code, COALESCE(sb.name, ths.name, s.name) AS name, s.nums,
       COALESCE(ths.first_lu_time, l.first_time) AS first_time,
       ths.last_lu_time AS last_limit_time,
       COALESCE(ths.lu_desc, l.up_stat, l.limit, '') AS limit_reason,
       COALESCE(ths.status, '') AS limit_status,
       COALESCE(ths.price, l.close, 0) AS close,
       COALESCE(ths.pct_chg, l.pct_chg, 0) AS pct_chg,
       COALESCE(ths.turnover_rate, l.turnover_ratio, 0) AS turnover_rate,
       COALESCE(ths.turnover, l.amount, 0) AS amount,
       COALESCE(ths.free_float, 0) AS float_cap,
       COALESCE(ths.sum_float, 0) AS total_cap,
       COALESCE(ths.limit_amount, 0) AS limit_amount,
       COALESCE(ths.open_num, 0) AS open_times,
       COALESCE(sb.industry, '') AS industry
FROM limit_step s
LEFT JOIN limit_list_ths ths ON ths.ts_code = s.ts_code AND ths.trade_date = s.trade_date
LEFT JOIN limit_list_d l ON l.ts_code = s.ts_code AND l.trade_date = s.trade_date AND l.pct_chg >= 9.8
LEFT JOIN stock_basic sb ON sb.ts_code = s.ts_code
WHERE s.trade_date = ?`
			rows, err := r.db.Query(ctx, sqlThs, tradeDate)
			if err == nil && len(rows) > 0 {
				return r.buildLimitLadderFromLimitStep(tradeDate, rows)
			}
		}
		// 其次：limit_step + limit_list_d + stock_basic
		sql := `
SELECT s.ts_code, COALESCE(sb.name, s.name) AS name, s.nums,
       l.first_time, COALESCE(l.up_stat, l.limit, '') AS limit_reason, l.close, l.pct_chg,
       COALESCE(l.turnover_ratio, 0) AS turnover_rate, COALESCE(l.amount, 0) AS amount,
       COALESCE(sb.industry, '') AS industry
FROM limit_step s
LEFT JOIN limit_list_d l ON l.ts_code = s.ts_code AND l.trade_date = s.trade_date AND l.pct_chg >= 9.8
LEFT JOIN stock_basic sb ON sb.ts_code = s.ts_code
WHERE s.trade_date = ?`
		rows, err := r.db.Query(ctx, sql, tradeDate)
		if err == nil && len(rows) > 0 {
			return r.buildLimitLadderFromLimitStep(tradeDate, rows)
		}
		// 无 detail 表时仅 limit_step
		rows, err = r.db.Query(ctx, "SELECT ts_code, name, nums FROM limit_step WHERE trade_date = ?", tradeDate)
		if err == nil && len(rows) > 0 {
			return r.buildLimitLadderFromLimitStep(tradeDate, rows)
		}
	}
	return r.getLimitLadderByDateFromLimitListD(ctx, tradeDate)
}

// isSTStock 判断是否为 ST/*ST 股（默认天梯统计中排除，除非调用方明确要求包含）
func isSTStock(name string) bool {
	n := strings.TrimSpace(name)
	return strings.HasPrefix(n, "*ST") || strings.HasPrefix(n, "ST")
}

// buildLimitLadderFromLimitStep 用 limit_step 表数据按 nums（连板数）聚合为天梯，默认排除 ST/*ST；展示字段来自 limit_list_d/stock_basic（若有）
func (r *Readers) buildLimitLadderFromLimitStep(tradeDate string, rows []map[string]any) (*analysis.LimitLadderStats, error) {
	byDays := make(map[int][]analysis.LimitStock)
	maxConsecutive := 0
	for _, m := range rows {
		if isSTStock(str(m, "name")) {
			continue
		}
		numsVal := m["nums"]
		var days int
		switch v := numsVal.(type) {
		case int64:
			days = int(v)
		case float64:
			days = int(v)
		case string:
			days, _ = strconv.Atoi(v)
		default:
			days = int_(m, "nums")
		}
		if days < 1 {
			days = 1
		}
		if days > maxConsecutive {
			maxConsecutive = days
		}
		st := analysis.LimitStock{
			TsCode:          str(m, "ts_code"),
			Name:            str(m, "name"),
			LimitTime:       str(m, "first_time"),
			LastLimitTime:   str(m, "last_limit_time"),
			LimitReason:     str(m, "limit_reason"),
			LimitStatus:     str(m, "limit_status"),
			ConsecutiveDays: days,
			FirstLimitDate:  tradeDate,
			Close:           float(m, "close"),
			PctChg:          float(m, "pct_chg"),
			TurnoverRate:    float(m, "turnover_rate"),
			Amount:          float(m, "amount"),
			FloatCap:        float(m, "float_cap"),
			TotalCap:        float(m, "total_cap"),
			LimitAmount:     float(m, "limit_amount"),
			OpenTimes:       int_(m, "open_times"),
			Industry:        str(m, "industry"),
		}
		byDays[days] = append(byDays[days], st)
	}
	ladders := make([]analysis.LimitLadder, 0)
	totalCount := 0
	for d := maxConsecutive; d >= 1; d-- {
		if stks, ok := byDays[d]; ok && len(stks) > 0 {
			ladders = append(ladders, analysis.LimitLadder{ConsecutiveDays: d, StockCount: len(stks), Stocks: stks})
			totalCount += len(stks)
		}
	}
	return &analysis.LimitLadderStats{
		TradeDate: tradeDate, TotalLimitUp: totalCount, Ladders: ladders, MaxConsecutive: maxConsecutive,
	}, nil
}

// getLimitLadderByDateFromLimitListD 从 limit_list_d 用 CTE 计算连续涨停天数（回退逻辑）
func (r *Readers) getLimitLadderByDateFromLimitListD(ctx context.Context, tradeDate string) (*analysis.LimitLadderStats, error) {
	sql := `
WITH limit_dates AS (
    SELECT ts_code, trade_date FROM limit_list_d WHERE pct_chg >= 9.8 AND trade_date <= ?
),
consecutive AS (
    SELECT ts_code, trade_date, ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
    FROM limit_dates
),
        grps AS (
            SELECT ts_code, trade_date, rn,
                   (CAST(strptime(trade_date, '%Y%m%d') AS DATE) + CAST(rn - 1 AS INTEGER)) AS grp
    FROM consecutive
),
days AS (
    SELECT ts_code, MIN(trade_date) AS first_limit_date, COUNT(*) AS consecutive_days
    FROM grps
    GROUP BY ts_code, grp
    HAVING MAX(trade_date) = ?
)
SELECT l.ts_code, COALESCE(s.name, l.name) AS name, l.first_time AS limit_time, COALESCE(l.up_stat, l.limit, '') AS limit_reason, l.close, l.pct_chg,
       COALESCE(l.turnover_ratio, 0) AS turnover_rate, COALESCE(l.amount, 0) AS amount, COALESCE(s.industry, l.industry) AS industry,
       d.first_limit_date, d.consecutive_days
FROM limit_list_d l
LEFT JOIN stock_basic s ON l.ts_code = s.ts_code
LEFT JOIN days d ON l.ts_code = d.ts_code
WHERE l.trade_date = ? AND l.pct_chg >= 9.8
ORDER BY d.consecutive_days DESC, l.first_time`
	rows, err := r.db.Query(ctx, sql, tradeDate, tradeDate, tradeDate)
	if err != nil {
		return nil, err
	}
	byDays := make(map[int][]analysis.LimitStock)
	maxConsecutive := 0
	for _, m := range rows {
		if isSTStock(str(m, "name")) {
			continue
		}
		days := int_(m, "consecutive_days")
		if days > maxConsecutive {
			maxConsecutive = days
		}
		st := analysis.LimitStock{
			TsCode: str(m, "ts_code"), Name: str(m, "name"), LimitTime: str(m, "limit_time"),
			LimitReason: str(m, "limit_reason"), ConsecutiveDays: days, FirstLimitDate: str(m, "first_limit_date"),
			Close: float(m, "close"), PctChg: float(m, "pct_chg"),
			TurnoverRate: float(m, "turnover_rate"), Amount: float(m, "amount"),
			OpenTimes: int_(m, "open_times"), Industry: str(m, "industry"), Concepts: nil,
		}
		byDays[days] = append(byDays[days], st)
	}
	ladders := make([]analysis.LimitLadder, 0)
	totalCount := 0
	for d := maxConsecutive; d >= 1; d-- {
		if stks, ok := byDays[d]; ok && len(stks) > 0 {
			ladders = append(ladders, analysis.LimitLadder{ConsecutiveDays: d, StockCount: len(stks), Stocks: stks})
			totalCount += len(stks)
		}
	}
	return &analysis.LimitLadderStats{
		TradeDate: tradeDate, TotalLimitUp: totalCount, Ladders: ladders, MaxConsecutive: maxConsecutive,
	}, nil
}

// GetFirstLimitUpStocksByDate 首板：当日 limit_list_d 涨停且不在 limit_step 中的股票（差集）
func (r *Readers) GetFirstLimitUpStocksByDate(ctx context.Context, tradeDate string) ([]analysis.LimitStock, error) {
	sql := `
SELECT l.ts_code, COALESCE(s.name, l.name, '') AS name, COALESCE(l.first_time, '') AS limit_time, COALESCE(l.up_stat, l.limit, '') AS limit_reason,
       l.close, l.pct_chg, COALESCE(l.turnover_ratio, 0) AS turnover_rate, COALESCE(l.amount, 0) AS amount, COALESCE(l.float_mv, 0) AS float_cap, COALESCE(s.industry, l.industry, '') AS industry
FROM limit_list_d l
LEFT JOIN stock_basic s ON l.ts_code = s.ts_code
WHERE l.trade_date = ? AND (l.pct_chg >= 9.8 OR l.limit = 'U')`
	args := []interface{}{tradeDate}
	if stepOk, _ := r.db.TableExists(ctx, "limit_step"); stepOk {
		sql += ` AND l.ts_code NOT IN (SELECT ts_code FROM limit_step WHERE trade_date = ?)`
		args = append(args, tradeDate)
	}
	sql += ` ORDER BY l.first_time`
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.LimitStock, 0, len(rows))
	for _, m := range rows {
		name := str(m, "name")
		if isSTStock(name) {
			continue
		}
		out = append(out, analysis.LimitStock{
			TsCode:          str(m, "ts_code"),
			Name:            name,
			LimitTime:       str(m, "limit_time"),
			LimitReason:     str(m, "limit_reason"),
			ConsecutiveDays: 1,
			FirstLimitDate:  tradeDate,
			Close:           float(m, "close"),
			PctChg:          float(m, "pct_chg"),
			TurnoverRate:    float(m, "turnover_rate"),
			Amount:          float(m, "amount"),
			FloatCap:        float(m, "float_cap"),
			Industry:        str(m, "industry"),
			Concepts:        nil,
		})
	}
	return out, nil
}

// GetLimitComparisonByDate LimitComparisonReader：今日 vs 昨日
func (r *Readers) GetLimitComparisonByDate(ctx context.Context, todayDate string) (*analysis.LimitComparison, error) {
	today, _ := r.GetLimitLadderByDate(ctx, todayDate)
	yesterdayDate := yesterday(todayDate)
	yesterday, _ := r.GetLimitLadderByDate(ctx, yesterdayDate)
	if today == nil {
		today = &analysis.LimitLadderStats{TradeDate: todayDate, Ladders: nil}
	}
	if yesterday == nil {
		yesterday = &analysis.LimitLadderStats{TradeDate: yesterdayDate, Ladders: nil}
	}
	change := today.TotalLimitUp - yesterday.TotalLimitUp
	changeRatio := 0.0
	if yesterday.TotalLimitUp > 0 {
		changeRatio = float64(change) / float64(yesterday.TotalLimitUp) * 100
	}
	return &analysis.LimitComparison{
		TodayDate: todayDate, YesterdayDate: yesterdayDate,
		TodayCount: today.TotalLimitUp, YesterdayCount: yesterday.TotalLimitUp,
		Change: change, ChangeRatio: changeRatio,
		TodayLadder: *today, YesterdayLadder: *yesterday,
	}, nil
}

func yesterday(ymd string) string {
	if len(ymd) != 8 {
		return ymd
	}
	// 简单减一天，不处理节假日
	y, _ := strconv.Atoi(ymd[:4])
	m, _ := strconv.Atoi(ymd[4:6])
	d, _ := strconv.Atoi(ymd[6:8])
	d--
	if d < 1 {
		m--
		if m < 1 {
			m = 12
			y--
		}
		d = 31 // 简化
	}
	return fmt.Sprintf("%04d%02d%02d", y, m, d)
}

// GetSectorLimitStatsByDate 内部实现
func (r *Readers) GetSectorLimitStatsByDate(ctx context.Context, tradeDate, sectorType string) ([]analysis.SectorLimitStats, error) {
	dim := "s.industry"
	if sectorType == "concept" {
		dim = "cd.concept_code"
	}
	sql := fmt.Sprintf(`
SELECT %s AS sector_code, %s AS sector_name, ? AS sector_type,
       COUNT(DISTINCT l.ts_code) AS limit_up_count, COUNT(DISTINCT s.ts_code) AS total_stocks,
       CAST(COUNT(DISTINCT l.ts_code) AS DOUBLE) / NULLIF(COUNT(DISTINCT s.ts_code), 0) * 100 AS limit_up_ratio,
       AVG(d.pct_chg) AS avg_pct_chg
FROM stock_basic s
LEFT JOIN limit_list_d l ON s.ts_code = l.ts_code AND l.trade_date = ? AND l.pct_chg >= 9.8
LEFT JOIN daily d ON s.ts_code = d.ts_code AND d.trade_date = ?
WHERE s.industry IS NOT NULL
  AND NOT (TRIM(COALESCE(s.name, '')) LIKE 'ST%%' OR TRIM(COALESCE(s.name, '')) LIKE '*ST%%')
GROUP BY %s
HAVING limit_up_count > 0
ORDER BY limit_up_count DESC`, dim, dim, dim)
	rows, err := r.db.Query(ctx, sql, sectorType, tradeDate, tradeDate)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.SectorLimitStats, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.SectorLimitStats{
			SectorCode: str(m, "sector_code"), SectorName: str(m, "sector_name"), SectorType: str(m, "sector_type"),
			LimitUpCount: int_(m, "limit_up_count"), TotalStocks: int_(m, "total_stocks"),
			LimitUpRatio: float(m, "limit_up_ratio"), AvgPctChg: float(m, "avg_pct_chg"),
		})
	}
	return out, nil
}

// GetSectorLimitStocksBySectorAndDate 内部实现
func (r *Readers) GetSectorLimitStocksBySectorAndDate(ctx context.Context, sectorCode, sectorType, tradeDate string) (*analysis.SectorLimitStocks, error) {
	cond := "s.industry = ?"
	if sectorType == "concept" {
		cond = "cd.concept_code = ?"
	}
	sql := fmt.Sprintf(`
SELECT l.ts_code, COALESCE(s.name, l.name) AS name, l.first_time AS limit_time, COALESCE(l.up_stat, l.limit, '') AS limit_reason, l.close, l.pct_chg,
       COALESCE(l.turnover_ratio, 0) AS turnover_rate, COALESCE(l.amount, 0) AS amount, COALESCE(s.industry, l.industry) AS industry
FROM limit_list_d l
JOIN stock_basic s ON l.ts_code = s.ts_code
WHERE l.trade_date = ? AND l.pct_chg >= 9.8 AND %s
ORDER BY l.first_time`, cond)
	rows, err := r.db.Query(ctx, sql, tradeDate, sectorCode)
	if err != nil {
		return nil, err
	}
	stocks := make([]analysis.LimitStock, 0, len(rows))
	for _, m := range rows {
		name := str(m, "name")
		if isSTStock(name) {
			continue
		}
		stocks = append(stocks, analysis.LimitStock{
			TsCode: str(m, "ts_code"), Name: name, LimitTime: str(m, "limit_time"),
			LimitReason: str(m, "limit_reason"), ConsecutiveDays: 0, FirstLimitDate: tradeDate,
			Close: float(m, "close"), PctChg: float(m, "pct_chg"),
			TurnoverRate: float(m, "turnover_rate"), Amount: float(m, "amount"),
			Industry: str(m, "industry"), Concepts: nil,
		})
	}
	return &analysis.SectorLimitStocks{
		SectorCode: sectorCode, SectorName: sectorCode, SectorType: sectorType,
		LimitUpCount: len(stocks), Stocks: stocks,
	}, nil
}

// GetByDate LimitLadderReader
func (l *limitLadderReaderImpl) GetByDate(ctx context.Context, tradeDate string) (*analysis.LimitLadderStats, error) {
	return l.GetLimitLadderByDate(ctx, tradeDate)
}

// GetComparison LimitComparisonReader
func (l *limitComparisonReaderImpl) GetComparison(ctx context.Context, todayDate string) (*analysis.LimitComparison, error) {
	return l.GetLimitComparisonByDate(ctx, todayDate)
}

// GetByDate SectorLimitStatsReader
func (s *sectorLimitStatsReaderImpl) GetByDate(ctx context.Context, tradeDate, sectorType string) ([]analysis.SectorLimitStats, error) {
	return s.GetSectorLimitStatsByDate(ctx, tradeDate, sectorType)
}

// GetBySectorAndDate SectorLimitStocksReader
func (s *sectorLimitStocksReaderImpl) GetBySectorAndDate(ctx context.Context, sectorCode, sectorType, tradeDate string) (*analysis.SectorLimitStocks, error) {
	return s.GetSectorLimitStocksBySectorAndDate(ctx, sectorCode, sectorType, tradeDate)
}

// GetConceptHeat ConceptHeatReader
func (r *Readers) GetConceptHeat(ctx context.Context, tradeDate string) ([]analysis.ConceptHeat, error) {
	sql := `
SELECT cd.concept_code, c.name AS concept_name, COUNT(DISTINCT cd.ts_code) AS stock_count,
       COUNT(DISTINCT CASE WHEN d.pct_chg >= 9.8
                             AND NOT (TRIM(COALESCE(s.name, '')) LIKE 'ST%' OR TRIM(COALESCE(s.name, '')) LIKE '*ST%')
                           THEN d.ts_code END) AS limit_up_count,
       AVG(d.pct_chg) AS avg_pct_chg
FROM concept_detail cd
JOIN concept c ON cd.concept_code = c.code
LEFT JOIN daily d ON cd.ts_code = d.ts_code AND d.trade_date = ?
LEFT JOIN stock_basic s ON cd.ts_code = s.ts_code
GROUP BY cd.concept_code, c.name
ORDER BY limit_up_count DESC, avg_pct_chg DESC`
	rows, err := r.db.Query(ctx, sql, tradeDate)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.ConceptHeat, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.ConceptHeat{
			ConceptCode: str(m, "concept_code"), ConceptName: str(m, "concept_name"),
			StockCount: int_(m, "stock_count"), LimitUpCount: int_(m, "limit_up_count"),
			AvgPctChg: float(m, "avg_pct_chg"),
		})
	}
	return out, nil
}

// GetConceptStocks ConceptStocksReader
func (r *Readers) GetConceptStocks(ctx context.Context, conceptCode, tradeDate string) ([]analysis.StockInfo, error) {
	sql := `
SELECT s.ts_code, s.symbol, s.name, s.area, s.industry, s.market, s.list_date, s.is_hs,
       d.close AS price, d.pct_chg AS pct_chg, d.change, d.vol AS volume, d.amount
FROM concept_detail cd
JOIN stock_basic s ON cd.ts_code = s.ts_code
LEFT JOIN daily d ON s.ts_code = d.ts_code AND d.trade_date = ?
WHERE cd.concept_code = ?
ORDER BY s.ts_code`
	rows, err := r.db.Query(ctx, sql, tradeDate, conceptCode)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.StockInfo, 0, len(rows))
	for _, m := range rows {
		si := analysis.StockInfo{
			TsCode: str(m, "ts_code"), Symbol: str(m, "symbol"), Name: str(m, "name"),
			Area: str(m, "area"), Industry: str(m, "industry"), Market: str(m, "market"),
			ListDate: str(m, "list_date"), IsHS: str(m, "is_hs"),
		}
		if v, ok := m["price"]; ok && v != nil {
			f := float(m, "price")
			si.Price = &f
		}
		if v, ok := m["pct_chg"]; ok && v != nil {
			f := float(m, "pct_chg")
			si.PctChg = &f
		}
		out = append(out, si)
	}
	return out, nil
}

// GetSnapshot StockSnapshotReader：按交易日与 ts_code 列表返回价格、涨跌幅等
func (r *Readers) GetSnapshot(ctx context.Context, tradeDate string, tsCodes []string) ([]analysis.StockInfo, error) {
	if len(tsCodes) == 0 {
		return nil, nil
	}
	// 构造 IN (?, ?, ...) 占位符
	placeholders := make([]string, 0, len(tsCodes))
	args := make([]any, 0, len(tsCodes)+1)
	args = append(args, tradeDate)
	for _, code := range tsCodes {
		if strings.TrimSpace(code) == "" {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, code)
	}
	if len(placeholders) == 0 {
		return nil, nil
	}
	sql := `
SELECT d.ts_code,
       COALESCE(s.symbol, '') AS symbol,
       COALESCE(s.name, '')   AS name,
       COALESCE(s.area, '')   AS area,
       COALESCE(s.industry, '') AS industry,
       COALESCE(s.market, '') AS market,
       COALESCE(s.list_date, '') AS list_date,
       COALESCE(s.is_hs, '')  AS is_hs,
       d.close  AS price,
       d.pct_chg AS pct_chg,
       d.change  AS change,
       d.vol     AS volume,
       d.amount  AS amount
FROM daily d
JOIN (
    SELECT ts_code, MAX(trade_date) AS last_trade_date
    FROM daily
    WHERE trade_date <= ? AND ts_code IN (` + strings.Join(placeholders, ",") + `)
    GROUP BY ts_code
) t ON d.ts_code = t.ts_code AND d.trade_date = t.last_trade_date
LEFT JOIN stock_basic s ON d.ts_code = s.ts_code`
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.StockInfo, 0, len(rows))
	for _, m := range rows {
		info := analysis.StockInfo{
			TsCode:   str(m, "ts_code"),
			Symbol:   str(m, "symbol"),
			Name:     str(m, "name"),
			Area:     str(m, "area"),
			Industry: str(m, "industry"),
			Market:   str(m, "market"),
			ListDate: str(m, "list_date"),
			IsHS:     str(m, "is_hs"),
		}
		if v, ok := m["price"]; ok && v != nil {
			f := float(m, "price")
			info.Price = &f
		}
		if v, ok := m["pct_chg"]; ok && v != nil {
			f := float(m, "pct_chg")
			info.PctChg = &f
		}
		if v, ok := m["change"]; ok && v != nil {
			f := float(m, "change")
			info.Change = &f
		}
		if v, ok := m["volume"]; ok && v != nil {
			f := float(m, "volume")
			info.Volume = &f
		}
		if v, ok := m["amount"]; ok && v != nil {
			f := float(m, "amount")
			info.Amount = &f
		}
		out = append(out, info)
	}
	return out, nil
}

// ListStocks StockListReader.List
func (r *Readers) ListStocks(ctx context.Context, req analysis.StockListRequest) ([]analysis.StockInfo, error) {
	sql := `SELECT ts_code, symbol, name, area, industry, market, list_date, is_hs FROM stock_basic WHERE 1=1`
	args := []any{}
	if req.Market != nil && *req.Market != "" {
		sql += " AND market = ?"
		args = append(args, *req.Market)
	}
	if req.Industry != nil && *req.Industry != "" {
		sql += " AND industry = ?"
		args = append(args, *req.Industry)
	}
	if req.ListStatus != nil && *req.ListStatus != "" {
		sql += " AND list_status = ?"
		args = append(args, *req.ListStatus)
	}
	if req.Query != nil && strings.TrimSpace(*req.Query) != "" {
		q := "%" + strings.TrimSpace(*req.Query) + "%"
		// DuckDB 的 LIKE 区分大小写，cnspell 多为大写（如 JNFD），用户输入小写（jnfd）需用 ILIKE
		if req.SearchType != nil && strings.TrimSpace(strings.ToLower(*req.SearchType)) == "cnspell" {
			sql += " AND (cnspell ILIKE ?)"
			args = append(args, q)
		} else {
			sql += " AND (name ILIKE ? OR ts_code ILIKE ? OR symbol ILIKE ? OR cnspell ILIKE ?)"
			args = append(args, q, q, q, q)
		}
	}
	sql += " ORDER BY ts_code LIMIT ? OFFSET ?"
	limit, offset := req.Limit, req.Offset
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.StockInfo, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.StockInfo{
			TsCode: str(m, "ts_code"), Symbol: str(m, "symbol"), Name: str(m, "name"),
			Area: str(m, "area"), Industry: str(m, "industry"), Market: str(m, "market"),
			ListDate: str(m, "list_date"), IsHS: str(m, "is_hs"),
		})
	}
	return out, nil
}

// ListIndices IndexListReader.List
func (r *Readers) ListIndices(ctx context.Context, req analysis.IndexListRequest) ([]analysis.IndexInfo, error) {
	sql := `SELECT ts_code, name, market, publisher, index_type, category, base_date, base_point, list_date FROM index_basic WHERE 1=1`
	args := []any{}
	if req.Market != nil && *req.Market != "" {
		sql += " AND market = ?"
		args = append(args, *req.Market)
	}
	if req.Category != nil && *req.Category != "" {
		sql += " AND category = ?"
		args = append(args, *req.Category)
	}
	sql += " ORDER BY ts_code LIMIT ? OFFSET ?"
	limit, offset := req.Limit, req.Offset
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.IndexInfo, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.IndexInfo{
			TsCode: str(m, "ts_code"), Name: str(m, "name"), Market: str(m, "market"),
			Publisher: str(m, "publisher"), IndexType: str(m, "index_type"), Category: str(m, "category"),
			BaseDate: str(m, "base_date"), BasePoint: float(m, "base_point"), ListDate: str(m, "list_date"),
		})
	}
	return out, nil
}

// ListConcepts ConceptListReader.List
func (r *Readers) ListConcepts(ctx context.Context, req analysis.ConceptListRequest) ([]analysis.ConceptInfo, error) {
	sql := `SELECT code, name, source FROM concept WHERE 1=1`
	args := []any{}
	if req.Source != nil && *req.Source != "" {
		sql += " AND source = ?"
		args = append(args, *req.Source)
	}
	sql += " ORDER BY code LIMIT ? OFFSET ?"
	limit, offset := req.Limit, req.Offset
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.ConceptInfo, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.ConceptInfo{
			Code: str(m, "code"), Name: str(m, "name"), Source: str(m, "source"), StockCount: 0,
		})
	}
	return out, nil
}

// GetDragonTigerList DragonTigerReader.GetList
// 表 top_list 由 Tushare 元数据建表，列名为 pct_change / l_buy / l_sell，此处用 AS 与实体字段对齐
func (r *Readers) GetDragonTigerList(ctx context.Context, req analysis.DragonTigerRequest) ([]analysis.DragonTigerList, error) {
	sql := `SELECT trade_date, ts_code, COALESCE(name, '') AS name, close,
       COALESCE(pct_change, 0) AS pct_chg, turnover_rate, amount, reason,
       COALESCE(l_buy, 0) AS buy_amount, COALESCE(l_sell, 0) AS sell_amount, COALESCE(net_amount, 0) AS net_amount
FROM top_list WHERE 1=1`
	args := []any{}
	if req.TradeDate != nil && *req.TradeDate != "" {
		sql += " AND trade_date = ?"
		args = append(args, *req.TradeDate)
	}
	if req.TsCode != nil && *req.TsCode != "" {
		sql += " AND ts_code = ?"
		args = append(args, *req.TsCode)
	}
	sql += " ORDER BY amount DESC LIMIT ? OFFSET ?"
	limit, offset := req.Limit, req.Offset
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.DragonTigerList, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.DragonTigerList{
			TradeDate: str(m, "trade_date"), TsCode: str(m, "ts_code"), Name: str(m, "name"),
			Close: float(m, "close"), PctChg: float(m, "pct_chg"), TurnoverRate: float(m, "turnover_rate"),
			Amount: float(m, "amount"), Reason: str(m, "reason"),
			BuyAmount: float(m, "buy_amount"), SellAmount: float(m, "sell_amount"), NetAmount: float(m, "net_amount"),
		})
	}
	return out, nil
}

// GetMoneyFlow MoneyFlowReader
// 优先使用 moneyflow（Tushare 个股资金流向），含小/中/大/特大单的买入、卖出及净额，可支撑主力流入/流出、资金占比等展示。
// 无 moneyflow 表时回退到 moneyflow_ths（同花顺，仅净额口径）；无表时返回空列表。
// 支持单日 (TradeDate)、日期范围 (StartDate+EndDate)、股票代码 (TsCode) 三种过滤方式。
func (r *Readers) GetMoneyFlow(ctx context.Context, req analysis.MoneyFlowRequest) ([]analysis.MoneyFlow, error) {
	limit, offset := req.Limit, req.Offset
	if limit <= 0 {
		limit = 100
	}
	var conds []string
	var args []any
	if req.TradeDate != nil && *req.TradeDate != "" {
		conds = append(conds, "trade_date = ?")
		args = append(args, *req.TradeDate)
	} else if req.StartDate != nil && *req.StartDate != "" && req.EndDate != nil && *req.EndDate != "" {
		conds = append(conds, "trade_date >= ? AND trade_date <= ?")
		args = append(args, *req.StartDate, *req.EndDate)
	}
	if req.TsCode != nil && *req.TsCode != "" {
		conds = append(conds, "ts_code = ?")
		args = append(args, *req.TsCode)
	}

	// 优先：moneyflow 表（完整买卖 + 特大单，单位万元）
	okMf, _ := r.db.TableExists(ctx, "moneyflow")
	if okMf {
		sql := `SELECT m.trade_date, m.ts_code,
       COALESCE(s.name, '') AS name,
       COALESCE(m.buy_sm_amount, 0) AS buy_sm_amount, COALESCE(m.sell_sm_amount, 0) AS sell_sm_amount,
       COALESCE(m.buy_md_amount, 0) AS buy_md_amount, COALESCE(m.sell_md_amount, 0) AS sell_md_amount,
       COALESCE(m.buy_lg_amount, 0) AS buy_lg_amount, COALESCE(m.sell_lg_amount, 0) AS sell_lg_amount,
       COALESCE(m.buy_elg_amount, 0) AS buy_elg_amount, COALESCE(m.sell_elg_amount, 0) AS sell_elg_amount,
       COALESCE(m.net_mf_amount, 0) AS net_mf_amount
FROM moneyflow m
LEFT JOIN stock_basic s ON m.ts_code = s.ts_code`
		if len(conds) > 0 {
			// 限定列名为 m. 避免与 stock_basic 的 ts_code 等歧义
			q := make([]string, len(conds))
			for i, c := range conds {
				q[i] = strings.Replace(strings.Replace(c, "trade_date", "m.trade_date", -1), "ts_code", "m.ts_code", -1)
			}
			sql += " WHERE " + q[0]
			for _, c := range q[1:] {
				sql += " AND " + c
			}
		}
		sql += " ORDER BY m.trade_date ASC, m.net_mf_amount DESC NULLS LAST LIMIT ? OFFSET ?"
		fullArgs := append(args, limit, offset)
		rows, err := r.db.Query(ctx, sql, fullArgs...)
		if err != nil {
			return nil, err
		}
		out := make([]analysis.MoneyFlow, 0, len(rows))
		for _, m := range rows {
			out = append(out, analysis.MoneyFlow{
				TradeDate:     str(m, "trade_date"),
				TsCode:        str(m, "ts_code"),
				Name:          str(m, "name"),
				BuySmAmount:   float(m, "buy_sm_amount"),
				SellSmAmount:  float(m, "sell_sm_amount"),
				BuyMdAmount:   float(m, "buy_md_amount"),
				SellMdAmount:  float(m, "sell_md_amount"),
				BuyLgAmount:   float(m, "buy_lg_amount"),
				SellLgAmount:  float(m, "sell_lg_amount"),
				BuyElgAmount:  float(m, "buy_elg_amount"),
				SellElgAmount: float(m, "sell_elg_amount"),
				NetMfAmount:   float(m, "net_mf_amount"),
				NetMfRatio:    0,
			})
		}
		return out, nil
	}

	// 回退：moneyflow_ths（同花顺，仅净额）
	okThs, _ := r.db.TableExists(ctx, "moneyflow_ths")
	if !okThs {
		return nil, nil
	}
	sql := `SELECT trade_date, ts_code, COALESCE(name, '') AS name,
       buy_sm_amount, buy_md_amount, buy_lg_amount, net_amount AS net_mf_amount
FROM moneyflow_ths`
	if len(conds) > 0 {
		sql += " WHERE " + conds[0]
		for _, c := range conds[1:] {
			sql += " AND " + c
		}
	}
	sql += " ORDER BY trade_date ASC, net_amount DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.MoneyFlow, 0, len(rows))
	for _, m := range rows {
		netAmt := float(m, "net_mf_amount")
		out = append(out, analysis.MoneyFlow{
			TradeDate:     str(m, "trade_date"),
			TsCode:        str(m, "ts_code"),
			Name:          str(m, "name"),
			BuySmAmount:   float(m, "buy_sm_amount"),
			SellSmAmount:  0,
			BuyMdAmount:   float(m, "buy_md_amount"),
			SellMdAmount:  0,
			BuyLgAmount:   float(m, "buy_lg_amount"),
			SellLgAmount:  0,
			BuyElgAmount:  0,
			SellElgAmount: 0,
			NetMfAmount:   netAmt,
			NetMfRatio:    0,
		})
	}
	return out, nil
}

// GetMoneyFlowConcept 同花顺概念板块资金流入（moneyflow_cnt_ths）
func (r *Readers) GetMoneyFlowConcept(ctx context.Context, req analysis.MoneyFlowConceptRequest) ([]analysis.MoneyFlowConcept, error) {
	ok, _ := r.db.TableExists(ctx, "moneyflow_cnt_ths")
	if !ok {
		return nil, nil
	}
	limit, offset := req.Limit, req.Offset
	if limit <= 0 {
		limit = 100
	}
	sql := `SELECT trade_date, code AS concept_code, name AS concept_name, net_mf_amount AS net_inflow
FROM moneyflow_cnt_ths WHERE trade_date = ?`
	args := []any{req.TradeDate}
	if req.Concept != nil && *req.Concept != "" {
		sql += " AND (name LIKE ? OR code LIKE ?)"
		q := "%" + *req.Concept + "%"
		args = append(args, q, q)
	}
	sql += " ORDER BY net_mf_amount DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.MoneyFlowConcept, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.MoneyFlowConcept{
			TradeDate:      str(m, "trade_date"),
			ConceptCode:    str(m, "concept_code"),
			ConceptName:    str(m, "concept_name"),
			NetInflow:      float(m, "net_inflow"),
			NetInflowRatio: float(m, "net_inflow_ratio"),
		})
	}
	return out, nil
}

// GetRank PopularityRankReader
func (r *Readers) GetRank(ctx context.Context, req analysis.PopularityRankRequest) ([]analysis.PopularityRank, error) {
	// 无统一人气表时返回空
	return nil, nil
}

// ListNews NewsReader.List：支持 order=time_desc、limit、sources 过滤；表名优先 news，其次 major_news
func (r *Readers) ListNews(ctx context.Context, req analysis.NewsListRequest) ([]analysis.NewsItem, error) {
	table := ""
	for _, t := range []string{"news", "major_news"} {
		ok, _ := r.db.TableExists(ctx, t)
		if ok {
			table = t
			break
		}
	}
	if table == "" {
		return nil, nil
	}
	order := "DESC"
	if strings.TrimSpace(strings.ToLower(req.Order)) == "time_asc" {
		order = "ASC"
	}
	// 常见字段：id, title, content, source, publish_time, 可能无 author/relate_stocks/category/tags
	sql := "SELECT id, title, content, source, publish_time FROM " + table + " WHERE 1=1"
	args := []any{}
	if req.StartDate != nil && *req.StartDate != "" {
		sql += " AND publish_time >= ?"
		args = append(args, *req.StartDate)
	}
	if req.EndDate != nil && *req.EndDate != "" {
		sql += " AND publish_time <= ?"
		args = append(args, *req.EndDate)
	}
	if req.Sources != nil && strings.TrimSpace(*req.Sources) != "" {
		parts := strings.Split(*req.Sources, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		if len(parts) > 0 {
			placeholders := strings.Repeat("?,", len(parts))
			sql += " AND source IN (" + placeholders[:len(placeholders)-1] + ")"
			for _, p := range parts {
				args = append(args, p)
			}
		}
	}
	sql += " ORDER BY publish_time " + order + " LIMIT ? OFFSET ?"
	limit, offset := req.Limit, req.Offset
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.NewsItem, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.NewsItem{
			ID:          str(m, "id"),
			Title:       str(m, "title"),
			Content:     str(m, "content"),
			Source:      str(m, "source"),
			PublishTime: str(m, "publish_time"),
			Author:      str(m, "author"),
			Category:    str(m, "category"),
		})
	}
	return out, nil
}

// GetLimitUpListByDate 当日涨停列表（供 LimitUpListReader 使用）
func (r *Readers) GetLimitUpListByDate(ctx context.Context, tradeDate string) ([]analysis.LimitUpStock, error) {
	sql := `
SELECT l.ts_code, COALESCE(s.name, l.name) AS name, l.trade_date, l.first_time AS limit_time, COALESCE(l.up_stat, l.limit, '') AS reason, l.close, l.pct_chg, 0 AS volume, l.amount, COALESCE(l.turnover_ratio, 0) AS turnover_rate, COALESCE(s.industry, l.industry) AS industry
FROM limit_list_d l
LEFT JOIN stock_basic s ON l.ts_code = s.ts_code
WHERE l.trade_date = ? AND l.pct_chg >= 9.8 ORDER BY l.first_time`
	rows, err := r.db.Query(ctx, sql, tradeDate)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.LimitUpStock, 0, len(rows))
	for _, m := range rows {
		name := str(m, "name")
		if isSTStock(name) {
			continue
		}
		out = append(out, analysis.LimitUpStock{
			TsCode: str(m, "ts_code"), Name: name, TradeDate: str(m, "trade_date"),
			LimitTime: str(m, "limit_time"), Reason: str(m, "reason"), ConsecutiveDays: 0,
			Close: float(m, "close"), PctChg: float(m, "pct_chg"), Volume: float(m, "volume"),
			Amount: float(m, "amount"), TurnoverRate: float(m, "turnover_rate"),
			Industry: str(m, "industry"), Concepts: nil,
		})
	}
	return out, nil
}

// GetLimitUpList LimitUpListReader.GetList
func (r *Readers) GetLimitUpList(ctx context.Context, req analysis.LimitUpListRequest) ([]analysis.LimitUpStock, error) {
	list, err := r.GetLimitUpListByDate(ctx, req.TradeDate)
	if err != nil {
		return nil, err
	}
	// 简单过滤：MinConsecutiveDays/MaxConsecutiveDays 需连板计算，此处暂不筛
	if req.Industry != nil && *req.Industry != "" {
		filtered := list[:0]
		for _, s := range list {
			if s.Industry == *req.Industry {
				filtered = append(filtered, s)
			}
		}
		list = filtered
	}
	offset, limit := req.Offset, req.Limit
	if limit <= 0 {
		limit = 100
	}
	if offset >= len(list) {
		return nil, nil
	}
	end := offset + limit
	if end > len(list) {
		end = len(list)
	}
	return list[offset:end], nil
}

// GetLimitUpLadderByDate 内部实现，供 LimitUpLadderReader 包装
func (r *Readers) GetLimitUpLadderByDate(ctx context.Context, tradeDate string) ([]analysis.LimitUpLadder, error) {
	stats, err := r.GetLimitLadderByDate(ctx, tradeDate)
	if err != nil || stats == nil {
		return nil, err
	}
	ladders := make([]analysis.LimitUpLadder, 0, len(stats.Ladders))
	for _, ld := range stats.Ladders {
		stks := make([]analysis.LimitUpStock, 0, len(ld.Stocks))
		for _, s := range ld.Stocks {
			stks = append(stks, analysis.LimitUpStock{
				TsCode: s.TsCode, Name: s.Name, TradeDate: tradeDate, LimitTime: s.LimitTime, Reason: s.LimitReason,
				ConsecutiveDays: s.ConsecutiveDays, Close: s.Close, PctChg: s.PctChg,
				Volume: 0, Amount: s.Amount, TurnoverRate: s.TurnoverRate, Industry: s.Industry, Concepts: s.Concepts,
			})
		}
		ladders = append(ladders, analysis.LimitUpLadder{ConsecutiveDays: ld.ConsecutiveDays, StockCount: ld.StockCount, Stocks: stks})
	}
	return ladders, nil
}

// GetLimitUpComparisonByDate 内部实现，供 LimitUpComparisonReader 包装
func (r *Readers) GetLimitUpComparisonByDate(ctx context.Context, todayDate string) (*analysis.LimitUpComparison, error) {
	todayLadders, _ := r.GetLimitUpLadderByDate(ctx, todayDate)
	yesterdayDate := yesterday(todayDate)
	yesterdayLadders, _ := r.GetLimitUpLadderByDate(ctx, yesterdayDate)
	if todayLadders == nil {
		todayLadders = nil
	}
	if yesterdayLadders == nil {
		yesterdayLadders = nil
	}
	todayCount := 0
	for _, ld := range todayLadders {
		todayCount += ld.StockCount
	}
	yesterdayCount := 0
	for _, ld := range yesterdayLadders {
		yesterdayCount += ld.StockCount
	}
	change := todayCount - yesterdayCount
	changeRatio := 0.0
	if yesterdayCount > 0 {
		changeRatio = float64(change) / float64(yesterdayCount) * 100
	}
	return &analysis.LimitUpComparison{
		TodayDate: todayDate, YesterdayDate: yesterdayDate,
		TodayCount: todayCount, YesterdayCount: yesterdayCount,
		Change: change, ChangeRatio: changeRatio,
		TodayLadder: todayLadders, YesterdayLadder: yesterdayLadders,
	}, nil
}

// GetByDate limitUpLadderReaderImpl
func (l *limitUpLadderReaderImpl) GetByDate(ctx context.Context, tradeDate string) ([]analysis.LimitUpLadder, error) {
	return l.GetLimitUpLadderByDate(ctx, tradeDate)
}

// GetByDate FirstLimitUpReaderImpl
func (f *FirstLimitUpReaderImpl) GetByDate(ctx context.Context, tradeDate string) ([]analysis.LimitStock, error) {
	return f.GetFirstLimitUpStocksByDate(ctx, tradeDate)
}

// GetComparison limitUpComparisonReaderImpl
func (l *limitUpComparisonReaderImpl) GetComparison(ctx context.Context, todayDate string) (*analysis.LimitUpComparison, error) {
	return l.GetLimitUpComparisonByDate(ctx, todayDate)
}

// GetLimitUpBySectorByDate 内部实现，供 LimitUpBySectorReader 包装
func (r *Readers) GetLimitUpBySectorByDate(ctx context.Context, tradeDate, sectorType string) ([]analysis.LimitUpBySector, error) {
	// 优先使用 limit_cpt_list 作为涨停板块排名主表
	if ok, _ := r.db.TableExists(ctx, "limit_cpt_list"); ok {
		sql := `
SELECT
  ts_code    AS sector_code,
  name       AS sector_name,
  ?          AS sector_type,
  up_nums    AS stock_count,
  up_nums    AS limit_up_count,
  up_nums    AS total_stock_count,
  0.0        AS limit_up_ratio,
  pct_chg    AS avg_pct_chg,
  days,
  up_stat,
  cons_nums,
  up_nums,
  rank
FROM limit_cpt_list
WHERE trade_date = ?
ORDER BY CAST(rank AS INTEGER)`
		rows, err := r.db.Query(ctx, sql, sectorType, tradeDate)
		if err != nil {
			return nil, err
		}
		out := make([]analysis.LimitUpBySector, 0, len(rows))
		for _, m := range rows {
			out = append(out, analysis.LimitUpBySector{
				SectorCode:      str(m, "sector_code"),
				SectorName:      str(m, "sector_name"),
				SectorType:      str(m, "sector_type"),
				StockCount:      int_(m, "stock_count"),
				LimitUpCount:    int_(m, "limit_up_count"),
				TotalStockCount: int_(m, "total_stock_count"),
				LimitUpRatio:    0,
				AvgPctChg:       float(m, "avg_pct_chg"),
				Stocks:          nil,
				Days:            int_(m, "days"),
				UpStat:          str(m, "up_stat"),
				ConsNums:        int_(m, "cons_nums"),
				UpNums:          int_(m, "up_nums"),
				Rank:            str(m, "rank"),
			})
		}
		return out, nil
	}

	// 兜底：若无 limit_cpt_list 表或无数据，退回旧实现（基于行业统计）
	stats, err := r.GetSectorLimitStatsByDate(ctx, tradeDate, sectorType)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.LimitUpBySector, 0, len(stats))
	for _, s := range stats {
		stocks, _ := r.GetSectorLimitStocksBySectorAndDate(ctx, s.SectorCode, sectorType, tradeDate)
		stkList := []analysis.LimitUpStock{}
		if stocks != nil {
			for _, st := range stocks.Stocks {
				stkList = append(stkList, analysis.LimitUpStock{
					TsCode:          st.TsCode,
					Name:            st.Name,
					TradeDate:       tradeDate,
					LimitTime:       st.LimitTime,
					Reason:          st.LimitReason,
					ConsecutiveDays: st.ConsecutiveDays,
					Close:           st.Close,
					PctChg:          st.PctChg,
					Volume:          0,
					Amount:          st.Amount,
					TurnoverRate:    st.TurnoverRate,
					Industry:        st.Industry,
					Concepts:        st.Concepts,
				})
			}
		}
		out = append(out, analysis.LimitUpBySector{
			SectorCode:      s.SectorCode,
			SectorName:      s.SectorName,
			SectorType:      s.SectorType,
			StockCount:      s.LimitUpCount,
			LimitUpCount:    s.LimitUpCount,
			TotalStockCount: s.TotalStocks,
			LimitUpRatio:    0,
			AvgPctChg:       s.AvgPctChg,
			Stocks:          stkList,
		})
	}
	return out, nil
}

// GetByDate limitUpBySectorReaderImpl
func (s *limitUpBySectorReaderImpl) GetByDate(ctx context.Context, tradeDate, sectorType string) ([]analysis.LimitUpBySector, error) {
	return s.GetLimitUpBySectorByDate(ctx, tradeDate, sectorType)
}

// GetStocks LimitUpStocksBySectorReader
func (r *Readers) GetStocks(ctx context.Context, sectorCode, sectorType, tradeDate string) ([]analysis.LimitUpStock, error) {
	sl, err := r.GetSectorLimitStocksBySectorAndDate(ctx, sectorCode, sectorType, tradeDate)
	if err != nil || sl == nil {
		return nil, err
	}
	out := make([]analysis.LimitUpStock, 0, len(sl.Stocks))
	for _, s := range sl.Stocks {
		out = append(out, analysis.LimitUpStock{
			TsCode: s.TsCode, Name: s.Name, TradeDate: tradeDate, LimitTime: s.LimitTime, Reason: s.LimitReason,
			ConsecutiveDays: s.ConsecutiveDays, Close: s.Close, PctChg: s.PctChg,
			Volume: 0, Amount: s.Amount, TurnoverRate: s.TurnoverRate, Industry: s.Industry, Concepts: s.Concepts,
		})
	}
	return out, nil
}

// 以下包装类型使 *Readers 的多种 List/GetList 满足不同 Reader 接口
type stockListReaderImpl struct{ *Readers }
type indexListReaderImpl struct{ *Readers }
type conceptListReaderImpl struct{ *Readers }
type newsReaderImpl struct{ *Readers }
type dragonTigerReaderImpl struct{ *Readers }
type limitUpListReaderImpl struct{ *Readers }

func (s *stockListReaderImpl) List(ctx context.Context, req analysis.StockListRequest) ([]analysis.StockInfo, error) {
	return s.ListStocks(ctx, req)
}
func (i *indexListReaderImpl) List(ctx context.Context, req analysis.IndexListRequest) ([]analysis.IndexInfo, error) {
	return i.ListIndices(ctx, req)
}
func (c *conceptListReaderImpl) List(ctx context.Context, req analysis.ConceptListRequest) ([]analysis.ConceptInfo, error) {
	return c.ListConcepts(ctx, req)
}
func (n *newsReaderImpl) List(ctx context.Context, req analysis.NewsListRequest) ([]analysis.NewsItem, error) {
	return n.ListNews(ctx, req)
}
func (d *dragonTigerReaderImpl) GetList(ctx context.Context, req analysis.DragonTigerRequest) ([]analysis.DragonTigerList, error) {
	return d.GetDragonTigerList(ctx, req)
}
func (l *limitUpListReaderImpl) GetByDate(ctx context.Context, tradeDate string) ([]analysis.LimitUpStock, error) {
	return l.GetLimitUpListByDate(ctx, tradeDate)
}
func (l *limitUpListReaderImpl) GetList(ctx context.Context, req analysis.LimitUpListRequest) ([]analysis.LimitUpStock, error) {
	return l.GetLimitUpList(ctx, req)
}

// GetByTsCode StockBasicReader
// 表 stock_basic 由 Tushare stock_basic API 建表，无 reg_capital/website/email 等（在 stock_company），仅查实际存在的列
func (r *Readers) GetByTsCode(ctx context.Context, tsCode string) (*analysis.StockBasicInfo, error) {
	sql := `SELECT ts_code, symbol, name, area, industry, market, list_date, list_status, is_hs,
       fullname, enname, cnspell, exchange, curr_type
FROM stock_basic WHERE ts_code = ?`
	rows, err := r.db.Query(ctx, sql, tsCode)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	m := rows[0]
	return &analysis.StockBasicInfo{
		TsCode: str(m, "ts_code"), Symbol: str(m, "symbol"), Name: str(m, "name"),
		Area: str(m, "area"), Industry: str(m, "industry"), Market: str(m, "market"),
		ListDate: str(m, "list_date"), ListStatus: str(m, "list_status"), IsHS: str(m, "is_hs"),
		Fullname: str(m, "fullname"), Enname: str(m, "enname"), Cnspell: str(m, "cnspell"),
		Exchange: str(m, "exchange"), CurrType: str(m, "curr_type"), RegCapital: float(m, "reg_capital"),
		Website: str(m, "website"), Email: str(m, "email"), Office: str(m, "office"),
		Employees: int_(m, "employees"), Introduction: str(m, "introduction"), Business: str(m, "business"), MainBusiness: str(m, "main_business"),
	}, nil
}

// GetIndicators FinancialIndicatorReader
// fina_indicator 表由 Tushare 元数据动态建表，列名因版本/接口而异，用 SELECT * 避免 Binder 报错，映射时按 key 取存在的字段即可
func (r *Readers) GetIndicators(ctx context.Context, req analysis.FinancialIndicatorRequest) ([]analysis.FinancialIndicator, error) {
	sql := `SELECT * FROM fina_indicator WHERE ts_code = ?`
	args := []any{req.TsCode}
	if req.EndDate != nil && *req.EndDate != "" {
		sql += " AND end_date <= ?"
		args = append(args, *req.EndDate)
	}
	if req.StartDate != nil && *req.StartDate != "" {
		sql += " AND end_date >= ?"
		args = append(args, *req.StartDate)
	}
	sql += " ORDER BY end_date DESC LIMIT ? OFFSET ?"
	limit, offset := req.Limit, req.Offset
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.FinancialIndicator, 0, len(rows))
	for _, m := range rows {
		out = append(out, rowToFinancialIndicator(m))
	}
	return out, nil
}

// allowedFinancialTables 白名单，防止 SQL 注入
var allowedFinancialTables = map[string]bool{
	"income":       true,
	"balancesheet": true,
	"cashflow":     true,
}

// GetTableData FinancialReportReader — 按表名查 income / balancesheet / cashflow
func (r *Readers) GetTableData(ctx context.Context, table string, req analysis.FinancialReportRequest) ([]map[string]any, error) {
	if !allowedFinancialTables[table] {
		return nil, fmt.Errorf("invalid financial table: %s", table)
	}
	sql := "SELECT * FROM " + table + " WHERE ts_code = ?"
	args := []any{req.TsCode}
	if req.EndDate != nil && *req.EndDate != "" {
		sql += " AND end_date <= ?"
		args = append(args, *req.EndDate)
	}
	if req.StartDate != nil && *req.StartDate != "" {
		sql += " AND end_date >= ?"
		args = append(args, *req.StartDate)
	}
	if req.ReportType != nil && *req.ReportType != "" {
		sql += " AND report_type = ?"
		args = append(args, *req.ReportType)
	}
	sql += " ORDER BY end_date DESC LIMIT ? OFFSET ?"
	limit, offset := req.Limit, req.Offset
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// GetTradingDates TradeCalendarReader：从 trade_cal 表取 is_open=1 的日期（cal_date 或 cal_trade + is_open）
func (r *Readers) GetTradingDates(ctx context.Context, startDate, endDate string) ([]string, error) {
	runQuery := func(dateCol string) ([]string, error) {
		sql := "SELECT " + dateCol + " AS d FROM trade_cal WHERE (is_open = 1 OR is_open = '1') AND " + dateCol + " >= ? AND " + dateCol + " <= ? ORDER BY " + dateCol
		rows, err := r.db.Query(ctx, sql, startDate, endDate)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(rows))
		for _, m := range rows {
			d := str(m, "d")
			if d != "" {
				out = append(out, strings.TrimSpace(d))
			}
		}
		return out, nil
	}
	out, err := runQuery("cal_date")
	if err != nil && (strings.Contains(err.Error(), "cal_date") || strings.Contains(err.Error(), "column") || strings.Contains(err.Error(), "CAL_DATE")) {
		out, err = runQuery("cal_trade")
	}
	if err != nil {
		return nil, fmt.Errorf("trade_cal query: %w", err)
	}
	return out, nil
}

// GetRealtimeTicks 当日实时分笔：ts_realtime_mkt_tick 按 trade_time 倒序，limit 条
func (r *Readers) GetRealtimeTicks(ctx context.Context, tsCode string, limit int) ([]analysis.TickRow, error) {
	ok, _ := r.db.TableExists(ctx, "ts_realtime_mkt_tick")
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
		FROM ts_realtime_mkt_tick WHERE ts_code = ? ORDER BY trade_time DESC LIMIT ?`
	rows, err := r.db.Query(ctx, sql, tsCode, limit)
	if err != nil {
		return nil, err
	}
	return mapRowsToTickRows(rows), nil
}

// GetIntradayTicks 按日分时+盘口回放：ts_realtime_mkt_tick 按 ts_code + 日期过滤，trade_time 升序
func (r *Readers) GetIntradayTicks(ctx context.Context, tsCode, tradeDate string) ([]analysis.TickRow, error) {
	ok, _ := r.db.TableExists(ctx, "ts_realtime_mkt_tick")
	if !ok || tsCode == "" || tradeDate == "" {
		return nil, nil
	}
	// trade_date YYYYMMDD -> 当日 00:00:00 与次日 00:00:00（DuckDB 兼容字符串比较）
	if len(tradeDate) != 8 {
		return nil, nil
	}
	start := tradeDate[:4] + "-" + tradeDate[4:6] + "-" + tradeDate[6:8] + " 00:00:00"
	end := tradeDate[:4] + "-" + tradeDate[4:6] + "-" + tradeDate[6:8] + " 23:59:59"
	// DuckDB: trade_time 为 TIMESTAMP，需将字符串参数显式转为 TIMESTAMP 再比较
	sql := `SELECT ts_code, name, trade_time, pre_price, price, open, high, low, close, volume, amount,
		ask_price1, ask_volume1, bid_price1, bid_volume1, ask_price2, ask_volume2, bid_price2, bid_volume2,
		ask_price3, ask_volume3, bid_price3, bid_volume3, ask_price4, ask_volume4, bid_price4, bid_volume4,
		ask_price5, ask_volume5, bid_price5, bid_volume5
		FROM ts_realtime_mkt_tick WHERE ts_code = ? AND trade_time >= CAST(? AS TIMESTAMP) AND trade_time <= CAST(? AS TIMESTAMP)
		ORDER BY trade_time ASC`
	rows, err := r.db.Query(ctx, sql, tsCode, start, end)
	if err != nil {
		return nil, err
	}
	return mapRowsToTickRows(rows), nil
}

func mapRowsToTickRows(rows []map[string]any) []analysis.TickRow {
	out := make([]analysis.TickRow, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.TickRow{
			TradeTime:  str(m, "trade_time"),
			TsCode:     str(m, "ts_code"),
			Name:       str(m, "name"),
			PrePrice:   float(m, "pre_price"),
			Price:      float(m, "price"),
			Open:       float(m, "open"),
			High:       float(m, "high"),
			Low:        float(m, "low"),
			Close:      float(m, "close"),
			Volume:     float(m, "volume"),
			Amount:     float(m, "amount"),
			AskPrice1:  float(m, "ask_price1"),
			AskVolume1: float(m, "ask_volume1"),
			BidPrice1:  float(m, "bid_price1"),
			BidVolume1: float(m, "bid_volume1"),
			AskPrice2:  float(m, "ask_price2"),
			AskVolume2: float(m, "ask_volume2"),
			BidPrice2:  float(m, "bid_price2"),
			BidVolume2: float(m, "bid_volume2"),
			AskPrice3:  float(m, "ask_price3"),
			AskVolume3: float(m, "ask_volume3"),
			BidPrice3:  float(m, "bid_price3"),
			BidVolume3: float(m, "bid_volume3"),
			AskPrice4:  float(m, "ask_price4"),
			AskVolume4: float(m, "ask_volume4"),
			BidPrice4:  float(m, "bid_price4"),
			BidVolume4: float(m, "bid_volume4"),
			AskPrice5:  float(m, "ask_price5"),
			AskVolume5: float(m, "ask_volume5"),
			BidPrice5:  float(m, "bid_price5"),
			BidVolume5: float(m, "bid_volume5"),
		})
	}
	return out
}

// GetIntradayKline 分钟 K 线：rt_min 表，按 ts_code + trade_date
func (r *Readers) GetIntradayKline(ctx context.Context, tsCode, tradeDate, period string) ([]analysis.IntradayKlineRow, error) {
	ok, _ := r.db.TableExists(ctx, "rt_min")
	if !ok || tsCode == "" || tradeDate == "" {
		return nil, nil
	}
	if period == "" {
		period = "1m"
	}
	// rt_min 常见字段：ts_code, time 或 trade_time, open, high, low, close, vol, amount
	sql := `SELECT time, open, high, low, close, vol AS volume, amount FROM rt_min WHERE ts_code = ? AND trade_date = ? ORDER BY time`
	rows, err := r.db.Query(ctx, sql, tsCode, tradeDate)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.IntradayKlineRow, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.IntradayKlineRow{
			Time:   str(m, "time"),
			Open:   float(m, "open"),
			High:   float(m, "high"),
			Low:    float(m, "low"),
			Close:  float(m, "close"),
			Volume: float(m, "volume"),
			Amount: float(m, "amount"),
		})
	}
	return out, nil
}

func str(m map[string]any, key string) string {
	if v, ok := m[key]; ok && v != nil {
		return strings.TrimSpace(fmt.Sprint(v))
	}
	return ""
}

func float(m map[string]any, key string) float64 {
	if v, ok := m[key]; ok && v != nil {
		switch x := v.(type) {
		case float64:
			return x
		case int:
			return float64(x)
		case int64:
			return float64(x)
		case string:
			f, _ := strconv.ParseFloat(x, 64)
			return f
		}
	}
	return 0
}

func int_(m map[string]any, key string) int {
	return int(float(m, key))
}

func rowToFinancialIndicator(m map[string]any) analysis.FinancialIndicator {
	f := analysis.FinancialIndicator{
		TsCode: str(m, "ts_code"), AnnDate: str(m, "ann_date"), EndDate: str(m, "end_date"),
		ReportType: str(m, "report_type"), CompType: str(m, "comp_type"),
	}
	setFloatPtr(&f.Roe, m, "roe")
	setFloatPtr(&f.RoeAvg, m, "roe_avg")
	setFloatPtr(&f.Roa, m, "roa")
	setFloatPtr(&f.GrossProfitMargin, m, "gross_profit_margin")
	setFloatPtr(&f.NetProfitMargin, m, "net_profit_margin")
	setFloatPtr(&f.TotalAssetTurnover, m, "total_asset_turnover")
	setFloatPtr(&f.CurrentRatio, m, "current_ratio")
	setFloatPtr(&f.QuickRatio, m, "quick_ratio")
	setFloatPtr(&f.DebtToAsset, m, "debt_to_asset")
	setFloatPtr(&f.RevenueYoy, m, "revenue_yoy")
	setFloatPtr(&f.ProfitYoy, m, "profit_yoy")
	setFloatPtr(&f.Eps, m, "eps")
	setFloatPtr(&f.Bps, m, "bps")
	setFloatPtr(&f.Pe, m, "pe")
	setFloatPtr(&f.Pb, m, "pb")
	return f
}

func setFloatPtr(dst **float64, m map[string]any, key string) {
	v := float(m, key)
	if v != 0 {
		*dst = &v
	}
}

func rowToFinancialReport(m map[string]any) analysis.FinancialReport {
	f := analysis.FinancialReport{
		TsCode: str(m, "ts_code"), AnnDate: str(m, "ann_date"), FAnnDate: str(m, "f_ann_date"),
		EndDate: str(m, "end_date"), ReportType: str(m, "report_type"), CompType: str(m, "comp_type"),
	}
	setFloatPtr(&f.TotalAssets, m, "total_assets")
	setFloatPtr(&f.TotalLiab, m, "total_liab")
	setFloatPtr(&f.TotalEquity, m, "total_equity")
	setFloatPtr(&f.FixedAssets, m, "fixed_assets")
	setFloatPtr(&f.CurrentAssets, m, "current_assets")
	setFloatPtr(&f.CurrentLiab, m, "current_liab")
	setFloatPtr(&f.Revenue, m, "revenue")
	setFloatPtr(&f.OperatingProfit, m, "operating_profit")
	setFloatPtr(&f.TotalProfit, m, "total_profit")
	setFloatPtr(&f.NetProfit, m, "net_profit")
	setFloatPtr(&f.NetProfitAfter, m, "net_profit_after")
	setFloatPtr(&f.OperatingCashFlow, m, "operating_cash_flow")
	setFloatPtr(&f.InvestingCashFlow, m, "investing_cash_flow")
	setFloatPtr(&f.FinancingCashFlow, m, "financing_cash_flow")
	setFloatPtr(&f.NetCashFlow, m, "net_cash_flow")
	return f
}
