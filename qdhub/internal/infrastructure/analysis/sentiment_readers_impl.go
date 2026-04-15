package analysis

import (
	"context"
	"fmt"
	"sort"

	"qdhub/internal/domain/analysis"
)

// sentimentReadersImpl 实现 SentimentRawReader 接口
type sentimentReadersImpl struct {
	r *Readers
}

var _ analysis.SentimentRawReader = (*sentimentReadersImpl)(nil)

// ── GetSentimentTradeDates ────────────────────────────────────────────────────

// GetSentimentTradeDates 从 trade_cal 取 endDate 往前 window 个交易日（含 endDate），升序返回
func (s *sentimentReadersImpl) GetSentimentTradeDates(ctx context.Context, endDate string, window int) ([]string, error) {
	if endDate == "" {
		rows, err := s.r.db.Query(ctx, `SELECT MAX(trade_date) AS td FROM daily WHERE trade_date IS NOT NULL`)
		if err != nil || len(rows) == 0 {
			return nil, fmt.Errorf("cannot resolve latest trade date: %w", err)
		}
		endDate = str(rows[0], "td")
	}
	rows, err := s.r.db.Query(ctx,
		`SELECT cal_date FROM trade_cal
		 WHERE exchange = 'SSE' AND is_open = 1 AND cal_date <= ?
		 ORDER BY cal_date DESC LIMIT ?`,
		endDate, window)
	if err != nil || len(rows) == 0 {
		// fallback: 直接从 daily 取
		rows, err = s.r.db.Query(ctx,
			`SELECT DISTINCT trade_date AS cal_date FROM daily
			 WHERE trade_date <= ? ORDER BY trade_date DESC LIMIT ?`,
			endDate, window)
		if err != nil {
			return nil, err
		}
	}
	dates := make([]string, 0, len(rows))
	for _, m := range rows {
		d := str(m, "cal_date")
		if d != "" {
			dates = append(dates, d)
		}
	}
	sort.Strings(dates) // 升序
	return dates, nil
}

// ── GetRelayRawData ───────────────────────────────────────────────────────────

func (s *sentimentReadersImpl) GetRelayRawData(ctx context.Context, tradeDate string) (*analysis.RelayRawData, error) {
	raw := &analysis.RelayRawData{TradeDate: tradeDate}

	// 1. 基础涨跌停数（limit_list_ths 优先，其次 limit_list_d，最后 daily）
	raw.LimitUpCount, raw.LimitDownCount = s.fetchLimitCounts(ctx, tradeDate)

	// 2. 天梯数据（max board + ladder completeness）
	raw.MaxBoard, raw.LadderComplete = s.fetchLadderStats(ctx, tradeDate)

	// 3. 炸板率
	raw.BurstRate = s.fetchBurstRate(ctx, tradeDate)

	// 4. 分层晋级率（limit_step 中对比昨日）
	raw.LayerPromotions = s.fetchLayerPromotions(ctx, tradeDate)

	// 5. 昨日涨停溢价（分层+综合）
	raw.YesterdayPremiumAvg, raw.YesterdayPremiumByLayer = s.fetchYesterdayPremium(ctx, tradeDate)

	// 6. 7板异动监管
	raw.HighBoardWatch = s.fetchHighBoardWatch(ctx, tradeDate)

	return raw, nil
}

func (s *sentimentReadersImpl) fetchLimitCounts(ctx context.Context, tradeDate string) (int, int) {
	sql := `
SELECT COUNT(DISTINCT CASE WHEN l."limit" IN ('U','Z') THEN l.ts_code END) AS limit_up,
       COUNT(DISTINCT CASE WHEN l."limit" = 'D' THEN l.ts_code END) AS limit_down
FROM limit_list_d l
LEFT JOIN stock_basic sb ON sb.ts_code = l.ts_code
WHERE l.trade_date = ?
  AND NOT (TRIM(COALESCE(sb.name, l.name, '')) LIKE 'ST%' OR TRIM(COALESCE(sb.name, l.name, '')) LIKE '*ST%')`
	ok, _ := s.r.db.TableExists(ctx, "limit_list_d")
	if ok {
		rows, err := s.r.db.Query(ctx, sql, tradeDate)
		if err == nil && len(rows) > 0 {
			return int_(rows[0], "limit_up"), int_(rows[0], "limit_down")
		}
	}
	// fallback: daily
	rows, err := s.r.db.Query(ctx, `
SELECT COUNT(DISTINCT CASE WHEN pct_chg >= 9.8 THEN ts_code END) AS limit_up,
       COUNT(DISTINCT CASE WHEN pct_chg <= -9.8 THEN ts_code END) AS limit_down
FROM daily WHERE trade_date = ?`, tradeDate)
	if err != nil || len(rows) == 0 {
		return 0, 0
	}
	return int_(rows[0], "limit_up"), int_(rows[0], "limit_down")
}

func (s *sentimentReadersImpl) fetchLadderStats(ctx context.Context, tradeDate string) (int, float64) {
	ok, _ := s.r.db.TableExists(ctx, "limit_step")
	if !ok {
		return 0, 0
	}
	rows, err := s.r.db.Query(ctx,
		`SELECT COALESCE(step, 1) AS board, COUNT(*) AS cnt
		 FROM limit_step WHERE trade_date = ?
		 GROUP BY board ORDER BY board`, tradeDate)
	if err != nil || len(rows) == 0 {
		return 0, 0
	}
	presentLayers := make(map[int]bool)
	maxBoard := 0
	for _, m := range rows {
		board := int_(m, "board")
		presentLayers[board] = true
		if board > maxBoard {
			maxBoard = board
		}
	}
	if maxBoard == 0 {
		return 0, 0
	}
	filled := 0
	for i := 1; i <= maxBoard; i++ {
		if presentLayers[i] {
			filled++
		}
	}
	return maxBoard, float64(filled) / float64(maxBoard)
}

func (s *sentimentReadersImpl) fetchBurstRate(ctx context.Context, tradeDate string) float64 {
	// 炸板率 = 炸板数 / 涨停总数（含炸板）
	ok, _ := s.r.db.TableExists(ctx, "limit_list_ths")
	if ok {
		rows, err := s.r.db.Query(ctx, `
SELECT COUNT(CASE WHEN COALESCE(open_num, fc_ratio_open, 0) > 0 THEN 1 END) AS burst,
       COUNT(*) AS total
FROM limit_list_ths WHERE trade_date = ? AND lu_status IN ('U','Z')`, tradeDate)
		if err == nil && len(rows) > 0 {
			total := int_(rows[0], "total")
			if total > 0 {
				return float64(int_(rows[0], "burst")) / float64(total)
			}
		}
	}
	ok2, _ := s.r.db.TableExists(ctx, "limit_list_d")
	if ok2 {
		rows, err := s.r.db.Query(ctx, `
SELECT COUNT(CASE WHEN "limit" = 'Z' THEN 1 END) AS burst,
       COUNT(CASE WHEN "limit" IN ('U','Z') THEN 1 END) AS total
FROM limit_list_d WHERE trade_date = ?`, tradeDate)
		if err == nil && len(rows) > 0 {
			total := int_(rows[0], "total")
			if total > 0 {
				return float64(int_(rows[0], "burst")) / float64(total)
			}
		}
	}
	return 0
}

func (s *sentimentReadersImpl) fetchLayerPromotions(ctx context.Context, tradeDate string) []analysis.LayerPromotion {
	ok, _ := s.r.db.TableExists(ctx, "limit_step")
	if !ok {
		return nil
	}
	// 今日天梯
	todayRows, err := s.r.db.Query(ctx,
		`SELECT COALESCE(step, 1) AS board, COUNT(*) AS cnt FROM limit_step WHERE trade_date = ? GROUP BY board`,
		tradeDate)
	if err != nil {
		return nil
	}
	// 昨日天梯：取 limit_step 中 tradeDate 之前的最近一个交易日
	prevRows, err := s.r.db.Query(ctx,
		`SELECT COALESCE(step, 1) AS board, COUNT(*) AS cnt FROM limit_step
		 WHERE trade_date = (
		   SELECT MAX(trade_date) FROM limit_step WHERE trade_date < ?
		 )
		 GROUP BY board`, tradeDate)
	if err != nil {
		return nil
	}

	todayMap := make(map[int]int)
	for _, m := range todayRows {
		todayMap[int_(m, "board")] = int_(m, "cnt")
	}
	prevMap := make(map[int]int)
	maxPrev := 0
	for _, m := range prevRows {
		b := int_(m, "board")
		prevMap[b] = int_(m, "cnt")
		if b > maxPrev {
			maxPrev = b
		}
	}

	var promotions []analysis.LayerPromotion
	for n := 1; n <= maxPrev; n++ {
		base := prevMap[n]
		if base == 0 {
			continue
		}
		promoted := todayMap[n+1]
		rate := float64(promoted) / float64(base)
		promotions = append(promotions, analysis.LayerPromotion{
			FromLayer: n, ToLayer: n + 1,
			Label:     fmt.Sprintf("%d→%d", n, n+1),
			BaseCount: base, Promoted: promoted, Rate: rate,
		})
	}
	return promotions
}

func (s *sentimentReadersImpl) fetchYesterdayPremium(ctx context.Context, tradeDate string) (float64, []analysis.LayerPremium) {
	// 取昨日涨停股（来自 limit_list_ths 或 limit_step）及连板数，再匹配今日 daily.pct_chg
	ok, _ := s.r.db.TableExists(ctx, "limit_step")
	if !ok {
		return 0, nil
	}

	sql := `
WITH prev_date AS (
  SELECT MAX(trade_date) AS pd FROM limit_step WHERE trade_date < ?
),
prev_limitup AS (
  SELECT ls.ts_code, COALESCE(ls.step, 1) AS board
  FROM limit_step ls
  CROSS JOIN prev_date
  WHERE ls.trade_date = prev_date.pd
),
today_pct AS (
  SELECT d.ts_code, d.pct_chg
  FROM daily d WHERE d.trade_date = ?
)
SELECT pl.board,
       COUNT(*) AS cnt,
       AVG(tp.pct_chg) AS avg_pct,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tp.pct_chg) AS median_pct
FROM prev_limitup pl
JOIN today_pct tp ON tp.ts_code = pl.ts_code
GROUP BY pl.board
ORDER BY pl.board`
	rows, err := s.r.db.Query(ctx, sql, tradeDate, tradeDate)
	if err != nil || len(rows) == 0 {
		return 0, nil
	}

	var layers []analysis.LayerPremium
	totalPct, totalCount := 0.0, 0
	for _, m := range rows {
		board := int_(m, "board")
		cnt := int_(m, "cnt")
		avg := float(m, "avg_pct")
		med := float(m, "median_pct")
		layers = append(layers, analysis.LayerPremium{
			Layer: board, Label: fmt.Sprintf("%d板", board),
			Count: cnt, AvgPct: avg, MedianPct: med,
		})
		totalPct += avg * float64(cnt)
		totalCount += cnt
	}
	overall := 0.0
	if totalCount > 0 {
		overall = totalPct / float64(totalCount)
	}
	return overall, layers
}

func (s *sentimentReadersImpl) fetchHighBoardWatch(ctx context.Context, tradeDate string) analysis.HighBoardWatch {
	ok, _ := s.r.db.TableExists(ctx, "limit_step")
	if !ok {
		return analysis.HighBoardWatch{}
	}
	// 7板及以上统计
	rows, err := s.r.db.Query(ctx, `
WITH prev_date AS (
  SELECT MAX(trade_date) AS pd FROM limit_step WHERE trade_date < ?
),
today7 AS (
  SELECT ls.ts_code, COALESCE(ls.step, 1) AS board
  FROM limit_step ls WHERE ls.trade_date = ? AND COALESCE(ls.step, 1) >= 7
),
prev7 AS (
  SELECT ls.ts_code
  FROM limit_step ls CROSS JOIN prev_date
  WHERE ls.trade_date = prev_date.pd AND COALESCE(ls.step, 1) = 6
),
prev_limitup AS (
  SELECT ls.ts_code, COALESCE(ls.step, 1) AS board
  FROM limit_step ls CROSS JOIN prev_date
  WHERE ls.trade_date = prev_date.pd AND COALESCE(ls.step, 1) >= 7
)
SELECT
  (SELECT COUNT(*) FROM today7) AS count7,
  (SELECT COUNT(*) FROM prev7) AS promoted_to_7,
  (SELECT MAX(board) FROM today7) AS max_board,
  COALESCE(AVG(d.pct_chg), 0) AS avg_premium
FROM prev_limitup pl
JOIN daily d ON d.ts_code = pl.ts_code AND d.trade_date = ?`,
		tradeDate, tradeDate, tradeDate)
	if err != nil || len(rows) == 0 {
		return analysis.HighBoardWatch{}
	}
	m := rows[0]
	return analysis.HighBoardWatch{
		Count:       int_(m, "count7"),
		PromotedTo7: int_(m, "promoted_to_7"),
		AvgPremium:  float(m, "avg_premium"),
		MaxBoard:    int_(m, "max_board"),
	}
}

// ── GetTrendRawData ───────────────────────────────────────────────────────────

func (s *sentimentReadersImpl) GetTrendRawData(ctx context.Context, tradeDate string, hotSrc string) (*analysis.TrendRawData, error) {
	raw := &analysis.TrendRawData{TradeDate: tradeDate}

	// 1. 涨跌家数
	raw.UpCount, raw.DownCount, raw.TotalCount = s.fetchUpDownCount(ctx, tradeDate)

	// 2. 多头排列（MA5>MA10>MA20>MA60）— DuckDB window function
	raw.BullMACount, raw.BullMATotal = s.fetchBullMACount(ctx, tradeDate)

	// 3. 20日新高
	raw.NewHighCount, raw.NewHighTotal = s.fetchNewHighCount(ctx, tradeDate)

	// 4. 热股持续性（今日 vs 昨日）
	raw.TodayHotCodes = s.fetchHotCodes(ctx, tradeDate, hotSrc, 100)
	raw.YesterdayHotCodes = s.fetchYesterdayHotCodes(ctx, tradeDate, hotSrc, 100)

	return raw, nil
}

func (s *sentimentReadersImpl) fetchUpDownCount(ctx context.Context, tradeDate string) (int, int, int) {
	rows, err := s.r.db.Query(ctx, `
SELECT COUNT(CASE WHEN pct_chg > 0 THEN 1 END) AS up_count,
       COUNT(CASE WHEN pct_chg < 0 THEN 1 END) AS down_count,
       COUNT(*) AS total_count
FROM daily d
JOIN stock_basic sb ON sb.ts_code = d.ts_code
WHERE d.trade_date = ?
  AND sb.list_status = 'L'
  AND NOT (TRIM(COALESCE(sb.name, '')) LIKE 'ST%' OR TRIM(COALESCE(sb.name, '')) LIKE '*ST%')`, tradeDate)
	if err != nil || len(rows) == 0 {
		return 0, 0, 0
	}
	m := rows[0]
	return int_(m, "up_count"), int_(m, "down_count"), int_(m, "total_count")
}

func (s *sentimentReadersImpl) fetchBullMACount(ctx context.Context, tradeDate string) (int, int) {
	// 使用 DuckDB 窗口函数计算 MA5/MA10/MA20/MA60
	// 只扫最近 65 个交易日以减少数据量
	sql := `
WITH hist AS (
  SELECT d.ts_code, d.trade_date, d.close
  FROM daily d
  JOIN stock_basic sb ON sb.ts_code = d.ts_code
  WHERE d.trade_date <= ?
    AND d.trade_date >= (
      SELECT MIN(td) FROM (
        SELECT DISTINCT trade_date AS td FROM daily
        WHERE trade_date <= ? ORDER BY trade_date DESC LIMIT 65
      )
    )
    AND sb.list_status = 'L'
),
ma AS (
  SELECT ts_code, trade_date,
    AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS 4 PRECEDING) AS ma5,
    AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS 9 PRECEDING) AS ma10,
    AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS 19 PRECEDING) AS ma20,
    AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS 59 PRECEDING) AS ma60,
    COUNT(*) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS 59 PRECEDING) AS data_cnt
  FROM hist
)
SELECT
  COUNT(CASE WHEN ma5 > ma10 AND ma10 > ma20 AND ma20 > ma60 THEN 1 END) AS bull_ma,
  COUNT(CASE WHEN data_cnt >= 60 THEN 1 END) AS total
FROM ma WHERE trade_date = ?`
	rows, err := s.r.db.Query(ctx, sql, tradeDate, tradeDate, tradeDate)
	if err != nil || len(rows) == 0 {
		return 0, 0
	}
	return int_(rows[0], "bull_ma"), int_(rows[0], "total")
}

func (s *sentimentReadersImpl) fetchNewHighCount(ctx context.Context, tradeDate string) (int, int) {
	sql := `
WITH hist AS (
  SELECT d.ts_code, d.trade_date, d.close
  FROM daily d
  JOIN stock_basic sb ON sb.ts_code = d.ts_code
  WHERE d.trade_date <= ?
    AND d.trade_date >= (
      SELECT MIN(td) FROM (
        SELECT DISTINCT trade_date AS td FROM daily
        WHERE trade_date <= ? ORDER BY trade_date DESC LIMIT 22
      )
    )
    AND sb.list_status = 'L'
),
high20 AS (
  SELECT ts_code, trade_date, close,
    MAX(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS 19 PRECEDING) AS max_20,
    COUNT(*) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS 19 PRECEDING) AS data_cnt
  FROM hist
)
SELECT
  COUNT(CASE WHEN close >= max_20 AND data_cnt >= 20 THEN 1 END) AS new_high,
  COUNT(CASE WHEN data_cnt >= 20 THEN 1 END) AS total
FROM high20 WHERE trade_date = ?`
	rows, err := s.r.db.Query(ctx, sql, tradeDate, tradeDate, tradeDate)
	if err != nil || len(rows) == 0 {
		return 0, 0
	}
	return int_(rows[0], "new_high"), int_(rows[0], "total")
}

func (s *sentimentReadersImpl) fetchHotCodes(ctx context.Context, tradeDate, hotSrc string, top int) []string {
	table, rankCol := hotTableAndRankCol(hotSrc)
	ok, _ := s.r.db.TableExists(ctx, table)
	if !ok {
		return nil
	}
	rows, err := s.r.db.Query(ctx,
		fmt.Sprintf(`SELECT ts_code FROM %s WHERE trade_date = ? ORDER BY %s ASC LIMIT %d`, table, rankCol, top),
		tradeDate)
	if err != nil {
		return nil
	}
	codes := make([]string, 0, len(rows))
	for _, m := range rows {
		if c := str(m, "ts_code"); c != "" {
			codes = append(codes, c)
		}
	}
	return codes
}

func (s *sentimentReadersImpl) fetchYesterdayHotCodes(ctx context.Context, tradeDate, hotSrc string, top int) []string {
	table, rankCol := hotTableAndRankCol(hotSrc)
	ok, _ := s.r.db.TableExists(ctx, table)
	if !ok {
		return nil
	}
	rows, err := s.r.db.Query(ctx,
		fmt.Sprintf(`SELECT ts_code FROM %s
		 WHERE trade_date = (SELECT MAX(trade_date) FROM %s WHERE trade_date < ?)
		 ORDER BY %s ASC LIMIT %d`, table, table, rankCol, top),
		tradeDate)
	if err != nil {
		return nil
	}
	codes := make([]string, 0, len(rows))
	for _, m := range rows {
		if c := str(m, "ts_code"); c != "" {
			codes = append(codes, c)
		}
	}
	return codes
}

// ── GetMatrixRawData ──────────────────────────────────────────────────────────

func (s *sentimentReadersImpl) GetMatrixRawData(ctx context.Context, tradeDate string, hotSrc string, top int) (*analysis.MatrixRawData, error) {
	raw := &analysis.MatrixRawData{TradeDate: tradeDate, HotSrc: hotSrc}

	// 全市场：从 daily 取 open/pre_close/close
	allRows, err := s.r.db.Query(ctx, `
SELECT d.open, d.pre_close, d.close
FROM daily d
JOIN stock_basic sb ON sb.ts_code = d.ts_code
WHERE d.trade_date = ?
  AND sb.list_status = 'L'
  AND d.pre_close > 0 AND d.open > 0 AND d.close > 0`, tradeDate)
	if err == nil {
		pts := make([]analysis.MatrixPoint, 0, len(allRows))
		for _, m := range allRows {
			open := float(m, "open")
			preClose := float(m, "pre_close")
			close_ := float(m, "close")
			if preClose > 0 && open > 0 {
				pts = append(pts, analysis.MatrixPoint{
					OpenGap:   (open - preClose) / preClose * 100,
					DayReturn: (close_ - open) / open * 100,
				})
			}
		}
		raw.FullMarket = pts
	}

	// 热股 top100
	hotCodes := s.fetchHotCodes(ctx, tradeDate, hotSrc, top)
	if len(hotCodes) > 0 {
		inClause := buildInClause(len(hotCodes))
		args := make([]any, len(hotCodes)+1)
		args[0] = tradeDate
		for i, c := range hotCodes {
			args[i+1] = c
		}
		hotRows, err2 := s.r.db.Query(ctx, fmt.Sprintf(`
SELECT d.open, d.pre_close, d.close
FROM daily d
WHERE d.trade_date = ? AND d.ts_code IN (%s)
  AND d.pre_close > 0 AND d.open > 0 AND d.close > 0`, inClause), args...)
		if err2 == nil {
			hotPts := make([]analysis.MatrixPoint, 0, len(hotRows))
			for _, m := range hotRows {
				open := float(m, "open")
				preClose := float(m, "pre_close")
				close_ := float(m, "close")
				if preClose > 0 && open > 0 {
					hotPts = append(hotPts, analysis.MatrixPoint{
						OpenGap:   (open - preClose) / preClose * 100,
						DayReturn: (close_ - open) / open * 100,
					})
				}
			}
			raw.HotTop100 = hotPts
		}
	}
	return raw, nil
}

// ── SectorLeaderReader implementation ─────────────────────────────────────────

// sectorLeaderReaderImpl 实现 SectorLeaderReader 接口
type sectorLeaderReaderImpl struct {
	r *Readers
}

var _ analysis.SectorLeaderReader = (*sectorLeaderReaderImpl)(nil)

func (sl *sectorLeaderReaderImpl) GetSectorLeaders(ctx context.Context, req analysis.SectorLeaderRequest) (*analysis.SectorLeaderResult, error) {
	// 解析日期范围
	endDate := req.EndDate
	if endDate == "" {
		rows, err := sl.r.db.Query(ctx, `SELECT MAX(trade_date) AS td FROM daily`)
		if err != nil || len(rows) == 0 {
			return nil, fmt.Errorf("cannot resolve latest trade date")
		}
		endDate = str(rows[0], "td")
	}
	startDate := req.StartDate
	if startDate == "" {
		// 近 5 个交易日
		rows, err := sl.r.db.Query(ctx,
			`SELECT DISTINCT trade_date FROM daily WHERE trade_date <= ? ORDER BY trade_date DESC LIMIT 5`,
			endDate)
		if err != nil || len(rows) == 0 {
			return nil, fmt.Errorf("cannot resolve start date")
		}
		startDate = str(rows[len(rows)-1], "trade_date")
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 20
	}

	// 使用 concept_detail + daily 聚合（与 src 无关的通用方法）
	// 后续可根据 src 切换到 ths_member / dc_member
	memberTable := "concept_detail"
	codeCol := "concept_code"
	if req.ConceptIndexSrc == "ths" {
		// 优先尝试 ths_member
		ok, _ := sl.r.db.TableExists(ctx, "ths_member")
		if ok {
			memberTable = "ths_member"
			codeCol = "ts_code" // ths_member.ts_code 为概念代码
		}
	} else if req.ConceptIndexSrc == "eastmoney" {
		ok, _ := sl.r.db.TableExists(ctx, "dc_member")
		if ok {
			memberTable = "dc_member"
			codeCol = "ts_code"
		}
	}

	result := &analysis.SectorLeaderResult{
		StartDate:       startDate,
		EndDate:         endDate,
		ConceptIndexSrc: req.ConceptIndexSrc,
	}

	// 聚合板块涨幅
	conceptSQL := fmt.Sprintf(`
WITH member_daily AS (
  SELECT m.%s AS concept_code, d.ts_code, d.trade_date, d.pct_chg
  FROM %s m
  JOIN daily d ON d.ts_code = m.%s
  WHERE d.trade_date BETWEEN ? AND ?
),
concept_stats AS (
  SELECT concept_code,
         SUM(pct_chg) AS period_pct,
         COUNT(DISTINCT trade_date) AS trading_days,
         SUM(pct_chg) / NULLIF(COUNT(DISTINCT trade_date), 0) AS avg_daily_pct
  FROM member_daily
  GROUP BY concept_code
),
daily_ranks AS (
  SELECT concept_code, trade_date,
         RANK() OVER (PARTITION BY trade_date ORDER BY AVG(pct_chg) DESC) AS day_rank
  FROM member_daily
  GROUP BY concept_code, trade_date
),
top5_counts AS (
  SELECT concept_code, COUNT(*) AS top5_count
  FROM daily_ranks WHERE day_rank <= 5
  GROUP BY concept_code
),
names AS (
  SELECT DISTINCT %s AS concept_code, COALESCE(name, %s) AS concept_name
  FROM %s
)
SELECT cs.concept_code,
       COALESCE(n.concept_name, cs.concept_code) AS concept_name,
       cs.period_pct, cs.avg_daily_pct,
       COALESCE(tc.top5_count, 0) AS top5_count
FROM concept_stats cs
LEFT JOIN top5_counts tc ON tc.concept_code = cs.concept_code
LEFT JOIN names n ON n.concept_code = cs.concept_code`,
		codeCol, memberTable, memberTable,
		codeCol, codeCol, memberTable)

	// 领涨板块
	leaderSQL := conceptSQL + fmt.Sprintf(`
ORDER BY cs.period_pct DESC LIMIT %d`, limit)
	leaderRows, err := sl.r.db.Query(ctx, leaderSQL, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("sector leader query: %w", err)
	}
	result.Leaders = sl.buildLeaderItems(ctx, leaderRows, memberTable, codeCol, startDate, endDate)

	// 领跌板块
	laggardSQL := conceptSQL + fmt.Sprintf(`
ORDER BY cs.period_pct ASC LIMIT %d`, limit)
	laggardRows, err := sl.r.db.Query(ctx, laggardSQL, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("sector laggard query: %w", err)
	}
	result.Laggards = sl.buildLeaderItems(ctx, laggardRows, memberTable, codeCol, startDate, endDate)

	// 热股持续性历史
	hotTable, hotRankCol := hotTableAndRankColBySrc(req.ConceptIndexSrc)
	result.SurvivorHistory = sl.fetchSurvivorHistory(ctx, startDate, endDate, hotTable, hotRankCol)

	return result, nil
}

func (sl *sectorLeaderReaderImpl) buildLeaderItems(ctx context.Context, rows []map[string]any, memberTable, codeCol, startDate, endDate string) []analysis.SectorLeaderItem {
	items := make([]analysis.SectorLeaderItem, 0, len(rows))
	for _, m := range rows {
		code := str(m, "concept_code")
		item := analysis.SectorLeaderItem{
			Code:        code,
			Name:        str(m, "concept_name"),
			ReturnPct:   float(m, "period_pct"),
			AvgDailyPct: float(m, "avg_daily_pct"),
			Top5Count:   int_(m, "top5_count"),
		}
		// 查询该板块内龙头股（前5，按区间涨幅排序）
		item.LeaderStocks = sl.fetchLeaderStocks(ctx, code, memberTable, codeCol, startDate, endDate)
		items = append(items, item)
	}
	return items
}

func (sl *sectorLeaderReaderImpl) fetchLeaderStocks(ctx context.Context, conceptCode, memberTable, codeCol, startDate, endDate string) []analysis.SectorLeaderStock {
	sql := fmt.Sprintf(`
WITH member_stocks AS (
  SELECT DISTINCT %s AS stock_code FROM %s WHERE %s = ?
),
stock_period AS (
  SELECT d.ts_code, sb.name,
         SUM(d.pct_chg) AS period_pct,
         LAST(d.close ORDER BY d.trade_date) AS last_price,
         COUNT(CASE WHEN d.pct_chg > (
             SELECT AVG(d2.pct_chg) FROM daily d2 WHERE d2.trade_date = d.trade_date
         ) THEN 1 END) AS repeat_rank_count
  FROM daily d
  JOIN member_stocks ms ON ms.stock_code = d.ts_code
  LEFT JOIN stock_basic sb ON sb.ts_code = d.ts_code
  WHERE d.trade_date BETWEEN ? AND ?
  GROUP BY d.ts_code, sb.name
)
SELECT ts_code, name, last_price AS price, period_pct, repeat_rank_count
FROM stock_period
ORDER BY period_pct DESC LIMIT 5`,
		conMemberStockCol(memberTable), memberTable, codeCol)
	rows, err := sl.r.db.Query(ctx, sql, conceptCode, startDate, endDate)
	if err != nil {
		return nil
	}
	stocks := make([]analysis.SectorLeaderStock, 0, len(rows))
	for _, m := range rows {
		stocks = append(stocks, analysis.SectorLeaderStock{
			TsCode:          str(m, "ts_code"),
			Name:            str(m, "name"),
			Price:           float(m, "price"),
			ReturnPct:       float(m, "period_pct"),
			RepeatRankCount: int_(m, "repeat_rank_count"),
		})
	}
	return stocks
}

func (sl *sectorLeaderReaderImpl) fetchSurvivorHistory(ctx context.Context, startDate, endDate string, hotTable, rankCol string) []analysis.SurvivorPoint {
	ok, _ := sl.r.db.TableExists(ctx, hotTable)
	if !ok {
		return nil
	}
	sql := fmt.Sprintf(`
WITH dates AS (
  SELECT DISTINCT trade_date FROM %s
  WHERE trade_date BETWEEN ? AND ?
),
daily_hot AS (
  SELECT trade_date, ts_code FROM %s
  WHERE trade_date BETWEEN ? AND ?
  AND %s <= 100
),
lagged AS (
  SELECT a.trade_date,
         COUNT(CASE WHEN b.ts_code IS NOT NULL THEN 1 END) AS survivor,
         COUNT(CASE WHEN b.ts_code IS NULL THEN 1 END) AS new_entry,
         0 AS exit_count
  FROM daily_hot a
  LEFT JOIN daily_hot b ON b.ts_code = a.ts_code
    AND b.trade_date = (
      SELECT MAX(c.trade_date) FROM %s c WHERE c.trade_date < a.trade_date
    )
  GROUP BY a.trade_date
)
SELECT trade_date, survivor AS survivor_count,
       new_entry AS new_entry_count, exit_count
FROM lagged ORDER BY trade_date`,
		hotTable, hotTable, rankCol, hotTable)
	rows, err := sl.r.db.Query(ctx, sql, startDate, endDate, startDate, endDate)
	if err != nil {
		return nil
	}
	pts := make([]analysis.SurvivorPoint, 0, len(rows))
	for _, m := range rows {
		pts = append(pts, analysis.SurvivorPoint{
			TradeDate:     str(m, "trade_date"),
			SurvivorCount: int_(m, "survivor_count"),
			NewEntryCount: int_(m, "new_entry_count"),
			ExitCount:     int_(m, "exit_count"),
		})
	}
	return pts
}

// ── helpers ───────────────────────────────────────────────────────────────────

// hotTableAndRankCol 返回热股表名和排名列
func hotTableAndRankCol(src string) (string, string) {
	switch analysis.PopularityRankSrc(src) {
	case analysis.PopularityRankSrcEastmoney:
		return "dc_hot", "rank"
	case analysis.PopularityRankSrcKPL:
		return "kpl_list", "rank"
	default:
		return "ths_hot", "rank"
	}
}

func hotTableAndRankColBySrc(conceptSrc string) (string, string) {
	if conceptSrc == "eastmoney" {
		return "dc_hot", "rank"
	}
	return "ths_hot", "rank"
}

func conMemberStockCol(memberTable string) string {
	switch memberTable {
	case "ths_member", "dc_member":
		return "con_code"
	default:
		return "ts_code"
	}
}

// buildInClause 构建 SQL IN 占位符列表 "?,?,?..."
func buildInClause(n int) string {
	if n == 0 {
		return ""
	}
	s := "?"
	for i := 1; i < n; i++ {
		s += ",?"
	}
	return s
}
