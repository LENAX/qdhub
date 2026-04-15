package analysis

import (
	"context"
	"fmt"
	"math"
	"sort"
)

// ── GetPopularityRank ──────────────────────────────────────────────────────────

func (s *analysisServiceImpl) GetPopularityRank(ctx context.Context, req PopularityRankRequest) ([]PopularityRank, error) {
	if req.Src == "" {
		req.Src = string(PopularityRankSrcTHS)
	}
	if !ValidPopularityRankSrc(req.Src) {
		return nil, fmt.Errorf("invalid popularity rank src %q, must be one of: ths, eastmoney, kpl", req.Src)
	}
	if req.Limit <= 0 {
		req.Limit = 50
	}
	return s.popularityRankReader.GetRank(ctx, req)
}

// ── GetMarketSentiment ────────────────────────────────────────────────────────

func (s *analysisServiceImpl) GetMarketSentiment(ctx context.Context, req MarketSentimentRequest) (*DailySentimentResult, error) {
	req = applyMarketSentimentDefaults(req)

	// 1. 取目标日期（如空则用最新）
	tradeDate := req.TradeDate

	// 2. 取 window 个历史交易日（含 tradeDate），用于计算分位数
	dates, err := s.sentimentRawReader.GetSentimentTradeDates(ctx, tradeDate, req.Window)
	if err != nil {
		return nil, fmt.Errorf("get trade dates: %w", err)
	}
	if len(dates) == 0 {
		return nil, fmt.Errorf("no trading dates found for %s", tradeDate)
	}
	// 实际目标日期为 dates 中最后一个（按升序排列时为最大值）
	targetDate := dates[len(dates)-1]

	weights := resolveWeights(req.Style)

	// 3. 计算目标日的完整情绪
	result, err := s.computeDailySentiment(ctx, targetDate, req.HotSrc, weights)
	if err != nil {
		return nil, err
	}

	// 4. 批量计算历史原始分，用于分位数排名
	allPoints := make([]rawPoint, 0, len(dates))
	for _, d := range dates {
		if d == targetDate {
			allPoints = append(allPoints, rawPoint{
				date: d, relay: result.RelayScoreRaw,
				trend: result.TrendScoreRaw, mat: result.MatrixScoreRaw,
				comp: weights.Relay*result.RelayScoreRaw + weights.Trend*result.TrendScoreRaw + weights.Matrix*result.MatrixScoreRaw,
			})
			continue
		}
		r, e := s.computeRawScores(ctx, d, req.HotSrc)
		if e != nil {
			continue
		}
		comp := weights.Relay*r[0] + weights.Trend*r[1] + weights.Matrix*r[2]
		allPoints = append(allPoints, rawPoint{date: d, relay: r[0], trend: r[1], mat: r[2], comp: comp})
	}

	// 5. 计算目标日在窗口内的分位数（0→1）
	relayPct := percentileRank(allPoints, targetDate, func(p rawPoint) float64 { return p.relay })
	trendPct := percentileRank(allPoints, targetDate, func(p rawPoint) float64 { return p.trend })
	matPct := percentileRank(allPoints, targetDate, func(p rawPoint) float64 { return p.mat })
	compPct := percentileRank(allPoints, targetDate, func(p rawPoint) float64 { return p.comp })

	result.RelayScore = relayPct * 100
	result.TrendScore = trendPct * 100
	result.MatrixScore = matPct * 100
	result.CompositeScore = compPct * 100
	result.TemperaturePercentile = compPct
	result.SentimentLevel = sentimentLevelFromPct(compPct)
	result.TemperatureLevel = temperatureLevelFromPct(compPct)
	return result, nil
}

// ── GetSentimentHistory ───────────────────────────────────────────────────────

func (s *analysisServiceImpl) GetSentimentHistory(ctx context.Context, req SentimentHistoryRequest) (*SentimentHistoryResult, error) {
	if req.Style == "" {
		req.Style = SentimentStyleBalanced
	}
	if req.Window <= 0 {
		req.Window = 120
	}
	if req.HotSrc == "" {
		req.HotSrc = string(PopularityRankSrcTHS)
	}

	weights := resolveWeights(req.Style)

	// 计算扩展窗口：从 StartDate - window 开始，取到 EndDate，保证每个点都有 window 个历史
	extDates, err := s.sentimentRawReader.GetSentimentTradeDates(ctx, req.EndDate, req.Window+200)
	if err != nil {
		return nil, fmt.Errorf("get extended trade dates: %w", err)
	}

	allRaw := make([]rawPoint, 0, len(extDates))
	for _, d := range extDates {
		r, e := s.computeRawScores(ctx, d, req.HotSrc)
		if e != nil {
			continue
		}
		allRaw = append(allRaw, rawPoint{date: d, relay: r[0], trend: r[1], mat: r[2]})
	}
	sort.Slice(allRaw, func(i, j int) bool { return allRaw[i].date < allRaw[j].date })

	out := &SentimentHistoryResult{
		Style:     string(req.Style),
		Window:    req.Window,
		StartDate: req.StartDate,
		EndDate:   req.EndDate,
	}

	for i, p := range allRaw {
		if p.date < req.StartDate || p.date > req.EndDate {
			continue
		}
		// window 个历史点（含当前）
		windowStart := i - req.Window + 1
		if windowStart < 0 {
			windowStart = 0
		}
		window := allRaw[windowStart : i+1]

		relayPct := percentileRankInSlice(window, p.relay, func(x rawPoint) float64 { return x.relay })
		trendPct := percentileRankInSlice(window, p.trend, func(x rawPoint) float64 { return x.trend })
		matPct := percentileRankInSlice(window, p.mat, func(x rawPoint) float64 { return x.mat })
		compRaw := weights.Relay*p.relay + weights.Trend*p.trend + weights.Matrix*p.mat
		windowComps := make([]float64, len(window))
		for j, w := range window {
			windowComps[j] = weights.Relay*w.relay + weights.Trend*w.trend + weights.Matrix*w.mat
		}
		compPct := percentileRankInFloats(windowComps, compRaw)

		_ = compRaw
		out.Points = append(out.Points, SentimentHistoryPoint{
			TradeDate:             p.date,
			RelayScore:            relayPct * 100,
			TrendScore:            trendPct * 100,
			MatrixScore:           matPct * 100,
			CompositeScore:        compPct * 100,
			SentimentLevel:        sentimentLevelFromPct(compPct),
			TemperatureLevel:      temperatureLevelFromPct(compPct),
			TemperaturePercentile: compPct,
		})
	}
	return out, nil
}

// ── GetSentimentExtremes ──────────────────────────────────────────────────────

func (s *analysisServiceImpl) GetSentimentExtremes(ctx context.Context, req SentimentExtremesRequest) (*SentimentExtremesResult, error) {
	if req.Style == "" {
		req.Style = SentimentStyleBalanced
	}
	if req.Window <= 0 {
		req.Window = 120
	}
	if req.FreezingThreshold <= 0 {
		req.FreezingThreshold = 0.15
	}
	if req.BoilingThreshold <= 0 {
		req.BoilingThreshold = 0.85
	}
	if req.HotSrc == "" {
		req.HotSrc = string(PopularityRankSrcTHS)
	}
	if req.ReversalHorizon <= 0 {
		req.ReversalHorizon = 5
	}

	weights := resolveWeights(req.Style)

	// 取最近 max(window, 60+window) 个交易日原始分
	horizon := req.Window + 60 + req.ReversalHorizon
	extDates, err := s.sentimentRawReader.GetSentimentTradeDates(ctx, req.EndDate, horizon)
	if err != nil {
		return nil, err
	}
	type rawPt struct {
		date string
		comp float64
	}
	allRaw := make([]rawPt, 0, len(extDates))
	for _, d := range extDates {
		r, e := s.computeRawScores(ctx, d, req.HotSrc)
		if e != nil {
			continue
		}
		comp := weights.Relay*r[0] + weights.Trend*r[1] + weights.Matrix*r[2]
		allRaw = append(allRaw, rawPt{date: d, comp: comp})
	}
	sort.Slice(allRaw, func(i, j int) bool { return allRaw[i].date < allRaw[j].date })

	// 为每个点计算百分位
	scored := make([]scoredPt, 0, len(allRaw))
	for i, p := range allRaw {
		wStart := i - req.Window + 1
		if wStart < 0 {
			wStart = 0
		}
		window := allRaw[wStart : i+1]
		comps := make([]float64, len(window))
		for j, w := range window {
			comps[j] = w.comp
		}
		pct := percentileRankInFloats(comps, p.comp)
		scored = append(scored, scoredPt{date: p.date, pct: pct})
	}

	endDate := req.EndDate
	if endDate == "" && len(scored) > 0 {
		endDate = scored[len(scored)-1].date
	}
	date20ago := nTradingDatesBack(scored, endDate, 20)
	date60ago := nTradingDatesBack(scored, endDate, 60)

	res := &SentimentExtremesResult{
		Style:             string(req.Style),
		Window:            req.Window,
		EndDate:           endDate,
		FreezingThreshold: req.FreezingThreshold,
		BoilingThreshold:  req.BoilingThreshold,
	}

	// 统计冰点 / 沸点
	var freezeDates, boilDates []string
	for _, sp := range scored {
		if sp.date > endDate {
			continue
		}
		if sp.pct <= req.FreezingThreshold {
			freezeDates = append(freezeDates, sp.date)
		}
		if sp.pct >= req.BoilingThreshold {
			boilDates = append(boilDates, sp.date)
		}
	}

	res.Freezing = buildExtremeSummary(freezeDates, date20ago, date60ago, scored, req.FreezingThreshold, req.ReversalHorizon, true)
	res.Boiling = buildExtremeSummary(boilDates, date20ago, date60ago, scored, req.BoilingThreshold, req.ReversalHorizon, false)
	return res, nil
}

// ── GetSectorLeaders ─────────────────────────────────────────────────────────

func (s *analysisServiceImpl) GetSectorLeaders(ctx context.Context, req SectorLeaderRequest) (*SectorLeaderResult, error) {
	if req.ConceptIndexSrc == "" {
		req.ConceptIndexSrc = "ths"
	}
	if req.Limit <= 0 {
		req.Limit = 20
	}
	return s.sectorLeaderReader.GetSectorLeaders(ctx, req)
}

// ── internal helpers ──────────────────────────────────────────────────────────

func applyMarketSentimentDefaults(req MarketSentimentRequest) MarketSentimentRequest {
	if req.Style == "" {
		req.Style = SentimentStyleBalanced
	}
	if req.Window <= 0 {
		req.Window = 120
	}
	if req.HotSrc == "" {
		req.HotSrc = string(PopularityRankSrcTHS)
	}
	return req
}

// computeRawScores 返回 [relay, trend, matrix] 三层原始分 [0,1]
func (s *analysisServiceImpl) computeRawScores(ctx context.Context, tradeDate string, hotSrc string) ([3]float64, error) {
	result, err := s.computeDailySentiment(ctx, tradeDate, hotSrc, resolveWeights(SentimentStyleBalanced))
	if err != nil {
		return [3]float64{}, err
	}
	return [3]float64{result.RelayScoreRaw, result.TrendScoreRaw, result.MatrixScoreRaw}, nil
}

// computeDailySentiment 计算单日情绪（原始分，不含分位标准化）
func (s *analysisServiceImpl) computeDailySentiment(ctx context.Context, tradeDate string, hotSrc string, weights sentimentWeights) (*DailySentimentResult, error) {
	relayRaw, err := s.sentimentRawReader.GetRelayRawData(ctx, tradeDate)
	if err != nil {
		return nil, fmt.Errorf("relay raw data [%s]: %w", tradeDate, err)
	}
	trendRaw, err := s.sentimentRawReader.GetTrendRawData(ctx, tradeDate, hotSrc)
	if err != nil {
		return nil, fmt.Errorf("trend raw data [%s]: %w", tradeDate, err)
	}
	matrixRaw, err := s.sentimentRawReader.GetMatrixRawData(ctx, tradeDate, hotSrc, 100)
	if err != nil {
		return nil, fmt.Errorf("matrix raw data [%s]: %w", tradeDate, err)
	}

	relayScore := computeRelayScore(relayRaw)
	trendScore := computeTrendScore(trendRaw)
	matrixScore := computeMatrixScore(matrixRaw)

	result := &DailySentimentResult{
		TradeDate:      tradeDate,
		Style:          string(SentimentStyleBalanced),
		RelayScoreRaw:  relayScore,
		TrendScoreRaw:  trendScore,
		MatrixScoreRaw: matrixScore,
		RelayBreakdown: buildRelayBreakdown(relayRaw),
		TrendBreakdown: buildTrendBreakdown(trendRaw),
		MatrixBreakdown: buildMatrixBreakdown(matrixRaw),
	}
	return result, nil
}

// computeRelayScore 接力层原始分 [0,1]
func computeRelayScore(raw *RelayRawData) float64 {
	if raw == nil {
		return 0
	}
	// 涨停家数：50家以上满分
	limitUpNorm := math.Min(float64(raw.LimitUpCount)/50.0, 1.0)
	// 最高连板：10板以上满分
	maxBoardNorm := math.Min(float64(raw.MaxBoard)/10.0, 1.0)
	// 天梯完整度 [0,1]
	ladderNorm := clamp01(raw.LadderComplete)
	// 低炸板率更佳（炸板率0=满分，1=0分）
	burstPenalty := clamp01(1.0 - raw.BurstRate)
	// 平均晋级率
	avgPromotion := 0.0
	if len(raw.LayerPromotions) > 0 {
		sum := 0.0
		for _, lp := range raw.LayerPromotions {
			sum += clamp01(lp.Rate)
		}
		avgPromotion = sum / float64(len(raw.LayerPromotions))
	}
	// 昨日涨停溢价：[-5%,+10%] 映射到 [0,1]
	premiumNorm := clamp01((raw.YesterdayPremiumAvg + 5.0) / 15.0)

	return 0.25*limitUpNorm + 0.20*maxBoardNorm + 0.15*ladderNorm +
		0.15*burstPenalty + 0.15*avgPromotion + 0.10*premiumNorm
}

// computeTrendScore 趋势层原始分 [0,1]
func computeTrendScore(raw *TrendRawData) float64 {
	if raw == nil {
		return 0
	}
	total := math.Max(float64(raw.TotalCount), 1)
	upDownNorm := clamp01(float64(raw.UpCount) / total)
	bullMARate := 0.0
	if raw.BullMATotal > 0 {
		bullMARate = clamp01(float64(raw.BullMACount) / float64(raw.BullMATotal))
	}
	newHighRate := 0.0
	if raw.NewHighTotal > 0 {
		newHighRate = clamp01(float64(raw.NewHighCount) / float64(raw.NewHighTotal))
	}
	survivorRate := 0.0
	if len(raw.YesterdayHotCodes) > 0 {
		survivorSet := make(map[string]struct{}, len(raw.TodayHotCodes))
		for _, c := range raw.TodayHotCodes {
			survivorSet[c] = struct{}{}
		}
		survived := 0
		for _, c := range raw.YesterdayHotCodes {
			if _, ok := survivorSet[c]; ok {
				survived++
			}
		}
		survivorRate = clamp01(float64(survived) / float64(len(raw.YesterdayHotCodes)))
	}
	return 0.35*upDownNorm + 0.30*bullMARate + 0.15*newHighRate + 0.20*survivorRate
}

// computeMatrixScore 四象限层原始分 [0,1]
func computeMatrixScore(raw *MatrixRawData) float64 {
	if raw == nil || len(raw.FullMarket) == 0 {
		return 0
	}
	total := float64(len(raw.FullMarket))
	posDay := 0
	var sumPosDayRet, sumNegDayRet float64
	posDayCount, negDayCount := 0, 0
	for _, pt := range raw.FullMarket {
		if pt.DayReturn >= 0 {
			posDay++
			sumPosDayRet += pt.DayReturn
			posDayCount++
		} else {
			sumNegDayRet += math.Abs(pt.DayReturn)
			negDayCount++
		}
	}
	directional := float64(posDay) / total
	avgPos := 0.0
	if posDayCount > 0 {
		avgPos = sumPosDayRet / float64(posDayCount)
	}
	avgNeg := 0.0
	if negDayCount > 0 {
		avgNeg = sumNegDayRet / float64(negDayCount)
	}
	// magnitude: 正收益均值 vs 负收益均值之差，归一化
	magnitude := clamp01((avgPos-avgNeg+2.0)/4.0)
	return 0.60*directional + 0.40*magnitude
}

// buildRelayBreakdown 将原始数据转为输出结构
func buildRelayBreakdown(r *RelayRawData) RelayBreakdown {
	if r == nil {
		return RelayBreakdown{}
	}
	return RelayBreakdown{
		LimitUpCount:            r.LimitUpCount,
		LimitDownCount:          r.LimitDownCount,
		MaxBoard:                r.MaxBoard,
		LadderComplete:          r.LadderComplete,
		BurstRate:               r.BurstRate,
		LayerPromotions:         r.LayerPromotions,
		YesterdayPremiumAvg:     r.YesterdayPremiumAvg,
		YesterdayPremiumByLayer: r.YesterdayPremiumByLayer,
		HighBoardWatch:          r.HighBoardWatch,
	}
}

// buildTrendBreakdown 将原始数据转为输出结构（含持续性）
func buildTrendBreakdown(r *TrendRawData) TrendBreakdown {
	if r == nil {
		return TrendBreakdown{}
	}
	total := math.Max(float64(r.TotalCount), 1)
	redBoardRate := float64(r.UpCount) / total
	bullMARate := 0.0
	if r.BullMATotal > 0 {
		bullMARate = float64(r.BullMACount) / float64(r.BullMATotal)
	}
	newHighRate := 0.0
	if r.NewHighTotal > 0 {
		newHighRate = float64(r.NewHighCount) / float64(r.NewHighTotal)
	}

	// 计算持续性
	todaySet := make(map[string]struct{}, len(r.TodayHotCodes))
	for _, c := range r.TodayHotCodes {
		todaySet[c] = struct{}{}
	}
	yesterdaySet := make(map[string]struct{}, len(r.YesterdayHotCodes))
	for _, c := range r.YesterdayHotCodes {
		yesterdaySet[c] = struct{}{}
	}
	survived, newEntry, exit := 0, 0, 0
	for _, c := range r.TodayHotCodes {
		if _, ok := yesterdaySet[c]; ok {
			survived++
		} else {
			newEntry++
		}
	}
	for _, c := range r.YesterdayHotCodes {
		if _, ok := todaySet[c]; !ok {
			exit++
		}
	}
	survivorRate := 0.0
	if len(r.YesterdayHotCodes) > 0 {
		survivorRate = float64(survived) / float64(len(r.YesterdayHotCodes))
	}
	direction := "stable"
	if newEntry > exit+5 {
		direction = "expanding"
	} else if exit > newEntry+5 {
		direction = "contracting"
	}

	return TrendBreakdown{
		UpCount:      r.UpCount,
		DownCount:    r.DownCount,
		TotalCount:   r.TotalCount,
		RedBoardRate: redBoardRate,
		BullMACount:  r.BullMACount,
		BullMARate:   bullMARate,
		NewHighCount: r.NewHighCount,
		NewHighRate:  newHighRate,
		Persistence: TrendPersistence{
			SurvivorCount: survived, SurvivorRate: survivorRate,
			NewEntryCount: newEntry, ExitCount: exit,
			Direction: direction,
		},
	}
}

// buildMatrixBreakdown 将原始数据转为输出结构
func buildMatrixBreakdown(r *MatrixRawData) MatrixBreakdown {
	if r == nil {
		return MatrixBreakdown{}
	}
	return MatrixBreakdown{
		FullMarket: buildMatrixStats(r.FullMarket),
		HotTop100:  buildMatrixStats(r.HotTop100),
		HotSrc:     r.HotSrc,
	}
}

func buildMatrixStats(pts []MatrixPoint) MatrixStats {
	if len(pts) == 0 {
		return MatrixStats{}
	}
	type accum struct {
		count        int
		sumOpenPct   float64
		sumDayPct    float64
	}
	quads := make([]accum, 4)
	labels := []string{"高开高走", "低开高走", "高开低走", "低开低走"}
	openSigns := []string{"pos", "neg", "pos", "neg"}
	daySigns := []string{"pos", "pos", "neg", "neg"}

	var sumPosOpen, sumNegOpen, sumPosDay, sumNegDay float64
	posOpenCount, negOpenCount, posDayCount, negDayCount := 0, 0, 0, 0

	for _, pt := range pts {
		idx := 0
		if pt.OpenGap < 0 {
			idx |= 1 // bit1: neg open
		}
		if pt.DayReturn < 0 {
			idx |= 2 // bit2: neg day
		}
		quads[idx].count++
		quads[idx].sumOpenPct += pt.OpenGap
		quads[idx].sumDayPct += pt.DayReturn

		if pt.OpenGap >= 0 {
			sumPosOpen += pt.OpenGap
			posOpenCount++
		} else {
			sumNegOpen += pt.OpenGap
			negOpenCount++
		}
		if pt.DayReturn >= 0 {
			sumPosDay += pt.DayReturn
			posDayCount++
		} else {
			sumNegDay += pt.DayReturn
			negDayCount++
		}
	}
	// 重映射：idx 0=高开高走(posOpen,posDay), 1=低开高走(negOpen,posDay), 2=高开低走(posOpen,negDay), 3=低开低走(negOpen,negDay)
	// idx bit mapping: bit0=negOpen, bit1=negDay
	// idx 0 → openSign=pos, daySign=pos → label[0]
	// idx 1 → openSign=neg, daySign=pos → label[1]
	// idx 2 → openSign=pos, daySign=neg → label[2]
	// idx 3 → openSign=neg, daySign=neg → label[3]
	total := len(pts)
	outQuads := make([]MatrixQuadrant, 4)
	for i := 0; i < 4; i++ {
		avgOpen, avgDay := 0.0, 0.0
		if quads[i].count > 0 {
			avgOpen = quads[i].sumOpenPct / float64(quads[i].count)
			avgDay = quads[i].sumDayPct / float64(quads[i].count)
		}
		outQuads[i] = MatrixQuadrant{
			Label:      labels[i],
			OpenSign:   openSigns[i],
			DaySign:    daySigns[i],
			Count:      quads[i].count,
			Ratio:      float64(quads[i].count) / float64(total),
			AvgOpenPct: avgOpen,
			AvgDayPct:  avgDay,
		}
	}

	posOpenAvg, negOpenAvg, posDayAvg, negDayAvg := 0.0, 0.0, 0.0, 0.0
	if posOpenCount > 0 {
		posOpenAvg = sumPosOpen / float64(posOpenCount)
	}
	if negOpenCount > 0 {
		negOpenAvg = sumNegOpen / float64(negOpenCount)
	}
	if posDayCount > 0 {
		posDayAvg = sumPosDay / float64(posDayCount)
	}
	if negDayCount > 0 {
		negDayAvg = sumNegDay / float64(negDayCount)
	}
	return MatrixStats{
		TotalCount: total, Quadrants: outQuads,
		PosOpenAvg: posOpenAvg, NegOpenAvg: negOpenAvg,
		PosDayAvg: posDayAvg, NegDayAvg: negDayAvg,
	}
}

// ── Percentile helpers ────────────────────────────────────────────────────────

type rawPoint struct {
	date  string
	relay float64
	trend float64
	mat   float64
	comp  float64 // 加权综合原始分
}

func percentileRank(pts []rawPoint, targetDate string, val func(rawPoint) float64) float64 {
	if len(pts) == 0 {
		return 0.5
	}
	var target float64
	found := false
	for _, p := range pts {
		if p.date == targetDate {
			target = val(p)
			found = true
			break
		}
	}
	if !found {
		return 0.5
	}
	return percentileRankInFloatsFromSlice(pts, target, val)
}

func percentileRankInSlice[T any](pts []T, current float64, val func(T) float64) float64 {
	vals := make([]float64, len(pts))
	for i, p := range pts {
		vals[i] = val(p)
	}
	return percentileRankInFloats(vals, current)
}

func percentileRankInFloatsFromSlice[T any](pts []T, current float64, val func(T) float64) float64 {
	vals := make([]float64, len(pts))
	for i, p := range pts {
		vals[i] = val(p)
	}
	return percentileRankInFloats(vals, current)
}

// percentileRankInFloats 返回 current 在 vals 中的百分位（0→1）
// 使用秩分数：rank / (n-1)，相等时取中间秩
func percentileRankInFloats(vals []float64, current float64) float64 {
	n := len(vals)
	if n == 0 {
		return 0.5
	}
	if n == 1 {
		return 0.5
	}
	below := 0
	equal := 0
	for _, v := range vals {
		if v < current {
			below++
		} else if v == current {
			equal++
		}
	}
	rank := float64(below) + float64(equal-1)/2.0 + 1.0
	return clamp01((rank - 1) / float64(n-1))
}

// ── Level helpers ──────────────────────────────────────────────────────────

func sentimentLevelFromPct(pct float64) SentimentLevel {
	if pct >= 0.70 {
		return SentimentLevelStrong
	}
	if pct <= 0.30 {
		return SentimentLevelWeak
	}
	return SentimentLevelNeutral
}

func temperatureLevelFromPct(pct float64) TemperatureLevel {
	if pct >= 0.85 {
		return TemperatureLevelBoiling
	}
	if pct <= 0.15 {
		return TemperatureLevelFreezing
	}
	return TemperatureLevelNormal
}

// ── Extreme summary builder ───────────────────────────────────────────────────

type scoredPt struct {
	date string
	pct  float64
}

func buildExtremeSummary(extremeDates []string, date20ago, date60ago string, scored []scoredPt, threshold float64, horizon int, isFreeze bool) ExtremeSummary {
	count20, count60 := 0, 0
	recentDate := ""
	for _, d := range extremeDates {
		if d >= date20ago {
			count20++
		}
		if d >= date60ago {
			count60++
		}
		if recentDate == "" || d > recentDate {
			recentDate = d
		}
	}

	// 计算反转概率：极值点出现后 horizon 天内分位数回到 "正常" 区间
	reversalProb := computeReversalProb(scored, extremeDates, threshold, horizon, isFreeze)
	return ExtremeSummary{
		Count20d: count20, Count60d: count60,
		RecentDate: recentDate, ReversalProb: reversalProb,
	}
}

// computeReversalProb 统计极值点后 horizon 天内情绪反转概率
func computeReversalProb(scored []scoredPt, extremeDates []string, threshold float64, horizon int, isFreeze bool) ReversalProbability {
	dateIdx := make(map[string]int, len(scored))
	for i, sp := range scored {
		dateIdx[sp.date] = i
	}

	checkReversal := func(idx int, h int) bool {
		end := idx + h
		if end >= len(scored) {
			end = len(scored) - 1
		}
		for j := idx + 1; j <= end; j++ {
			if isFreeze && scored[j].pct > 0.30 {
				return true
			}
			if !isFreeze && scored[j].pct < 0.70 {
				return true
			}
		}
		return false
	}

	var n, r1, r3, r5 int
	for _, d := range extremeDates {
		idx, ok := dateIdx[d]
		if !ok {
			continue
		}
		n++
		if checkReversal(idx, 1) {
			r1++
		}
		if checkReversal(idx, 3) {
			r3++
		}
		if checkReversal(idx, 5) {
			r5++
		}
	}
	if n == 0 {
		return ReversalProbability{SampleN: 0}
	}
	return ReversalProbability{
		Horizon1: float64(r1) / float64(n),
		Horizon3: float64(r3) / float64(n),
		Horizon5: float64(r5) / float64(n),
		SampleN:  n,
	}
}

// nTradingDatesBack 返回在 scored 中不超过 endDate 的第 n 个早交易日
func nTradingDatesBack(scored []scoredPt, endDate string, n int) string {
	count := 0
	for i := len(scored) - 1; i >= 0; i-- {
		if scored[i].date > endDate {
			continue
		}
		count++
		if count >= n {
			return scored[i].date
		}
	}
	if len(scored) > 0 {
		return scored[0].date
	}
	return ""
}

// clamp01 将值限制在 [0,1]
func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
