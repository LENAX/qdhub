package analysis

import (
	"context"
	"math"
	"sort"
	"time"
)

// analysisServiceImpl 实现 AnalysisService，仅依赖 Reader 接口，不执行 SQL
type analysisServiceImpl struct {
	kLineReader                 KLineReader
	limitStatsReader            LimitStatsReader
	limitStockListReader        LimitStockListReader
	limitLadderReader           LimitLadderReader
	limitComparisonReader       LimitComparisonReader
	sectorLimitStatsReader      SectorLimitStatsReader
	sectorLimitStocksReader     SectorLimitStocksReader
	conceptHeatReader           ConceptHeatReader
	conceptStocksReader         ConceptStocksReader
	conceptRotationReader       ConceptRotationReader
	stockListReader             StockListReader
	stockSnapshotReader         StockSnapshotReader
	indexListReader             IndexListReader
	conceptListReader           ConceptListReader
	dragonTigerReader           DragonTigerReader
	moneyFlowReader             MoneyFlowReader
	moneyFlowConceptReader      MoneyFlowConceptReader
	popularityRankReader        PopularityRankReader
	newsReader                  NewsReader
	limitUpListReader           LimitUpListReader
	limitUpLadderReader         LimitUpLadderReader
	limitUpComparisonReader     LimitUpComparisonReader
	limitUpBySectorReader       LimitUpBySectorReader
	FirstLimitUpReader          FirstLimitUpReader
	limitUpStocksBySectorReader LimitUpStocksBySectorReader
	stockBasicReader            StockBasicReader
	financialIndicatorReader    FinancialIndicatorReader
	financialReportReader       FinancialReportReader
	customQueryExecutor         CustomQueryExecutor
	tradeCalendarReader         TradeCalendarReader
	realtimeTickReader          RealtimeTickReader
	intradayTickReader          IntradayTickReader
	intradayKlineReader         IntradayKlineReader
	moneyFlowRankReader         MoneyFlowRankReader
	indexOHLCVReader            IndexOHLCVReader
	indexSectorReader           IndexSectorReader
	indexSectorMemberReader     IndexSectorMemberReader
}

// NewAnalysisService 构造分析领域服务，依赖各 Reader 与 CustomQueryExecutor
func NewAnalysisService(
	kLineReader KLineReader,
	limitStatsReader LimitStatsReader,
	limitStockListReader LimitStockListReader,
	limitLadderReader LimitLadderReader,
	limitComparisonReader LimitComparisonReader,
	sectorLimitStatsReader SectorLimitStatsReader,
	sectorLimitStocksReader SectorLimitStocksReader,
	conceptHeatReader ConceptHeatReader,
	conceptStocksReader ConceptStocksReader,
	conceptRotationReader ConceptRotationReader,
	stockListReader StockListReader,
	stockSnapshotReader StockSnapshotReader,
	indexListReader IndexListReader,
	conceptListReader ConceptListReader,
	dragonTigerReader DragonTigerReader,
	moneyFlowReader MoneyFlowReader,
	moneyFlowConceptReader MoneyFlowConceptReader,
	popularityRankReader PopularityRankReader,
	newsReader NewsReader,
	limitUpListReader LimitUpListReader,
	limitUpLadderReader LimitUpLadderReader,
	limitUpComparisonReader LimitUpComparisonReader,
	limitUpBySectorReader LimitUpBySectorReader,
	FirstLimitUpReader FirstLimitUpReader,
	limitUpStocksBySectorReader LimitUpStocksBySectorReader,
	stockBasicReader StockBasicReader,
	financialIndicatorReader FinancialIndicatorReader,
	financialReportReader FinancialReportReader,
	customQueryExecutor CustomQueryExecutor,
	tradeCalendarReader TradeCalendarReader,
	realtimeTickReader RealtimeTickReader,
	intradayTickReader IntradayTickReader,
	intradayKlineReader IntradayKlineReader,
	moneyFlowRankReader MoneyFlowRankReader,
	indexOHLCVReader IndexOHLCVReader,
	indexSectorReader IndexSectorReader,
	indexSectorMemberReader IndexSectorMemberReader,
) AnalysisService {
	return &analysisServiceImpl{
		kLineReader:                 kLineReader,
		limitStatsReader:            limitStatsReader,
		limitStockListReader:        limitStockListReader,
		limitLadderReader:           limitLadderReader,
		limitComparisonReader:       limitComparisonReader,
		sectorLimitStatsReader:      sectorLimitStatsReader,
		sectorLimitStocksReader:     sectorLimitStocksReader,
		conceptHeatReader:           conceptHeatReader,
		conceptStocksReader:         conceptStocksReader,
		conceptRotationReader:       conceptRotationReader,
		stockListReader:             stockListReader,
		stockSnapshotReader:         stockSnapshotReader,
		indexListReader:             indexListReader,
		conceptListReader:           conceptListReader,
		dragonTigerReader:           dragonTigerReader,
		moneyFlowReader:             moneyFlowReader,
		moneyFlowConceptReader:      moneyFlowConceptReader,
		popularityRankReader:        popularityRankReader,
		newsReader:                  newsReader,
		limitUpListReader:           limitUpListReader,
		limitUpLadderReader:         limitUpLadderReader,
		limitUpComparisonReader:     limitUpComparisonReader,
		limitUpBySectorReader:       limitUpBySectorReader,
		FirstLimitUpReader:          FirstLimitUpReader,
		limitUpStocksBySectorReader: limitUpStocksBySectorReader,
		stockBasicReader:            stockBasicReader,
		financialIndicatorReader:    financialIndicatorReader,
		financialReportReader:       financialReportReader,
		customQueryExecutor:         customQueryExecutor,
		tradeCalendarReader:         tradeCalendarReader,
		realtimeTickReader:          realtimeTickReader,
		intradayTickReader:          intradayTickReader,
		intradayKlineReader:         intradayKlineReader,
		moneyFlowRankReader:         moneyFlowRankReader,
		indexOHLCVReader:            indexOHLCVReader,
		indexSectorReader:           indexSectorReader,
		indexSectorMemberReader:     indexSectorMemberReader,
	}
}

// GetKLine 获取 K 线，在领域内做复权计算，数据来自 KLineReader
func (s *analysisServiceImpl) GetKLine(ctx context.Context, req KLineRequest) ([]KLineData, error) {
	rows, err := s.kLineReader.GetDailyWithAdjFactor(ctx, req.TsCode, req.StartDate, req.EndDate)
	if err != nil {
		return nil, err
	}
	return applyAdjustAndToKLineData(rows, req.AdjustType), nil
}

// round2 保留两位小数（价格、涨跌幅等展示用）
func round2(x float64) float64 { return math.Round(x*100) / 100 }

// applyAdjustAndToKLineData 按 Tushare 规则做动态复权：前复权以最近一日为基准，后复权为价格×复权因子；涨跌额/涨跌幅用复权后的 close、pre_close 重算。
func applyAdjustAndToKLineData(rows []RawDailyRow, adj AdjustType) []KLineData {
	if len(rows) == 0 {
		return nil
	}
	// 前复权基准：查询区间内最近一日的复权因子（rows 已按 trade_date 升序）
	qfqBase := rows[len(rows)-1].AdjFactor
	if qfqBase == 0 {
		qfqBase = 1
	}
	out := make([]KLineData, 0, len(rows))
	for _, r := range rows {
		var ratio float64
		switch adj {
		case AdjustQfq:
			ratio = r.AdjFactor / qfqBase
		case AdjustHfq:
			// 后复权：price * adj_factor（Tushare 写法，不除基准）
			ratio = r.AdjFactor
			if ratio == 0 {
				ratio = 1
			}
		default:
			ratio = 1
		}
		closeAdj := round2(r.Close * ratio)
		preCloseAdj := round2(r.PreClose * ratio)
		changeAdj := round2(closeAdj - preCloseAdj)
		pctChgAdj := 0.0
		if preCloseAdj != 0 {
			pctChgAdj = round2((changeAdj / preCloseAdj) * 100)
		}
		out = append(out, KLineData{
			TradeDate: r.TradeDate,
			Name:      r.Name,
			Open:      round2(r.Open * ratio),
			High:      round2(r.High * ratio),
			Low:       round2(r.Low * ratio),
			Close:     closeAdj,
			Volume:    r.Vol,
			Amount:    round2(r.Amount),
			PreClose:  preCloseAdj,
			Change:    changeAdj,
			PctChg:    pctChgAdj,
		})
	}
	return out
}

func (s *analysisServiceImpl) GetLimitStats(ctx context.Context, startDate, endDate string) ([]LimitStats, error) {
	return s.limitStatsReader.GetLimitStats(ctx, startDate, endDate)
}

func (s *analysisServiceImpl) GetLimitStockList(ctx context.Context, tradeDate string, limitType string) ([]LimitStock, error) {
	return s.limitStockListReader.GetByDateAndType(ctx, tradeDate, limitType)
}

func (s *analysisServiceImpl) GetLimitLadder(ctx context.Context, tradeDate string) (*LimitLadderStats, error) {
	return s.limitLadderReader.GetByDate(ctx, tradeDate)
}

func (s *analysisServiceImpl) GetLimitComparison(ctx context.Context, tradeDate string) (*LimitComparison, error) {
	return s.limitComparisonReader.GetComparison(ctx, tradeDate)
}

func (s *analysisServiceImpl) GetSectorLimitStats(ctx context.Context, tradeDate string, sectorType string) ([]SectorLimitStats, error) {
	return s.sectorLimitStatsReader.GetByDate(ctx, tradeDate, sectorType)
}

func (s *analysisServiceImpl) GetSectorLimitStocks(ctx context.Context, tradeDate string, sectorCode string, sectorType string) (*SectorLimitStocks, error) {
	return s.sectorLimitStocksReader.GetBySectorAndDate(ctx, sectorCode, sectorType, tradeDate)
}

func (s *analysisServiceImpl) GetConceptHeat(ctx context.Context, tradeDate string) ([]ConceptHeat, error) {
	return s.conceptHeatReader.GetConceptHeat(ctx, tradeDate)
}

func (s *analysisServiceImpl) GetConceptStocks(ctx context.Context, conceptCode, tradeDate string) ([]StockInfo, error) {
	return s.conceptStocksReader.GetConceptStocks(ctx, conceptCode, tradeDate)
}

// CalculateFactors 因子计算：依赖 KLineReader 取数，因子表达式求值由上层或 FactorEngine 完成，此处仅委托取数并返回占位
func (s *analysisServiceImpl) CalculateFactors(ctx context.Context, req FactorRequest) ([]FactorValue, error) {
	// 领域内不执行 SQL，仅可依赖 KLineReader 取 K 线后做内存内因子计算
	// 具体因子表达式解析与计算可在 FactorEngine 或应用层编排，这里返回空切片表示由上层实现
	_ = req
	return nil, nil
}

func (s *analysisServiceImpl) ListStocks(ctx context.Context, req StockListRequest) ([]StockInfo, error) {
	return s.stockListReader.List(ctx, req)
}

func (s *analysisServiceImpl) GetStockSnapshot(ctx context.Context, tradeDate string, adjustType AdjustType, tsCodes []string) ([]StockInfo, error) {
	if len(tsCodes) == 0 {
		return nil, nil
	}

	// 统一计算交易日历查询起始日期：tradeDate 往前推 1 个月
	startCalDate := tradeDate
	if len(tradeDate) == 8 {
		if t, err := time.Parse("20060102", tradeDate); err == nil {
			start := t.AddDate(0, -1, 0)
			startCalDate = start.Format("20060102")
		}
	}

	// 使用 trade_cal 在最近 1 个月区间内查找 <= tradeDate 的最近一个交易日，
	// 在这个交易日上做一次批量快照查询，避免对每个 ts_code 循环查 K 线。
	tradingDates, err := s.tradeCalendarReader.GetTradingDates(ctx, startCalDate, tradeDate)
	if err == nil && len(tradingDates) > 0 {
		lastTradeDate := tradingDates[len(tradingDates)-1]
		// 这里直接委托给 StockSnapshotReader 做批量 SQL 查询。
		// adjustType 暂未参与快照价格计算，快照主要用于列表展示，精确复权价格由 K 线图承担。
		return s.stockSnapshotReader.GetSnapshot(ctx, lastTradeDate, tsCodes)
	}

	// 如果 trade_cal 表无数据或查询失败，则回退到基于 daily 表的按股票扫描方案（限定最近 1 个月窗口），
	// 保持在未同步 trade_cal 场景下也能返回最近有数据的快照。
	startDate := startCalDate
	out := make([]StockInfo, 0, len(tsCodes))
	for _, code := range tsCodes {
		if code == "" {
			continue
		}
		rows, err := s.kLineReader.GetDailyWithAdjFactor(ctx, code, startDate, tradeDate)
		if err != nil || len(rows) == 0 {
			continue
		}
		kline := applyAdjustAndToKLineData(rows, adjustType)
		if len(kline) == 0 {
			continue
		}
		latest := kline[len(kline)-1]
		price := latest.Close
		change := latest.Change
		pct := latest.PctChg
		snap := StockInfo{
			TsCode:   code,
			ListDate: "", // 其余基础信息如需展示可后续通过 StockBasic 补全
		}
		snap.Price = &price
		snap.Change = &change
		snap.PctChg = &pct
		out = append(out, snap)
	}
	return out, nil
}

func (s *analysisServiceImpl) ListIndices(ctx context.Context, req IndexListRequest) ([]IndexInfo, error) {
	return s.indexListReader.List(ctx, req)
}

func (s *analysisServiceImpl) ListConcepts(ctx context.Context, req ConceptListRequest) ([]ConceptInfo, error) {
	return s.conceptListReader.List(ctx, req)
}

func (s *analysisServiceImpl) GetDragonTigerList(ctx context.Context, req DragonTigerRequest) ([]DragonTigerList, error) {
	return s.dragonTigerReader.GetList(ctx, req)
}

func (s *analysisServiceImpl) GetMoneyFlow(ctx context.Context, req MoneyFlowRequest) ([]MoneyFlow, error) {
	return s.moneyFlowReader.GetMoneyFlow(ctx, req)
}

func (s *analysisServiceImpl) GetMoneyFlowConcept(ctx context.Context, req MoneyFlowConceptRequest) ([]MoneyFlowConcept, error) {
	return s.moneyFlowConceptReader.GetMoneyFlowConcept(ctx, req)
}

func (s *analysisServiceImpl) GetMoneyFlowRank(ctx context.Context, req MoneyFlowRankRequest) (*MoneyFlowRankResult, error) {
	return s.moneyFlowRankReader.GetMoneyFlowRank(ctx, req)
}

func (s *analysisServiceImpl) GetIndexOHLCV(ctx context.Context, req IndexOHLCVRequest) (*IndexOHLCVResult, error) {
	return s.indexOHLCVReader.GetIndexOHLCV(ctx, req)
}

func (s *analysisServiceImpl) ListIndexSectors(ctx context.Context, req IndexSectorListRequest) ([]IndexSectorInfo, error) {
	return s.indexSectorReader.ListIndexSectors(ctx, req)
}

func (s *analysisServiceImpl) ListIndexSectorMembers(ctx context.Context, req IndexSectorMemberRequest) ([]IndexSectorMember, error) {
	return s.indexSectorMemberReader.ListIndexSectorMembers(ctx, req)
}

func (s *analysisServiceImpl) GetPopularityRank(ctx context.Context, req PopularityRankRequest) ([]PopularityRank, error) {
	return s.popularityRankReader.GetRank(ctx, req)
}

func (s *analysisServiceImpl) ListNews(ctx context.Context, req NewsListRequest) ([]NewsItem, error) {
	return s.newsReader.List(ctx, req)
}

func (s *analysisServiceImpl) GetLimitUpLadder(ctx context.Context, tradeDate string) ([]LimitUpLadder, error) {
	return s.limitUpLadderReader.GetByDate(ctx, tradeDate)
}

func (s *analysisServiceImpl) GetFirstLimitUpStocks(ctx context.Context, tradeDate string) ([]LimitStock, error) {
	return s.FirstLimitUpReader.GetByDate(ctx, tradeDate)
}

func (s *analysisServiceImpl) GetLimitUpComparison(ctx context.Context, todayDate string) (*LimitUpComparison, error) {
	return s.limitUpComparisonReader.GetComparison(ctx, todayDate)
}

func (s *analysisServiceImpl) GetLimitUpList(ctx context.Context, req LimitUpListRequest) ([]LimitUpStock, error) {
	return s.limitUpListReader.GetList(ctx, req)
}

func (s *analysisServiceImpl) GetLimitUpBySector(ctx context.Context, tradeDate string, sectorType string) ([]LimitUpBySector, error) {
	return s.limitUpBySectorReader.GetByDate(ctx, tradeDate, sectorType)
}

func (s *analysisServiceImpl) GetLimitUpStocksBySector(ctx context.Context, sectorCode, sectorType, tradeDate string) ([]LimitUpStock, error) {
	return s.limitUpStocksBySectorReader.GetStocks(ctx, sectorCode, sectorType, tradeDate)
}

// GetConceptRotationStats 题材轮动：从 ConceptRotationReader 取行数据，在领域内按日分组组装
func (s *analysisServiceImpl) GetConceptRotationStats(ctx context.Context, req ConceptRotationRequest) (*ConceptRotationStats, error) {
	q := ConceptRotationQuery{
		StartDate:     req.StartDate,
		EndDate:       req.EndDate,
		RankBy:        req.RankBy,
		TopN:          req.TopN,
		ConceptSource: req.ConceptSource,
	}
	rows, err := s.conceptRotationReader.GetRankedConcepts(ctx, q)
	if err != nil {
		return nil, err
	}
	byDate := make(map[string][]ConceptRotationRank)
	for _, r := range rows {
		byDate[r.TradeDate] = append(byDate[r.TradeDate], ConceptRotationRank{
			TradeDate:    r.TradeDate,
			ConceptCode:  r.ConceptCode,
			ConceptName:  r.ConceptName,
			Rank:         r.Rank,
			AvgPctChg:    r.AvgPctChg,
			LimitUpCount: r.LimitUpCount,
			TotalVolume:  r.TotalVolume,
			NetInflow:    r.NetInflow,
			StockCount:   r.StockCount,
			RankValue:    r.RankValue,
		})
	}
	var dates []string
	for d := range byDate {
		dates = append(dates, d)
	}
	sort.Strings(dates)
	dailyRanks := make([]ConceptDailyRank, 0, len(dates))
	for _, d := range dates {
		dailyRanks = append(dailyRanks, ConceptDailyRank{TradeDate: d, Concepts: byDate[d]})
	}
	return &ConceptRotationStats{
		StartDate:  req.StartDate,
		EndDate:    req.EndDate,
		RankBy:     req.RankBy,
		DailyRanks: dailyRanks,
	}, nil
}

func (s *analysisServiceImpl) GetStockBasicInfo(ctx context.Context, tsCode string) (*StockBasicInfo, error) {
	return s.stockBasicReader.GetByTsCode(ctx, tsCode)
}

func (s *analysisServiceImpl) GetFinancialIndicators(ctx context.Context, req FinancialIndicatorRequest) ([]FinancialIndicator, error) {
	return s.financialIndicatorReader.GetIndicators(ctx, req)
}

func (s *analysisServiceImpl) GetFinancialTableData(ctx context.Context, table string, req FinancialReportRequest) ([]map[string]any, error) {
	return s.financialReportReader.GetTableData(ctx, table, req)
}

func (s *analysisServiceImpl) ExecuteReadOnlyQuery(ctx context.Context, req CustomQueryRequest) (*CustomQueryResult, error) {
	return s.customQueryExecutor.ExecuteReadOnlyQuery(ctx, req)
}

func (s *analysisServiceImpl) GetTradeCalendar(ctx context.Context, startDate, endDate string) ([]string, error) {
	return s.tradeCalendarReader.GetTradingDates(ctx, startDate, endDate)
}

func (s *analysisServiceImpl) GetRealtimeTicks(ctx context.Context, tsCode string, limit int) ([]TickRow, error) {
	return s.realtimeTickReader.GetRealtimeTicks(ctx, tsCode, limit)
}

func (s *analysisServiceImpl) GetIntradayTicks(ctx context.Context, tsCode, tradeDate string) ([]TickRow, error) {
	return s.intradayTickReader.GetIntradayTicks(ctx, tsCode, tradeDate)
}

func (s *analysisServiceImpl) GetIntradayKline(ctx context.Context, tsCode, tradeDate, period string) ([]IntradayKlineRow, error) {
	return s.intradayKlineReader.GetIntradayKline(ctx, tsCode, tradeDate, period)
}

// GetTechnicalIndicators 基于复权后的 K 线在内存中计算简单技术指标（MA/RSI/MACD）
func (s *analysisServiceImpl) GetTechnicalIndicators(ctx context.Context, req TechnicalIndicatorCalcRequest) ([]TechnicalIndicator, error) {
	if req.TsCode == "" || req.StartDate == "" || req.EndDate == "" || len(req.Indicators) == 0 {
		return nil, nil
	}
	rows, err := s.kLineReader.GetDailyWithAdjFactor(ctx, req.TsCode, req.StartDate, req.EndDate)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	kline := applyAdjustAndToKLineData(rows, req.AdjustType)
	if len(kline) == 0 {
		return nil, nil
	}
	closes := make([]float64, len(kline))
	for i, d := range kline {
		closes[i] = d.Close
	}

	var out []TechnicalIndicator
	colorMap := map[string]string{
		"MA5":  "#F59E0B",
		"MA10": "#10B981",
		"MA20": "#3B82F6",
		"RSI":  "#A855F7",
		"MACD": "#EF4444",
	}

	for _, name := range req.Indicators {
		switch {
		case name == "RSI":
			vals := calcRSI(closes, 14)
			out = append(out, TechnicalIndicator{
				Name:   name,
				Values: vals,
				Color:  colorMap[name],
			})
		case name == "MACD":
			vals := calcMACDDiff(closes, 12, 26)
			out = append(out, TechnicalIndicator{
				Name:   name,
				Values: vals,
				Color:  colorMap[name],
			})
		case len(name) > 2 && name[:2] == "MA":
			// 简单移动平均：如 MA5/MA10/MA20
			window := 0
			for i := 2; i < len(name); i++ {
				window = window*10 + int(name[i]-'0')
			}
			if window <= 0 {
				continue
			}
			vals := calcSMA(closes, window)
			out = append(out, TechnicalIndicator{
				Name:   name,
				Values: vals,
				Color:  colorMap[name],
			})
		default:
			// 未识别的指标暂不计算
		}
	}
	return out, nil
}

// calcSMA 计算简单移动平均
func calcSMA(values []float64, window int) []float64 {
	n := len(values)
	out := make([]float64, n)
	if window <= 0 || n == 0 {
		return out
	}
	var sum float64
	for i := 0; i < n; i++ {
		sum += values[i]
		if i >= window {
			sum -= values[i-window]
		}
		if i+1 >= window {
			out[i] = sum / float64(window)
		} else {
			out[i] = 0
		}
	}
	return out
}

// calcRSI 计算相对强弱指标（简单 14 日版本）
func calcRSI(values []float64, period int) []float64 {
	n := len(values)
	out := make([]float64, n)
	if period <= 0 || n < 2 {
		return out
	}
	var gainSum, lossSum float64
	for i := 1; i <= period && i < n; i++ {
		diff := values[i] - values[i-1]
		if diff > 0 {
			gainSum += diff
		} else {
			lossSum -= diff
		}
	}
	if period < n {
		avgGain := gainSum / float64(period)
		avgLoss := lossSum / float64(period)
		var rsi float64
		if avgLoss == 0 {
			rsi = 100
		} else {
			rs := avgGain / avgLoss
			rsi = 100 - (100 / (1 + rs))
		}
		out[period] = rsi
		for i := period + 1; i < n; i++ {
			diff := values[i] - values[i-1]
			gain, loss := 0.0, 0.0
			if diff > 0 {
				gain = diff
			} else {
				loss = -diff
			}
			avgGain = (avgGain*float64(period-1) + gain) / float64(period)
			avgLoss = (avgLoss*float64(period-1) + loss) / float64(period)
			if avgLoss == 0 {
				rsi = 100
			} else {
				rs := avgGain / avgLoss
				rsi = 100 - (100 / (1 + rs))
			}
			out[i] = rsi
		}
	}
	return out
}

// calcMACDDiff 计算 MACD 的 DIF 线（12/26 EMA 差）
func calcMACDDiff(values []float64, shortPeriod, longPeriod int) []float64 {
	n := len(values)
	out := make([]float64, n)
	if n == 0 || shortPeriod <= 0 || longPeriod <= 0 {
		return out
	}
	shortEMA := calcEMA(values, shortPeriod)
	longEMA := calcEMA(values, longPeriod)
	for i := 0; i < n; i++ {
		out[i] = shortEMA[i] - longEMA[i]
	}
	return out
}

// calcEMA 计算指数移动平均
func calcEMA(values []float64, period int) []float64 {
	n := len(values)
	out := make([]float64, n)
	if n == 0 || period <= 0 {
		return out
	}
	k := 2.0 / (float64(period) + 1)
	out[0] = values[0]
	for i := 1; i < n; i++ {
		out[i] = values[i]*k + out[i-1]*(1-k)
	}
	return out
}
