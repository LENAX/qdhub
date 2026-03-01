package analysis

import (
	"context"
	"math"
	"sort"
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
	indexListReader             IndexListReader
	conceptListReader           ConceptListReader
	dragonTigerReader           DragonTigerReader
	moneyFlowReader             MoneyFlowReader
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
	indexListReader IndexListReader,
	conceptListReader ConceptListReader,
	dragonTigerReader DragonTigerReader,
	moneyFlowReader MoneyFlowReader,
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
		indexListReader:             indexListReader,
		conceptListReader:           conceptListReader,
		dragonTigerReader:           dragonTigerReader,
		moneyFlowReader:             moneyFlowReader,
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

// GetStockIndicators 基于 K 线计算技术指标，与 K 线同区间、同复权
func (s *analysisServiceImpl) GetStockIndicators(ctx context.Context, req StockIndicatorRequest) ([]StockIndicatorItem, error) {
	klineReq := KLineRequest{
		TsCode: req.TsCode, StartDate: req.StartDate, EndDate: req.EndDate,
		AdjustType: req.AdjustType, Period: req.Period,
	}
	klines, err := s.GetKLine(ctx, klineReq)
	if err != nil || len(klines) == 0 {
		return nil, err
	}
	closes := make([]float64, len(klines))
	for i := range klines {
		closes[i] = klines[i].Close
	}
	out := make([]StockIndicatorItem, 0, len(req.Indicators))
	seen := make(map[string]bool)
	for _, name := range req.Indicators {
		if seen[name] {
			continue
		}
		seen[name] = true
		var item StockIndicatorItem
		switch name {
		case "MA5":
			item = StockIndicatorItem{Name: "MA5", Values: calcMA(closes, 5), Color: "#ff6b6b"}
		case "MA10":
			item = StockIndicatorItem{Name: "MA10", Values: calcMA(closes, 10), Color: "#4ecdc4"}
		case "MA20":
			item = StockIndicatorItem{Name: "MA20", Values: calcMA(closes, 20), Color: "#45b7d1"}
		case "RSI":
			item = StockIndicatorItem{Name: "RSI", Values: calcRSI(closes, 14), Color: "#f39c12"}
		case "MACD":
			item = StockIndicatorItem{Name: "MACD", Values: calcMACDDIF(closes, 12, 26), Color: "#9b59b6"}
		default:
			continue
		}
		out = append(out, item)
	}
	return out, nil
}

func calcMA(closes []float64, period int) []float64 {
	out := make([]float64, len(closes))
	for i := range closes {
		if i < period-1 {
			out[i] = 0
			continue
		}
		sum := 0.0
		for j := i - period + 1; j <= i; j++ {
			sum += closes[j]
		}
		out[i] = round2(sum / float64(period))
	}
	return out
}

func calcRSI(closes []float64, period int) []float64 {
	out := make([]float64, len(closes))
	if len(closes) == 0 {
		return out
	}
	out[0] = 50
	for i := 1; i < len(closes); i++ {
		gains := 0.0
		losses := 0.0
		start := i - period + 1
		if start < 0 {
			start = 0
		}
		for j := start + 1; j <= i; j++ {
			ch := closes[j] - closes[j-1]
			if ch > 0 {
				gains += ch
			} else {
				losses -= ch
			}
		}
		n := float64(i - start)
		if n == 0 {
			out[i] = 50
			continue
		}
		avgGain := gains / n
		avgLoss := losses / n
		if avgLoss == 0 {
			out[i] = 100
			continue
		}
		rs := avgGain / avgLoss
		out[i] = round2(100 - 100/(1+rs))
	}
	return out
}

// calcMACDDIF 返回 MACD DIF 线（12,26），与 K 线等长，前 26 个为 0
func calcMACDDIF(closes []float64, short, long int) []float64 {
	out := make([]float64, len(closes))
	if len(closes) < long {
		return out
	}
	emaShort := ema(closes, short)
	emaLong := ema(closes, long)
	for i := range closes {
		if i < long-1 {
			out[i] = 0
			continue
		}
		out[i] = round2(emaShort[i] - emaLong[i])
	}
	return out
}

func ema(closes []float64, period int) []float64 {
	out := make([]float64, len(closes))
	if len(closes) == 0 {
		return out
	}
	k := 2.0 / float64(period+1)
	out[0] = closes[0]
	for i := 1; i < len(closes); i++ {
		out[i] = closes[i]*k + out[i-1]*(1-k)
	}
	return out
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
