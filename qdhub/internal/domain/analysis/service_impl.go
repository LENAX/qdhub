package analysis

import (
	"context"
	"sort"
)

// analysisServiceImpl 实现 AnalysisService，仅依赖 Reader 接口，不执行 SQL
type analysisServiceImpl struct {
	kLineReader              KLineReader
	limitStatsReader         LimitStatsReader
	limitStockListReader     LimitStockListReader
	limitLadderReader        LimitLadderReader
	limitComparisonReader    LimitComparisonReader
	sectorLimitStatsReader   SectorLimitStatsReader
	sectorLimitStocksReader  SectorLimitStocksReader
	conceptHeatReader        ConceptHeatReader
	conceptStocksReader      ConceptStocksReader
	conceptRotationReader    ConceptRotationReader
	stockListReader          StockListReader
	indexListReader          IndexListReader
	conceptListReader        ConceptListReader
	dragonTigerReader        DragonTigerReader
	moneyFlowReader          MoneyFlowReader
	popularityRankReader     PopularityRankReader
	newsReader               NewsReader
	limitUpListReader        LimitUpListReader
	limitUpLadderReader      LimitUpLadderReader
	limitUpComparisonReader  LimitUpComparisonReader
	limitUpBySectorReader    LimitUpBySectorReader
	limitUpStocksBySectorReader LimitUpStocksBySectorReader
	stockBasicReader         StockBasicReader
	financialIndicatorReader FinancialIndicatorReader
	financialReportReader    FinancialReportReader
	customQueryExecutor      CustomQueryExecutor
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
	limitUpStocksBySectorReader LimitUpStocksBySectorReader,
	stockBasicReader StockBasicReader,
	financialIndicatorReader FinancialIndicatorReader,
	financialReportReader FinancialReportReader,
	customQueryExecutor CustomQueryExecutor,
) AnalysisService {
	return &analysisServiceImpl{
		kLineReader:                 kLineReader,
		limitStatsReader:            limitStatsReader,
		limitStockListReader:        limitStockListReader,
		limitLadderReader:           limitLadderReader,
		limitComparisonReader:      limitComparisonReader,
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
		limitUpBySectorReader:        limitUpBySectorReader,
		limitUpStocksBySectorReader:  limitUpStocksBySectorReader,
		stockBasicReader:            stockBasicReader,
		financialIndicatorReader:    financialIndicatorReader,
		financialReportReader:       financialReportReader,
		customQueryExecutor:          customQueryExecutor,
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

func applyAdjustAndToKLineData(rows []RawDailyRow, adj AdjustType) []KLineData {
	if len(rows) == 0 {
		return nil
	}
	var firstFactor, latestFactor float64
	for i, r := range rows {
		if i == 0 || r.AdjFactor < firstFactor {
			firstFactor = r.AdjFactor
		}
		if r.AdjFactor > latestFactor {
			latestFactor = r.AdjFactor
		}
	}
	if latestFactor == 0 {
		latestFactor = 1
	}
	if firstFactor == 0 {
		firstFactor = 1
	}
	out := make([]KLineData, 0, len(rows))
	for _, r := range rows {
		var ratio float64
		switch adj {
		case AdjustQfq:
			ratio = r.AdjFactor / latestFactor
		case AdjustHfq:
			ratio = r.AdjFactor / firstFactor
		default:
			ratio = 1
		}
		out = append(out, KLineData{
			TradeDate: r.TradeDate,
			Open:      r.Open * ratio,
			High:      r.High * ratio,
			Low:       r.Low * ratio,
			Close:     r.Close * ratio,
			Volume:    r.Vol,
			Amount:    r.Amount,
			PreClose:  r.PreClose * ratio,
			Change:    r.Change * ratio,
			PctChg:    r.PctChg,
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

func (s *analysisServiceImpl) GetFinancialReports(ctx context.Context, req FinancialReportRequest) ([]FinancialReport, error) {
	return s.financialReportReader.GetReports(ctx, req)
}

func (s *analysisServiceImpl) ExecuteReadOnlyQuery(ctx context.Context, req CustomQueryRequest) (*CustomQueryResult, error) {
	return s.customQueryExecutor.ExecuteReadOnlyQuery(ctx, req)
}
