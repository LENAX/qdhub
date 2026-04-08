package impl

import (
	"context"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/analysis"
)

// AnalysisApplicationServiceImpl 实现分析应用服务，委托领域服务
type AnalysisApplicationServiceImpl struct {
	svc                analysis.AnalysisService
	realtimeNewsReader analysis.NewsReader // 可选：供 ListNewsFromRealtime/stream 使用
	realtimeTickReader analysis.TickReader // 可选：从 realtime DuckDB 读分时 tick
}

// NewAnalysisApplicationService 创建分析应用服务。
// realtimeNewsReader/realtimeTickReader 可选，为 nil 时 fallback 到主库。
func NewAnalysisApplicationService(svc analysis.AnalysisService, realtimeNewsReader analysis.NewsReader, realtimeTickReader analysis.TickReader) contracts.AnalysisApplicationService {
	return &AnalysisApplicationServiceImpl{svc: svc, realtimeNewsReader: realtimeNewsReader, realtimeTickReader: realtimeTickReader}
}

func (a *AnalysisApplicationServiceImpl) GetKLine(ctx context.Context, req analysis.KLineRequest) ([]analysis.KLineData, error) {
	return a.svc.GetKLine(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetLimitStats(ctx context.Context, startDate, endDate string) ([]analysis.LimitStats, error) {
	return a.svc.GetLimitStats(ctx, startDate, endDate)
}
func (a *AnalysisApplicationServiceImpl) GetLimitStockList(ctx context.Context, tradeDate string, limitType string) ([]analysis.LimitStock, error) {
	return a.svc.GetLimitStockList(ctx, tradeDate, limitType)
}
func (a *AnalysisApplicationServiceImpl) GetLimitLadder(ctx context.Context, tradeDate string) (*analysis.LimitLadderStats, error) {
	return a.svc.GetLimitLadder(ctx, tradeDate)
}
func (a *AnalysisApplicationServiceImpl) GetLimitComparison(ctx context.Context, tradeDate string) (*analysis.LimitComparison, error) {
	return a.svc.GetLimitComparison(ctx, tradeDate)
}
func (a *AnalysisApplicationServiceImpl) GetSectorLimitStats(ctx context.Context, tradeDate string, sectorType string) ([]analysis.SectorLimitStats, error) {
	return a.svc.GetSectorLimitStats(ctx, tradeDate, sectorType)
}
func (a *AnalysisApplicationServiceImpl) GetSectorLimitStocks(ctx context.Context, tradeDate string, sectorCode string, sectorType string) (*analysis.SectorLimitStocks, error) {
	return a.svc.GetSectorLimitStocks(ctx, tradeDate, sectorCode, sectorType)
}
func (a *AnalysisApplicationServiceImpl) GetConceptHeat(ctx context.Context, tradeDate string) ([]analysis.ConceptHeat, error) {
	return a.svc.GetConceptHeat(ctx, tradeDate)
}
func (a *AnalysisApplicationServiceImpl) GetConceptStocks(ctx context.Context, conceptCode, tradeDate string) ([]analysis.StockInfo, error) {
	return a.svc.GetConceptStocks(ctx, conceptCode, tradeDate)
}
func (a *AnalysisApplicationServiceImpl) CalculateFactors(ctx context.Context, req analysis.FactorRequest) ([]analysis.FactorValue, error) {
	return a.svc.CalculateFactors(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) ListStocks(ctx context.Context, req analysis.StockListRequest) ([]analysis.StockInfo, error) {
	return a.svc.ListStocks(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) ListIndices(ctx context.Context, req analysis.IndexListRequest) ([]analysis.IndexInfo, error) {
	return a.svc.ListIndices(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) ListConcepts(ctx context.Context, req analysis.ConceptListRequest) ([]analysis.ConceptInfo, error) {
	return a.svc.ListConcepts(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetStockSnapshot(ctx context.Context, tradeDate string, adjustType analysis.AdjustType, tsCodes []string) ([]analysis.StockInfo, error) {
	return a.svc.GetStockSnapshot(ctx, tradeDate, adjustType, tsCodes)
}
func (a *AnalysisApplicationServiceImpl) GetDragonTigerList(ctx context.Context, req analysis.DragonTigerRequest) ([]analysis.DragonTigerList, error) {
	return a.svc.GetDragonTigerList(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetMoneyFlow(ctx context.Context, req analysis.MoneyFlowRequest) ([]analysis.MoneyFlow, error) {
	return a.svc.GetMoneyFlow(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetMoneyFlowConcept(ctx context.Context, req analysis.MoneyFlowConceptRequest) ([]analysis.MoneyFlowConcept, error) {
	return a.svc.GetMoneyFlowConcept(ctx, req)
}

func (a *AnalysisApplicationServiceImpl) GetMoneyFlowRank(ctx context.Context, req analysis.MoneyFlowRankRequest) (*analysis.MoneyFlowRankResult, error) {
	return a.svc.GetMoneyFlowRank(ctx, req)
}

func (a *AnalysisApplicationServiceImpl) GetIndexOHLCV(ctx context.Context, req analysis.IndexOHLCVRequest) (*analysis.IndexOHLCVResult, error) {
	return a.svc.GetIndexOHLCV(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) ListIndexSectors(ctx context.Context, req analysis.IndexSectorListRequest) ([]analysis.IndexSectorInfo, error) {
	return a.svc.ListIndexSectors(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) ListIndexSectorMembers(ctx context.Context, req analysis.IndexSectorMemberRequest) ([]analysis.IndexSectorMember, error) {
	return a.svc.ListIndexSectorMembers(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetPopularityRank(ctx context.Context, req analysis.PopularityRankRequest) ([]analysis.PopularityRank, error) {
	return a.svc.GetPopularityRank(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) ListNews(ctx context.Context, req analysis.NewsListRequest) ([]analysis.NewsItem, error) {
	if a.realtimeNewsReader != nil {
		items, err := a.realtimeNewsReader.List(ctx, req)
		if err == nil && len(items) > 0 {
			return items, nil
		}
	}
	return a.svc.ListNews(ctx, req)
}

func (a *AnalysisApplicationServiceImpl) ListNewsFromRealtime(ctx context.Context, req analysis.NewsListRequest) ([]analysis.NewsItem, error) {
	if a.realtimeNewsReader != nil {
		return a.realtimeNewsReader.List(ctx, req)
	}
	return a.svc.ListNews(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetLimitUpLadder(ctx context.Context, tradeDate string) ([]analysis.LimitUpLadder, error) {
	return a.svc.GetLimitUpLadder(ctx, tradeDate)
}
func (a *AnalysisApplicationServiceImpl) GetFirstLimitUpStocks(ctx context.Context, tradeDate string) ([]analysis.LimitStock, error) {
	return a.svc.GetFirstLimitUpStocks(ctx, tradeDate)
}
func (a *AnalysisApplicationServiceImpl) GetLimitUpComparison(ctx context.Context, todayDate string) (*analysis.LimitUpComparison, error) {
	return a.svc.GetLimitUpComparison(ctx, todayDate)
}
func (a *AnalysisApplicationServiceImpl) GetLimitUpList(ctx context.Context, req analysis.LimitUpListRequest) ([]analysis.LimitUpStock, error) {
	return a.svc.GetLimitUpList(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetLimitUpBySector(ctx context.Context, tradeDate string, sectorType string) ([]analysis.LimitUpBySector, error) {
	return a.svc.GetLimitUpBySector(ctx, tradeDate, sectorType)
}
func (a *AnalysisApplicationServiceImpl) GetLimitUpStocksBySector(ctx context.Context, sectorCode, sectorType, tradeDate string) ([]analysis.LimitUpStock, error) {
	return a.svc.GetLimitUpStocksBySector(ctx, sectorCode, sectorType, tradeDate)
}
func (a *AnalysisApplicationServiceImpl) GetConceptRotationStats(ctx context.Context, req analysis.ConceptRotationRequest) (*analysis.ConceptRotationStats, error) {
	return a.svc.GetConceptRotationStats(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetStockBasicInfo(ctx context.Context, tsCode string) (*analysis.StockBasicInfo, error) {
	return a.svc.GetStockBasicInfo(ctx, tsCode)
}
func (a *AnalysisApplicationServiceImpl) GetFinancialIndicators(ctx context.Context, req analysis.FinancialIndicatorRequest) ([]analysis.FinancialIndicator, error) {
	return a.svc.GetFinancialIndicators(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetFinancialTableData(ctx context.Context, table string, req analysis.FinancialReportRequest) ([]map[string]any, error) {
	return a.svc.GetFinancialTableData(ctx, table, req)
}
func (a *AnalysisApplicationServiceImpl) ExecuteReadOnlyQuery(ctx context.Context, req analysis.CustomQueryRequest) (*analysis.CustomQueryResult, error) {
	return a.svc.ExecuteReadOnlyQuery(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetTradeCalendar(ctx context.Context, startDate, endDate string) ([]string, error) {
	return a.svc.GetTradeCalendar(ctx, startDate, endDate)
}
func (a *AnalysisApplicationServiceImpl) GetTechnicalIndicators(ctx context.Context, req analysis.TechnicalIndicatorCalcRequest) ([]analysis.TechnicalIndicator, error) {
	return a.svc.GetTechnicalIndicators(ctx, req)
}
func (a *AnalysisApplicationServiceImpl) GetRealtimeTicks(ctx context.Context, tsCode string, limit int) ([]analysis.TickRow, error) {
	if a.realtimeTickReader != nil {
		items, err := a.realtimeTickReader.GetRealtimeTicks(ctx, tsCode, limit)
		if err == nil && len(items) > 0 {
			return items, nil
		}
	}
	return a.svc.GetRealtimeTicks(ctx, tsCode, limit)
}
func (a *AnalysisApplicationServiceImpl) GetIntradayTicks(ctx context.Context, tsCode, tradeDate string) ([]analysis.TickRow, error) {
	if a.realtimeTickReader != nil {
		items, err := a.realtimeTickReader.GetIntradayTicks(ctx, tsCode, tradeDate)
		if err == nil && len(items) > 0 {
			return items, nil
		}
	}
	return a.svc.GetIntradayTicks(ctx, tsCode, tradeDate)
}
func (a *AnalysisApplicationServiceImpl) GetIntradayKline(ctx context.Context, tsCode, tradeDate, period string) ([]analysis.IntradayKlineRow, error) {
	return a.svc.GetIntradayKline(ctx, tsCode, tradeDate, period)
}
