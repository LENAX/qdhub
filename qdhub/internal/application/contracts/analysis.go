// Package contracts defines application service interfaces for analysis.
package contracts

import (
	"context"

	"qdhub/internal/domain/analysis"
)

// AnalysisApplicationService 分析应用服务：编排领域服务，对外提供分析与可视化 API
type AnalysisApplicationService interface {
	GetKLine(ctx context.Context, req analysis.KLineRequest) ([]analysis.KLineData, error)
	GetLimitStats(ctx context.Context, startDate, endDate string) ([]analysis.LimitStats, error)
	GetLimitStockList(ctx context.Context, tradeDate string, limitType string) ([]analysis.LimitStock, error)
	GetLimitLadder(ctx context.Context, tradeDate string) (*analysis.LimitLadderStats, error)
	GetLimitComparison(ctx context.Context, tradeDate string) (*analysis.LimitComparison, error)
	GetSectorLimitStats(ctx context.Context, tradeDate string, sectorType string) ([]analysis.SectorLimitStats, error)
	GetSectorLimitStocks(ctx context.Context, tradeDate string, sectorCode string, sectorType string) (*analysis.SectorLimitStocks, error)
	GetConceptHeat(ctx context.Context, tradeDate string) ([]analysis.ConceptHeat, error)
	GetConceptStocks(ctx context.Context, conceptCode, tradeDate string) ([]analysis.StockInfo, error)
	CalculateFactors(ctx context.Context, req analysis.FactorRequest) ([]analysis.FactorValue, error)
	ListStocks(ctx context.Context, req analysis.StockListRequest) ([]analysis.StockInfo, error)
	ListIndices(ctx context.Context, req analysis.IndexListRequest) ([]analysis.IndexInfo, error)
	ListConcepts(ctx context.Context, req analysis.ConceptListRequest) ([]analysis.ConceptInfo, error)
	GetStockSnapshot(ctx context.Context, tradeDate string, adjustType analysis.AdjustType, tsCodes []string) ([]analysis.StockInfo, error)
	GetDragonTigerList(ctx context.Context, req analysis.DragonTigerRequest) ([]analysis.DragonTigerList, error)
	GetMoneyFlow(ctx context.Context, req analysis.MoneyFlowRequest) ([]analysis.MoneyFlow, error)
	GetMoneyFlowConcept(ctx context.Context, req analysis.MoneyFlowConceptRequest) ([]analysis.MoneyFlowConcept, error)
	GetMoneyFlowRank(ctx context.Context, req analysis.MoneyFlowRankRequest) (*analysis.MoneyFlowRankResult, error)
	GetIndexOHLCV(ctx context.Context, req analysis.IndexOHLCVRequest) (*analysis.IndexOHLCVResult, error)
	ListIndexSectors(ctx context.Context, req analysis.IndexSectorListRequest) ([]analysis.IndexSectorInfo, error)
	ListIndexSectorMembers(ctx context.Context, req analysis.IndexSectorMemberRequest) ([]analysis.IndexSectorMember, error)
	GetPopularityRank(ctx context.Context, req analysis.PopularityRankRequest) ([]analysis.PopularityRank, error)
	ListNews(ctx context.Context, req analysis.NewsListRequest) ([]analysis.NewsItem, error)
	// ListNewsFromRealtime 从 realtime 数据源（realtime DuckDB news 表）拉取新闻，供 /analysis/news/stream 使用；无 realtime 时回退到 ListNews。
	ListNewsFromRealtime(ctx context.Context, req analysis.NewsListRequest) ([]analysis.NewsItem, error)
	GetLimitUpLadder(ctx context.Context, tradeDate string) ([]analysis.LimitUpLadder, error)
	GetFirstLimitUpStocks(ctx context.Context, tradeDate string) ([]analysis.LimitStock, error)
	GetLimitUpComparison(ctx context.Context, todayDate string) (*analysis.LimitUpComparison, error)
	GetLimitUpList(ctx context.Context, req analysis.LimitUpListRequest) ([]analysis.LimitUpStock, error)
	GetLimitUpBySector(ctx context.Context, tradeDate string, sectorType string) ([]analysis.LimitUpBySector, error)
	GetLimitUpStocksBySector(ctx context.Context, sectorCode, sectorType, tradeDate string) ([]analysis.LimitUpStock, error)
	GetConceptRotationStats(ctx context.Context, req analysis.ConceptRotationRequest) (*analysis.ConceptRotationStats, error)
	GetStockBasicInfo(ctx context.Context, tsCode string) (*analysis.StockBasicInfo, error)
	GetFinancialIndicators(ctx context.Context, req analysis.FinancialIndicatorRequest) ([]analysis.FinancialIndicator, error)
	GetFinancialTableData(ctx context.Context, table string, req analysis.FinancialReportRequest) ([]map[string]any, error)
	ExecuteReadOnlyQuery(ctx context.Context, req analysis.CustomQueryRequest) (*analysis.CustomQueryResult, error)
	GetTradeCalendar(ctx context.Context, startDate, endDate string) ([]string, error)
	GetTechnicalIndicators(ctx context.Context, req analysis.TechnicalIndicatorCalcRequest) ([]analysis.TechnicalIndicator, error)
	GetRealtimeTicks(ctx context.Context, tsCode string, limit int) ([]analysis.TickRow, error)
	GetIntradayTicks(ctx context.Context, tsCode, tradeDate string) ([]analysis.TickRow, error)
	GetIntradayKline(ctx context.Context, tsCode, tradeDate, period string) ([]analysis.IntradayKlineRow, error)
}
