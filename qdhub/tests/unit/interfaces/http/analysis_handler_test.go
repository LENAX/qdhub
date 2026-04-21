package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/analysis"
	httpapi "qdhub/internal/interfaces/http"
)

// stubAnalysisApplicationService 仅用于 Analysis HTTP 单测；未覆盖的方法返回空值。
type stubAnalysisApplicationService struct {
	MoneyFlowRankHook          func(ctx context.Context, req analysis.MoneyFlowRankRequest) (*analysis.MoneyFlowRankResult, error)
	IndexOHLCVHook             func(ctx context.Context, req analysis.IndexOHLCVRequest) (*analysis.IndexOHLCVResult, error)
	ListIndexSectorsHook       func(ctx context.Context, req analysis.IndexSectorListRequest) ([]analysis.IndexSectorInfo, error)
	ListIndexSectorMembersHook func(ctx context.Context, req analysis.IndexSectorMemberRequest) ([]analysis.IndexSectorMember, error)
}

var _ contracts.AnalysisApplicationService = (*stubAnalysisApplicationService)(nil)

func (s *stubAnalysisApplicationService) GetMoneyFlowRank(ctx context.Context, req analysis.MoneyFlowRankRequest) (*analysis.MoneyFlowRankResult, error) {
	if s.MoneyFlowRankHook != nil {
		return s.MoneyFlowRankHook(ctx, req)
	}
	return &analysis.MoneyFlowRankResult{}, nil
}

func (s *stubAnalysisApplicationService) GetIndexOHLCV(ctx context.Context, req analysis.IndexOHLCVRequest) (*analysis.IndexOHLCVResult, error) {
	if s.IndexOHLCVHook != nil {
		return s.IndexOHLCVHook(ctx, req)
	}
	return &analysis.IndexOHLCVResult{}, nil
}

func (s *stubAnalysisApplicationService) ListIndexSectors(ctx context.Context, req analysis.IndexSectorListRequest) ([]analysis.IndexSectorInfo, error) {
	if s.ListIndexSectorsHook != nil {
		return s.ListIndexSectorsHook(ctx, req)
	}
	return nil, nil
}

func (s *stubAnalysisApplicationService) ListIndexSectorMembers(ctx context.Context, req analysis.IndexSectorMemberRequest) ([]analysis.IndexSectorMember, error) {
	if s.ListIndexSectorMembersHook != nil {
		return s.ListIndexSectorMembersHook(ctx, req)
	}
	return nil, nil
}

func (s *stubAnalysisApplicationService) GetKLine(ctx context.Context, req analysis.KLineRequest) ([]analysis.KLineData, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetLimitStats(ctx context.Context, startDate, endDate string) ([]analysis.LimitStats, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetLimitStockList(ctx context.Context, tradeDate string, limitType string) ([]analysis.LimitStock, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetLimitLadder(ctx context.Context, tradeDate string) (*analysis.LimitLadderStats, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetLimitComparison(ctx context.Context, tradeDate string) (*analysis.LimitComparison, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetSectorLimitStats(ctx context.Context, tradeDate string, sectorType string) ([]analysis.SectorLimitStats, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetSectorLimitStocks(ctx context.Context, tradeDate string, sectorCode string, sectorType string) (*analysis.SectorLimitStocks, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetConceptHeat(ctx context.Context, tradeDate string) ([]analysis.ConceptHeat, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetConceptStocks(ctx context.Context, conceptCode, tradeDate string) ([]analysis.StockInfo, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) CalculateFactors(ctx context.Context, req analysis.FactorRequest) ([]analysis.FactorValue, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) ListStocks(ctx context.Context, req analysis.StockListRequest) ([]analysis.StockInfo, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) ListIndices(ctx context.Context, req analysis.IndexListRequest) ([]analysis.IndexInfo, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) ListConcepts(ctx context.Context, req analysis.ConceptListRequest) ([]analysis.ConceptInfo, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetStockSnapshot(ctx context.Context, tradeDate string, adjustType analysis.AdjustType, tsCodes []string) ([]analysis.StockInfo, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetDragonTigerList(ctx context.Context, req analysis.DragonTigerRequest) ([]analysis.DragonTigerList, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetMoneyFlow(ctx context.Context, req analysis.MoneyFlowRequest) ([]analysis.MoneyFlow, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetMoneyFlowConcept(ctx context.Context, req analysis.MoneyFlowConceptRequest) ([]analysis.MoneyFlowConcept, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetPopularityRank(ctx context.Context, req analysis.PopularityRankRequest) ([]analysis.PopularityRank, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetMarketSentiment(ctx context.Context, req analysis.MarketSentimentRequest) (*analysis.DailySentimentResult, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetSentimentHistory(ctx context.Context, req analysis.SentimentHistoryRequest) (*analysis.SentimentHistoryResult, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetSentimentExtremes(ctx context.Context, req analysis.SentimentExtremesRequest) (*analysis.SentimentExtremesResult, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetSectorLeaders(ctx context.Context, req analysis.SectorLeaderRequest) (*analysis.SectorLeaderResult, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) ListNews(ctx context.Context, req analysis.NewsListRequest) ([]analysis.NewsItem, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) ListNewsFromRealtime(ctx context.Context, req analysis.NewsListRequest) ([]analysis.NewsItem, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetLimitUpLadder(ctx context.Context, tradeDate string) ([]analysis.LimitUpLadder, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetFirstLimitUpStocks(ctx context.Context, tradeDate string) ([]analysis.LimitStock, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetLimitUpComparison(ctx context.Context, todayDate string) (*analysis.LimitUpComparison, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetLimitUpList(ctx context.Context, req analysis.LimitUpListRequest) ([]analysis.LimitUpStock, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetLimitUpBySector(ctx context.Context, tradeDate string, sectorType string) ([]analysis.LimitUpBySector, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetLimitUpStocksBySector(ctx context.Context, sectorCode, sectorType, tradeDate string) ([]analysis.LimitUpStock, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetConceptRotationStats(ctx context.Context, req analysis.ConceptRotationRequest) (*analysis.ConceptRotationStats, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetStockBasicInfo(ctx context.Context, tsCode string) (*analysis.StockBasicInfo, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetFinancialIndicators(ctx context.Context, req analysis.FinancialIndicatorRequest) ([]analysis.FinancialIndicator, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetFinancialTableData(ctx context.Context, table string, req analysis.FinancialReportRequest) ([]map[string]any, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) ExecuteReadOnlyQuery(ctx context.Context, req analysis.CustomQueryRequest) (*analysis.CustomQueryResult, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetTradeCalendar(ctx context.Context, startDate, endDate string) ([]string, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetTechnicalIndicators(ctx context.Context, req analysis.TechnicalIndicatorCalcRequest) ([]analysis.TechnicalIndicator, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetRealtimeTicks(ctx context.Context, tsCode string, limit int) ([]analysis.TickRow, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetIntradayTicks(ctx context.Context, tsCode, tradeDate string) ([]analysis.TickRow, error) {
	return nil, nil
}
func (s *stubAnalysisApplicationService) GetIntradayKline(ctx context.Context, tsCode, tradeDate, period string) ([]analysis.IntradayKlineRow, error) {
	return nil, nil
}

func setupAnalysisRouter(svc contracts.AnalysisApplicationService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := httpapi.NewAnalysisHandler(svc, nil)
	h.RegisterRoutes(r.Group("/api/v1"))
	return r
}

func TestAnalysisHandler_GetMoneyFlowRank_InvalidScope(t *testing.T) {
	stub := &stubAnalysisApplicationService{}
	r := setupAnalysisRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analysis/moneyflow-rank?scope=bad", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, float64(400), body["code"])
}

func TestAnalysisHandler_GetMoneyFlowRank_QueryParamsAndSuccess(t *testing.T) {
	var got analysis.MoneyFlowRankRequest
	stub := &stubAnalysisApplicationService{
		MoneyFlowRankHook: func(ctx context.Context, req analysis.MoneyFlowRankRequest) (*analysis.MoneyFlowRankResult, error) {
			got = req
			return &analysis.MoneyFlowRankResult{
				TradeDate:   "20250102",
				StockSource: "moneyflow",
				StockItems: []analysis.MoneyFlowStockRankItem{
					{Rank: 1, TsCode: "000001.SZ", NetMfAmount: 1.5, DataSource: "moneyflow"},
				},
			}, nil
		},
	}
	r := setupAnalysisRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analysis/moneyflow-rank?scope=stock&trade_date=20250102&limit=20&offset=5", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "20250102", got.TradeDate)
	assert.Equal(t, "stock", got.Scope)
	assert.Equal(t, 20, got.Limit)
	assert.Equal(t, 5, got.Offset)
	var resp struct {
		Code    int                          `json:"code"`
		Message string                       `json:"message"`
		Data    analysis.MoneyFlowRankResult `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, 0, resp.Code)
	assert.Equal(t, "20250102", resp.Data.TradeDate)
	assert.Len(t, resp.Data.StockItems, 1)
	assert.Equal(t, "000001.SZ", resp.Data.StockItems[0].TsCode)
}

func TestAnalysisHandler_GetMoneyFlowRank_ServiceError(t *testing.T) {
	stub := &stubAnalysisApplicationService{
		MoneyFlowRankHook: func(ctx context.Context, req analysis.MoneyFlowRankRequest) (*analysis.MoneyFlowRankResult, error) {
			return nil, errors.New("db down")
		},
	}
	r := setupAnalysisRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analysis/moneyflow-rank", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.NotEqual(t, http.StatusOK, w.Code)
}

func TestAnalysisHandler_GetIndexOHLCV_MissingTsCode(t *testing.T) {
	stub := &stubAnalysisApplicationService{}
	r := setupAnalysisRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analysis/index-ohlcv", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAnalysisHandler_GetIndexOHLCV_QueryParamsAndSuccess(t *testing.T) {
	var got analysis.IndexOHLCVRequest
	stub := &stubAnalysisApplicationService{
		IndexOHLCVHook: func(ctx context.Context, req analysis.IndexOHLCVRequest) (*analysis.IndexOHLCVResult, error) {
			got = req
			return &analysis.IndexOHLCVResult{
				TsCode: "000001.SH", WindowDays: 5, EndDate: "20250310",
				Items: []analysis.IndexOHLCVRow{{TradeDate: "20250310", Close: 3000}},
			}, nil
		},
	}
	r := setupAnalysisRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analysis/index-ohlcv?ts_code=000001.SH&days=5&end_date=20250310", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "000001.SH", got.TsCode)
	assert.Equal(t, 5, got.Days)
	assert.Equal(t, "20250310", got.EndDate)
	var resp struct {
		Code int                       `json:"code"`
		Data analysis.IndexOHLCVResult `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, 5, resp.Data.WindowDays)
	assert.Len(t, resp.Data.Items, 1)
}

func TestAnalysisHandler_GetIndexOHLCV_DefaultDays(t *testing.T) {
	var got analysis.IndexOHLCVRequest
	stub := &stubAnalysisApplicationService{
		IndexOHLCVHook: func(ctx context.Context, req analysis.IndexOHLCVRequest) (*analysis.IndexOHLCVResult, error) {
			got = req
			return &analysis.IndexOHLCVResult{TsCode: req.TsCode, WindowDays: req.Days}, nil
		},
	}
	r := setupAnalysisRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analysis/index-ohlcv?ts_code=399001.SZ", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 10, got.Days, "days 缺省应为 10")
}

func TestAnalysisHandler_ListIndexSectorMembers_MissingIndexCode(t *testing.T) {
	stub := &stubAnalysisApplicationService{}
	r := setupAnalysisRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analysis/index-sector-members", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAnalysisHandler_ListIndexSectorMembers_QueryParamsAndSuccess(t *testing.T) {
	var got analysis.IndexSectorMemberRequest
	stub := &stubAnalysisApplicationService{
		ListIndexSectorMembersHook: func(ctx context.Context, req analysis.IndexSectorMemberRequest) ([]analysis.IndexSectorMember, error) {
			got = req
			return []analysis.IndexSectorMember{{IndexCode: req.IndexCode, ConCode: "000001.SZ", DataSource: "index_weight"}}, nil
		},
	}
	r := setupAnalysisRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analysis/index-sector-members?index_code=000300.SH&trade_date=20250301&limit=50&offset=2", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "000300.SH", got.IndexCode)
	assert.Equal(t, "20250301", got.TradeDate)
	assert.Equal(t, 50, got.Limit)
	assert.Equal(t, 2, got.Offset)
}

func TestAnalysisHandler_ListIndexSectors_Success(t *testing.T) {
	var got analysis.IndexSectorListRequest
	stub := &stubAnalysisApplicationService{
		ListIndexSectorsHook: func(ctx context.Context, req analysis.IndexSectorListRequest) ([]analysis.IndexSectorInfo, error) {
			got = req
			return []analysis.IndexSectorInfo{{IndexCode: "801010.SI", IndustryName: "农林牧渔"}}, nil
		},
	}
	r := setupAnalysisRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analysis/index-sectors?src=SW2021&level=L1&limit=20&offset=5", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.NotNil(t, got.Src)
	assert.Equal(t, "SW2021", *got.Src)
	assert.NotNil(t, got.Level)
	assert.Equal(t, "L1", *got.Level)
	assert.Equal(t, 20, got.Limit)
	assert.Equal(t, 5, got.Offset)
}

func TestAnalysisHandler_GetIndexOHLCV_InvalidDaysFallsBackToDefault(t *testing.T) {
	var got analysis.IndexOHLCVRequest
	stub := &stubAnalysisApplicationService{
		IndexOHLCVHook: func(ctx context.Context, req analysis.IndexOHLCVRequest) (*analysis.IndexOHLCVResult, error) {
			got = req
			return &analysis.IndexOHLCVResult{}, nil
		},
	}
	r := setupAnalysisRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/analysis/index-ohlcv?ts_code=000001.SH&days=0", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 10, got.Days, "days<=0 时 defaultInt 回退为 10")
}
