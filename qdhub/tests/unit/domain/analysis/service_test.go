package analysis_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"qdhub/internal/domain/analysis"
)

// mockKLineReader 仅用于 GetKLine 的 KLineReader 模拟
type mockKLineReader struct {
	mock.Mock
}

func (m *mockKLineReader) GetDailyWithAdjFactor(ctx context.Context, tsCode, startDate, endDate string) ([]analysis.RawDailyRow, error) {
	args := m.Called(ctx, tsCode, startDate, endDate)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]analysis.RawDailyRow), args.Error(1)
}

// noop 通用空实现（无重载方法）
type noop struct{}

func (noop) GetLimitStats(ctx context.Context, startDate, endDate string) ([]analysis.LimitStats, error) {
	return nil, nil
}
func (noop) GetByDateAndType(ctx context.Context, tradeDate, limitType string) ([]analysis.LimitStock, error) {
	return nil, nil
}
func (noop) GetBySectorAndDate(ctx context.Context, sectorCode, sectorType, tradeDate string) (*analysis.SectorLimitStocks, error) {
	return nil, nil
}
func (noop) GetConceptHeat(ctx context.Context, tradeDate string) ([]analysis.ConceptHeat, error) {
	return nil, nil
}
func (noop) GetConceptStocks(ctx context.Context, conceptCode, tradeDate string) ([]analysis.StockInfo, error) {
	return nil, nil
}
func (noop) GetRankedConcepts(ctx context.Context, q analysis.ConceptRotationQuery) ([]analysis.ConceptRotationRow, error) {
	return nil, nil
}
func (noop) GetList(ctx context.Context, req analysis.DragonTigerRequest) ([]analysis.DragonTigerList, error) {
	return nil, nil
}
func (noop) GetMoneyFlow(ctx context.Context, req analysis.MoneyFlowRequest) ([]analysis.MoneyFlow, error) {
	return nil, nil
}
func (noop) GetRank(ctx context.Context, req analysis.PopularityRankRequest) ([]analysis.PopularityRank, error) {
	return nil, nil
}
func (noop) GetStocks(ctx context.Context, sectorCode, sectorType, tradeDate string) ([]analysis.LimitUpStock, error) {
	return nil, nil
}
func (noop) GetByTsCode(ctx context.Context, tsCode string) (*analysis.StockBasicInfo, error) {
	return nil, nil
}
func (noop) GetIndicators(ctx context.Context, req analysis.FinancialIndicatorRequest) ([]analysis.FinancialIndicator, error) {
	return nil, nil
}
func (noop) GetReports(ctx context.Context, req analysis.FinancialReportRequest) ([]analysis.FinancialReport, error) {
	return nil, nil
}
func (noop) ExecuteReadOnlyQuery(ctx context.Context, req analysis.CustomQueryRequest) (*analysis.CustomQueryResult, error) {
	return nil, nil
}

// noopSector 仅 SectorLimitStatsReader：GetByDate(ctx, tradeDate, sectorType)
type noopSector struct{}

func (noopSector) GetByDate(ctx context.Context, tradeDate, sectorType string) ([]analysis.SectorLimitStats, error) {
	return nil, nil
}

// noopLimitUpBySector 仅 LimitUpBySectorReader：GetByDate(ctx, tradeDate, sectorType) -> []LimitUpBySector
type noopLimitUpBySector struct{}

func (noopLimitUpBySector) GetByDate(ctx context.Context, tradeDate, sectorType string) ([]analysis.LimitUpBySector, error) {
	return nil, nil
}

// noopStockList 仅 StockListReader
type noopStockList struct{}

func (noopStockList) List(ctx context.Context, req analysis.StockListRequest) ([]analysis.StockInfo, error) {
	return nil, nil
}

// noopIndexList 仅 IndexListReader
type noopIndexList struct{}

func (noopIndexList) List(ctx context.Context, req analysis.IndexListRequest) ([]analysis.IndexInfo, error) {
	return nil, nil
}

// noopConceptList 仅 ConceptListReader
type noopConceptList struct{}

func (noopConceptList) List(ctx context.Context, req analysis.ConceptListRequest) ([]analysis.ConceptInfo, error) {
	return nil, nil
}

// noopNews 仅 NewsReader
type noopNews struct{}

func (noopNews) List(ctx context.Context, req analysis.NewsListRequest) ([]analysis.NewsItem, error) {
	return nil, nil
}

// noopLadder LimitLadderReader
type noopLadder struct{}

func (noopLadder) GetByDate(ctx context.Context, tradeDate string) (*analysis.LimitLadderStats, error) {
	return nil, nil
}

// noopComparison LimitComparisonReader
type noopComparison struct{}

func (noopComparison) GetComparison(ctx context.Context, todayDate string) (*analysis.LimitComparison, error) {
	return nil, nil
}

// noopLimitUpList LimitUpListReader
type noopLimitUpList struct{}

func (noopLimitUpList) GetByDate(ctx context.Context, tradeDate string) ([]analysis.LimitUpStock, error) {
	return nil, nil
}
func (noopLimitUpList) GetList(ctx context.Context, req analysis.LimitUpListRequest) ([]analysis.LimitUpStock, error) {
	return nil, nil
}

// noopLimitUpLadder LimitUpLadderReader
type noopLimitUpLadder struct{}

func (noopLimitUpLadder) GetByDate(ctx context.Context, tradeDate string) ([]analysis.LimitUpLadder, error) {
	return nil, nil
}

// noopLimitUpComparison LimitUpComparisonReader
type noopLimitUpComparison struct{}

func (noopLimitUpComparison) GetComparison(ctx context.Context, todayDate string) (*analysis.LimitUpComparison, error) {
	return nil, nil
}

func TestAnalysisService_GetKLine(t *testing.T) {
	ctx := context.Background()
	n := noop{}
	nSector := noopSector{}
	nLimitUpBySector := noopLimitUpBySector{}
	nStock := noopStockList{}
	nIndex := noopIndexList{}
	nConcept := noopConceptList{}
	nNews := noopNews{}
	nLadder := noopLadder{}
	nCmp := noopComparison{}
	nLimitUp := noopLimitUpList{}
	nLimitUpLadder := noopLimitUpLadder{}
	nLimitUpCmp := noopLimitUpComparison{}

	t.Run("none_adjust", func(t *testing.T) {
		mockK := new(mockKLineReader)
		rows := []analysis.RawDailyRow{
			{TradeDate: "20240102", Open: 10, High: 11, Low: 9.5, Close: 10.5, Vol: 1e6, Amount: 1e7, PreClose: 10, Change: 0.5, PctChg: 5, AdjFactor: 1.0},
		}
		mockK.On("GetDailyWithAdjFactor", ctx, "000001.SZ", "20240101", "20240131").Return(rows, nil)

		svc := analysis.NewAnalysisService(
			mockK, n, n, nLadder, nCmp, nSector, n, n, n, n,
			nStock, nIndex, nConcept, n, n, n, nNews, nLimitUp, nLimitUpLadder, nLimitUpCmp, nLimitUpBySector, n, n, n, n, n,
		)
		data, err := svc.GetKLine(ctx, analysis.KLineRequest{
			TsCode: "000001.SZ", StartDate: "20240101", EndDate: "20240131", AdjustType: analysis.AdjustNone,
		})
		assert.NoError(t, err)
		assert.Len(t, data, 1)
		assert.Equal(t, "20240102", data[0].TradeDate)
		assert.Equal(t, 10.5, data[0].Close)
		mockK.AssertExpectations(t)
	})

	t.Run("qfq", func(t *testing.T) {
		mockK := new(mockKLineReader)
		rows := []analysis.RawDailyRow{
			{TradeDate: "20240102", Open: 10, High: 11, Low: 9.5, Close: 10.5, Vol: 1e6, Amount: 1e7, PreClose: 10, Change: 0.5, PctChg: 5, AdjFactor: 1.0},
			{TradeDate: "20240103", Open: 11, High: 12, Low: 10.5, Close: 11.5, Vol: 1.2e6, Amount: 1.2e7, PreClose: 10.5, Change: 1, PctChg: 9.52, AdjFactor: 2.0},
		}
		mockK.On("GetDailyWithAdjFactor", ctx, "000001.SZ", "20240101", "20240131").Return(rows, nil)

		svc := analysis.NewAnalysisService(
			mockK, n, n, nLadder, nCmp, nSector, n, n, n, n,
			nStock, nIndex, nConcept, n, n, n, nNews, nLimitUp, nLimitUpLadder, nLimitUpCmp, nLimitUpBySector, n, n, n, n, n,
		)
		data, err := svc.GetKLine(ctx, analysis.KLineRequest{
			TsCode: "000001.SZ", StartDate: "20240101", EndDate: "20240131", AdjustType: analysis.AdjustQfq,
		})
		assert.NoError(t, err)
		assert.Len(t, data, 2)
		assert.InDelta(t, 10.5*0.5, data[0].Close, 1e-6)
		assert.InDelta(t, 11.5*1.0, data[1].Close, 1e-6)
		mockK.AssertExpectations(t)
	})

	t.Run("empty", func(t *testing.T) {
		mockK := new(mockKLineReader)
		mockK.On("GetDailyWithAdjFactor", ctx, "999999.SZ", "20240101", "20240131").Return([]analysis.RawDailyRow(nil), nil)

		svc := analysis.NewAnalysisService(
			mockK, n, n, nLadder, nCmp, nSector, n, n, n, n,
			nStock, nIndex, nConcept, n, n, n, nNews, nLimitUp, nLimitUpLadder, nLimitUpCmp, nLimitUpBySector, n, n, n, n, n,
		)
		data, err := svc.GetKLine(ctx, analysis.KLineRequest{
			TsCode: "999999.SZ", StartDate: "20240101", EndDate: "20240131", AdjustType: analysis.AdjustNone,
		})
		assert.NoError(t, err)
		assert.Nil(t, data)
		mockK.AssertExpectations(t)
	})

	t.Run("reader_error", func(t *testing.T) {
		mockK := new(mockKLineReader)
		mockK.On("GetDailyWithAdjFactor", ctx, "000001.SZ", "20240101", "20240131").Return(nil, assert.AnError)

		svc := analysis.NewAnalysisService(
			mockK, n, n, nLadder, nCmp, nSector, n, n, n, n,
			nStock, nIndex, nConcept, n, n, n, nNews, nLimitUp, nLimitUpLadder, nLimitUpCmp, nLimitUpBySector, n, n, n, n, n,
		)
		data, err := svc.GetKLine(ctx, analysis.KLineRequest{
			TsCode: "000001.SZ", StartDate: "20240101", EndDate: "20240131", AdjustType: analysis.AdjustNone,
		})
		assert.Error(t, err)
		assert.Nil(t, data)
		mockK.AssertExpectations(t)
	})
}
