package analysis_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	analysisinfra "qdhub/internal/infrastructure/analysis"
	"qdhub/internal/infrastructure/datasource"
)

// mockTokenResolver 模拟 TokenResolver
type mockTokenResolver struct {
	mock.Mock
}

func (m *mockTokenResolver) TokenForDataSource(ctx context.Context, dataSourceName string) (string, error) {
	args := m.Called(ctx, dataSourceName)
	return args.String(0), args.Error(1)
}

func TestTokenResolverImpl_NilRepo(t *testing.T) {
	ctx := context.Background()
	impl := &analysisinfra.TokenResolverImpl{Repo: nil}
	token, err := impl.TokenForDataSource(ctx, "tushare")
	assert.Empty(t, token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestFallbackProvider_FetchDaily_Mapping(t *testing.T) {
	ctx := context.Background()

	reg := datasource.NewRegistry()
	mockClient := &mockDailyClient{
		data: []map[string]interface{}{
			{"trade_date": "20240102", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "vol": 100000.0, "amount": 1050000.0, "pre_close": 10.0, "change": 0.5, "pct_chg": 5.0},
		},
	}
	_ = reg.RegisterClient(mockClient)

	tokenResolver := new(mockTokenResolver)
	tokenResolver.On("TokenForDataSource", ctx, "tushare").Return("test-token", nil)

	fb := analysisinfra.NewFallbackProvider("tushare", reg, tokenResolver)
	rows, err := fb.FetchDaily(ctx, "000001.SZ", "20240101", "20240131")
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "20240102", rows[0].TradeDate)
	assert.Equal(t, 10.5, rows[0].Close)
	assert.Equal(t, 1.0, rows[0].AdjFactor)
	tokenResolver.AssertExpectations(t)
}

func TestFallbackProvider_EmptyResult(t *testing.T) {
	ctx := context.Background()
	reg := datasource.NewRegistry()
	mockClient := &mockDailyClient{data: nil}
	_ = reg.RegisterClient(mockClient)

	tokenResolver := new(mockTokenResolver)
	tokenResolver.On("TokenForDataSource", ctx, "tushare").Return("test-token", nil)

	fb := analysisinfra.NewFallbackProvider("tushare", reg, tokenResolver)
	rows, err := fb.FetchDaily(ctx, "000001.SZ", "20240101", "20240131")
	assert.NoError(t, err)
	assert.Nil(t, rows)
	tokenResolver.AssertExpectations(t)
}

func TestFallbackProvider_TokenError(t *testing.T) {
	ctx := context.Background()
	reg := datasource.NewRegistry()
	_ = reg.RegisterClient(&mockDailyClient{})

	tokenResolver := new(mockTokenResolver)
	tokenResolver.On("TokenForDataSource", ctx, "tushare").Return("", assert.AnError)

	fb := analysisinfra.NewFallbackProvider("tushare", reg, tokenResolver)
	rows, err := fb.FetchDaily(ctx, "000001.SZ", "20240101", "20240131")
	assert.Error(t, err)
	assert.Nil(t, rows)
	tokenResolver.AssertExpectations(t)
}

type mockDailyClient struct {
	data []map[string]interface{}
}

func (c *mockDailyClient) Name() string                  { return "tushare" }
func (c *mockDailyClient) SetToken(token string)         {}
func (c *mockDailyClient) Query(ctx context.Context, apiName string, params map[string]interface{}) (*datasource.QueryResult, error) {
	if apiName != "daily" {
		return &datasource.QueryResult{Data: nil, Total: int64(0), HasMore: false}, nil
	}
	return &datasource.QueryResult{Data: c.data, Total: int64(len(c.data)), HasMore: false}, nil
}
func (c *mockDailyClient) ValidateToken(ctx context.Context) (bool, error) { return true, nil }
