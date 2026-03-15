package analysis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"

	"qdhub/internal/domain/analysis"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/infrastructure/datasource"
)

// TokenResolver 按数据源名称解析 API Token（如从元数据库读取）
type TokenResolver interface {
	TokenForDataSource(ctx context.Context, dataSourceName string) (string, error)
}

// TokenResolverImpl 使用 metadata.Repository 按名称查数据源并取 Token
type TokenResolverImpl struct {
	Repo metadata.Repository
}

// TokenForDataSource 返回指定数据源的 Token；若未配置或未找到返回空与 error
func (t *TokenResolverImpl) TokenForDataSource(ctx context.Context, dataSourceName string) (string, error) {
	if t.Repo == nil {
		return "", fmt.Errorf("metadata repo is nil")
	}
	ds, err := t.Repo.GetDataSourceByName(ctx, dataSourceName)
	if err != nil {
		return "", fmt.Errorf("get data source by name %s: %w", dataSourceName, err)
	}
	if ds == nil {
		return "", fmt.Errorf("data source not found: %s", dataSourceName)
	}
	tok, err := t.Repo.GetToken(ctx, ds.ID)
	if err != nil {
		return "", fmt.Errorf("get token for data source %s: %w", dataSourceName, err)
	}
	if tok == nil || strings.TrimSpace(tok.TokenValue) == "" {
		return "", fmt.Errorf("token not configured for data source: %s", dataSourceName)
	}
	return tok.TokenValue, nil
}

// FallbackProvider 当本地库无数据时，从远程数据源（如 Tushare）拉取
type FallbackProvider interface {
	// FetchDaily 从数据源拉取日线，返回 RawDailyRow（adj_factor 可为 1.0）
	FetchDaily(ctx context.Context, tsCode, startDate, endDate string) ([]analysis.RawDailyRow, error)
}

// fallbackProviderImpl 使用 datasource.Registry 的 APIClient 拉取 daily
type fallbackProviderImpl struct {
	clientName    string
	registry      *datasource.Registry
	tokenResolver TokenResolver
}

// NewFallbackProvider 创建兜底提供者，clientName 如 "tushare"
func NewFallbackProvider(clientName string, registry *datasource.Registry, tokenResolver TokenResolver) FallbackProvider {
	return &fallbackProviderImpl{
		clientName:    clientName,
		registry:      registry,
		tokenResolver: tokenResolver,
	}
}

// FetchDaily 调用数据源 daily 接口并转换为 RawDailyRow
func (f *fallbackProviderImpl) FetchDaily(ctx context.Context, tsCode, startDate, endDate string) ([]analysis.RawDailyRow, error) {
	client, err := f.registry.GetClient(f.clientName)
	if err != nil {
		return nil, fmt.Errorf("fallback get client %s: %w", f.clientName, err)
	}
	token, err := f.tokenResolver.TokenForDataSource(ctx, f.clientName)
	if err != nil {
		logrus.Debugf("[analysis fallback] token for %s: %v", f.clientName, err)
		return nil, fmt.Errorf("fallback token: %w", err)
	}
	client.SetToken(token)
	params := map[string]interface{}{
		"ts_code":    tsCode,
		"start_date": startDate,
		"end_date":   endDate,
	}
	res, err := client.Query(ctx, "daily", params)
	if err != nil {
		return nil, fmt.Errorf("fallback query daily: %w", err)
	}
	if res == nil || len(res.Data) == 0 {
		return nil, nil
	}
	rows := make([]analysis.RawDailyRow, 0, len(res.Data))
	for _, m := range res.Data {
		rows = append(rows, mapToRawDailyRow(m))
	}
	logrus.Infof("[analysis fallback] %s daily: ts_code=%s range=%s~%s rows=%d", f.clientName, tsCode, startDate, endDate, len(rows))
	return rows, nil
}

func mapToRawDailyRow(m map[string]interface{}) analysis.RawDailyRow {
	return analysis.RawDailyRow{
		TradeDate: strVal(m, "trade_date"),
		Open:      floatVal(m, "open"),
		High:      floatVal(m, "high"),
		Low:       floatVal(m, "low"),
		Close:     floatVal(m, "close"),
		Vol:       floatVal(m, "vol"),
		Amount:    floatVal(m, "amount"),
		PreClose:  floatVal(m, "pre_close"),
		Change:    floatVal(m, "change"),
		PctChg:    floatVal(m, "pct_chg"),
		AdjFactor: 1.0, // 兜底数据暂不复权，由上层按 none 处理或后续再查 adj_factor
	}
}

func strVal(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func floatVal(m map[string]interface{}, key string) float64 {
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}
