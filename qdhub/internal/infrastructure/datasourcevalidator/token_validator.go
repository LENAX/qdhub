// Package datasourcevalidator implements token validation by calling data source adapters.
package datasourcevalidator

import (
	"context"
	"strings"

	"qdhub/internal/application/contracts"
	tushareclient "qdhub/internal/infrastructure/datasource/tushare"
)

// TokenValidator implements contracts.DataSourceTokenValidator using data source adapters.
type TokenValidator struct{}

// NewTokenValidator creates a new TokenValidator.
func NewTokenValidator() *TokenValidator {
	return &TokenValidator{}
}

// Validate 使用对应数据源 adapter 发一次真实请求来校验 token。
// 数据源识别：dataSourceName 含 "tushare" 时使用 Tushare adapter（请求 stock_basic limit=1）。
// 返回的 message 在失败时来自数据源接口原样返回（如 Tushare 的「服务异常，请稍后再试！」）。
func (v *TokenValidator) Validate(ctx context.Context, dataSourceName, baseURL, token string) (valid bool, message string, err error) {
	adapterName := resolveAdapterName(dataSourceName)
	switch adapterName {
	case "tushare":
		client := tushareclient.NewClient(
			tushareclient.WithBaseURL(baseURL),
			tushareclient.WithToken(token),
		)
		valid, err := client.ValidateToken(ctx)
		if err != nil {
			// 将 Tushare 返回的错误信息原样带给前端
			return false, err.Error(), nil
		}
		if !valid {
			return false, "token无效", nil
		}
		return true, "", nil
	default:
		return false, "不支持的数据源类型: " + dataSourceName, nil
	}
}

// Ensure TokenValidator implements the interface.
var _ contracts.DataSourceTokenValidator = (*TokenValidator)(nil)

// resolveAdapterName 根据数据源名称解析使用的 adapter，目前仅支持 tushare（名称含 "tushare" 不区分大小写）。
func resolveAdapterName(dataSourceName string) string {
	name := strings.ToLower(strings.TrimSpace(dataSourceName))
	if strings.Contains(name, "tushare") {
		return "tushare"
	}
	return ""
}
