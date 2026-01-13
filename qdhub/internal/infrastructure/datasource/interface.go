// Package datasource provides interfaces and implementations for data source adapters.
package datasource

import (
	"context"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// ==================== API 客户端接口 ====================

// APIClient defines the interface for data source API clients.
// Implementation: tushare/client.go, akshare/client.go
type APIClient interface {
	// Name returns the data source name (e.g., "tushare", "akshare").
	Name() string

	// SetToken sets the API token for authentication.
	SetToken(token string)

	// Query executes an API query with the given parameters.
	// Returns the result as a slice of maps.
	Query(ctx context.Context, apiName string, params map[string]interface{}) (*QueryResult, error)

	// ValidateToken validates if the token is valid.
	ValidateToken(ctx context.Context) (bool, error)
}

// QueryResult represents the result of an API query.
type QueryResult struct {
	// Data contains the query result rows.
	Data []map[string]interface{}
	// Total is the total number of records (may differ from len(Data) for paginated results).
	Total int64
	// HasMore indicates if there are more records to fetch.
	HasMore bool
}

// ==================== 文档爬虫接口 ====================

// Crawler defines the interface for document crawlers.
// Implementation: tushare/crawler.go, akshare/crawler.go
type Crawler interface {
	// Name returns the data source name.
	Name() string

	// FetchCatalogPage fetches the catalog page content.
	// Returns: content, document type, error
	FetchCatalogPage(ctx context.Context, dataSourceID shared.ID) (string, metadata.DocumentType, error)

	// FetchAPIDetailPage fetches an API detail page content.
	// Returns: content, document type, error
	FetchAPIDetailPage(ctx context.Context, apiURL string) (string, metadata.DocumentType, error)
}

// ==================== 文档解析器接口 ====================

// DocumentParser defines the interface for parsing data source documentation.
// This interface is defined in the domain layer (metadata.DocumentParser),
// but we re-export it here for convenience.
type DocumentParser = metadata.DocumentParser

// DocumentParserFactory defines the interface for creating document parsers.
type DocumentParserFactory = metadata.DocumentParserFactory

// ==================== 数据源适配器接口 ====================

// DataSourceAdapter combines API client and crawler capabilities.
// This is the main interface for interacting with a data source.
type DataSourceAdapter interface {
	// Name returns the data source name.
	Name() string

	// Client returns the API client for this data source.
	Client() APIClient

	// Crawler returns the document crawler for this data source.
	Crawler() Crawler

	// Parser returns the document parser for this data source.
	Parser() DocumentParser
}

// ==================== 配置 ====================

// ClientConfig represents the configuration for an API client.
type ClientConfig struct {
	// BaseURL is the base URL for API requests.
	BaseURL string
	// Timeout is the request timeout in seconds.
	Timeout int
	// RetryCount is the number of retries on failure.
	RetryCount int
	// RetryDelay is the delay between retries in milliseconds.
	RetryDelay int
}

// CrawlerConfig represents the configuration for a crawler.
type CrawlerConfig struct {
	// DocURL is the base URL for documentation.
	DocURL string
	// Timeout is the request timeout in seconds.
	Timeout int
	// RateLimitPerMinute is the rate limit for requests per minute.
	RateLimitPerMinute int
}

// ==================== 错误类型 ====================

// Error codes for datasource operations.
const (
	ErrCodeTokenInvalid       = "TOKEN_INVALID"
	ErrCodeAPINotFound        = "API_NOT_FOUND"
	ErrCodeRateLimited        = "RATE_LIMITED"         // 每分钟请求频率限制，可重试
	ErrCodeDailyLimitExceeded = "DAILY_LIMIT_EXCEEDED" // 每日请求上限，不可重试
	ErrCodeNetworkError       = "NETWORK_ERROR"
	ErrCodeParseError         = "PARSE_ERROR"
	ErrCodePermissionDeny     = "PERMISSION_DENY"
	ErrCodeUnknown            = "UNKNOWN"
)

// DataSourceError represents an error from data source operations.
type DataSourceError struct {
	Code    string
	Message string
	Cause   error
}

func (e *DataSourceError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

func (e *DataSourceError) Unwrap() error {
	return e.Cause
}

// NewDataSourceError creates a new DataSourceError.
func NewDataSourceError(code, message string, cause error) *DataSourceError {
	return &DataSourceError{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

// ==================== 通用错误映射 ====================

// ErrorMappingRule 定义单条错误映射规则
// 用于将数据源特定的错误码/消息映射到通用错误码
type ErrorMappingRule struct {
	// SourceCodes 数据源返回的错误码列表（任意匹配即触发）
	SourceCodes []int
	// Keywords 消息中的关键词（任意匹配即触发，为空则不检查消息）
	Keywords []string
	// TargetCode 映射到的通用错误码
	TargetCode string
}

// ErrorMapper 通用错误映射器
// 各数据源可通过配置规则实现错误映射
type ErrorMapper struct {
	// Rules 映射规则列表，按优先级排序（先匹配的优先）
	Rules []ErrorMappingRule
	// DefaultCode 默认错误码（无规则匹配时使用）
	DefaultCode string
}

// NewErrorMapper 创建错误映射器
func NewErrorMapper(rules []ErrorMappingRule, defaultCode string) *ErrorMapper {
	if defaultCode == "" {
		defaultCode = ErrCodeUnknown
	}
	return &ErrorMapper{
		Rules:       rules,
		DefaultCode: defaultCode,
	}
}

// MapError 将数据源错误映射为 DataSourceError
func (m *ErrorMapper) MapError(sourceCode int, msg string) *DataSourceError {
	for _, rule := range m.Rules {
		if m.matchRule(rule, sourceCode, msg) {
			return NewDataSourceError(rule.TargetCode, msg, nil)
		}
	}
	return NewDataSourceError(m.DefaultCode, msg, nil)
}

// matchRule 检查是否匹配规则
func (m *ErrorMapper) matchRule(rule ErrorMappingRule, sourceCode int, msg string) bool {
	// 检查错误码是否匹配
	codeMatched := false
	if len(rule.SourceCodes) == 0 {
		codeMatched = true // 未指定错误码则任意匹配
	} else {
		for _, code := range rule.SourceCodes {
			if code == sourceCode {
				codeMatched = true
				break
			}
		}
	}

	if !codeMatched {
		return false
	}

	// 检查关键词是否匹配
	if len(rule.Keywords) == 0 {
		return true // 未指定关键词则只看错误码
	}

	for _, keyword := range rule.Keywords {
		if containsString(msg, keyword) {
			return true
		}
	}
	return false
}

// containsString 检查字符串是否包含子串
func containsString(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
