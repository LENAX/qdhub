// Package tushare provides Tushare data source adapter implementation.
package tushare

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"qdhub/internal/infrastructure/datasource"

	"golang.org/x/sync/semaphore"
)

const (
	// DefaultBaseURL is the default Tushare Pro API base URL.
	DefaultBaseURL = "http://api.tushare.pro"
	// DefaultTimeout is the default request timeout in seconds.
	DefaultTimeout = 30
	// DefaultRetryCount is the default retry count.
	DefaultRetryCount = 5
	// DefaultRetryDelay is the default retry delay in milliseconds.
	DefaultRetryDelay = 3000
	// DefaultMaxConcurrent is the default maximum concurrent requests.
	DefaultMaxConcurrent = 100
	// DefaultMaxIdleConns is the default maximum idle connections.
	DefaultMaxIdleConns = 10
	// DefaultMaxIdleConnsPerHost is the default maximum idle connections per host.
	DefaultMaxIdleConnsPerHost = 10
	// DefaultIdleConnTimeout is the default idle connection timeout in seconds.
	DefaultIdleConnTimeout = 90
)

// Client implements datasource.APIClient for Tushare Pro API.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
	transport  *http.Transport
	retryCount int
	retryDelay time.Duration
	// Concurrency control
	maxConcurrent int64
	sem           *semaphore.Weighted
	// Error mapping
	errorMapper *datasource.ErrorMapper
}

// ClientOption is a function that configures Client.
type ClientOption func(*Client)

// WithBaseURL sets the base URL.
func WithBaseURL(url string) ClientOption {
	return func(c *Client) {
		c.baseURL = url
	}
}

// WithTimeout sets the request timeout.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

// WithRetryCount sets the retry count.
func WithRetryCount(count int) ClientOption {
	return func(c *Client) {
		c.retryCount = count
	}
}

// WithRetryDelay sets the retry delay.
func WithRetryDelay(delay time.Duration) ClientOption {
	return func(c *Client) {
		c.retryDelay = delay
	}
}

// WithToken sets the API token.
func WithToken(token string) ClientOption {
	return func(c *Client) {
		c.token = token
	}
}

// WithMaxConcurrent sets the maximum concurrent requests.
// This controls how many requests can be in-flight at the same time.
func WithMaxConcurrent(n int) ClientOption {
	return func(c *Client) {
		if n > 0 {
			c.maxConcurrent = int64(n)
		}
	}
}

// WithMaxIdleConns sets the maximum number of idle connections in the pool.
func WithMaxIdleConns(n int) ClientOption {
	return func(c *Client) {
		if n > 0 {
			c.transport.MaxIdleConns = n
		}
	}
}

// WithMaxIdleConnsPerHost sets the maximum number of idle connections per host.
func WithMaxIdleConnsPerHost(n int) ClientOption {
	return func(c *Client) {
		if n > 0 {
			c.transport.MaxIdleConnsPerHost = n
		}
	}
}

// WithIdleConnTimeout sets the idle connection timeout.
func WithIdleConnTimeout(d time.Duration) ClientOption {
	return func(c *Client) {
		c.transport.IdleConnTimeout = d
	}
}

// WithHTTPClient sets a custom HTTP client (for testing).
// Note: This will override the default transport settings.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = client
	}
}

// TushareErrorMappingRules 定义 Tushare 特有的错误映射规则
// 规则按优先级排序，先匹配的优先
var TushareErrorMappingRules = []datasource.ErrorMappingRule{
	// Token 无效或过期
	{
		SourceCodes: []int{-2001, -2002},
		Keywords:    []string{"Token", "token", "无效", "expired", "过期", "invalid", "令牌"},
		TargetCode:  datasource.ErrCodeTokenInvalid,
	},
	// 每日请求上限（不可重试）- 必须在频率限制之前，因为都是 -2003
	{
		SourceCodes: []int{-2003},
		Keywords:    []string{"每日", "日上限", "daily", "今日", "每天", "exceed", "超过", "上限", "超限", "限制", "请求次数"},
		TargetCode:  datasource.ErrCodeDailyLimitExceeded,
	},
	// 每分钟频率限制（可重试）
	{
		SourceCodes: []int{-2003},
		Keywords:    []string{"每分钟", "分钟", "频率", "minute", "rate", "节流", "limit", "过快", "访问频率", "过于频繁"},
		TargetCode:  datasource.ErrCodeRateLimited,
	},
	// -2003 默认作为频率限制（可重试）
	{
		SourceCodes: []int{-2003},
		TargetCode:  datasource.ErrCodeRateLimited,
	},
	// 权限不足
	{
		SourceCodes: []int{-2004},
		Keywords:    []string{"权限", "permission", "无权", "denied", "禁止", "unauthorized", "拒绝", "禁止访问", "无访问", "not allowed", "无权限"},
		TargetCode:  datasource.ErrCodePermissionDeny,
	},
}

// NewClient creates a new Tushare API client with connection pooling and concurrency control.
func NewClient(opts ...ClientOption) *Client {
	// Create transport with connection pool settings
	transport := &http.Transport{
		MaxIdleConns:        DefaultMaxIdleConns,
		MaxIdleConnsPerHost: DefaultMaxIdleConnsPerHost,
		MaxConnsPerHost:     DefaultMaxConcurrent, // Limit connections per host
		IdleConnTimeout:     DefaultIdleConnTimeout * time.Second,
	}

	c := &Client{
		baseURL:   DefaultBaseURL,
		transport: transport,
		httpClient: &http.Client{
			Timeout:   DefaultTimeout * time.Second,
			Transport: transport,
		},
		retryCount:    DefaultRetryCount,
		retryDelay:    DefaultRetryDelay * time.Millisecond,
		maxConcurrent: DefaultMaxConcurrent,
		errorMapper:   datasource.NewErrorMapper(TushareErrorMappingRules, datasource.ErrCodeUnknown),
	}

	// Apply options
	for _, opt := range opts {
		opt(c)
	}

	// Initialize semaphore after options are applied
	c.sem = semaphore.NewWeighted(c.maxConcurrent)

	return c
}

// Name returns the data source name.
func (c *Client) Name() string {
	return "tushare"
}

// SetToken sets the API token.
func (c *Client) SetToken(token string) {
	c.token = token
}

// tushareRequest represents a Tushare API request.
type tushareRequest struct {
	APIName string                 `json:"api_name"`
	Token   string                 `json:"token"`
	Params  map[string]interface{} `json:"params,omitempty"`
	Fields  string                 `json:"fields,omitempty"`
}

// tushareResponse represents a Tushare API response.
type tushareResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Fields []string        `json:"fields"`
		Items  [][]interface{} `json:"items"`
		Count  int64           `json:"count,omitempty"`
	} `json:"data"`
}

// Query executes an API query with the given parameters.
// It uses semaphore to control concurrency and connection pool for efficient connection reuse.
func (c *Client) Query(ctx context.Context, apiName string, params map[string]interface{}) (*datasource.QueryResult, error) {
	if c.token == "" {
		return nil, datasource.NewDataSourceError(
			datasource.ErrCodeTokenInvalid,
			"token is not set",
			nil,
		)
	}

	// Acquire semaphore to control concurrency
	if err := c.sem.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("failed to acquire semaphore: %w", err)
	}
	defer c.sem.Release(1)

	// Build request
	reqBody := tushareRequest{
		APIName: apiName,
		Token:   c.token,
		Params:  params,
	}

	// Extract fields if specified in params
	if fields, ok := params["fields"]; ok {
		if fieldsStr, ok := fields.(string); ok {
			reqBody.Fields = fieldsStr
			delete(reqBody.Params, "fields")
		}
	}

	// Execute with retry
	var lastErr error
	for i := 0; i <= c.retryCount; i++ {
		result, err := c.doRequest(ctx, reqBody)
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Check if error is retryable
		if dsErr, ok := err.(*datasource.DataSourceError); ok {
			switch dsErr.Code {
			case datasource.ErrCodeTokenInvalid, datasource.ErrCodePermissionDeny,
				datasource.ErrCodeAPINotFound, datasource.ErrCodeDailyLimitExceeded:
				// Non-retryable errors
				return nil, err
			}
		}

		// Wait before retry
		if i < c.retryCount {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.retryDelay):
			}
		}
	}

	return nil, lastErr
}

// doRequest executes a single request.
func (c *Client) doRequest(ctx context.Context, reqBody tushareRequest) (*datasource.QueryResult, error) {
	// Marshal request body
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, datasource.NewDataSourceError(
			datasource.ErrCodeNetworkError,
			"network error",
			err,
		)
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Parse response
	var tushareResp tushareResponse
	if err := json.Unmarshal(respBody, &tushareResp); err != nil {
		return nil, datasource.NewDataSourceError(
			datasource.ErrCodeParseError,
			"failed to parse response",
			err,
		)
	}

	// Check for errors
	if tushareResp.Code != 0 {
		return nil, c.errorMapper.MapError(tushareResp.Code, tushareResp.Msg)
	}

	// Convert response to QueryResult
	result := &datasource.QueryResult{
		Data:    make([]map[string]interface{}, 0, len(tushareResp.Data.Items)),
		Total:   int64(len(tushareResp.Data.Items)),
		HasMore: false,
	}

	// Use count from response if available
	if tushareResp.Data.Count > 0 {
		result.Total = tushareResp.Data.Count
		result.HasMore = tushareResp.Data.Count > int64(len(tushareResp.Data.Items))
	}

	// Convert items to maps
	fields := tushareResp.Data.Fields
	for _, item := range tushareResp.Data.Items {
		row := make(map[string]interface{})
		for j, field := range fields {
			if j < len(item) {
				row[field] = item[j]
			}
		}
		result.Data = append(result.Data, row)
	}

	return result, nil
}

// ValidateToken validates if the token is valid by making a test API call.
func (c *Client) ValidateToken(ctx context.Context) (bool, error) {
	if c.token == "" {
		return false, nil
	}

	// Use a simple API to validate token (e.g., stock_basic with limit=1)
	_, err := c.Query(ctx, "stock_basic", map[string]interface{}{
		"limit": 1,
	})
	if err != nil {
		if dsErr, ok := err.(*datasource.DataSourceError); ok {
			if dsErr.Code == datasource.ErrCodeTokenInvalid {
				return false, nil
			}
		}
		return false, err
	}

	return true, nil
}

// MaxConcurrent returns the maximum concurrent requests setting.
func (c *Client) MaxConcurrent() int64 {
	return c.maxConcurrent
}

// Ensure Client implements datasource.APIClient
var _ datasource.APIClient = (*Client)(nil)
