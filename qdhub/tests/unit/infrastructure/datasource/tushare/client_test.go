package tushare_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"qdhub/internal/infrastructure/datasource"
	"qdhub/internal/infrastructure/datasource/tushare"
)

func TestClient_Name(t *testing.T) {
	client := tushare.NewClient()
	if client.Name() != "tushare" {
		t.Errorf("expected name to be 'tushare', got '%s'", client.Name())
	}
}

func TestClient_SetToken(t *testing.T) {
	client := tushare.NewClient()
	client.SetToken("test_token")
	// Token is set internally, verify through ValidateToken which requires a token
}

func TestClient_Query_NoToken(t *testing.T) {
	client := tushare.NewClient()
	ctx := context.Background()

	_, err := client.Query(ctx, "stock_basic", nil)
	if err == nil {
		t.Error("expected error when token is not set")
	}

	dsErr, ok := err.(*datasource.DataSourceError)
	if !ok {
		t.Errorf("expected DataSourceError, got %T", err)
	}
	if dsErr.Code != datasource.ErrCodeTokenInvalid {
		t.Errorf("expected error code %s, got %s", datasource.ErrCodeTokenInvalid, dsErr.Code)
	}
}

func TestClient_Query_Success(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method
		if r.Method != http.MethodPost {
			t.Errorf("expected POST method, got %s", r.Method)
		}

		// Verify content type
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
		}

		// Return mock response
		response := map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{
				"fields": []string{"ts_code", "symbol", "name"},
				"items": [][]interface{}{
					{"000001.SZ", "000001", "平安银行"},
					{"000002.SZ", "000002", "万科A"},
				},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	// Create client with mock server URL
	client := tushare.NewClient(
		tushare.WithBaseURL(server.URL),
		tushare.WithToken("test_token"),
	)

	// Execute query
	ctx := context.Background()
	result, err := client.Query(ctx, "stock_basic", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify result
	if len(result.Data) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Data))
	}

	if result.Data[0]["ts_code"] != "000001.SZ" {
		t.Errorf("expected ts_code '000001.SZ', got %v", result.Data[0]["ts_code"])
	}
}

func TestClient_Query_APIError(t *testing.T) {
	// Create mock server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"code": -2001,
			"msg":  "token无效",
			"data": nil,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := tushare.NewClient(
		tushare.WithBaseURL(server.URL),
		tushare.WithToken("invalid_token"),
		tushare.WithRetryCount(0), // Disable retry for test
	)

	ctx := context.Background()
	_, err := client.Query(ctx, "stock_basic", nil)
	if err == nil {
		t.Error("expected error for invalid token")
	}

	dsErr, ok := err.(*datasource.DataSourceError)
	if !ok {
		t.Errorf("expected DataSourceError, got %T", err)
	}
	if dsErr.Code != datasource.ErrCodeTokenInvalid {
		t.Errorf("expected error code %s, got %s", datasource.ErrCodeTokenInvalid, dsErr.Code)
	}
}

func TestClient_Query_RateLimited_MinuteLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"code": -2003,
			"msg":  "每分钟访问频率过快，请稍后再试", // 使用不含"上限"的消息，避免匹配 DAILY_LIMIT
			"data": nil,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := tushare.NewClient(
		tushare.WithBaseURL(server.URL),
		tushare.WithToken("test_token"),
		tushare.WithRetryCount(0),
	)

	ctx := context.Background()
	_, err := client.Query(ctx, "daily", nil)
	if err == nil {
		t.Error("expected error for minute rate limit")
	}

	dsErr, ok := err.(*datasource.DataSourceError)
	if !ok {
		t.Errorf("expected DataSourceError, got %T", err)
	}
	if dsErr.Code != datasource.ErrCodeRateLimited {
		t.Errorf("expected error code %s, got %s", datasource.ErrCodeRateLimited, dsErr.Code)
	}
}

func TestClient_Query_DailyLimitExceeded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"code": -2003,
			"msg":  "每日请求已达到上限",
			"data": nil,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := tushare.NewClient(
		tushare.WithBaseURL(server.URL),
		tushare.WithToken("test_token"),
		tushare.WithRetryCount(0),
	)

	ctx := context.Background()
	_, err := client.Query(ctx, "daily", nil)
	if err == nil {
		t.Error("expected error for daily limit")
	}

	dsErr, ok := err.(*datasource.DataSourceError)
	if !ok {
		t.Errorf("expected DataSourceError, got %T", err)
	}
	if dsErr.Code != datasource.ErrCodeDailyLimitExceeded {
		t.Errorf("expected error code %s, got %s", datasource.ErrCodeDailyLimitExceeded, dsErr.Code)
	}
}

func TestClient_Query_PermissionDenied(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"code": -2004,
			"msg":  "权限不足，请联系管理员开通",
			"data": nil,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := tushare.NewClient(
		tushare.WithBaseURL(server.URL),
		tushare.WithToken("test_token"),
		tushare.WithRetryCount(0),
	)

	ctx := context.Background()
	_, err := client.Query(ctx, "fund_nav", nil)
	if err == nil {
		t.Error("expected error for permission denied")
	}

	dsErr, ok := err.(*datasource.DataSourceError)
	if !ok {
		t.Errorf("expected DataSourceError, got %T", err)
	}
	if dsErr.Code != datasource.ErrCodePermissionDeny {
		t.Errorf("expected error code %s, got %s", datasource.ErrCodePermissionDeny, dsErr.Code)
	}
}

func TestClient_ValidateToken_Valid(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{
				"fields": []string{"ts_code"},
				"items":  [][]interface{}{{"000001.SZ"}},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := tushare.NewClient(
		tushare.WithBaseURL(server.URL),
		tushare.WithToken("valid_token"),
	)

	ctx := context.Background()
	valid, err := client.ValidateToken(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Error("expected token to be valid")
	}
}

func TestClient_ValidateToken_Invalid(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"code": -2001,
			"msg":  "token无效",
			"data": nil,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := tushare.NewClient(
		tushare.WithBaseURL(server.URL),
		tushare.WithToken("invalid_token"),
		tushare.WithRetryCount(0),
	)

	ctx := context.Background()
	valid, err := client.ValidateToken(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Error("expected token to be invalid")
	}
}

func TestClient_ValidateToken_NoToken(t *testing.T) {
	client := tushare.NewClient()

	ctx := context.Background()
	valid, err := client.ValidateToken(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Error("expected token to be invalid when not set")
	}
}

func TestClient_MaxConcurrent(t *testing.T) {
	client := tushare.NewClient(tushare.WithMaxConcurrent(10))
	if client.MaxConcurrent() != 10 {
		t.Errorf("expected MaxConcurrent to be 10, got %d", client.MaxConcurrent())
	}

	// Default value (DefaultMaxConcurrent = 100)
	defaultClient := tushare.NewClient()
	if defaultClient.MaxConcurrent() != tushare.DefaultMaxConcurrent {
		t.Errorf("expected default MaxConcurrent to be %d, got %d", tushare.DefaultMaxConcurrent, defaultClient.MaxConcurrent())
	}
}

func TestClient_ConcurrencyControl(t *testing.T) {
	maxConcurrent := 3
	var currentConcurrent int32
	var maxObserved int32

	// Create mock server that tracks concurrent requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Increment concurrent counter
		current := atomic.AddInt32(&currentConcurrent, 1)
		defer atomic.AddInt32(&currentConcurrent, -1)

		// Track max observed concurrent requests
		for {
			old := atomic.LoadInt32(&maxObserved)
			if current <= old || atomic.CompareAndSwapInt32(&maxObserved, old, current) {
				break
			}
		}

		// Simulate some work
		time.Sleep(50 * time.Millisecond)

		// Return mock response
		response := map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{
				"fields": []string{"ts_code"},
				"items":  [][]interface{}{{"000001.SZ"}},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := tushare.NewClient(
		tushare.WithBaseURL(server.URL),
		tushare.WithToken("test_token"),
		tushare.WithMaxConcurrent(maxConcurrent),
		tushare.WithRetryCount(0),
	)

	// Launch more requests than maxConcurrent
	numRequests := 10
	var wg sync.WaitGroup
	wg.Add(numRequests)

	ctx := context.Background()
	for i := 0; i < numRequests; i++ {
		go func() {
			defer wg.Done()
			_, err := client.Query(ctx, "stock_basic", nil)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	wg.Wait()

	// Verify that concurrent requests never exceeded maxConcurrent
	if int(maxObserved) > maxConcurrent {
		t.Errorf("max observed concurrent requests (%d) exceeded limit (%d)", maxObserved, maxConcurrent)
	}
	t.Logf("Max observed concurrent requests: %d (limit: %d)", maxObserved, maxConcurrent)
}

func TestClient_ConcurrencyControl_ContextCancellation(t *testing.T) {
	// Create a client with very low concurrency
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate slow request
		time.Sleep(500 * time.Millisecond)
		response := map[string]interface{}{
			"code": 0,
			"msg":  "success",
			"data": map[string]interface{}{
				"fields": []string{"ts_code"},
				"items":  [][]interface{}{{"000001.SZ"}},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := tushare.NewClient(
		tushare.WithBaseURL(server.URL),
		tushare.WithToken("test_token"),
		tushare.WithMaxConcurrent(1), // Only allow 1 concurrent request
		tushare.WithRetryCount(0),
	)

	// Start a long-running request
	ctx1 := context.Background()
	go func() {
		client.Query(ctx1, "stock_basic", nil)
	}()

	// Give the first request time to start
	time.Sleep(50 * time.Millisecond)

	// Try to make another request with a short timeout
	ctx2, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := client.Query(ctx2, "stock_basic", nil)
	if err == nil {
		t.Error("expected error due to context timeout while waiting for semaphore")
	}
}

func TestClient_ConnectionPoolOptions(t *testing.T) {
	// Test that connection pool options can be set without error
	client := tushare.NewClient(
		tushare.WithMaxIdleConns(20),
		tushare.WithMaxIdleConnsPerHost(10),
		tushare.WithIdleConnTimeout(120*time.Second),
	)

	if client == nil {
		t.Error("expected client to be created successfully")
	}
}
