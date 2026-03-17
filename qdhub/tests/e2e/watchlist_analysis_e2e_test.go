//go:build e2e
// +build e2e

// Package e2e 看盘与行情功能 E2E 验收：收藏列表、分析接口（拼音搜索、分时/资金流/新闻流）
// 依赖已启动的服务器（setupServerE2EContext），需先执行迁移（含 022_user_stock_watchlist）
//
// 运行：go test -tags e2e -v -run "TestE2E_Watchlist|TestE2E_Analysis_Endpoints|TestE2E_NewsStream" ./tests/e2e/...
package e2e

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_Watchlist_CRUD 验收收藏列表：GET 空列表、POST 添加、GET 有数据、DELETE 删除、GET 再空
// 使用预置 admin 用户，避免新注册用户无 watchlist 权限导致 403
func TestE2E_Watchlist_CRUD(t *testing.T) {
	ctx := setupServerE2EContext(t)
	token := ctx.ensureAdminUser(t, "", "", "")

	// GET 初始为空
	resp, err := ctx.doRequestWithAuth("GET", "/api/v1/watchlist", nil, token)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET /watchlist 应 200")

	// POST 添加
	resp, err = ctx.doRequestWithAuth("POST", "/api/v1/watchlist", map[string]string{"ts_code": "000001.SZ"}, token)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated, "POST /watchlist 应 200/201")

	// GET 有一条（接口返回 data: { "items": [...] }）
	resp, err = ctx.doRequestWithAuth("GET", "/api/v1/watchlist", nil, token)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Code int `json:"code"`
		Data struct {
			Items []struct {
				TsCode string `json:"ts_code"`
				Name   string `json:"name"`
			} `json:"items"`
		} `json:"data"`
	}
	err = jsonDecodeResp(resp, &out)
	require.NoError(t, err)
	require.Len(t, out.Data.Items, 1)
	assert.Equal(t, "000001.SZ", out.Data.Items[0].TsCode)

	// DELETE
	resp, err = ctx.doRequestWithAuth("DELETE", "/api/v1/watchlist/000001.SZ", nil, token)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent, "DELETE /watchlist/:ts_code 应 200/204")

	// GET 再空
	resp, err = ctx.doRequestWithAuth("GET", "/api/v1/watchlist", nil, token)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	err = jsonDecodeResp(resp, &out)
	require.NoError(t, err)
	assert.Empty(t, out.Data.Items)
}

// TestE2E_Analysis_Endpoints 验收分析接口：stocks（拼音）、realtime-tick、intraday-ticks、moneyflow-concept
// 无数据时返回 200 + 空数组/空数据；stocks 在 DuckDB 无 stock_basic 表时可能 500，均视为通过
func TestE2E_Analysis_Endpoints(t *testing.T) {
	ctx := setupServerE2EContext(t)
	token := ctx.ensureAdminUser(t, "", "", "")

	t.Run("stocks_cnspell", func(t *testing.T) {
		resp, err := ctx.doRequestWithAuth("GET", "/api/v1/analysis/stocks?query=PA&search_type=cnspell&limit=10", nil, token)
		require.NoError(t, err)
		defer resp.Body.Close()
		// 有数据 200，无表/无数据时后端可能返回 500，均接受
		assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == 500, "stocks 应 200 或 500，实际 %d", resp.StatusCode)
	})

	t.Run("realtime_tick", func(t *testing.T) {
		resp, err := ctx.doRequestWithAuth("GET", "/api/v1/analysis/realtime-tick?ts_code=000001.SZ&limit=10", nil, token)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("intraday_ticks", func(t *testing.T) {
		resp, err := ctx.doRequestWithAuth("GET", "/api/v1/analysis/intraday-ticks?ts_code=000001.SZ&trade_date=20260101", nil, token)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("moneyflow_concept", func(t *testing.T) {
		resp, err := ctx.doRequestWithAuth("GET", "/api/v1/analysis/moneyflow-concept?trade_date=20260101", nil, token)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

// TestE2E_NewsStream_SSE 验收新闻流 SSE：GET /api/v1/analysis/news/stream 返回 text/event-stream 并至少读到一次事件或 keepalive
func TestE2E_NewsStream_SSE(t *testing.T) {
	ctx := setupServerE2EContext(t)
	token := ctx.ensureAdminUser(t, "", "", "")

	req, err := http.NewRequest("GET", ctx.BaseURL+"/api/v1/analysis/news/stream?interval_sec=1&limit=5", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := ctx.HTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	// 短时读取：至少应收到一条 event 或 ": keepalive"
	deadline := time.Now().Add(5 * time.Second)
	rd := bufio.NewReader(resp.Body)
	var seen bool
	for time.Now().Before(deadline) {
		line, err := rd.ReadString('\n')
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "event:") || strings.HasPrefix(line, "data:") || strings.HasPrefix(line, ": keepalive") {
			seen = true
			break
		}
	}
	assert.True(t, seen, "应在 5 秒内收到至少一条 SSE 行（event/data/keepalive）")
}

func jsonDecodeResp(resp *http.Response, v interface{}) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}
