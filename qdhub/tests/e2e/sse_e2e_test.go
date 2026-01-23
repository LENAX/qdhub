//go:build e2e
// +build e2e

// Package e2e 提供针对 SSE 接口的端到端测试
package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/domain/shared"
)

// TestE2E_WorkflowInstance_SSEProgress 验证 /instances/:id/progress-stream SSE 接口
func TestE2E_WorkflowInstance_SSEProgress(t *testing.T) {
	ctx := setupE2EFullTestContext(t)
	defer ctx.cleanup()

	// 随机生成一个 workflow instance ID（在 mock 适配器中始终返回 Completed）
	instanceID := shared.NewID().String()

	resp, err := ctx.doRequest("GET", "/api/v1/instances/"+instanceID+"/progress-stream", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	body := string(bodyBytes)

	// 应该至少包含一条 data: 事件，并包含 instance_id 字段
	assert.Contains(t, body, "data: ")
	assert.Contains(t, body, "\"instance_id\"")
	assert.Contains(t, body, instanceID)

	// 状态应为 Completed 或 Success（取决于适配器实现）
	assert.True(t,
		strings.Contains(body, "\"status\":\"Completed\"") ||
			strings.Contains(body, "\"status\":\"Success\""),
		"SSE body should contain terminal status, got: %s", body,
	)
}

