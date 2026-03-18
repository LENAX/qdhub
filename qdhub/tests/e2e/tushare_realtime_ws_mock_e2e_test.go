//go:build e2e
// +build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	httpapi "qdhub/internal/interfaces/http"
	"qdhub/internal/infrastructure/realtimestore"
	"qdhub/internal/infrastructure/taskengine/jobs"
)

// TestE2E_TushareRealtimeWSMockFlow 验证 mock 数据在内存缓存与 WS 推送链路可达。
func TestE2E_TushareRealtimeWSMockFlow(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := httpapi.NewRealtimeWSHandler(realtimestore.DefaultLatestQuoteStore(), nil, nil)
	code := fmt.Sprintf("E2E_%d.SZ", time.Now().UnixNano())

	r := gin.New()
	handler.RegisterRoutes(r.Group("/api/v1"))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/v1/ws/realtime-quotes"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer conn.Close()

	// 首次写入模拟行情（通过真实 Job，避免 mock 对象）。
	tc := &task.TaskContext{
		TaskID:             "e2e-task-1",
		TaskName:           "TushareTickFrontendPush",
		WorkflowInstanceID: "e2e-wf-1",
		Params: map[string]interface{}{
			"data": []map[string]interface{}{
				{"ts_code": code, "price": 10.01},
			},
		},
	}
	_, err = jobs.TushareTickFrontendPushJob(tc)
	require.NoError(t, err)

	// 订阅单个 code。
	err = conn.WriteJSON(map[string]interface{}{
		"action":   "subscribe",
		"ts_codes": []string{code},
	})
	require.NoError(t, err)

	// 第二次写入更新价格，检查 WS 能收到更新快照。
	tc.Params["data"] = []map[string]interface{}{
		{"ts_code": code, "price": 10.88},
	}
	_, err = jobs.TushareTickFrontendPushJob(tc)
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for i := 0; i < 8; i++ {
		_, payload, rerr := conn.ReadMessage()
		require.NoError(t, rerr)

		var msg map[string]interface{}
		require.NoError(t, json.Unmarshal(payload, &msg))
		scope, _ := msg["scope"].(string)
		if scope != "subset" {
			continue
		}
		items, ok := msg["items"].(map[string]interface{})
		require.True(t, ok)
		itemRaw, exists := items[code]
		require.True(t, exists)
		item, ok := itemRaw.(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, 10.88, item["price"])
		return
	}
	t.Fatalf("did not receive updated subset snapshot")
}

