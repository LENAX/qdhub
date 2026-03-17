package http_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"qdhub/internal/infrastructure/realtimestore"
	httpapi "qdhub/internal/interfaces/http"
)

func TestRealtimeWSHandler_SubscribeSubset(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := realtimestore.NewLatestQuoteStore()
	store.Update("000001.SZ", map[string]interface{}{"ts_code": "000001.SZ", "price": 10.11})
	store.Update("000002.SZ", map[string]interface{}{"ts_code": "000002.SZ", "price": 20.22})

	r := gin.New()
	h := httpapi.NewRealtimeWSHandler(store, nil, nil)
	h.RegisterRoutes(r.Group("/api/v1"))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/v1/ws/realtime-quotes"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws failed: %v", err)
	}
	defer conn.Close()

	// 先读一帧默认 full 快照，确保连接健康。
	_, _, err = conn.ReadMessage()
	if err != nil {
		t.Fatalf("read initial snapshot failed: %v", err)
	}

	err = conn.WriteJSON(map[string]interface{}{
		"action":   "subscribe",
		"ts_codes": []string{"000001.SZ"},
	})
	if err != nil {
		t.Fatalf("send subscribe failed: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 4; i++ {
		_, payload, rerr := conn.ReadMessage()
		if rerr != nil {
			t.Fatalf("read subscribed snapshot failed: %v", rerr)
		}
		var msg map[string]interface{}
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Fatalf("unmarshal snapshot failed: %v", err)
		}
		scope, _ := msg["scope"].(string)
		if scope != "subset" {
			continue
		}
		items, ok := msg["items"].(map[string]interface{})
		if !ok {
			t.Fatalf("items should be map, got %T", msg["items"])
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 subscribed item, got %d", len(items))
		}
		if _, exists := items["000001.SZ"]; !exists {
			t.Fatalf("expected subscribed code 000001.SZ in items")
		}
		return
	}
	t.Fatalf("did not receive subset snapshot after subscribe")
}
