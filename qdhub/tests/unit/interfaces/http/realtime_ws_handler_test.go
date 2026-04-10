package http_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/watchlist"
	"qdhub/internal/infrastructure/realtimestore"
	httpapi "qdhub/internal/interfaces/http"
)

type stubWatchlistService struct {
	calls   atomic.Int32
	entries []watchlist.WatchlistEntry
}

func (s *stubWatchlistService) GetWatchlist(_ context.Context, _ shared.ID) ([]watchlist.WatchlistEntry, error) {
	s.calls.Add(1)
	out := make([]watchlist.WatchlistEntry, len(s.entries))
	copy(out, s.entries)
	return out, nil
}

func (s *stubWatchlistService) Add(_ context.Context, _ shared.ID, _ string) error {
	return nil
}

func (s *stubWatchlistService) Remove(_ context.Context, _ shared.ID, _ string) error {
	return nil
}

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

func TestRealtimeWSHandler_DefaultWatchlistLoadedOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := realtimestore.NewLatestQuoteStore()
	store.Update("000001.SZ", map[string]interface{}{"ts_code": "000001.SZ", "price": 10.11})

	watchlistSvc := &stubWatchlistService{
		entries: []watchlist.WatchlistEntry{{TsCode: "000001.SZ"}},
	}

	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(httpapi.UserIDKey, "user-1")
		c.Next()
	})
	h := httpapi.NewRealtimeWSHandler(store, watchlistSvc, nil)
	h.RegisterRoutes(r.Group("/api/v1"))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/v1/ws/realtime-quotes"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws failed: %v", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 3; i++ {
		_, payload, rerr := conn.ReadMessage()
		if rerr != nil {
			t.Fatalf("read snapshot failed: %v", rerr)
		}
		var msg map[string]interface{}
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Fatalf("unmarshal snapshot failed: %v", err)
		}
		if scope, _ := msg["scope"].(string); scope != "subset" {
			t.Fatalf("expected default watchlist to downgrade full to subset, got %v", msg["scope"])
		}
	}

	if calls := watchlistSvc.calls.Load(); calls != 1 {
		t.Fatalf("expected watchlist loaded once per connection, got %d", calls)
	}
}
