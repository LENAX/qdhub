package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/realtimestore"
)

type RealtimeWSHandler struct {
	store        *realtimestore.LatestQuoteStore
	watchlistSvc contracts.WatchlistApplicationService // 可选：为空时不做收藏联动
	upgrader     websocket.Upgrader
}

// NewRealtimeWSHandler store 为 nil 时使用 DefaultLatestQuoteStore；watchlistSvc 为 nil 时未订阅或全市场仍推全市场
func NewRealtimeWSHandler(store *realtimestore.LatestQuoteStore, watchlistSvc contracts.WatchlistApplicationService) *RealtimeWSHandler {
	if store == nil {
		store = realtimestore.DefaultLatestQuoteStore()
	}
	return &RealtimeWSHandler{
		store:        store,
		watchlistSvc: watchlistSvc,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (h *RealtimeWSHandler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/ws/realtime-quotes", h.HandleQuotesWS)
}

type realtimeSubscribeReq struct {
	Action  string   `json:"action"`
	TSCodes []string `json:"ts_codes"`
}

func (h *RealtimeWSHandler) HandleQuotesWS(c *gin.Context) {
	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// 连接时取当前用户 ID，用于「未订阅或全市场」时用收藏列表作为默认推送
	var userID string
	if v, ok := c.Get(UserIDKey); ok && v != nil {
		userID, _ = v.(string)
	}

	var subMu sync.RWMutex
	subs := map[string]struct{}{"full": {}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var req realtimeSubscribeReq
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if strings.ToLower(strings.TrimSpace(req.Action)) != "subscribe" {
				continue
			}
			next := make(map[string]struct{})
			for _, x := range req.TSCodes {
				s := strings.TrimSpace(x)
				if s == "" {
					continue
				}
				next[s] = struct{}{}
			}
			if len(next) == 0 {
				next["full"] = struct{}{}
			}
			subMu.Lock()
			subs = next
			subMu.Unlock()
		}
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			subMu.RLock()
			current := make([]string, 0, len(subs))
			full := false
			for k := range subs {
				current = append(current, k)
				if strings.EqualFold(k, "full") {
					full = true
				}
			}
			subMu.RUnlock()

			// 当为「全市场」且已登录且有收藏时，改为按收藏列表推送（subset）
			pushCodes := current
			pushFull := full
			if full && h.watchlistSvc != nil && userID != "" {
				entries, err := h.watchlistSvc.GetWatchlist(context.Background(), shared.ID(userID))
				if err == nil && len(entries) > 0 {
					codes := make([]string, 0, len(entries))
					for _, e := range entries {
						codes = append(codes, e.TsCode)
					}
					pushFull = false
					pushCodes = codes
				}
			}

			var payload map[string]interface{}
			if pushFull {
				payload = map[string]interface{}{
					"type":      "snapshot",
					"scope":     "full",
					"timestamp": time.Now().UnixMilli(),
					"items":     h.store.GetAll(),
				}
			} else {
				payload = map[string]interface{}{
					"type":      "snapshot",
					"scope":     "subset",
					"timestamp": time.Now().UnixMilli(),
					"ts_codes":  pushCodes,
					"items":     h.store.GetBatch(pushCodes),
				}
			}
			b, err := json.Marshal(payload)
			if err != nil {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
				return
			}
		}
	}
}
