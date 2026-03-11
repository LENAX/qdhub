package http

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"qdhub/internal/infrastructure/realtimestore"
)

type RealtimeWSHandler struct {
	store    *realtimestore.LatestQuoteStore
	upgrader websocket.Upgrader
}

func NewRealtimeWSHandler(store *realtimestore.LatestQuoteStore) *RealtimeWSHandler {
	if store == nil {
		store = realtimestore.DefaultLatestQuoteStore()
	}
	return &RealtimeWSHandler{
		store: store,
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

			var payload map[string]interface{}
			if full {
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
					"ts_codes":  current,
					"items":     h.store.GetBatch(current),
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
