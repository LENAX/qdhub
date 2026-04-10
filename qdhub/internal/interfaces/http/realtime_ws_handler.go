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
	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/realtimestore"
)

type RealtimeWSHandler struct {
	store        *realtimestore.LatestQuoteStore
	watchlistSvc contracts.WatchlistApplicationService // 可选：为空时不做收藏联动
	selector     *realtimestore.RealtimeSourceSelector // 可选：多源时返回 current_source/sources_health/sources_error
	upgrader     websocket.Upgrader
}

// NewRealtimeWSHandler store 为 nil 时使用 DefaultLatestQuoteStore；watchlistSvc 为 nil 时未订阅或全市场仍推全市场；selector 为 nil 时仅返回 current_source=sina、sources_health 仅 sina、sources_error 空。
func NewRealtimeWSHandler(store *realtimestore.LatestQuoteStore, watchlistSvc contracts.WatchlistApplicationService, selector *realtimestore.RealtimeSourceSelector) *RealtimeWSHandler {
	if store == nil {
		store = realtimestore.DefaultLatestQuoteStore()
	}
	return &RealtimeWSHandler{
		store:        store,
		watchlistSvc: watchlistSvc,
		selector:     selector,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (h *RealtimeWSHandler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/ws/realtime-quotes", h.HandleQuotesWS)
}

type fullSnapshotPayload struct {
	Type          string            `json:"type"`
	Scope         string            `json:"scope"`
	Timestamp     int64             `json:"timestamp"`
	Items         json.RawMessage   `json:"items"`
	CurrentSource string            `json:"current_source"`
	SourcesHealth map[string]string `json:"sources_health"`
	SourcesError  map[string]string `json:"sources_error"`
}

type subsetSnapshotPayload struct {
	Type          string                            `json:"type"`
	Scope         string                            `json:"scope"`
	Timestamp     int64                             `json:"timestamp"`
	TSCodes       []string                          `json:"ts_codes,omitempty"`
	Items         map[string]realtimestore.Quote    `json:"items"`
	CurrentSource string                            `json:"current_source"`
	SourcesHealth map[string]string                 `json:"sources_health"`
	SourcesError  map[string]string                 `json:"sources_error"`
}

// sourceAwareness 返回 current_source、sources_health、sources_error，供前端有限感知多源与故障原因。
func (h *RealtimeWSHandler) sourceAwareness() (string, map[string]string, map[string]string) {
	if h.selector == nil {
		return realtimestore.SourceSina,
			map[string]string{realtimestore.SourceSina: realtimestore.HealthHealthy},
			map[string]string{realtimestore.SourceSina: ""}
	}
	return h.selector.CurrentSource(), h.selector.SourcesHealth(), h.selector.SourcesError()
}

type realtimeSubscribeReq struct {
	Action  string   `json:"action"`
	TSCodes []string `json:"ts_codes"`
}

func (h *RealtimeWSHandler) loadDefaultWatchlistCodes(userID string) []string {
	if h.watchlistSvc == nil || strings.TrimSpace(userID) == "" {
		return nil
	}
	entries, err := h.watchlistSvc.GetWatchlist(context.Background(), shared.ID(userID))
	if err != nil || len(entries) == 0 {
		return nil
	}
	codes := make([]string, 0, len(entries))
	for _, e := range entries {
		code := strings.TrimSpace(e.TsCode)
		if code == "" {
			continue
		}
		codes = append(codes, code)
	}
	return codes
}

func resolveRealtimePushScope(subs map[string]struct{}, defaultWatchlistCodes []string) ([]string, bool) {
	current := make([]string, 0, len(subs))
	full := false
	for k := range subs {
		current = append(current, k)
		if strings.EqualFold(k, "full") {
			full = true
		}
	}
	if full {
		if len(defaultWatchlistCodes) > 0 {
			return append([]string(nil), defaultWatchlistCodes...), false
		}
		return current, true
	}
	return current, false
}

func (h *RealtimeWSHandler) buildFullSnapshot() ([]byte, error) {
	itemsJSON, _, err := h.store.BuildFullItemsJSON()
	if err != nil {
		return nil, err
	}
	currentSource, sourcesHealth, sourcesError := h.sourceAwareness()
	payload := fullSnapshotPayload{
		Type:          "snapshot",
		Scope:         "full",
		Timestamp:     time.Now().UnixMilli(),
		Items:         json.RawMessage(itemsJSON),
		CurrentSource: currentSource,
		SourcesHealth: sourcesHealth,
		SourcesError:  sourcesError,
	}
	return json.Marshal(payload)
}

func (h *RealtimeWSHandler) buildSubsetSnapshot(pushCodes []string) ([]byte, error) {
	items := h.store.GetSubsetQuotes(pushCodes)
	if len(items) == 0 && len(pushCodes) > 0 {
		logrus.Debugf("[RealtimeWS] subset snapshot has no items for ts_codes=%v (store may not have received ticks yet)", pushCodes)
	}
	currentSource, sourcesHealth, sourcesError := h.sourceAwareness()
	payload := subsetSnapshotPayload{
		Type:          "snapshot",
		Scope:         "subset",
		Timestamp:     time.Now().UnixMilli(),
		TSCodes:       append([]string(nil), pushCodes...),
		Items:         items,
		CurrentSource: currentSource,
		SourcesHealth: sourcesHealth,
		SourcesError:  sourcesError,
	}
	return json.Marshal(payload)
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
	defaultWatchlistCodes := h.loadDefaultWatchlistCodes(userID)

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
			currentSubs := make(map[string]struct{}, len(subs))
			for k := range subs {
				currentSubs[k] = struct{}{}
			}
			subMu.RUnlock()

			pushCodes, pushFull := resolveRealtimePushScope(currentSubs, defaultWatchlistCodes)

			var b []byte
			if pushFull {
				b, err = h.buildFullSnapshot()
			} else {
				b, err = h.buildSubsetSnapshot(pushCodes)
			}
			if err != nil {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
				return
			}
		}
	}
}
