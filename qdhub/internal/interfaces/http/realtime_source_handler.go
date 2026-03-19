package http

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/realtimestore"
)

// RealtimeSourceHealthSnapshot is the payload for GET /realtime-sources/health and SSE heartbeat.
type RealtimeSourceHealthSnapshot struct {
	Sources         []RealtimeSourceHealthItem `json:"sources"`
	CurrentSource    string                    `json:"current_source"`
	SourcesHealth    map[string]string         `json:"sources_health"`
	SourcesError     map[string]string         `json:"sources_error"`
}

// RealtimeSourceHealthItem is one item in the health snapshot sources list.
type RealtimeSourceHealthItem struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Type             string    `json:"type"`
	Running          bool      `json:"running"`                     // 该源是否在运行（已连接/同步中），前端统一据此判断是否连接
	Connected        bool      `json:"connected"`                   // 与 running 同义，保留兼容
	LastHealthStatus string    `json:"last_health_status,omitempty"`
	LastHealthAt     time.Time `json:"last_health_at,omitempty"`
	LastHealthError  string    `json:"last_health_error,omitempty"`
}

// RealtimeSourceConnector can connect/disconnect a realtime source by ID (optional; may be nil).
type RealtimeSourceConnector interface {
	Connect(id string) error
	Disconnect(id string) error
	// IsConnected 返回该源是否已连接（同步运行中），供前端展示连接/断开按钮状态。
	IsConnected(id string) bool
}

// RealtimeSourceHandler handles realtime data source management HTTP API.
type RealtimeSourceHandler struct {
	svc      contracts.RealtimeSourceApplicationService
	selector *realtimestore.RealtimeSourceSelector
	connector RealtimeSourceConnector
}

// NewRealtimeSourceHandler creates a new RealtimeSourceHandler.
func NewRealtimeSourceHandler(svc contracts.RealtimeSourceApplicationService, selector *realtimestore.RealtimeSourceSelector, connector RealtimeSourceConnector) *RealtimeSourceHandler {
	return &RealtimeSourceHandler{svc: svc, selector: selector, connector: connector}
}

// RegisterRoutes registers realtime-sources routes. Register /health and /heartbeat before /:id.
func (h *RealtimeSourceHandler) RegisterRoutes(rg *gin.RouterGroup) {
	realtime := rg.Group("/realtime-sources")
	{
		realtime.GET("/health", h.Health)
		realtime.GET("/heartbeat", h.Heartbeat)
		realtime.GET("", h.List)
		realtime.POST("", h.Create)
		realtime.GET("/:id", h.Get)
		realtime.PUT("/:id", h.Update)
		realtime.DELETE("/:id", h.Delete)
		realtime.GET("/:id/health", h.SingleHealth)
		realtime.POST("/:id/connect", h.Connect)
		realtime.POST("/:id/disconnect", h.Disconnect)
	}
}

// List handles GET /api/v1/realtime-sources.
func (h *RealtimeSourceHandler) List(c *gin.Context) {
	list, err := h.svc.List(c.Request.Context())
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, list)
}

// Get handles GET /api/v1/realtime-sources/:id.
func (h *RealtimeSourceHandler) Get(c *gin.Context) {
	id := shared.ID(c.Param("id"))
	src, err := h.svc.Get(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, src)
}

// Create handles POST /api/v1/realtime-sources.
func (h *RealtimeSourceHandler) Create(c *gin.Context) {
	var req struct {
		Name                  string `json:"name"`
		Type                  string `json:"type"`
		Config                string `json:"config"`
		Priority              int    `json:"priority"`
		IsPrimary             bool   `json:"is_primary"`
		HealthCheckOnStartup  bool   `json:"health_check_on_startup"`
		Enabled               bool   `json:"enabled"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}
	src, err := h.svc.Create(c.Request.Context(), contracts.CreateRealtimeSourceRequest{
		Name:                 req.Name,
		Type:                 req.Type,
		Config:               req.Config,
		Priority:             req.Priority,
		IsPrimary:            req.IsPrimary,
		HealthCheckOnStartup: req.HealthCheckOnStartup,
		Enabled:              req.Enabled,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, src)
}

// Update handles PUT /api/v1/realtime-sources/:id.
func (h *RealtimeSourceHandler) Update(c *gin.Context) {
	id := shared.ID(c.Param("id"))
	var req struct {
		Name                  *string `json:"name"`
		Config                *string `json:"config"`
		Priority              *int    `json:"priority"`
		IsPrimary             *bool   `json:"is_primary"`
		HealthCheckOnStartup  *bool   `json:"health_check_on_startup"`
		Enabled               *bool   `json:"enabled"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}
	src, err := h.svc.Update(c.Request.Context(), id, contracts.UpdateRealtimeSourceRequest{
		Name:                 req.Name,
		Config:               req.Config,
		Priority:             req.Priority,
		IsPrimary:            req.IsPrimary,
		HealthCheckOnStartup: req.HealthCheckOnStartup,
		Enabled:              req.Enabled,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, src)
}

// Delete handles DELETE /api/v1/realtime-sources/:id.
func (h *RealtimeSourceHandler) Delete(c *gin.Context) {
	id := shared.ID(c.Param("id"))
	if err := h.svc.Delete(c.Request.Context(), id); err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
}

// Health handles GET /api/v1/realtime-sources/health (one-shot JSON).
func (h *RealtimeSourceHandler) Health(c *gin.Context) {
	snapshot, err := h.buildHealthSnapshot(c.Request.Context())
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, snapshot)
}

// Heartbeat handles GET /api/v1/realtime-sources/heartbeat (SSE).
func (h *RealtimeSourceHandler) Heartbeat(c *gin.Context) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	interval := 5 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-ticker.C:
			snapshot, err := h.buildHealthSnapshot(c.Request.Context())
			if err != nil {
				return
			}
			data, _ := json.Marshal(snapshot)
			c.SSEvent("message", string(data))
			c.Writer.Flush()
		}
	}
}

// SingleHealth handles GET /api/v1/realtime-sources/:id/health (trigger one-off check and return result).
func (h *RealtimeSourceHandler) SingleHealth(c *gin.Context) {
	id := shared.ID(c.Param("id"))
	status, errMsg, err := h.svc.TriggerHealthCheck(c.Request.Context(), id)
	if err != nil {
		if err.Error() == "health check not configured" {
			Error(c, http.StatusServiceUnavailable, 503, "health check not configured")
			return
		}
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": status, "error": errMsg})
}

// Connect handles POST /api/v1/realtime-sources/:id/connect.
// 同步执行，启动完成后返回 200；前端可再请求 GET /health 获取 connected 状态并切换为「断开」按钮。
func (h *RealtimeSourceHandler) Connect(c *gin.Context) {
	id := c.Param("id")
	if h.connector == nil {
		Success(c, gin.H{"message": "connect request accepted", "note": "connector not configured"})
		return
	}
	if err := h.connector.Connect(id); err != nil {
		logrus.Warnf("[RealtimeSourceHandler] connect %s: %v", id, err)
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"message": "connect request accepted", "connected": h.connector.IsConnected(id)})
}

// Disconnect handles POST /api/v1/realtime-sources/:id/disconnect.
// 同步执行，断开完成后返回 200；前端可再请求 GET /health 确认 connected 为 false。
func (h *RealtimeSourceHandler) Disconnect(c *gin.Context) {
	id := c.Param("id")
	if h.connector == nil {
		Success(c, gin.H{"message": "disconnect request accepted", "note": "connector not configured"})
		return
	}
	if err := h.connector.Disconnect(id); err != nil {
		logrus.Warnf("[RealtimeSourceHandler] disconnect %s: %v", id, err)
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"message": "disconnect request accepted", "connected": false})
}

func (h *RealtimeSourceHandler) buildHealthSnapshot(ctx context.Context) (*RealtimeSourceHealthSnapshot, error) {
	list, err := h.svc.List(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]RealtimeSourceHealthItem, 0, len(list))
	for _, s := range list {
		running := false
		if h.connector != nil {
			running = h.connector.IsConnected(s.ID.String())
		}
		items = append(items, RealtimeSourceHealthItem{
			ID:               s.ID.String(),
			Name:             s.Name,
			Type:             s.Type,
			Running:          running,
			Connected:        running,
			LastHealthStatus: s.LastHealthStatus,
			LastHealthAt:     s.LastHealthAt.ToTime(),
			LastHealthError:  s.LastHealthError,
		})
	}
	current := ""
	sourcesHealth := make(map[string]string)
	sourcesError := make(map[string]string)
	if h.selector != nil {
		current = h.selector.CurrentSource()
		sourcesHealth = h.selector.SourcesHealth()
		sourcesError = h.selector.SourcesError()
	}
	return &RealtimeSourceHealthSnapshot{
		Sources:        items,
		CurrentSource:   current,
		SourcesHealth:   sourcesHealth,
		SourcesError:    sourcesError,
	}, nil
}
