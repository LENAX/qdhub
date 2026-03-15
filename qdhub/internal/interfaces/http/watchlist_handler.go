package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
)

// WatchlistHandler 用户股票收藏 HTTP 处理
type WatchlistHandler struct {
	watchlistSvc contracts.WatchlistApplicationService
	analysisSvc  contracts.AnalysisApplicationService // 可选，用于 GET 时填充 name
}

// NewWatchlistHandler 创建 WatchlistHandler
func NewWatchlistHandler(watchlistSvc contracts.WatchlistApplicationService, analysisSvc contracts.AnalysisApplicationService) *WatchlistHandler {
	return &WatchlistHandler{watchlistSvc: watchlistSvc, analysisSvc: analysisSvc}
}

// WatchlistItemResponse 收藏项（含名称，来自 stock_basic）
type WatchlistItemResponse struct {
	TsCode    string `json:"ts_code"`
	Name      string `json:"name,omitempty"`
	SortOrder int    `json:"sort_order"`
	CreatedAt string `json:"created_at"`
}

// RegisterRoutes 注册 /watchlist 路由（需在 protected 组下，JWT）
func (h *WatchlistHandler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/watchlist", h.GetWatchlist)
	rg.POST("/watchlist", h.AddToWatchlist)
	rg.DELETE("/watchlist/:ts_code", h.RemoveFromWatchlist)
}

// GetWatchlist GET /api/v1/watchlist
func (h *WatchlistHandler) GetWatchlist(c *gin.Context) {
	userIDVal, exists := c.Get(UserIDKey)
	if !exists {
		Error(c, http.StatusUnauthorized, 401, "user not authenticated")
		return
	}
	userID := shared.ID(userIDVal.(string))

	entries, err := h.watchlistSvc.GetWatchlist(c.Request.Context(), userID)
	if err != nil {
		HandleError(c, err)
		return
	}

	items := make([]WatchlistItemResponse, 0, len(entries))
	for _, e := range entries {
		item := WatchlistItemResponse{
			TsCode:    e.TsCode,
			SortOrder: e.SortOrder,
			CreatedAt: e.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		}
		if h.analysisSvc != nil {
			if basic, err := h.analysisSvc.GetStockBasicInfo(c.Request.Context(), e.TsCode); err == nil && basic != nil {
				item.Name = basic.Name
			}
		}
		items = append(items, item)
	}
	Success(c, gin.H{"items": items})
}

// AddToWatchlistRequest POST body
type AddToWatchlistRequest struct {
	TsCode string `json:"ts_code"`
}

// AddToWatchlist POST /api/v1/watchlist
func (h *WatchlistHandler) AddToWatchlist(c *gin.Context) {
	userIDVal, exists := c.Get(UserIDKey)
	if !exists {
		Error(c, http.StatusUnauthorized, 401, "user not authenticated")
		return
	}
	userID := shared.ID(userIDVal.(string))

	var req AddToWatchlistRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}
	tsCode := strings.TrimSpace(req.TsCode)
	if tsCode == "" {
		BadRequest(c, "ts_code required")
		return
	}

	if err := h.watchlistSvc.Add(c.Request.Context(), userID, tsCode); err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"message": "added", "ts_code": tsCode})
}

// RemoveFromWatchlist DELETE /api/v1/watchlist/:ts_code
func (h *WatchlistHandler) RemoveFromWatchlist(c *gin.Context) {
	userIDVal, exists := c.Get(UserIDKey)
	if !exists {
		Error(c, http.StatusUnauthorized, 401, "user not authenticated")
		return
	}
	userID := shared.ID(userIDVal.(string))

	tsCode := c.Param("ts_code")
	if tsCode == "" {
		BadRequest(c, "ts_code required")
		return
	}
	tsCode = strings.TrimSpace(tsCode)

	if err := h.watchlistSvc.Remove(c.Request.Context(), userID, tsCode); err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"message": "removed", "ts_code": tsCode})
}
