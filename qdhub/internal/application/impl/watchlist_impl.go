package impl

import (
	"context"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/watchlist"
)

// WatchlistApplicationServiceImpl 实现 WatchlistApplicationService
type WatchlistApplicationServiceImpl struct {
	repo watchlist.Repository
}

// NewWatchlistApplicationService 创建收藏应用服务
func NewWatchlistApplicationService(repo watchlist.Repository) contracts.WatchlistApplicationService {
	return &WatchlistApplicationServiceImpl{repo: repo}
}

// GetWatchlist 返回当前用户的收藏列表（按 sort_order 升序）
func (s *WatchlistApplicationServiceImpl) GetWatchlist(ctx context.Context, userID shared.ID) ([]watchlist.WatchlistEntry, error) {
	return s.repo.GetByUserID(ctx, userID)
}

// Add 添加收藏
func (s *WatchlistApplicationServiceImpl) Add(ctx context.Context, userID shared.ID, tsCode string) error {
	return s.repo.Add(ctx, userID, tsCode, 0)
}

// Remove 取消收藏
func (s *WatchlistApplicationServiceImpl) Remove(ctx context.Context, userID shared.ID, tsCode string) error {
	return s.repo.Remove(ctx, userID, tsCode)
}
