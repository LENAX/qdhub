package watchlist

import (
	"context"

	"qdhub/internal/domain/shared"
)

// Repository 用户股票收藏表读写接口
type Repository interface {
	// GetByUserID 按用户 ID 返回收藏列表（按 sort_order 升序）
	GetByUserID(ctx context.Context, userID shared.ID) ([]WatchlistEntry, error)
	// Add 添加收藏，已存在则更新 sort_order
	Add(ctx context.Context, userID shared.ID, tsCode string, sortOrder int) error
	// Remove 取消收藏
	Remove(ctx context.Context, userID shared.ID, tsCode string) error
}
