package contracts

import (
	"context"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/watchlist"
)

// WatchlistApplicationService 用户股票收藏应用服务
type WatchlistApplicationService interface {
	GetWatchlist(ctx context.Context, userID shared.ID) ([]watchlist.WatchlistEntry, error)
	Add(ctx context.Context, userID shared.ID, tsCode string) error
	Remove(ctx context.Context, userID shared.ID, tsCode string) error
}
