package watchlist

import "time"

// WatchlistEntry 用户收藏的一条股票记录
type WatchlistEntry struct {
	UserID    string    `json:"-"`
	TsCode    string    `json:"ts_code"`
	SortOrder int       `json:"sort_order"`
	CreatedAt time.Time `json:"created_at"`
}
