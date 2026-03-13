package repository

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/watchlist"
	"qdhub/internal/infrastructure/persistence"
)

// WatchlistRepositoryImpl 实现 watchlist.Repository，使用主库 SQLite（与 auth 同库）
type WatchlistRepositoryImpl struct {
	db *persistence.DB
}

// NewWatchlistRepository 创建收藏表仓储
func NewWatchlistRepository(db *persistence.DB) *WatchlistRepositoryImpl {
	return &WatchlistRepositoryImpl{db: db}
}

// GetByUserID 按用户 ID 返回收藏列表，按 sort_order 升序
func (r *WatchlistRepositoryImpl) GetByUserID(ctx context.Context, userID shared.ID) ([]watchlist.WatchlistEntry, error) {
	query := `SELECT user_id, ts_code, sort_order, created_at FROM user_stock_watchlist WHERE user_id = ? ORDER BY sort_order ASC, created_at ASC`
	var rows []struct {
		UserID    string         `db:"user_id"`
		TsCode    string         `db:"ts_code"`
		SortOrder int            `db:"sort_order"`
		CreatedAt sql.NullTime   `db:"created_at"`
	}
	if err := sqlx.SelectContext(ctx, r.db, &rows, query, userID.String()); err != nil {
		return nil, err
	}
	out := make([]watchlist.WatchlistEntry, 0, len(rows))
	for _, row := range rows {
		ent := watchlist.WatchlistEntry{UserID: row.UserID, TsCode: row.TsCode, SortOrder: row.SortOrder}
		if row.CreatedAt.Valid {
			ent.CreatedAt = row.CreatedAt.Time
		}
		out = append(out, ent)
	}
	return out, nil
}

// Add 添加收藏；已存在则更新 sort_order
func (r *WatchlistRepositoryImpl) Add(ctx context.Context, userID shared.ID, tsCode string, sortOrder int) error {
	query := `INSERT INTO user_stock_watchlist (user_id, ts_code, sort_order, created_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(user_id, ts_code) DO UPDATE SET sort_order = excluded.sort_order`
	_, err := r.db.ExecContext(ctx, query, userID.String(), tsCode, sortOrder)
	return err
}

// Remove 取消收藏
func (r *WatchlistRepositoryImpl) Remove(ctx context.Context, userID shared.ID, tsCode string) error {
	query := `DELETE FROM user_stock_watchlist WHERE user_id = ? AND ts_code = ?`
	_, err := r.db.ExecContext(ctx, query, userID.String(), tsCode)
	return err
}
