-- User stock watchlist (same DB as auth, SQLite only)
CREATE TABLE IF NOT EXISTS user_stock_watchlist (
    user_id TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, ts_code),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_user_stock_watchlist_user_id ON user_stock_watchlist(user_id);
CREATE INDEX IF NOT EXISTS idx_user_stock_watchlist_sort ON user_stock_watchlist(user_id, sort_order);
