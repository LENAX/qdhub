//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/impl"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// runWatchlistMigration 执行收藏表迁移 022（依赖 002 auth 的 users 表）
func runWatchlistMigration(t *testing.T, db *persistence.DB) {
	t.Helper()
	migrationSQL, err := os.ReadFile(filepath.Join("../../migrations", "022_user_stock_watchlist.sqlite.up.sql"))
	if err != nil {
		t.Fatalf("Failed to read watchlist migration: %v", err)
	}
	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		t.Fatalf("Failed to execute watchlist migration: %v", err)
	}
}

// TestWatchlistRepository_AddGetRemove 测试收藏表仓储：添加、按用户查询、删除
func TestWatchlistRepository_AddGetRemove(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()
	runAuthMigration(t, db)
	runWatchlistMigration(t, db)

	// 插入测试用户（022 表外键依赖 users）
	userID := shared.NewID().String()
	_, err := db.Exec(`INSERT INTO users (id, username, email, password_hash, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		userID, "watchlist_test", "wl@test.com", "hash")
	require.NoError(t, err)

	repo := repository.NewWatchlistRepository(db)
	ctx := context.Background()
	uid := shared.ID(userID)

	// 初始为空
	entries, err := repo.GetByUserID(ctx, uid)
	require.NoError(t, err)
	assert.Empty(t, entries)

	// 添加两只
	err = repo.Add(ctx, uid, "000001.SZ", 0)
	require.NoError(t, err)
	err = repo.Add(ctx, uid, "600000.SH", 1)
	require.NoError(t, err)

	entries, err = repo.GetByUserID(ctx, uid)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "000001.SZ", entries[0].TsCode)
	assert.Equal(t, "600000.SH", entries[1].TsCode)

	// 删除一只
	err = repo.Remove(ctx, uid, "000001.SZ")
	require.NoError(t, err)
	entries, err = repo.GetByUserID(ctx, uid)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "600000.SH", entries[0].TsCode)

	// 重复删除无报错
	err = repo.Remove(ctx, uid, "000001.SZ")
	require.NoError(t, err)
}

// TestWatchlistApplicationService_GetAddRemove 测试收藏应用服务
func TestWatchlistApplicationService_GetAddRemove(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()
	runAuthMigration(t, db)
	runWatchlistMigration(t, db)

	userID := shared.NewID().String()
	_, err := db.Exec(`INSERT INTO users (id, username, email, password_hash, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		userID, "wl_svc_test", "wlsvc@test.com", "hash")
	require.NoError(t, err)

	repo := repository.NewWatchlistRepository(db)
	svc := impl.NewWatchlistApplicationService(repo)
	ctx := context.Background()
	uid := shared.ID(userID)

	list, err := svc.GetWatchlist(ctx, uid)
	require.NoError(t, err)
	assert.Empty(t, list)

	err = svc.Add(ctx, uid, "000001.SZ")
	require.NoError(t, err)
	err = svc.Add(ctx, uid, "600519.SH")
	require.NoError(t, err)

	list, err = svc.GetWatchlist(ctx, uid)
	require.NoError(t, err)
	require.Len(t, list, 2)

	err = svc.Remove(ctx, uid, "000001.SZ")
	require.NoError(t, err)
	list, err = svc.GetWatchlist(ctx, uid)
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "600519.SH", list[0].TsCode)
}
