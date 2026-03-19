//go:build integration
// +build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/realtime"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/healthcheck"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

func setupRealtimeSourceIntegration(t *testing.T) (*repository.RealtimeSourceRepositoryImpl, contracts.RealtimeSourceApplicationService, func()) {
	t.Helper()
	db, cleanup := setupIntegrationDB(t)
	ensureRealtimeSourcesTable(t, db)
	repo := repository.NewRealtimeSourceRepository(db)
	svc := impl.NewRealtimeSourceApplicationService(repo, &healthcheck.Checker{})
	return repo, svc, cleanup
}

// ensureRealtimeSourcesTable 在 realtime_sources 表不存在时执行 028/029/030 迁移（兼容 cwd 为 tests/integration 时 migrations 未跑全的情况）。
func ensureRealtimeSourcesTable(t *testing.T, db *persistence.DB) {
	t.Helper()
	var count int
	err := db.Get(&count, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='realtime_sources'")
	if err != nil || count > 0 {
		return
	}
	for _, name := range []string{"028_realtime_sources.up.sql", "029_seed_realtime_sources.sqlite.up.sql", "030_fix_realtime_sources.sqlite.up.sql"} {
		for _, base := range []string{"migrations", "../migrations", "../../migrations"} {
			p := filepath.Join(base, name)
			body, err := os.ReadFile(p)
			if err != nil {
				continue
			}
			_, err = db.Exec(string(body))
			require.NoError(t, err, "run migration %s", p)
			break
		}
	}
}

// TestRealtimeSources_AfterMigration030_ThreeCoreRecords 验证 migration 030 后三条核心 realtime_sources 的 type、priority、is_primary、enabled、health_check_on_startup。
func TestRealtimeSources_AfterMigration030_ThreeCoreRecords(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()
	ensureRealtimeSourcesTable(t, db)

	repo := repository.NewRealtimeSourceRepository(db)
	list, err := repo.List()
	require.NoError(t, err)

	byType := make(map[string]*realtime.RealtimeSource)
	for _, s := range list {
		byType[s.Type] = s
	}

	// ts_proxy (type=tushare_proxy)
	if s, ok := byType[realtime.TypeTushareProxy]; ok {
		assert.Equal(t, "ts_proxy", s.Name, "tushare_proxy display name")
		assert.Equal(t, 1, s.Priority)
		assert.True(t, s.IsPrimary)
		assert.True(t, s.Enabled)
		assert.True(t, s.HealthCheckOnStartup)
	}
	// sina
	if s, ok := byType[realtime.TypeSina]; ok {
		assert.Equal(t, 2, s.Priority)
		assert.True(t, s.Enabled)
		assert.False(t, s.HealthCheckOnStartup)
	}
	// tushare_ws
	if s, ok := byType[realtime.TypeTushareWS]; ok {
		assert.Equal(t, 4, s.Priority)
		assert.False(t, s.IsPrimary)
		assert.False(t, s.Enabled)
		assert.False(t, s.HealthCheckOnStartup)
	}
}

// TestRealtimeSource_CRUD 测试 Repository 与 ApplicationService 层 CRUD。
func TestRealtimeSource_CRUD(t *testing.T) {
	repo, svc, cleanup := setupRealtimeSourceIntegration(t)
	defer cleanup()
	ctx := context.Background()

	// Create
	req := contracts.CreateRealtimeSourceRequest{
		Name:                 "Test Forward",
		Type:                 realtime.TypeTushareProxy,
		Config:               `{"ws_url":"ws://localhost:8888/realtime","rsa_public_key_path":"/tmp/pub.pem"}`,
		Priority:             1,
		IsPrimary:            true,
		HealthCheckOnStartup: false,
		Enabled:              true,
	}
	src, err := svc.Create(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, src)
	assert.NotEmpty(t, src.ID.String())
	assert.Equal(t, req.Name, src.Name)
	assert.Equal(t, req.Type, src.Type)
	assert.Equal(t, 1, src.Priority)

	// Get
	got, err := svc.Get(ctx, src.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, src.ID, got.ID)
	assert.Equal(t, src.Config, got.Config)

	// List (seed + 1)
	list, err := svc.List(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(list), 1)
	var found bool
	for _, s := range list {
		if s.ID == src.ID {
			found = true
			break
		}
	}
	assert.True(t, found, "created source should appear in list")

	// Update
	name2 := "Test Forward Updated"
	prio2 := 2
	updated, err := svc.Update(ctx, src.ID, contracts.UpdateRealtimeSourceRequest{
		Name:     &name2,
		Priority: &prio2,
	})
	require.NoError(t, err)
	assert.Equal(t, "Test Forward Updated", updated.Name)
	assert.Equal(t, 2, updated.Priority)

	// GetOrderedByPurpose (repo)
	ordered, err := repo.GetOrderedByPurpose(realtime.PurposeTsRealtimeMktTick)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(ordered), 1)

	// Delete
	err = svc.Delete(ctx, src.ID)
	require.NoError(t, err)
	got2, err := svc.Get(ctx, src.ID)
	require.Error(t, err)
	assert.Nil(t, got2)
	assert.True(t, shared.IsNotFoundError(err))
}

// TestRealtimeSource_Validation 测试按 type 的 config 必填校验。
func TestRealtimeSource_Validation(t *testing.T) {
	_, svc, cleanup := setupRealtimeSourceIntegration(t)
	defer cleanup()
	ctx := context.Background()

	// tushare_proxy 缺少 ws_url
	_, err := svc.Create(ctx, contracts.CreateRealtimeSourceRequest{
		Name:     "Bad Forward",
		Type:     realtime.TypeTushareProxy,
		Config:   `{"rsa_public_key_path":"/tmp/pub.pem"}`,
		Priority: 1,
		Enabled:  true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ws_url")

	// tushare_proxy 缺少 rsa_public_key_path
	_, err = svc.Create(ctx, contracts.CreateRealtimeSourceRequest{
		Name:     "Bad Forward 2",
		Type:     realtime.TypeTushareProxy,
		Config:   `{"ws_url":"ws://x/realtime"}`,
		Priority: 1,
		Enabled:  true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rsa_public_key_path")

	// invalid type
	_, err = svc.Create(ctx, contracts.CreateRealtimeSourceRequest{
		Name:     "Bad Type",
		Type:     "invalid_type",
		Config:   "{}",
		Priority: 1,
		Enabled:  true,
	})
	require.Error(t, err)

	// valid tushare_proxy
	src, err := svc.Create(ctx, contracts.CreateRealtimeSourceRequest{
		Name:     "Valid Forward",
		Type:     realtime.TypeTushareProxy,
		Config:   `{"ws_url":"ws://localhost/realtime","rsa_public_key_path":"/tmp/pub.pem"}`,
		Priority: 1,
		Enabled:  true,
	})
	require.NoError(t, err)
	require.NotNil(t, src)
}

// TestRealtimeSource_ListEnabledForHealthCheck 测试只返回 enabled 且 health_check_on_startup 的源。
func TestRealtimeSource_ListEnabledForHealthCheck(t *testing.T) {
	repo, svc, cleanup := setupRealtimeSourceIntegration(t)
	defer cleanup()
	ctx := context.Background()

	list, err := repo.ListEnabledForHealthCheck()
	require.NoError(t, err)
	// Seed 中可能有 0 或若干条开启启动检查的
	for _, s := range list {
		assert.True(t, s.Enabled)
		assert.True(t, s.HealthCheckOnStartup)
	}

	// 创建一条 enabled 且 health_check_on_startup 的
	_, err = svc.Create(ctx, contracts.CreateRealtimeSourceRequest{
		Name:                 "HealthCheck Source",
		Type:                 realtime.TypeSina,
		Config:               "{}",
		Priority:             10,
		Enabled:              true,
		HealthCheckOnStartup: true,
	})
	require.NoError(t, err)
	list2, err := repo.ListEnabledForHealthCheck()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(list2), len(list))
}

// TestRealtimeSource_ConfigSerialization 测试 config JSON 序列化与 ConfigMap。
func TestRealtimeSource_ConfigSerialization(t *testing.T) {
	_, svc, cleanup := setupRealtimeSourceIntegration(t)
	defer cleanup()
	ctx := context.Background()

	config := map[string]interface{}{
		"ws_url":              "ws://test:8888/realtime",
		"rsa_public_key_path": "/root/.key/public.pem",
	}
	configJSON, _ := json.Marshal(config)
	src, err := svc.Create(ctx, contracts.CreateRealtimeSourceRequest{
		Name:     "Config Test",
		Type:     realtime.TypeTushareProxy,
		Config:   string(configJSON),
		Priority: 1,
		Enabled:  true,
	})
	require.NoError(t, err)
	m, err := src.ConfigMap()
	require.NoError(t, err)
	assert.Equal(t, "ws://test:8888/realtime", m["ws_url"])
	assert.Equal(t, "/root/.key/public.pem", m["rsa_public_key_path"])
}

// TestRealtimeSource_TriggerHealthCheck 测试单源健康检查（无 checker 时返回错误；有 checker 时依赖外部可达性，此处仅测调用不 panic）。
func TestRealtimeSource_TriggerHealthCheck(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()
	ensureRealtimeSourcesTable(t, db)
	repo := repository.NewRealtimeSourceRepository(db)
	svcNoChecker := impl.NewRealtimeSourceApplicationService(repo, nil)
	svcWithChecker := impl.NewRealtimeSourceApplicationService(repo, &healthcheck.Checker{})
	ctx := context.Background()

	// 创建一条源
	src, err := svcWithChecker.Create(ctx, contracts.CreateRealtimeSourceRequest{
		Name:     "Health Target",
		Type:     realtime.TypeTushareProxy,
		Config:   `{"ws_url":"ws://127.0.0.1:99999/realtime","rsa_public_key_path":"/nonexistent"}`,
		Priority: 1,
		Enabled:  true,
	})
	require.NoError(t, err)

	// 无 checker 时应返回错误
	_, _, err = svcNoChecker.TriggerHealthCheck(ctx, src.ID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "health check not configured")

	// 有 checker 时调用会执行检查（可能 unhealthy/unavailable）
	status, errMsg, err := svcWithChecker.TriggerHealthCheck(ctx, src.ID)
	if err != nil {
		t.Logf("TriggerHealthCheck err (expected for unreachable): %v", err)
		return
	}
	t.Logf("status=%s errMsg=%s", status, errMsg)
	assert.NotEmpty(t, status)
}

// TestRealtimeSource_GetOrderedByPurpose 测试按 purpose 返回的源顺序与类型。
func TestRealtimeSource_GetOrderedByPurpose(t *testing.T) {
	repo, _, cleanup := setupRealtimeSourceIntegration(t)
	defer cleanup()

	// ts_realtime_mkt_tick -> tushare_proxy, tushare_ws
	ordered, err := repo.GetOrderedByPurpose(realtime.PurposeTsRealtimeMktTick)
	require.NoError(t, err)
	for _, s := range ordered {
		assert.Contains(t, []string{realtime.TypeTushareProxy, realtime.TypeTushareWS}, s.Type)
	}
	// priority 升序
	for i := 1; i < len(ordered); i++ {
		assert.GreaterOrEqual(t, ordered[i].Priority, ordered[i-1].Priority)
	}

	// realtime_quote -> sina, eastmoney
	quoteOrdered, err := repo.GetOrderedByPurpose(realtime.PurposeRealtimeQuote)
	require.NoError(t, err)
	for _, s := range quoteOrdered {
		assert.Contains(t, []string{realtime.TypeSina, realtime.TypeEastmoney}, s.Type)
	}

	// realtime_tick -> eastmoney
	tickOrdered, err := repo.GetOrderedByPurpose(realtime.PurposeRealtimeTick)
	require.NoError(t, err)
	for _, s := range tickOrdered {
		assert.Equal(t, realtime.TypeEastmoney, s.Type)
	}
}

// TestRealtimeSource_NotFound 测试 Get/Update/Delete 不存在的 ID。
func TestRealtimeSource_NotFound(t *testing.T) {
	_, svc, cleanup := setupRealtimeSourceIntegration(t)
	defer cleanup()
	ctx := context.Background()
	badID := shared.NewID()

	got, err := svc.Get(ctx, badID)
	require.Error(t, err)
	assert.Nil(t, got)
	assert.True(t, shared.IsNotFoundError(err))

	x := "x"
	_, err = svc.Update(ctx, badID, contracts.UpdateRealtimeSourceRequest{Name: &x})
	require.Error(t, err)
	assert.True(t, shared.IsNotFoundError(err))

	err = svc.Delete(ctx, badID)
	require.Error(t, err)
	assert.True(t, shared.IsNotFoundError(err))
}
