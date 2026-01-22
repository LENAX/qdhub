//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"testing"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/infrastructure/persistence/repository"
)

func TestAPISyncStrategyRepository_Integration(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	// Create data source first
	dataSourceRepo := repository.NewDataSourceRepository(db)
	ds := metadata.NewDataSource("Tushare Integration Test", "Tushare Data Source", "https://api.tushare.pro", "https://tushare.pro")
	err := dataSourceRepo.Create(ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	t.Run("Save and GetByAPIName", func(t *testing.T) {
		strategy := metadata.NewAPISyncStrategy(ds.ID, "daily", metadata.SyncParamTradeDate)
		strategy.SetSupportDateRange(true)
		strategy.SetDependencies([]string{"FetchTradeCal"})
		strategy.SetDescription("日线行情数据")

		err := repo.SaveAPISyncStrategy(ctx, strategy)
		if err != nil {
			t.Fatalf("SaveAPISyncStrategy() error = %v", err)
		}

		if strategy.ID.IsEmpty() {
			t.Error("SaveAPISyncStrategy() should set strategy ID")
		}

		got, err := repo.GetAPISyncStrategyByAPIName(ctx, ds.ID, "daily")
		if err != nil {
			t.Fatalf("GetAPISyncStrategyByAPIName() error = %v", err)
		}

		if got == nil {
			t.Fatal("GetAPISyncStrategyByAPIName() returned nil")
		}

		if got.APIName != "daily" {
			t.Errorf("GetAPISyncStrategyByAPIName() APIName = %s, want daily", got.APIName)
		}

		if got.PreferredParam != metadata.SyncParamTradeDate {
			t.Errorf("GetAPISyncStrategyByAPIName() PreferredParam = %s, want %s", got.PreferredParam, metadata.SyncParamTradeDate)
		}

		if !got.SupportDateRange {
			t.Error("GetAPISyncStrategyByAPIName() SupportDateRange should be true")
		}
	})

	t.Run("Save updates existing strategy", func(t *testing.T) {
		strategy := metadata.NewAPISyncStrategy(ds.ID, "weekly", metadata.SyncParamTradeDate)
		strategy.SetDescription("Original description")

		err := repo.SaveAPISyncStrategy(ctx, strategy)
		if err != nil {
			t.Fatalf("SaveAPISyncStrategy() error = %v", err)
		}

		originalID := strategy.ID

		// Update strategy
		strategy.SetDescription("Updated description")
		strategy.SetSupportDateRange(true)

		err = repo.SaveAPISyncStrategy(ctx, strategy)
		if err != nil {
			t.Fatalf("SaveAPISyncStrategy() update error = %v", err)
		}

		// ID should remain the same
		if strategy.ID != originalID {
			t.Errorf("SaveAPISyncStrategy() update changed ID from %s to %s", originalID, strategy.ID)
		}

		got, err := repo.GetAPISyncStrategyByAPIName(ctx, ds.ID, "weekly")
		if err != nil {
			t.Fatalf("GetAPISyncStrategyByAPIName() error = %v", err)
		}

		if got.Description != "Updated description" {
			t.Errorf("SaveAPISyncStrategy() update Description = %s, want Updated description", got.Description)
		}

		if !got.SupportDateRange {
			t.Error("SaveAPISyncStrategy() update SupportDateRange should be true")
		}
	})

	t.Run("SaveBatch", func(t *testing.T) {
		strategies := []*metadata.APISyncStrategy{
			metadata.NewAPISyncStrategy(ds.ID, "monthly", metadata.SyncParamTradeDate),
			metadata.NewAPISyncStrategy(ds.ID, "adj_factor", metadata.SyncParamTradeDate),
			metadata.NewAPISyncStrategy(ds.ID, "top_list", metadata.SyncParamTradeDate),
		}

		strategies[0].SetDescription("月线数据")
		strategies[1].SetSupportDateRange(true).SetDescription("复权因子")
		strategies[2].SetDescription("龙虎榜数据")

		err := repo.SaveAPISyncStrategyBatch(ctx, strategies)
		if err != nil {
			t.Fatalf("SaveAPISyncStrategyBatch() error = %v", err)
		}

		// Verify all strategies were saved
		for _, s := range strategies {
			got, err := repo.GetAPISyncStrategyByAPIName(ctx, ds.ID, s.APIName)
			if err != nil {
				t.Fatalf("GetAPISyncStrategyByAPIName() for %s error = %v", s.APIName, err)
			}

			if got == nil {
				t.Errorf("GetAPISyncStrategyByAPIName() for %s returned nil", s.APIName)
			}
		}
	})

	t.Run("ListByDataSource", func(t *testing.T) {
		list, err := repo.ListAPISyncStrategiesByDataSource(ctx, ds.ID)
		if err != nil {
			t.Fatalf("ListAPISyncStrategiesByDataSource() error = %v", err)
		}

		// Should have at least the strategies created in previous tests
		if len(list) < 5 {
			t.Errorf("ListAPISyncStrategiesByDataSource() returned %d strategies, want at least 5", len(list))
		}

		// Verify all strategies belong to the data source
		for _, s := range list {
			if s.DataSourceID != ds.ID {
				t.Errorf("ListAPISyncStrategiesByDataSource() strategy %s has wrong DataSourceID = %s, want %s", s.APIName, s.DataSourceID, ds.ID)
			}
		}
	})

	t.Run("ListByAPINames", func(t *testing.T) {
		apiNames := []string{"daily", "weekly", "monthly"}
		list, err := repo.ListAPISyncStrategiesByAPINames(ctx, ds.ID, apiNames)
		if err != nil {
			t.Fatalf("ListAPISyncStrategiesByAPINames() error = %v", err)
		}

		if len(list) != 3 {
			t.Errorf("ListAPISyncStrategiesByAPINames() returned %d strategies, want 3", len(list))
		}

		apiNameMap := make(map[string]bool)
		for _, s := range list {
			apiNameMap[s.APIName] = true
		}

		for _, name := range apiNames {
			if !apiNameMap[name] {
				t.Errorf("ListAPISyncStrategiesByAPINames() missing API %s", name)
			}
		}
	})

	t.Run("Delete", func(t *testing.T) {
		strategy := metadata.NewAPISyncStrategy(ds.ID, "to_delete", metadata.SyncParamNone)
		err := repo.SaveAPISyncStrategy(ctx, strategy)
		if err != nil {
			t.Fatalf("SaveAPISyncStrategy() error = %v", err)
		}

		err = repo.DeleteAPISyncStrategy(ctx, strategy.ID)
		if err != nil {
			t.Fatalf("DeleteAPISyncStrategy() error = %v", err)
		}

		got, err := repo.GetAPISyncStrategyByAPIName(ctx, ds.ID, "to_delete")
		if err != nil {
			t.Fatalf("GetAPISyncStrategyByAPIName() error = %v", err)
		}

		if got != nil {
			t.Error("DeleteAPISyncStrategy() should remove the strategy")
		}
	})

	t.Run("DeleteByDataSource", func(t *testing.T) {
		// Create another data source
		ds2 := metadata.NewDataSource("Another Source", "Another Data Source", "https://api.another.com", "https://another.com")
		err := dataSourceRepo.Create(ds2)
		if err != nil {
			t.Fatalf("Failed to create data source: %v", err)
		}

		// Create strategies for ds2
		strategy1 := metadata.NewAPISyncStrategy(ds2.ID, "api1", metadata.SyncParamNone)
		strategy2 := metadata.NewAPISyncStrategy(ds2.ID, "api2", metadata.SyncParamNone)
		err = repo.SaveAPISyncStrategyBatch(ctx, []*metadata.APISyncStrategy{strategy1, strategy2})
		if err != nil {
			t.Fatalf("SaveAPISyncStrategyBatch() error = %v", err)
		}

		// Delete all strategies for ds2
		err = repo.DeleteAPISyncStrategiesByDataSource(ctx, ds2.ID)
		if err != nil {
			t.Fatalf("DeleteAPISyncStrategiesByDataSource() error = %v", err)
		}

		// Verify ds2 strategies are deleted
		list, err := repo.ListAPISyncStrategiesByDataSource(ctx, ds2.ID)
		if err != nil {
			t.Fatalf("ListAPISyncStrategiesByDataSource() error = %v", err)
		}

		if len(list) != 0 {
			t.Errorf("DeleteAPISyncStrategiesByDataSource() should remove all strategies, got %d remaining", len(list))
		}

		// Verify ds strategies are still there
		dsList, err := repo.ListAPISyncStrategiesByDataSource(ctx, ds.ID)
		if err != nil {
			t.Fatalf("ListAPISyncStrategiesByDataSource() for ds error = %v", err)
		}

		if len(dsList) == 0 {
			t.Error("DeleteAPISyncStrategiesByDataSource() should not affect other data sources")
		}
	})

	t.Run("Complex strategy with all fields", func(t *testing.T) {
		strategy := metadata.NewAPISyncStrategy(ds.ID, "complex_api", metadata.SyncParamTsCode)
		strategy.SetSupportDateRange(true)
		strategy.SetRequiredParams([]string{"market", "exchange"})
		strategy.SetDependencies([]string{"FetchStockBasic", "FetchTradeCal"})
		strategy.SetDescription("复杂API策略 - 包含所有字段")

		err := repo.SaveAPISyncStrategy(ctx, strategy)
		if err != nil {
			t.Fatalf("SaveAPISyncStrategy() error = %v", err)
		}

		got, err := repo.GetAPISyncStrategyByAPIName(ctx, ds.ID, "complex_api")
		if err != nil {
			t.Fatalf("GetAPISyncStrategyByAPIName() error = %v", err)
		}

		if got.PreferredParam != metadata.SyncParamTsCode {
			t.Errorf("PreferredParam = %s, want %s", got.PreferredParam, metadata.SyncParamTsCode)
		}

		if !got.SupportDateRange {
			t.Error("SupportDateRange should be true")
		}

		if len(got.RequiredParams) != 2 {
			t.Errorf("RequiredParams length = %d, want 2", len(got.RequiredParams))
		}

		if len(got.Dependencies) != 2 {
			t.Errorf("Dependencies length = %d, want 2", len(got.Dependencies))
		}

		if got.Description != "复杂API策略 - 包含所有字段" {
			t.Errorf("Description = %s, want 复杂API策略 - 包含所有字段", got.Description)
		}
	})
}

func TestAPISyncStrategyRepository_MigrationIntegration(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	// Create Tushare data source
	dataSourceRepo := repository.NewDataSourceRepository(db)
	ds := metadata.NewDataSource("tushare", "Tushare Data Source", "https://api.tushare.pro", "https://tushare.pro")
	err := dataSourceRepo.Create(ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Run migration to insert strategies
	migrationSQL, err := os.ReadFile("../../migrations/004_api_sync_strategy.up.sql")
	if err != nil {
		t.Fatalf("Failed to read migration file: %v", err)
	}

	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		t.Fatalf("Failed to execute migration: %v", err)
	}

	// Verify strategies were created
	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	list, err := repo.ListAPISyncStrategiesByDataSource(ctx, ds.ID)
	if err != nil {
		t.Fatalf("ListAPISyncStrategiesByDataSource() error = %v", err)
	}

	// Should have many strategies (220+ APIs)
	if len(list) < 100 {
		t.Errorf("Migration should create many strategies, got %d", len(list))
	}

	// Verify some common APIs exist
	commonAPIs := []string{"daily", "weekly", "monthly", "stock_basic", "trade_cal"}
	for _, apiName := range commonAPIs {
		strategy, err := repo.GetAPISyncStrategyByAPIName(ctx, ds.ID, apiName)
		if err != nil {
			t.Fatalf("GetAPISyncStrategyByAPIName() for %s error = %v", apiName, err)
		}

		if strategy == nil {
			t.Errorf("Migration should create strategy for %s", apiName)
		}
	}
}
