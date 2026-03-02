package dao_test

import (
	"os"
	"testing"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
)

func setupAPISyncStrategyTestDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "test_api_sync_strategy_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	_, _ = db.Exec(`DROP TABLE IF EXISTS api_sync_strategies; DROP TABLE IF EXISTS data_sources`)
	// Create required tables
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS data_sources (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL UNIQUE,
			description TEXT,
			base_url VARCHAR(512),
			doc_url VARCHAR(512),
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			common_data_apis TEXT
		);
		
		CREATE TABLE IF NOT EXISTS api_sync_strategies (
			id               VARCHAR(64) PRIMARY KEY,
			data_source_id   VARCHAR(64) NOT NULL,
			api_name         VARCHAR(128) NOT NULL,
			preferred_param  VARCHAR(32) NOT NULL DEFAULT 'ts_code',
			support_date_range INTEGER DEFAULT 0,
			required_params  TEXT,
			dependencies     TEXT,
			description      TEXT,
			created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(data_source_id, api_name),
			FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE
		);
	`)
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create tables: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dsn)
	}

	return db, cleanup
}

func TestAPISyncStrategyDAO_Create(t *testing.T) {
	db, cleanup := setupAPISyncStrategyTestDB(t)
	defer cleanup()

	// Create data source first
	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Tushare", "Tushare Data Source", "https://api.tushare.pro", "https://tushare.pro")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	strategyDAO := dao.NewAPISyncStrategyDAO(db.DB)
	strategy := metadata.NewAPISyncStrategy(ds.ID, "daily", metadata.SyncParamTradeDate)
	strategy.SetSupportDateRange(true)
	strategy.SetDependencies([]string{"FetchTradeCal"})
	strategy.SetDescription("日线行情数据")

	err = strategyDAO.Create(nil, strategy)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if strategy.ID.IsEmpty() {
		t.Error("APISyncStrategy ID should be set")
	}
}

func TestAPISyncStrategyDAO_GetByID(t *testing.T) {
	db, cleanup := setupAPISyncStrategyTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Tushare", "Tushare Data Source", "https://api.tushare.pro", "https://tushare.pro")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	strategyDAO := dao.NewAPISyncStrategyDAO(db.DB)
	strategy := metadata.NewAPISyncStrategy(ds.ID, "daily", metadata.SyncParamTradeDate)
	strategy.SetSupportDateRange(true)
	strategy.SetRequiredParams([]string{"ts_code"})
	strategy.SetDependencies([]string{"FetchTradeCal"})
	strategy.SetDescription("日线行情数据")

	err = strategyDAO.Create(nil, strategy)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := strategyDAO.GetByID(nil, strategy.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != strategy.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, strategy.ID)
	}

	if got.APIName != "daily" {
		t.Errorf("GetByID() APIName = %s, want daily", got.APIName)
	}

	if got.PreferredParam != metadata.SyncParamTradeDate {
		t.Errorf("GetByID() PreferredParam = %s, want %s", got.PreferredParam, metadata.SyncParamTradeDate)
	}

	if !got.SupportDateRange {
		t.Error("GetByID() SupportDateRange should be true")
	}

	if len(got.RequiredParams) != 1 || got.RequiredParams[0] != "ts_code" {
		t.Errorf("GetByID() RequiredParams = %v, want [ts_code]", got.RequiredParams)
	}
}

func TestAPISyncStrategyDAO_GetByID_NotFound(t *testing.T) {
	db, cleanup := setupAPISyncStrategyTestDB(t)
	defer cleanup()

	strategyDAO := dao.NewAPISyncStrategyDAO(db.DB)

	got, err := strategyDAO.GetByID(nil, shared.NewID())
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("GetByID() should return nil for non-existent ID")
	}
}

func TestAPISyncStrategyDAO_GetByDataSourceAndAPIName(t *testing.T) {
	db, cleanup := setupAPISyncStrategyTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Tushare", "Tushare Data Source", "https://api.tushare.pro", "https://tushare.pro")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	strategyDAO := dao.NewAPISyncStrategyDAO(db.DB)
	strategy := metadata.NewAPISyncStrategy(ds.ID, "daily", metadata.SyncParamTradeDate)
	strategy.SetDescription("日线行情数据")

	err = strategyDAO.Create(nil, strategy)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := strategyDAO.GetByDataSourceAndAPIName(nil, ds.ID, "daily")
	if err != nil {
		t.Fatalf("GetByDataSourceAndAPIName() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByDataSourceAndAPIName() returned nil")
	}

	if got.APIName != "daily" {
		t.Errorf("GetByDataSourceAndAPIName() APIName = %s, want daily", got.APIName)
	}
}

func TestAPISyncStrategyDAO_Update(t *testing.T) {
	db, cleanup := setupAPISyncStrategyTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Tushare", "Tushare Data Source", "https://api.tushare.pro", "https://tushare.pro")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	strategyDAO := dao.NewAPISyncStrategyDAO(db.DB)
	strategy := metadata.NewAPISyncStrategy(ds.ID, "daily", metadata.SyncParamTradeDate)
	strategy.SetDescription("Original description")

	err = strategyDAO.Create(nil, strategy)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Update strategy
	strategy.SetDescription("Updated description")
	strategy.SetSupportDateRange(true)
	strategy.SetRequiredParams([]string{"ts_code"})

	err = strategyDAO.Update(nil, strategy)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := strategyDAO.GetByID(nil, strategy.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.Description != "Updated description" {
		t.Errorf("Update() Description = %s, want Updated description", got.Description)
	}

	if !got.SupportDateRange {
		t.Error("Update() SupportDateRange should be true")
	}
}

func TestAPISyncStrategyDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupAPISyncStrategyTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Tushare", "Tushare Data Source", "https://api.tushare.pro", "https://tushare.pro")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	strategyDAO := dao.NewAPISyncStrategyDAO(db.DB)
	strategy := metadata.NewAPISyncStrategy(ds.ID, "daily", metadata.SyncParamTradeDate)

	err = strategyDAO.Create(nil, strategy)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = strategyDAO.DeleteByID(nil, strategy.ID)
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := strategyDAO.GetByID(nil, strategy.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the strategy")
	}
}

func TestAPISyncStrategyDAO_ListByDataSource(t *testing.T) {
	db, cleanup := setupAPISyncStrategyTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Tushare", "Tushare Data Source", "https://api.tushare.pro", "https://tushare.pro")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	strategyDAO := dao.NewAPISyncStrategyDAO(db.DB)

	// Create multiple strategies
	strategy1 := metadata.NewAPISyncStrategy(ds.ID, "daily", metadata.SyncParamTradeDate)
	err = strategyDAO.Create(nil, strategy1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	strategy2 := metadata.NewAPISyncStrategy(ds.ID, "weekly", metadata.SyncParamTradeDate)
	err = strategyDAO.Create(nil, strategy2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := strategyDAO.ListByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("ListByDataSource() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("ListByDataSource() returned %d strategies, want at least 2", len(list))
	}
}

func TestAPISyncStrategyDAO_ListByAPINames(t *testing.T) {
	db, cleanup := setupAPISyncStrategyTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Tushare", "Tushare Data Source", "https://api.tushare.pro", "https://tushare.pro")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	strategyDAO := dao.NewAPISyncStrategyDAO(db.DB)

	// Create multiple strategies
	strategy1 := metadata.NewAPISyncStrategy(ds.ID, "daily", metadata.SyncParamTradeDate)
	err = strategyDAO.Create(nil, strategy1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	strategy2 := metadata.NewAPISyncStrategy(ds.ID, "weekly", metadata.SyncParamTradeDate)
	err = strategyDAO.Create(nil, strategy2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	strategy3 := metadata.NewAPISyncStrategy(ds.ID, "monthly", metadata.SyncParamTradeDate)
	err = strategyDAO.Create(nil, strategy3)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := strategyDAO.ListByAPINames(nil, ds.ID, []string{"daily", "weekly"})
	if err != nil {
		t.Fatalf("ListByAPINames() error = %v", err)
	}

	if len(list) != 2 {
		t.Errorf("ListByAPINames() returned %d strategies, want 2", len(list))
	}

	apiNames := make(map[string]bool)
	for _, s := range list {
		apiNames[s.APIName] = true
	}

	if !apiNames["daily"] || !apiNames["weekly"] {
		t.Error("ListByAPINames() should return daily and weekly strategies")
	}
}

func TestAPISyncStrategyDAO_DeleteByDataSource(t *testing.T) {
	db, cleanup := setupAPISyncStrategyTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Tushare", "Tushare Data Source", "https://api.tushare.pro", "https://tushare.pro")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	strategyDAO := dao.NewAPISyncStrategyDAO(db.DB)

	// Create multiple strategies
	strategy1 := metadata.NewAPISyncStrategy(ds.ID, "daily", metadata.SyncParamTradeDate)
	err = strategyDAO.Create(nil, strategy1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	strategy2 := metadata.NewAPISyncStrategy(ds.ID, "weekly", metadata.SyncParamTradeDate)
	err = strategyDAO.Create(nil, strategy2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete all strategies for the data source
	err = strategyDAO.DeleteByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("DeleteByDataSource() error = %v", err)
	}

	// Verify all strategies are deleted
	list, err := strategyDAO.ListByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("ListByDataSource() error = %v", err)
	}

	if len(list) != 0 {
		t.Errorf("DeleteByDataSource() should remove all strategies, got %d remaining", len(list))
	}
}
