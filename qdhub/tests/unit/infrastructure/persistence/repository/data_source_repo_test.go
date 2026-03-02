package repository_test

import (
	"os"
	"testing"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// setupDataSourceTestDB creates a temporary database for DataSource testing
func setupDataSourceTestDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "test_datasource_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Drop and create tables so schema is up to date
	_, _ = db.Exec(`DROP TABLE IF EXISTS tokens; DROP TABLE IF EXISTS api_metadata; DROP TABLE IF EXISTS api_categories; DROP TABLE IF EXISTS data_sources`)

	// Create tables for DataSource aggregate
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
		
		CREATE TABLE IF NOT EXISTS api_categories (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			description TEXT,
			parent_id VARCHAR(64),
			sort_order INTEGER DEFAULT 0,
			doc_path VARCHAR(512),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE
		);
		
		CREATE TABLE IF NOT EXISTS api_metadata (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			category_id VARCHAR(64),
			name VARCHAR(128) NOT NULL,
			display_name VARCHAR(256),
			description TEXT,
			endpoint VARCHAR(512),
			request_params TEXT,
			response_fields TEXT,
			rate_limit TEXT,
			permission VARCHAR(64),
			param_dependencies TEXT,
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(data_source_id, name),
			FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
			FOREIGN KEY (category_id) REFERENCES api_categories(id)
		);
		
		CREATE TABLE IF NOT EXISTS tokens (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			token_value TEXT NOT NULL,
			expires_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
			UNIQUE(data_source_id)
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

func TestDataSourceRepository_Create(t *testing.T) {
	db, cleanup := setupDataSourceTestDB(t)
	defer cleanup()

	repo := repository.NewDataSourceRepository(db)

	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")

	err := repo.Create(ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if ds.ID.IsEmpty() {
		t.Error("DataSource ID should be set after creation")
	}
}

func TestDataSourceRepository_Create_WithAggregates(t *testing.T) {
	db, cleanup := setupDataSourceTestDB(t)
	defer cleanup()

	repo := repository.NewDataSourceRepository(db)

	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	
	// Add category
	category := metadata.NewAPICategory(ds.ID, "Stock", "Stock data", "/stock", nil, 1)
	ds.Categories = []metadata.APICategory{*category}
	
	// Add API metadata
	api := metadata.NewAPIMetadata(ds.ID, "daily", "Daily Price", "Daily stock price", "/daily")
	api.CategoryID = &category.ID
	ds.APIs = []metadata.APIMetadata{*api}
	
	// Add token
	token := metadata.NewToken(ds.ID, "test-token-123", nil)
	ds.Token = token

	err := repo.Create(ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Verify aggregates were created
	got, err := repo.Get(ds.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if len(got.Categories) != 1 {
		t.Errorf("Categories count = %d, want 1", len(got.Categories))
	}

	if len(got.APIs) != 1 {
		t.Errorf("APIs count = %d, want 1", len(got.APIs))
	}

	if got.Token == nil {
		t.Error("Token should be created")
	}
}

func TestDataSourceRepository_Get(t *testing.T) {
	db, cleanup := setupDataSourceTestDB(t)
	defer cleanup()

	repo := repository.NewDataSourceRepository(db)

	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := repo.Create(ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := repo.Get(ds.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got == nil {
		t.Fatal("Get() returned nil")
	}

	if got.ID != ds.ID {
		t.Errorf("Get() ID = %s, want %s", got.ID, ds.ID)
	}

	if got.Name != "Test Source" {
		t.Errorf("Get() Name = %s, want Test Source", got.Name)
	}
}

func TestDataSourceRepository_Get_NotFound(t *testing.T) {
	db, cleanup := setupDataSourceTestDB(t)
	defer cleanup()

	repo := repository.NewDataSourceRepository(db)

	got, err := repo.Get(shared.NewID())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Get() should return nil for non-existent ID")
	}
}

func TestDataSourceRepository_Update(t *testing.T) {
	db, cleanup := setupDataSourceTestDB(t)
	defer cleanup()

	repo := repository.NewDataSourceRepository(db)

	ds := metadata.NewDataSource("Original Name", "Original Desc", "https://old.com", "https://old-docs.com")
	err := repo.Create(ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	ds.UpdateInfo("Updated Name", "Updated Desc", "https://new.com", "https://new-docs.com")
	err = repo.Update(ds)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := repo.Get(ds.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got.Name != "Updated Name" {
		t.Errorf("Update() Name = %s, want Updated Name", got.Name)
	}
}

func TestDataSourceRepository_Delete(t *testing.T) {
	db, cleanup := setupDataSourceTestDB(t)
	defer cleanup()

	repo := repository.NewDataSourceRepository(db)

	ds := metadata.NewDataSource("To Delete", "Desc", "https://test.com", "https://docs.com")
	err := repo.Create(ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = repo.Delete(ds.ID)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got, err := repo.Get(ds.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Delete() should remove the data source")
	}
}

func TestDataSourceRepository_Delete_Cascade(t *testing.T) {
	db, cleanup := setupDataSourceTestDB(t)
	defer cleanup()

	repo := repository.NewDataSourceRepository(db)

	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	category := metadata.NewAPICategory(ds.ID, "Stock", "Stock data", "/stock", nil, 1)
	ds.Categories = []metadata.APICategory{*category}
	
	err := repo.Create(ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete should cascade to categories
	err = repo.Delete(ds.ID)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify data source is deleted
	got, err := repo.Get(ds.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Delete() should remove the data source")
	}
}

func TestDataSourceRepository_List(t *testing.T) {
	db, cleanup := setupDataSourceTestDB(t)
	defer cleanup()

	repo := repository.NewDataSourceRepository(db)

	ds1 := metadata.NewDataSource("Source 1", "Desc 1", "https://api1.com", "https://docs1.com")
	ds2 := metadata.NewDataSource("Source 2", "Desc 2", "https://api2.com", "https://docs2.com")

	err := repo.Create(ds1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = repo.Create(ds2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := repo.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("List() returned %d data sources, want at least 2", len(list))
	}
}
