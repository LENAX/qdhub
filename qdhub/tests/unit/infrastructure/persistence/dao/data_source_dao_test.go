package dao_test

import (
	"os"
	"testing"
	"time"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
)

// setupDAOTestDB creates a temporary database for DAO testing
func setupDAOTestDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "test_dao_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create tables for DAO testing
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS data_sources (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL,
			description TEXT,
			base_url VARCHAR(512),
			doc_url VARCHAR(512),
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

func TestDataSourceDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dao := dao.NewDataSourceDAO(db.DB)

	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")

	err := dao.Create(nil, ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if ds.ID.IsEmpty() {
		t.Error("DataSource ID should be set")
	}
}

func TestDataSourceDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dao := dao.NewDataSourceDAO(db.DB)

	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dao.Create(nil, ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := dao.GetByID(nil, ds.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != ds.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, ds.ID)
	}

	if got.Name != "Test Source" {
		t.Errorf("GetByID() Name = %s, want Test Source", got.Name)
	}
}

func TestDataSourceDAO_GetByID_NotFound(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dao := dao.NewDataSourceDAO(db.DB)

	got, err := dao.GetByID(nil, shared.NewID())
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("GetByID() should return nil for non-existent ID")
	}
}

func TestDataSourceDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dao := dao.NewDataSourceDAO(db.DB)

	ds := metadata.NewDataSource("Original Name", "Original Desc", "https://old.com", "https://old-docs.com")
	err := dao.Create(nil, ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	ds.UpdateInfo("Updated Name", "Updated Desc", "https://new.com", "https://new-docs.com")
	err = dao.Update(nil, ds)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := dao.GetByID(nil, ds.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.Name != "Updated Name" {
		t.Errorf("Update() Name = %s, want Updated Name", got.Name)
	}
}

func TestDataSourceDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dao := dao.NewDataSourceDAO(db.DB)

	ds := metadata.NewDataSource("To Delete", "Desc", "https://test.com", "https://docs.com")
	err := dao.Create(nil, ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = dao.DeleteByID(nil, ds.ID)
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := dao.GetByID(nil, ds.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the data source")
	}
}

func TestDataSourceDAO_ListAll(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dao := dao.NewDataSourceDAO(db.DB)

	ds1 := metadata.NewDataSource("Source 1", "Desc 1", "https://api1.com", "https://docs1.com")
	ds2 := metadata.NewDataSource("Source 2", "Desc 2", "https://api2.com", "https://docs2.com")

	err := dao.Create(nil, ds1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = dao.Create(nil, ds2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := dao.ListAll(nil)
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("ListAll() returned %d data sources, want at least 2", len(list))
	}
}

func TestDataSourceDAO_GetCreatedAt(t *testing.T) {
	// Create a minimal DB instance for the test
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)

	// Test GetCreatedAt helper method
	now := time.Now()
	result := dsDAO.GetCreatedAt(now)

	if result.ToTime().Unix() != now.Unix() {
		t.Errorf("GetCreatedAt() = %v, want %v", result.ToTime(), now)
	}
}

func TestAPICategoryDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// First create a data source
	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	categoryDAO := dao.NewAPICategoryDAO(db.DB)
	category := metadata.NewAPICategory(ds.ID, "Stock", "Stock data", "/stock", nil, 1)

	err = categoryDAO.Create(nil, category)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if category.ID.IsEmpty() {
		t.Error("APICategory ID should be set")
	}
}

func TestAPICategoryDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	categoryDAO := dao.NewAPICategoryDAO(db.DB)
	category := metadata.NewAPICategory(ds.ID, "Stock", "Stock data", "/stock", nil, 1)
	err = categoryDAO.Create(nil, category)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := categoryDAO.GetByID(nil, category.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != category.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, category.ID)
	}
}

func TestAPICategoryDAO_ListByDataSource(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	categoryDAO := dao.NewAPICategoryDAO(db.DB)
	category1 := metadata.NewAPICategory(ds.ID, "Stock", "Stock data", "/stock", nil, 1)
	category2 := metadata.NewAPICategory(ds.ID, "Index", "Index data", "/index", nil, 2)

	err = categoryDAO.Create(nil, category1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = categoryDAO.Create(nil, category2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := categoryDAO.ListByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("ListByDataSource() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("ListByDataSource() returned %d categories, want at least 2", len(list))
	}
}

func TestAPICategoryDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	categoryDAO := dao.NewAPICategoryDAO(db.DB)
	category := metadata.NewAPICategory(ds.ID, "Original", "Original Desc", "/original", nil, 1)
	err = categoryDAO.Create(nil, category)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Update category
	category.Name = "Updated"
	category.Description = "Updated Desc"
	err = categoryDAO.Update(nil, category)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := categoryDAO.GetByID(nil, category.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.Name != "Updated" {
		t.Errorf("Update() Name = %s, want Updated", got.Name)
	}
}

func TestAPICategoryDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	categoryDAO := dao.NewAPICategoryDAO(db.DB)
	category := metadata.NewAPICategory(ds.ID, "To Delete", "Desc", "/delete", nil, 1)
	err = categoryDAO.Create(nil, category)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = categoryDAO.DeleteByID(nil, category.ID)
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := categoryDAO.GetByID(nil, category.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the category")
	}
}

func TestAPICategoryDAO_DeleteByDataSource(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	categoryDAO := dao.NewAPICategoryDAO(db.DB)
	category1 := metadata.NewAPICategory(ds.ID, "Category 1", "Desc 1", "/cat1", nil, 1)
	category2 := metadata.NewAPICategory(ds.ID, "Category 2", "Desc 2", "/cat2", nil, 2)

	err = categoryDAO.Create(nil, category1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = categoryDAO.Create(nil, category2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete all categories for the data source
	err = categoryDAO.DeleteByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("DeleteByDataSource() error = %v", err)
	}

	// Verify all categories are deleted
	list, err := categoryDAO.ListByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("ListByDataSource() error = %v", err)
	}

	if len(list) != 0 {
		t.Errorf("DeleteByDataSource() should remove all categories, got %d remaining", len(list))
	}
}
