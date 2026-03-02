package repository_test

import (
	"os"
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// setupQuantDataStoreTestDB creates a temporary database for QuantDataStore testing
func setupQuantDataStoreTestDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "test_datastore_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create tables for QuantDataStore aggregate
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS quant_data_stores (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL,
			description TEXT,
			type VARCHAR(32) NOT NULL,
			dsn TEXT,
			storage_path VARCHAR(512),
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS table_schemas (
			id VARCHAR(64) PRIMARY KEY,
			data_store_id VARCHAR(64) NOT NULL,
			api_meta_id VARCHAR(64) NOT NULL,
			table_name VARCHAR(128) NOT NULL,
			columns TEXT NOT NULL,
			primary_keys TEXT,
			indexes TEXT,
			status VARCHAR(32) DEFAULT 'pending',
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (data_store_id) REFERENCES quant_data_stores(id) ON DELETE CASCADE,
			UNIQUE(data_store_id, table_name)
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

func TestQuantDataStoreRepository_Create(t *testing.T) {
	db, cleanup := setupQuantDataStoreTestDB(t)
	defer cleanup()

	repo := repository.NewQuantDataStoreRepository(db)

	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")

	err := repo.Create(ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if ds.ID.IsEmpty() {
		t.Error("QuantDataStore ID should be set after creation")
	}
}

func TestQuantDataStoreRepository_Create_WithSchemas(t *testing.T) {
	db, cleanup := setupQuantDataStoreTestDB(t)
	defer cleanup()

	repo := repository.NewQuantDataStoreRepository(db)

	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	
	// Add table schema
	apiMetaID := shared.NewID()
	schema := datastore.NewTableSchema(ds.ID, apiMetaID, "test_table")
	ds.Schemas = []datastore.TableSchema{*schema}

	err := repo.Create(ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Verify schema was created
	got, err := repo.Get(ds.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if len(got.Schemas) != 1 {
		t.Errorf("Schemas count = %d, want 1", len(got.Schemas))
	}
}

func TestQuantDataStoreRepository_Get(t *testing.T) {
	db, cleanup := setupQuantDataStoreTestDB(t)
	defer cleanup()

	repo := repository.NewQuantDataStoreRepository(db)

	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
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

	if got.Name != "Test Store" {
		t.Errorf("Get() Name = %s, want Test Store", got.Name)
	}
}

func TestQuantDataStoreRepository_Get_NotFound(t *testing.T) {
	db, cleanup := setupQuantDataStoreTestDB(t)
	defer cleanup()

	repo := repository.NewQuantDataStoreRepository(db)

	got, err := repo.Get(shared.NewID())
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Get() should return nil for non-existent ID")
	}
}

func TestQuantDataStoreRepository_Update(t *testing.T) {
	db, cleanup := setupQuantDataStoreTestDB(t)
	defer cleanup()

	repo := repository.NewQuantDataStoreRepository(db)

	ds := datastore.NewQuantDataStore("Original Name", "Original Desc", datastore.DataStoreTypeDuckDB, "duckdb://old.db", "/data/old.db")
	err := repo.Create(ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	ds.UpdateConnection("duckdb://new.db", "/data/new.db")
	err = repo.Update(ds)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := repo.Get(ds.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got.DSN != "duckdb://new.db" {
		t.Errorf("Update() DSN = %s, want duckdb://new.db", got.DSN)
	}
}

func TestQuantDataStoreRepository_Delete(t *testing.T) {
	db, cleanup := setupQuantDataStoreTestDB(t)
	defer cleanup()

	repo := repository.NewQuantDataStoreRepository(db)

	ds := datastore.NewQuantDataStore("To Delete", "Desc", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
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
		t.Error("Delete() should remove the data store")
	}
}

func TestQuantDataStoreRepository_Delete_Cascade(t *testing.T) {
	db, cleanup := setupQuantDataStoreTestDB(t)
	defer cleanup()

	repo := repository.NewQuantDataStoreRepository(db)

	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	apiMetaID := shared.NewID()
	schema := datastore.NewTableSchema(ds.ID, apiMetaID, "test_table")
	ds.Schemas = []datastore.TableSchema{*schema}
	
	err := repo.Create(ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete should cascade to schemas
	err = repo.Delete(ds.ID)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify data store is deleted
	got, err := repo.Get(ds.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Delete() should remove the data store and cascade to schemas")
	}
}

func TestQuantDataStoreRepository_List(t *testing.T) {
	db, cleanup := setupQuantDataStoreTestDB(t)
	defer cleanup()

	repo := repository.NewQuantDataStoreRepository(db)

	ds1 := datastore.NewQuantDataStore("Store 1", "Desc 1", datastore.DataStoreTypeDuckDB, "duckdb://store1.db", "/data/store1.db")
	ds2 := datastore.NewQuantDataStore("Store 2", "Desc 2", datastore.DataStoreTypeDuckDB, "duckdb://store2.db", "/data/store2.db")

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
		t.Errorf("List() returned %d data stores, want at least 2", len(list))
	}
}

// Note: TableSchemaRepository has been integrated into QuantDataStoreRepository
// following DDD aggregate patterns. Use QuantDataStoreRepository methods for TableSchema operations.
