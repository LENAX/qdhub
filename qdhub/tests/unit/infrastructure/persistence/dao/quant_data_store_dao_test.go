package dao_test

import (
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/infrastructure/persistence/dao"
)

func TestQuantDataStoreDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Add quant_data_stores table
	_, err := db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewQuantDataStoreDAO(db.DB)

	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")

	err = dao.Create(nil, ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if ds.ID.IsEmpty() {
		t.Error("QuantDataStore ID should be set")
	}
}

func TestQuantDataStoreDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewQuantDataStoreDAO(db.DB)

	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	err = dao.Create(nil, ds)
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
}

func TestQuantDataStoreDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewQuantDataStoreDAO(db.DB)

	ds := datastore.NewQuantDataStore("Original", "Original Desc", datastore.DataStoreTypeDuckDB, "duckdb://old.db", "/data/old.db")
	err = dao.Create(nil, ds)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	ds.UpdateConnection("duckdb://new.db", "/data/new.db")
	err = dao.Update(nil, ds)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := dao.GetByID(nil, ds.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.DSN != "duckdb://new.db" {
		t.Errorf("Update() DSN = %s, want duckdb://new.db", got.DSN)
	}
}

func TestQuantDataStoreDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewQuantDataStoreDAO(db.DB)

	ds := datastore.NewQuantDataStore("To Delete", "Desc", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	err = dao.Create(nil, ds)
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
		t.Error("DeleteByID() should remove the data store")
	}
}

func TestQuantDataStoreDAO_ListAll(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewQuantDataStoreDAO(db.DB)

	ds1 := datastore.NewQuantDataStore("Store 1", "Desc 1", datastore.DataStoreTypeDuckDB, "duckdb://store1.db", "/data/store1.db")
	ds2 := datastore.NewQuantDataStore("Store 2", "Desc 2", datastore.DataStoreTypeDuckDB, "duckdb://store2.db", "/data/store2.db")

	err = dao.Create(nil, ds1)
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
		t.Errorf("ListAll() returned %d data stores, want at least 2", len(list))
	}
}
