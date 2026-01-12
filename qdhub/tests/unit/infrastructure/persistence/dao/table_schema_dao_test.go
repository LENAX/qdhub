package dao_test

import (
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence/dao"
)

func TestTableSchemaDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Add required tables
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
		
		CREATE TABLE IF NOT EXISTS api_metadata (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
			FOREIGN KEY (data_store_id) REFERENCES quant_data_stores(id),
			FOREIGN KEY (api_meta_id) REFERENCES api_metadata(id)
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	// Create data source first for foreign key constraint
	dataSourceDAO := dao.NewDataSourceDAO(db.DB)
	dataSource := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err = dataSourceDAO.Create(nil, dataSource)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create data store
	dsDAO := dao.NewQuantDataStoreDAO(db.DB)
	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	err = dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data store: %v", err)
	}

	// Create a minimal API metadata
	apiMetaID := shared.NewID()
	_, err = db.Exec(`INSERT INTO api_metadata (id, data_source_id, name, status, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		apiMetaID.String(), dataSource.ID.String(), "test_api", "active")
	if err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	schemaDAO := dao.NewTableSchemaDAO(db.DB)
	schema := datastore.NewTableSchema(ds.ID, apiMetaID, "test_table")

	err = schemaDAO.Create(nil, schema)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if schema.ID.IsEmpty() {
		t.Error("TableSchema ID should be set")
	}
}

func TestTableSchemaDAO_GetByID(t *testing.T) {
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
		
		CREATE TABLE IF NOT EXISTS api_metadata (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	dsDAO := dao.NewQuantDataStoreDAO(db.DB)
	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	err = dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data store: %v", err)
	}

	// Create data source first
	dataSourceDAO := dao.NewDataSourceDAO(db.DB)
	dataSource := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err = dataSourceDAO.Create(nil, dataSource)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiMetaID := shared.NewID()
	_, err = db.Exec(`INSERT INTO api_metadata (id, data_source_id, name, status, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		apiMetaID.String(), dataSource.ID.String(), "test_api", "active")
	if err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	schemaDAO := dao.NewTableSchemaDAO(db.DB)
	schema := datastore.NewTableSchema(ds.ID, apiMetaID, "test_table")
	err = schemaDAO.Create(nil, schema)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := schemaDAO.GetByID(nil, schema.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != schema.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, schema.ID)
	}
}

func TestTableSchemaDAO_GetByDataStore(t *testing.T) {
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
		
		CREATE TABLE IF NOT EXISTS api_metadata (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	dsDAO := dao.NewQuantDataStoreDAO(db.DB)
	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	err = dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data store: %v", err)
	}

	// Create data source first
	dataSourceDAO := dao.NewDataSourceDAO(db.DB)
	dataSource := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err = dataSourceDAO.Create(nil, dataSource)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiMetaID1 := shared.NewID()
	apiMetaID2 := shared.NewID()
	_, err = db.Exec(`INSERT INTO api_metadata (id, data_source_id, name, status, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		apiMetaID1.String(), dataSource.ID.String(), "test_api1", "active")
	if err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}
	_, err = db.Exec(`INSERT INTO api_metadata (id, data_source_id, name, status, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		apiMetaID2.String(), dataSource.ID.String(), "test_api2", "active")
	if err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	schemaDAO := dao.NewTableSchemaDAO(db.DB)
	schema1 := datastore.NewTableSchema(ds.ID, apiMetaID1, "table1")
	schema2 := datastore.NewTableSchema(ds.ID, apiMetaID2, "table2")

	err = schemaDAO.Create(nil, schema1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = schemaDAO.Create(nil, schema2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	schemas, err := schemaDAO.GetByDataStore(nil, ds.ID)
	if err != nil {
		t.Fatalf("GetByDataStore() error = %v", err)
	}

	if len(schemas) < 2 {
		t.Errorf("GetByDataStore() returned %d schemas, want at least 2", len(schemas))
	}
}

func TestTableSchemaDAO_GetByAPIMetadata(t *testing.T) {
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
		
		CREATE TABLE IF NOT EXISTS api_metadata (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	dsDAO := dao.NewQuantDataStoreDAO(db.DB)
	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	err = dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data store: %v", err)
	}

	// Create data source first
	dataSourceDAO := dao.NewDataSourceDAO(db.DB)
	dataSource := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err = dataSourceDAO.Create(nil, dataSource)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiMetaID := shared.NewID()
	_, err = db.Exec(`INSERT INTO api_metadata (id, data_source_id, name, status, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		apiMetaID.String(), dataSource.ID.String(), "test_api", "active")
	if err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	schemaDAO := dao.NewTableSchemaDAO(db.DB)
	schema := datastore.NewTableSchema(ds.ID, apiMetaID, "test_table")
	err = schemaDAO.Create(nil, schema)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := schemaDAO.GetByAPIMetadata(nil, apiMetaID)
	if err != nil {
		t.Fatalf("GetByAPIMetadata() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByAPIMetadata() returned nil")
	}

	if got.ID != schema.ID {
		t.Errorf("GetByAPIMetadata() ID = %s, want %s", got.ID, schema.ID)
	}
}

func TestTableSchemaDAO_Update(t *testing.T) {
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
		
		CREATE TABLE IF NOT EXISTS api_metadata (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	dsDAO := dao.NewQuantDataStoreDAO(db.DB)
	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	err = dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data store: %v", err)
	}

	// Create data source first
	dataSourceDAO := dao.NewDataSourceDAO(db.DB)
	dataSource := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err = dataSourceDAO.Create(nil, dataSource)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiMetaID := shared.NewID()
	_, err = db.Exec(`INSERT INTO api_metadata (id, data_source_id, name, status, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		apiMetaID.String(), dataSource.ID.String(), "test_api", "active")
	if err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	schemaDAO := dao.NewTableSchemaDAO(db.DB)
	schema := datastore.NewTableSchema(ds.ID, apiMetaID, "test_table")
	err = schemaDAO.Create(nil, schema)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	schema.SetPrimaryKeys([]string{"id", "date"})
	err = schemaDAO.Update(nil, schema)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := schemaDAO.GetByID(nil, schema.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if len(got.PrimaryKeys) != 2 {
		t.Errorf("Update() PrimaryKeys count = %d, want 2", len(got.PrimaryKeys))
	}
}

func TestTableSchemaDAO_DeleteByID(t *testing.T) {
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
		
		CREATE TABLE IF NOT EXISTS api_metadata (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	dsDAO := dao.NewQuantDataStoreDAO(db.DB)
	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	err = dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data store: %v", err)
	}

	// Create data source first
	dataSourceDAO := dao.NewDataSourceDAO(db.DB)
	dataSource := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err = dataSourceDAO.Create(nil, dataSource)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiMetaID := shared.NewID()
	_, err = db.Exec(`INSERT INTO api_metadata (id, data_source_id, name, status, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		apiMetaID.String(), dataSource.ID.String(), "test_api", "active")
	if err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	schemaDAO := dao.NewTableSchemaDAO(db.DB)
	schema := datastore.NewTableSchema(ds.ID, apiMetaID, "test_table")
	err = schemaDAO.Create(nil, schema)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = schemaDAO.DeleteByID(nil, schema.ID)
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := schemaDAO.GetByID(nil, schema.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the schema")
	}
}

func TestTableSchemaDAO_DeleteByDataStore(t *testing.T) {
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
		
		CREATE TABLE IF NOT EXISTS api_metadata (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create tables: %v", err)
	}

	dsDAO := dao.NewQuantDataStoreDAO(db.DB)
	ds := datastore.NewQuantDataStore("Test Store", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")
	err = dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data store: %v", err)
	}

	// Create data source first
	dataSourceDAO := dao.NewDataSourceDAO(db.DB)
	dataSource := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err = dataSourceDAO.Create(nil, dataSource)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiMetaID1 := shared.NewID()
	apiMetaID2 := shared.NewID()
	_, err = db.Exec(`INSERT INTO api_metadata (id, data_source_id, name, status, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		apiMetaID1.String(), dataSource.ID.String(), "test_api1", "active")
	if err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}
	_, err = db.Exec(`INSERT INTO api_metadata (id, data_source_id, name, status, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		apiMetaID2.String(), dataSource.ID.String(), "test_api2", "active")
	if err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	schemaDAO := dao.NewTableSchemaDAO(db.DB)
	schema1 := datastore.NewTableSchema(ds.ID, apiMetaID1, "table1")
	schema2 := datastore.NewTableSchema(ds.ID, apiMetaID2, "table2")

	err = schemaDAO.Create(nil, schema1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = schemaDAO.Create(nil, schema2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete all schemas for the data store
	err = schemaDAO.DeleteByDataStore(nil, ds.ID)
	if err != nil {
		t.Fatalf("DeleteByDataStore() error = %v", err)
	}

	// Verify all schemas are deleted
	schemas, err := schemaDAO.GetByDataStore(nil, ds.ID)
	if err != nil {
		t.Fatalf("GetByDataStore() error = %v", err)
	}

	if len(schemas) != 0 {
		t.Errorf("DeleteByDataStore() should remove all schemas, got %d remaining", len(schemas))
	}
}
