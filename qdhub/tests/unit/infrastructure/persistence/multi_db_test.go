package persistence_test

import (
	"os"
	"testing"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
	"qdhub/internal/infrastructure/persistence/repository"
)

// ==================== Database Connection Tests ====================

// TestSQLiteConnection tests SQLite database connection.
func TestSQLiteConnection(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test_sqlite_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	db, err := persistence.NewDB(tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to connect to SQLite: %v", err)
	}
	defer db.Close()

	if db.DriverName() != "sqlite3" {
		t.Errorf("DriverName() = %s, want sqlite3", db.DriverName())
	}

	var result int
	if err = db.Get(&result, "SELECT 1"); err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	t.Log("✓ SQLite connection successful")
}

// TestPostgresConnection tests PostgreSQL database connection.
func TestPostgresConnection(t *testing.T) {
	dsn := "postgres://postgres:testpass@localhost:5432/testdb?sslmode=disable"
	db, err := persistence.NewDBWithType(persistence.DBTypePostgres, dsn)
	if err != nil {
		t.Skipf("Skipping PostgreSQL test: %v", err)
	}
	defer db.Close()

	if db.DriverName() != "postgres" {
		t.Errorf("DriverName() = %s, want postgres", db.DriverName())
	}
	t.Log("✓ PostgreSQL connection successful")
}

// TestMySQLConnection tests MySQL database connection.
func TestMySQLConnection(t *testing.T) {
	dsn := "root:testpass@tcp(localhost:3306)/testdb?parseTime=true"
	db, err := persistence.NewDBWithType(persistence.DBTypeMySQL, dsn)
	if err != nil {
		t.Skipf("Skipping MySQL test: %v", err)
	}
	defer db.Close()

	if db.DriverName() != "mysql" {
		t.Errorf("DriverName() = %s, want mysql", db.DriverName())
	}
	t.Log("✓ MySQL connection successful")
}

// ==================== DAO/Repository CRUD Tests ====================

// dbTestCase represents a database test configuration.
type dbTestCase struct {
	name       string
	dbType     persistence.DBType
	dsn        string
	createSQLs []string // Multiple create statements for all required tables
}

// getTestCases returns all database test configurations.
func getTestCases(t *testing.T) []dbTestCase {
	// Create temp SQLite file
	tmpfile, err := os.CreateTemp("", "test_crud_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	// Common table definitions (SQLite/PostgreSQL compatible)
	sqlitePostgresTables := []string{
		`CREATE TABLE IF NOT EXISTS data_sources (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL,
			description TEXT,
			base_url VARCHAR(512),
			doc_url VARCHAR(512),
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS api_categories (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			description TEXT,
			parent_id VARCHAR(64),
			sort_order INTEGER DEFAULT 0,
			doc_path VARCHAR(512),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS api_metadata (
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
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS tokens (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			token_value TEXT NOT NULL,
			expires_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	// MySQL table definitions
	mysqlTables := []string{
		`CREATE TABLE IF NOT EXISTS data_sources (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL,
			description TEXT,
			base_url VARCHAR(512),
			doc_url VARCHAR(512),
			status VARCHAR(32) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS api_categories (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			description TEXT,
			parent_id VARCHAR(64),
			sort_order INTEGER DEFAULT 0,
			doc_path VARCHAR(512),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS api_metadata (
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
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS tokens (
			id VARCHAR(64) PRIMARY KEY,
			data_source_id VARCHAR(64) NOT NULL,
			token_value TEXT NOT NULL,
			expires_at TIMESTAMP NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	return []dbTestCase{
		{
			name:       "SQLite",
			dbType:     persistence.DBTypeSQLite,
			dsn:        tmpfile.Name(),
			createSQLs: sqlitePostgresTables,
		},
		{
			name:       "PostgreSQL",
			dbType:     persistence.DBTypePostgres,
			dsn:        "postgres://postgres:testpass@localhost:5432/testdb?sslmode=disable",
			createSQLs: sqlitePostgresTables,
		},
		{
			name:       "MySQL",
			dbType:     persistence.DBTypeMySQL,
			dsn:        "root:testpass@tcp(localhost:3306)/testdb?parseTime=true",
			createSQLs: mysqlTables,
		},
	}
}

// dropAllTables drops all test tables in correct order (respecting FK constraints).
func dropAllTables(db *persistence.DB) {
	tables := []string{"tokens", "api_metadata", "api_categories", "data_sources"}
	for _, table := range tables {
		db.Exec("DROP TABLE IF EXISTS " + table)
	}
}

// createAllTables creates all required tables.
func createAllTables(t *testing.T, db *persistence.DB, createSQLs []string) {
	for _, sql := range createSQLs {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
	}
}

// TestDAOCRUDOnAllDatabases tests DAO CRUD operations on all databases.
func TestDAOCRUDOnAllDatabases(t *testing.T) {
	testCases := getTestCases(t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Connect to database
			db, err := persistence.NewDBWithType(tc.dbType, tc.dsn)
			if err != nil {
				t.Skipf("Skipping %s test: %v", tc.name, err)
			}
			defer db.Close()

			// Clean up and create tables
			dropAllTables(db)
			createAllTables(t, db, tc.createSQLs)

			// Create DAO
			dataSourceDAO := dao.NewDataSourceDAO(db.DB)

			// Test Create
			entity := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
			err = dataSourceDAO.Create(nil, entity)
			if err != nil {
				t.Fatalf("DAO Create failed: %v", err)
			}
			t.Logf("  ✓ Create: ID=%s", entity.ID)

			// Test GetByID
			got, err := dataSourceDAO.GetByID(nil, entity.ID)
			if err != nil {
				t.Fatalf("DAO GetByID failed: %v", err)
			}
			if got == nil {
				t.Fatal("DAO GetByID returned nil")
			}
			if got.Name != "Test Source" {
				t.Errorf("GetByID Name = %s, want 'Test Source'", got.Name)
			}
			t.Log("  ✓ GetByID")

			// Test Update
			entity.Name = "Updated Source"
			err = dataSourceDAO.Update(nil, entity)
			if err != nil {
				t.Fatalf("DAO Update failed: %v", err)
			}
			got, _ = dataSourceDAO.GetByID(nil, entity.ID)
			if got.Name != "Updated Source" {
				t.Errorf("Update Name = %s, want 'Updated Source'", got.Name)
			}
			t.Log("  ✓ Update")

			// Test ListAll
			list, err := dataSourceDAO.ListAll(nil)
			if err != nil {
				t.Fatalf("DAO ListAll failed: %v", err)
			}
			if len(list) != 1 {
				t.Errorf("ListAll count = %d, want 1", len(list))
			}
			t.Log("  ✓ ListAll")

			// Test DeleteByID
			err = dataSourceDAO.DeleteByID(nil, entity.ID)
			if err != nil {
				t.Fatalf("DAO DeleteByID failed: %v", err)
			}
			got, _ = dataSourceDAO.GetByID(nil, entity.ID)
			if got != nil {
				t.Error("DeleteByID should remove the entity")
			}
			t.Log("  ✓ DeleteByID")

			t.Logf("✓ %s DAO CRUD test passed", tc.name)
		})
	}
}

// TestRepositoryCRUDOnAllDatabases tests Repository CRUD operations on all databases.
func TestRepositoryCRUDOnAllDatabases(t *testing.T) {
	testCases := getTestCases(t)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Connect to database
			db, err := persistence.NewDBWithType(tc.dbType, tc.dsn)
			if err != nil {
				t.Skipf("Skipping %s test: %v", tc.name, err)
			}
			defer db.Close()

			// Clean up and create all tables
			dropAllTables(db)
			createAllTables(t, db, tc.createSQLs)

			// Create Repository
			repo := repository.NewDataSourceRepository(db)

			// Test Create
			entity := metadata.NewDataSource("Repo Test", "Description", "https://api.test.com", "https://docs.test.com")
			err = repo.Create(entity)
			if err != nil {
				t.Fatalf("Repository Create failed: %v", err)
			}
			t.Logf("  ✓ Create: ID=%s", entity.ID)

			// Test Get
			got, err := repo.Get(entity.ID)
			if err != nil {
				t.Fatalf("Repository Get failed: %v", err)
			}
			if got == nil || got.Name != "Repo Test" {
				t.Error("Repository Get returned incorrect data")
			}
			t.Log("  ✓ Get")

			// Test FindBy with conditions
			results, err := repo.FindBy(shared.Eq("name", "Repo Test"))
			if err != nil {
				t.Fatalf("Repository FindBy failed: %v", err)
			}
			if len(results) != 1 {
				t.Errorf("FindBy count = %d, want 1", len(results))
			}
			t.Log("  ✓ FindBy")

			// Test Update
			entity.Name = "Updated Repo Test"
			err = repo.Update(entity)
			if err != nil {
				t.Fatalf("Repository Update failed: %v", err)
			}
			t.Log("  ✓ Update")

			// Test List
			list, err := repo.List()
			if err != nil {
				t.Fatalf("Repository List failed: %v", err)
			}
			if len(list) != 1 {
				t.Errorf("List count = %d, want 1", len(list))
			}
			t.Log("  ✓ List")

			// Test Delete
			err = repo.Delete(entity.ID)
			if err != nil {
				t.Fatalf("Repository Delete failed: %v", err)
			}
			got, _ = repo.Get(entity.ID)
			if got != nil {
				t.Error("Delete should remove the entity")
			}
			t.Log("  ✓ Delete")

			t.Logf("✓ %s Repository CRUD test passed", tc.name)
		})
	}
}
