package repository_test

import (
	"context"
	"os"
	"testing"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// setupMetadataTestDB creates a temporary database for Metadata testing
func setupMetadataTestDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "test_metadata_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create tables for Metadata aggregate
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS data_sources (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL UNIQUE,
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

// Helper to create a data source in the database (for setting up test data)
func createTestDataSource(t *testing.T, db *persistence.DB, name string) *metadata.DataSource {
	ds := metadata.NewDataSource(name, "Test Description", "https://api.test.com", "https://docs.test.com")
	_, err := db.Exec(
		`INSERT INTO data_sources (id, name, description, base_url, doc_url, status, created_at, updated_at) 
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		ds.ID.String(), ds.Name, ds.Description, ds.BaseURL, ds.DocURL, ds.Status, 
		ds.CreatedAt.ToTime(), ds.UpdatedAt.ToTime(),
	)
	if err != nil {
		t.Fatalf("Failed to create test data source: %v", err)
	}
	return ds
}

// ==================== Category Tests ====================

func TestMetadataRepository_SaveCategories(t *testing.T) {
	db, cleanup := setupMetadataTestDB(t)
	defer cleanup()

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	ds := createTestDataSource(t, db, "Test Source")

	t.Run("Save new categories", func(t *testing.T) {
		cat1 := metadata.NewAPICategory(ds.ID, "Stock", "Stock data", "/stock", nil, 1)
		cat2 := metadata.NewAPICategory(ds.ID, "Forex", "Forex data", "/forex", nil, 2)

		err := repo.SaveCategories(ctx, []metadata.APICategory{*cat1, *cat2})
		if err != nil {
			t.Fatalf("SaveCategories() error = %v", err)
		}

		// Verify
		categories, err := repo.ListCategoriesByDataSource(ctx, ds.ID)
		if err != nil {
			t.Fatalf("ListCategoriesByDataSource() error = %v", err)
		}
		if len(categories) != 2 {
			t.Errorf("Category count = %d, want 2", len(categories))
		}
	})

	t.Run("Update existing category (upsert)", func(t *testing.T) {
		categories, _ := repo.ListCategoriesByDataSource(ctx, ds.ID)
		if len(categories) == 0 {
			t.Skip("No categories to update")
		}

		existingCat := categories[0]
		existingCat.Description = "Updated description"

		err := repo.SaveCategories(ctx, []metadata.APICategory{existingCat})
		if err != nil {
			t.Fatalf("SaveCategories() error = %v", err)
		}

		// Verify update
		updatedCategories, _ := repo.ListCategoriesByDataSource(ctx, ds.ID)
		found := false
		for _, cat := range updatedCategories {
			if cat.ID == existingCat.ID && cat.Description == "Updated description" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Category description was not updated")
		}
	})
}

func TestMetadataRepository_DeleteCategoriesByDataSource(t *testing.T) {
	db, cleanup := setupMetadataTestDB(t)
	defer cleanup()

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	ds := createTestDataSource(t, db, "Test Source")

	// Create categories
	cat1 := metadata.NewAPICategory(ds.ID, "Stock", "Stock data", "/stock", nil, 1)
	cat2 := metadata.NewAPICategory(ds.ID, "Forex", "Forex data", "/forex", nil, 2)
	repo.SaveCategories(ctx, []metadata.APICategory{*cat1, *cat2})

	// Delete
	err := repo.DeleteCategoriesByDataSource(ctx, ds.ID)
	if err != nil {
		t.Fatalf("DeleteCategoriesByDataSource() error = %v", err)
	}

	// Verify
	categories, _ := repo.ListCategoriesByDataSource(ctx, ds.ID)
	if len(categories) != 0 {
		t.Errorf("Category count = %d, want 0 after deletion", len(categories))
	}
}

func TestMetadataRepository_ListCategoriesByDataSource(t *testing.T) {
	db, cleanup := setupMetadataTestDB(t)
	defer cleanup()

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	ds1 := createTestDataSource(t, db, "Source 1")
	ds2 := createTestDataSource(t, db, "Source 2")

	// Create categories for ds1
	cat1 := metadata.NewAPICategory(ds1.ID, "Stock", "Stock data", "/stock", nil, 1)
	cat2 := metadata.NewAPICategory(ds1.ID, "Forex", "Forex data", "/forex", nil, 2)
	repo.SaveCategories(ctx, []metadata.APICategory{*cat1, *cat2})

	// Create category for ds2
	cat3 := metadata.NewAPICategory(ds2.ID, "Crypto", "Crypto data", "/crypto", nil, 1)
	repo.SaveCategories(ctx, []metadata.APICategory{*cat3})

	t.Run("List categories for ds1", func(t *testing.T) {
		categories, err := repo.ListCategoriesByDataSource(ctx, ds1.ID)
		if err != nil {
			t.Fatalf("ListCategoriesByDataSource() error = %v", err)
		}
		if len(categories) != 2 {
			t.Errorf("Category count = %d, want 2", len(categories))
		}
	})

	t.Run("List categories for ds2", func(t *testing.T) {
		categories, err := repo.ListCategoriesByDataSource(ctx, ds2.ID)
		if err != nil {
			t.Fatalf("ListCategoriesByDataSource() error = %v", err)
		}
		if len(categories) != 1 {
			t.Errorf("Category count = %d, want 1", len(categories))
		}
	})

	t.Run("List categories for non-existent source", func(t *testing.T) {
		categories, err := repo.ListCategoriesByDataSource(ctx, shared.NewID())
		if err != nil {
			t.Fatalf("ListCategoriesByDataSource() error = %v", err)
		}
		if len(categories) != 0 {
			t.Errorf("Category count = %d, want 0", len(categories))
		}
	})
}

// ==================== API Metadata Tests ====================

func TestMetadataRepository_SaveAPIMetadata(t *testing.T) {
	db, cleanup := setupMetadataTestDB(t)
	defer cleanup()

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	ds := createTestDataSource(t, db, "Test Source")

	t.Run("Save new API metadata", func(t *testing.T) {
		api := metadata.NewAPIMetadata(ds.ID, "daily", "Daily Price", "Daily stock price", "/daily")

		err := repo.SaveAPIMetadata(ctx, api)
		if err != nil {
			t.Fatalf("SaveAPIMetadata() error = %v", err)
		}

		// Verify
		got, err := repo.GetAPIMetadata(ctx, api.ID)
		if err != nil {
			t.Fatalf("GetAPIMetadata() error = %v", err)
		}
		if got == nil {
			t.Fatal("GetAPIMetadata() returned nil")
		}
		if got.Name != "daily" {
			t.Errorf("API name = %s, want 'daily'", got.Name)
		}
	})

	t.Run("Update existing API metadata (upsert)", func(t *testing.T) {
		// Create initial
		api := metadata.NewAPIMetadata(ds.ID, "weekly", "Weekly Price", "Weekly stock price", "/weekly")
		repo.SaveAPIMetadata(ctx, api)

		// Update
		api.Description = "Updated weekly description"
		err := repo.SaveAPIMetadata(ctx, api)
		if err != nil {
			t.Fatalf("SaveAPIMetadata() error = %v", err)
		}

		// Verify
		got, _ := repo.GetAPIMetadata(ctx, api.ID)
		if got.Description != "Updated weekly description" {
			t.Errorf("API description = %s, want 'Updated weekly description'", got.Description)
		}
	})
}

func TestMetadataRepository_SaveAPIMetadataBatch(t *testing.T) {
	db, cleanup := setupMetadataTestDB(t)
	defer cleanup()

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	ds := createTestDataSource(t, db, "Test Source")

	t.Run("Batch save API metadata", func(t *testing.T) {
		api1 := metadata.NewAPIMetadata(ds.ID, "api1", "API 1", "First API", "/api1")
		api2 := metadata.NewAPIMetadata(ds.ID, "api2", "API 2", "Second API", "/api2")
		api3 := metadata.NewAPIMetadata(ds.ID, "api3", "API 3", "Third API", "/api3")

		err := repo.SaveAPIMetadataBatch(ctx, []metadata.APIMetadata{*api1, *api2, *api3})
		if err != nil {
			t.Fatalf("SaveAPIMetadataBatch() error = %v", err)
		}

		// Verify
		apis, err := repo.ListAPIMetadataByDataSource(ctx, ds.ID)
		if err != nil {
			t.Fatalf("ListAPIMetadataByDataSource() error = %v", err)
		}
		if len(apis) != 3 {
			t.Errorf("API count = %d, want 3", len(apis))
		}
	})
}

func TestMetadataRepository_DeleteAPIMetadata(t *testing.T) {
	db, cleanup := setupMetadataTestDB(t)
	defer cleanup()

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	ds := createTestDataSource(t, db, "Test Source")

	// Create API
	api := metadata.NewAPIMetadata(ds.ID, "daily", "Daily Price", "Daily stock price", "/daily")
	repo.SaveAPIMetadata(ctx, api)

	// Delete
	err := repo.DeleteAPIMetadata(ctx, api.ID)
	if err != nil {
		t.Fatalf("DeleteAPIMetadata() error = %v", err)
	}

	// Verify
	got, _ := repo.GetAPIMetadata(ctx, api.ID)
	if got != nil {
		t.Error("API metadata should be deleted")
	}
}

func TestMetadataRepository_DeleteAPIMetadataByDataSource(t *testing.T) {
	db, cleanup := setupMetadataTestDB(t)
	defer cleanup()

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	ds := createTestDataSource(t, db, "Test Source")

	// Create multiple APIs
	api1 := metadata.NewAPIMetadata(ds.ID, "api1", "API 1", "First API", "/api1")
	api2 := metadata.NewAPIMetadata(ds.ID, "api2", "API 2", "Second API", "/api2")
	repo.SaveAPIMetadataBatch(ctx, []metadata.APIMetadata{*api1, *api2})

	// Delete all
	err := repo.DeleteAPIMetadataByDataSource(ctx, ds.ID)
	if err != nil {
		t.Fatalf("DeleteAPIMetadataByDataSource() error = %v", err)
	}

	// Verify
	apis, _ := repo.ListAPIMetadataByDataSource(ctx, ds.ID)
	if len(apis) != 0 {
		t.Errorf("API count = %d, want 0 after deletion", len(apis))
	}
}

func TestMetadataRepository_ListAPIMetadataByDataSource(t *testing.T) {
	db, cleanup := setupMetadataTestDB(t)
	defer cleanup()

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	ds1 := createTestDataSource(t, db, "Source 1")
	ds2 := createTestDataSource(t, db, "Source 2")

	// Create APIs for ds1
	api1 := metadata.NewAPIMetadata(ds1.ID, "api1", "API 1", "First API", "/api1")
	api2 := metadata.NewAPIMetadata(ds1.ID, "api2", "API 2", "Second API", "/api2")
	repo.SaveAPIMetadataBatch(ctx, []metadata.APIMetadata{*api1, *api2})

	// Create API for ds2
	api3 := metadata.NewAPIMetadata(ds2.ID, "api3", "API 3", "Third API", "/api3")
	repo.SaveAPIMetadata(ctx, api3)

	t.Run("List APIs for ds1", func(t *testing.T) {
		apis, err := repo.ListAPIMetadataByDataSource(ctx, ds1.ID)
		if err != nil {
			t.Fatalf("ListAPIMetadataByDataSource() error = %v", err)
		}
		if len(apis) != 2 {
			t.Errorf("API count = %d, want 2", len(apis))
		}
	})

	t.Run("List APIs for ds2", func(t *testing.T) {
		apis, err := repo.ListAPIMetadataByDataSource(ctx, ds2.ID)
		if err != nil {
			t.Fatalf("ListAPIMetadataByDataSource() error = %v", err)
		}
		if len(apis) != 1 {
			t.Errorf("API count = %d, want 1", len(apis))
		}
	})
}

// ==================== GetDataSource (Aggregated Query) Tests ====================

func TestMetadataRepository_GetDataSource(t *testing.T) {
	db, cleanup := setupMetadataTestDB(t)
	defer cleanup()

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	ds := createTestDataSource(t, db, "Test Source")

	// Create categories
	cat := metadata.NewAPICategory(ds.ID, "Stock", "Stock data", "/stock", nil, 1)
	repo.SaveCategories(ctx, []metadata.APICategory{*cat})

	// Create APIs
	api := metadata.NewAPIMetadata(ds.ID, "daily", "Daily Price", "Daily stock price", "/daily")
	repo.SaveAPIMetadata(ctx, api)

	// Create token
	token := metadata.NewToken(ds.ID, "test-token-123", nil)
	_, err := db.Exec(
		`INSERT INTO tokens (id, data_source_id, token_value, expires_at, created_at) VALUES (?, ?, ?, ?, ?)`,
		token.ID.String(), token.DataSourceID.String(), token.TokenValue, token.ExpiresAt, token.CreatedAt.ToTime(),
	)
	if err != nil {
		t.Fatalf("Failed to create token: %v", err)
	}

	t.Run("Get data source with all aggregates", func(t *testing.T) {
		got, err := repo.GetDataSource(ctx, ds.ID)
		if err != nil {
			t.Fatalf("GetDataSource() error = %v", err)
		}
		if got == nil {
			t.Fatal("GetDataSource() returned nil")
		}

		if got.ID != ds.ID {
			t.Errorf("ID = %s, want %s", got.ID, ds.ID)
		}

		if len(got.Categories) != 1 {
			t.Errorf("Categories count = %d, want 1", len(got.Categories))
		}

		if len(got.APIs) != 1 {
			t.Errorf("APIs count = %d, want 1", len(got.APIs))
		}

		if got.Token == nil {
			t.Error("Token should not be nil")
		}
	})

	t.Run("Get non-existent data source", func(t *testing.T) {
		got, err := repo.GetDataSource(ctx, shared.NewID())
		if err != nil {
			t.Fatalf("GetDataSource() error = %v", err)
		}
		if got != nil {
			t.Error("GetDataSource() should return nil for non-existent ID")
		}
	})
}

// ==================== GetToken Tests ====================

func TestMetadataRepository_GetToken(t *testing.T) {
	db, cleanup := setupMetadataTestDB(t)
	defer cleanup()

	repo := repository.NewMetadataRepository(db)
	ctx := context.Background()

	ds := createTestDataSource(t, db, "Test Source")

	t.Run("Get non-existent token", func(t *testing.T) {
		token, err := repo.GetToken(ctx, ds.ID)
		if err != nil {
			t.Fatalf("GetToken() error = %v", err)
		}
		if token != nil {
			t.Error("GetToken() should return nil for data source without token")
		}
	})

	t.Run("Get existing token", func(t *testing.T) {
		// Create token
		token := metadata.NewToken(ds.ID, "test-token-456", nil)
		_, err := db.Exec(
			`INSERT INTO tokens (id, data_source_id, token_value, expires_at, created_at) VALUES (?, ?, ?, ?, ?)`,
			token.ID.String(), token.DataSourceID.String(), token.TokenValue, token.ExpiresAt, token.CreatedAt.ToTime(),
		)
		if err != nil {
			t.Fatalf("Failed to create token: %v", err)
		}

		got, err := repo.GetToken(ctx, ds.ID)
		if err != nil {
			t.Fatalf("GetToken() error = %v", err)
		}
		if got == nil {
			t.Fatal("GetToken() returned nil")
		}
		if got.TokenValue != "test-token-456" {
			t.Errorf("TokenValue = %s, want 'test-token-456'", got.TokenValue)
		}
	})
}
