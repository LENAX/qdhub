// +build integration

package integration

import (
	"testing"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/infrastructure/persistence/repository"
)

func TestDataSourceRepository_Integration(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	repo := repository.NewDataSourceRepository(db)

	t.Run("Create and Get with aggregates", func(t *testing.T) {
		ds := metadata.NewDataSource("Integration Test", "Test Description", "https://api.test.com", "https://docs.test.com")
		
		category := metadata.NewAPICategory(ds.ID, "Stock", "Stock data", "/stock", nil, 1)
		ds.Categories = []metadata.APICategory{*category}
		
		api := metadata.NewAPIMetadata(ds.ID, "daily", "Daily Price", "Daily stock price", "/daily")
		api.CategoryID = &category.ID
		ds.APIs = []metadata.APIMetadata{*api}
		
		token := metadata.NewToken(ds.ID, "test-token-123", nil)
		ds.Token = token

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

		if len(got.Categories) != 1 {
			t.Errorf("Get() Categories count = %d, want 1", len(got.Categories))
		}

		if len(got.APIs) != 1 {
			t.Errorf("Get() APIs count = %d, want 1", len(got.APIs))
		}

		if got.Token == nil {
			t.Error("Get() Token should not be nil")
		}
	})

	t.Run("Update", func(t *testing.T) {
		ds := metadata.NewDataSource("Original", "Original Desc", "https://old.com", "https://old-docs.com")
		err := repo.Create(ds)
		if err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		ds.UpdateInfo("Updated", "Updated Desc", "https://new.com", "https://new-docs.com")
		err = repo.Update(ds)
		if err != nil {
			t.Fatalf("Update() error = %v", err)
		}

		got, err := repo.Get(ds.ID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}

		if got.Name != "Updated" {
			t.Errorf("Update() Name = %s, want Updated", got.Name)
		}
	})

	t.Run("Delete with cascade", func(t *testing.T) {
		ds := metadata.NewDataSource("To Delete", "Desc", "https://test.com", "https://docs.com")
		category := metadata.NewAPICategory(ds.ID, "Stock", "Stock data", "/stock", nil, 1)
		ds.Categories = []metadata.APICategory{*category}
		
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
			t.Error("Delete() should remove the data source and cascade to categories")
		}
	})

	t.Run("List", func(t *testing.T) {
		ds1 := metadata.NewDataSource("List Test 1", "Desc 1", "https://api1.com", "https://docs1.com")
		ds2 := metadata.NewDataSource("List Test 2", "Desc 2", "https://api2.com", "https://docs2.com")

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

		// Verify both are in the list
		found1, found2 := false, false
		for _, ds := range list {
			if ds.ID == ds1.ID {
				found1 = true
			}
			if ds.ID == ds2.ID {
				found2 = true
			}
		}

		if !found1 {
			t.Error("List() should contain ds1")
		}
		if !found2 {
			t.Error("List() should contain ds2")
		}
	})
}
