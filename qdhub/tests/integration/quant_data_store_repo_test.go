//go:build integration
// +build integration

package integration

import (
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/infrastructure/persistence/repository"
)

func TestQuantDataStoreRepository_Integration(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	repo := repository.NewQuantDataStoreRepository(db)

	t.Run("Create and Get", func(t *testing.T) {
		ds := datastore.NewQuantDataStore("Integration Test", "Test Description", datastore.DataStoreTypeDuckDB, "duckdb://test.db", "/data/test.db")

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

		// Note: Schema creation requires api_metadata to exist, so we skip schema test here
	})

	t.Run("Update", func(t *testing.T) {
		ds := datastore.NewQuantDataStore("Original", "Original Desc", datastore.DataStoreTypeDuckDB, "duckdb://old.db", "/data/old.db")
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
	})

	t.Run("Delete", func(t *testing.T) {
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
			t.Error("Delete() should remove the data store and cascade to schemas")
		}
	})

	t.Run("List", func(t *testing.T) {
		ds1 := datastore.NewQuantDataStore("List Test 1", "Desc 1", datastore.DataStoreTypeDuckDB, "duckdb://store1.db", "/data/store1.db")
		ds2 := datastore.NewQuantDataStore("List Test 2", "Desc 2", datastore.DataStoreTypeDuckDB, "duckdb://store2.db", "/data/store2.db")

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
