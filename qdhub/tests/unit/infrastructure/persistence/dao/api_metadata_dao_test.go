package dao_test

import (
	"testing"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/infrastructure/persistence/dao"
)

func TestAPIMetadataDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// First create data source and category
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
		t.Fatalf("Failed to create category: %v", err)
	}

	apiDAO := dao.NewAPIMetadataDAO(db.DB)
	api := metadata.NewAPIMetadata(ds.ID, "daily", "Daily Price", "Daily stock price", "/daily")
	api.CategoryID = &category.ID

	err = apiDAO.Create(nil, api)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if api.ID.IsEmpty() {
		t.Error("APIMetadata ID should be set")
	}
}

func TestAPIMetadataDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiDAO := dao.NewAPIMetadataDAO(db.DB)
	api := metadata.NewAPIMetadata(ds.ID, "daily", "Daily Price", "Daily stock price", "/daily")
	err = apiDAO.Create(nil, api)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := apiDAO.GetByID(nil, api.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != api.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, api.ID)
	}
}

func TestAPIMetadataDAO_ListByDataSource(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiDAO := dao.NewAPIMetadataDAO(db.DB)
	api1 := metadata.NewAPIMetadata(ds.ID, "daily", "Daily Price", "Daily stock price", "/daily")
	api2 := metadata.NewAPIMetadata(ds.ID, "weekly", "Weekly Price", "Weekly stock price", "/weekly")

	err = apiDAO.Create(nil, api1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = apiDAO.Create(nil, api2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := apiDAO.ListByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("ListByDataSource() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("ListByDataSource() returned %d APIs, want at least 2", len(list))
	}
}

func TestAPIMetadataDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiDAO := dao.NewAPIMetadataDAO(db.DB)
	api := metadata.NewAPIMetadata(ds.ID, "original", "Original Name", "Original Desc", "/original")
	err = apiDAO.Create(nil, api)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Update API
	api.DisplayName = "Updated Name"
	api.Description = "Updated Desc"
	err = apiDAO.Update(nil, api)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := apiDAO.GetByID(nil, api.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.DisplayName != "Updated Name" {
		t.Errorf("Update() DisplayName = %s, want Updated Name", got.DisplayName)
	}
}

func TestAPIMetadataDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiDAO := dao.NewAPIMetadataDAO(db.DB)
	api := metadata.NewAPIMetadata(ds.ID, "to_delete", "To Delete", "Desc", "/delete")
	err = apiDAO.Create(nil, api)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = apiDAO.DeleteByID(nil, api.ID)
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := apiDAO.GetByID(nil, api.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the API metadata")
	}
}

func TestTokenDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// First create a data source
	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	tokenDAO := dao.NewTokenDAO(db.DB)
	token := metadata.NewToken(ds.ID, "test-token-123", nil)

	err = tokenDAO.Create(nil, token)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if token.ID.IsEmpty() {
		t.Error("Token ID should be set")
	}
}

func TestTokenDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	tokenDAO := dao.NewTokenDAO(db.DB)
	token := metadata.NewToken(ds.ID, "test-token-123", nil)
	err = tokenDAO.Create(nil, token)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := tokenDAO.GetByID(nil, token.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != token.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, token.ID)
	}
}

func TestTokenDAO_GetByDataSource(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	tokenDAO := dao.NewTokenDAO(db.DB)
	token := metadata.NewToken(ds.ID, "test-token-123", nil)
	err = tokenDAO.Create(nil, token)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := tokenDAO.GetByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("GetByDataSource() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByDataSource() returned nil")
	}

	if got.ID != token.ID {
		t.Errorf("GetByDataSource() ID = %s, want %s", got.ID, token.ID)
	}
}

func TestTokenDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	tokenDAO := dao.NewTokenDAO(db.DB)
	token := metadata.NewToken(ds.ID, "old-token", nil)
	err = tokenDAO.Create(nil, token)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Update token
	token.TokenValue = "new-token"
	err = tokenDAO.Update(nil, token)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := tokenDAO.GetByID(nil, token.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.TokenValue != "new-token" {
		t.Errorf("Update() TokenValue = %s, want new-token", got.TokenValue)
	}
}

func TestTokenDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	tokenDAO := dao.NewTokenDAO(db.DB)
	token := metadata.NewToken(ds.ID, "to-delete", nil)
	err = tokenDAO.Create(nil, token)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = tokenDAO.DeleteByID(nil, token.ID)
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := tokenDAO.GetByID(nil, token.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the token")
	}
}

func TestAPIMetadataDAO_ListByCategory(t *testing.T) {
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
		t.Fatalf("Failed to create category: %v", err)
	}

	apiDAO := dao.NewAPIMetadataDAO(db.DB)
	api1 := metadata.NewAPIMetadata(ds.ID, "daily", "Daily Price", "Daily stock price", "/daily")
	api1.CategoryID = &category.ID
	api2 := metadata.NewAPIMetadata(ds.ID, "weekly", "Weekly Price", "Weekly stock price", "/weekly")
	api2.CategoryID = &category.ID

	err = apiDAO.Create(nil, api1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = apiDAO.Create(nil, api2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := apiDAO.ListByCategory(nil, category.ID)
	if err != nil {
		t.Fatalf("ListByCategory() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("ListByCategory() returned %d APIs, want at least 2", len(list))
	}
}

func TestAPIMetadataDAO_DeleteByDataSource(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	apiDAO := dao.NewAPIMetadataDAO(db.DB)
	api1 := metadata.NewAPIMetadata(ds.ID, "api1", "API 1", "Desc 1", "/api1")
	api2 := metadata.NewAPIMetadata(ds.ID, "api2", "API 2", "Desc 2", "/api2")

	err = apiDAO.Create(nil, api1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = apiDAO.Create(nil, api2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete all APIs for the data source
	err = apiDAO.DeleteByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("DeleteByDataSource() error = %v", err)
	}

	// Verify all APIs are deleted
	list, err := apiDAO.ListByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("ListByDataSource() error = %v", err)
	}

	if len(list) != 0 {
		t.Errorf("DeleteByDataSource() should remove all APIs, got %d remaining", len(list))
	}
}

func TestTokenDAO_DeleteByDataSource(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	dsDAO := dao.NewDataSourceDAO(db.DB)
	ds := metadata.NewDataSource("Test Source", "Test Description", "https://api.test.com", "https://docs.test.com")
	err := dsDAO.Create(nil, ds)
	if err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	tokenDAO := dao.NewTokenDAO(db.DB)
	token := metadata.NewToken(ds.ID, "test-token", nil)
	err = tokenDAO.Create(nil, token)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Delete token by data source
	err = tokenDAO.DeleteByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("DeleteByDataSource() error = %v", err)
	}

	// Verify token is deleted
	got, err := tokenDAO.GetByDataSource(nil, ds.ID)
	if err != nil {
		t.Fatalf("GetByDataSource() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByDataSource() should remove the token")
	}
}
