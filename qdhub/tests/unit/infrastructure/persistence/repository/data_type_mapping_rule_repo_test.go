package repository_test

import (
	"os"
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// setupMappingRuleTestDB creates a temporary database for DataTypeMappingRule testing
func setupMappingRuleTestDB(t *testing.T) (*persistence.DB, func()) {
	tmpfile, err := os.CreateTemp("", "test_mapping_rule_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()

	dsn := tmpfile.Name()
	db, err := persistence.NewDB(dsn)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create table for DataTypeMappingRule
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS data_type_mapping_rules (
			id VARCHAR(64) PRIMARY KEY,
			data_source_type VARCHAR(32) NOT NULL,
			source_type VARCHAR(64) NOT NULL,
			target_db_type VARCHAR(32) NOT NULL,
			target_type VARCHAR(64) NOT NULL,
			field_pattern VARCHAR(256),
			priority INTEGER DEFAULT 0,
			is_default INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(data_source_type, source_type, target_db_type, field_pattern)
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

func TestDataTypeMappingRuleRepository_Create(t *testing.T) {
	db, cleanup := setupMappingRuleTestDB(t)
	defer cleanup()

	repo := repository.NewDataTypeMappingRuleRepository(db)

	rule := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)

	err := repo.Create(rule)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if rule.ID.IsEmpty() {
		t.Error("DataTypeMappingRule ID should be set after creation")
	}
}

func TestDataTypeMappingRuleRepository_Get(t *testing.T) {
	db, cleanup := setupMappingRuleTestDB(t)
	defer cleanup()

	repo := repository.NewDataTypeMappingRuleRepository(db)

	rule := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	err := repo.Create(rule)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := repo.Get(rule.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got == nil {
		t.Fatal("Get() returned nil")
	}

	if got.ID != rule.ID {
		t.Errorf("Get() ID = %s, want %s", got.ID, rule.ID)
	}

	if got.SourceType != "str" {
		t.Errorf("Get() SourceType = %s, want str", got.SourceType)
	}
}

func TestDataTypeMappingRuleRepository_GetBySourceAndTarget(t *testing.T) {
	db, cleanup := setupMappingRuleTestDB(t)
	defer cleanup()

	repo := repository.NewDataTypeMappingRuleRepository(db)

	rule1 := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	rule2 := datastore.NewDataTypeMappingRule("tushare", "float", "duckdb", "DOUBLE", 100, true)

	err := repo.Create(rule1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = repo.Create(rule2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	rules, err := repo.GetBySourceAndTarget("tushare", "duckdb")
	if err != nil {
		t.Fatalf("GetBySourceAndTarget() error = %v", err)
	}

	if len(rules) < 2 {
		t.Errorf("GetBySourceAndTarget() returned %d rules, want at least 2", len(rules))
	}
}

func TestDataTypeMappingRuleRepository_SaveBatch(t *testing.T) {
	db, cleanup := setupMappingRuleTestDB(t)
	defer cleanup()

	repo := repository.NewDataTypeMappingRuleRepository(db)

	rules := []*datastore.DataTypeMappingRule{
		datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true),
		datastore.NewDataTypeMappingRule("tushare", "float", "duckdb", "DOUBLE", 100, true),
		datastore.NewDataTypeMappingRule("tushare", "int", "duckdb", "INTEGER", 100, true),
	}

	err := repo.SaveBatch(rules)
	if err != nil {
		t.Fatalf("SaveBatch() error = %v", err)
	}

	// Verify all rules were created
	list, err := repo.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(list) < 3 {
		t.Errorf("List() returned %d rules, want at least 3", len(list))
	}
}

func TestDataTypeMappingRuleRepository_InitDefaultRules(t *testing.T) {
	db, cleanup := setupMappingRuleTestDB(t)
	defer cleanup()

	repo := repository.NewDataTypeMappingRuleRepository(db)

	err := repo.InitDefaultRules()
	if err != nil {
		t.Fatalf("InitDefaultRules() error = %v", err)
	}

	// Verify default rules were created
	list, err := repo.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(list) < 5 {
		t.Errorf("InitDefaultRules() should create at least 5 rules, got %d", len(list))
	}
}

func TestDataTypeMappingRuleRepository_Update(t *testing.T) {
	db, cleanup := setupMappingRuleTestDB(t)
	defer cleanup()

	repo := repository.NewDataTypeMappingRuleRepository(db)

	rule := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	err := repo.Create(rule)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Update rule
	rule.TargetType = "TEXT"
	rule.Priority = 200
	err = repo.Update(rule)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := repo.Get(rule.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got.TargetType != "TEXT" {
		t.Errorf("Update() TargetType = %s, want TEXT", got.TargetType)
	}

	if got.Priority != 200 {
		t.Errorf("Update() Priority = %d, want 200", got.Priority)
	}
}

func TestDataTypeMappingRuleRepository_Delete(t *testing.T) {
	db, cleanup := setupMappingRuleTestDB(t)
	defer cleanup()

	repo := repository.NewDataTypeMappingRuleRepository(db)

	rule := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	err := repo.Create(rule)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = repo.Delete(rule.ID)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got, err := repo.Get(rule.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != nil {
		t.Error("Delete() should remove the rule")
	}
}

func TestDataTypeMappingRuleRepository_List(t *testing.T) {
	db, cleanup := setupMappingRuleTestDB(t)
	defer cleanup()

	repo := repository.NewDataTypeMappingRuleRepository(db)

	rule1 := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	rule2 := datastore.NewDataTypeMappingRule("tushare", "float", "duckdb", "DOUBLE", 100, true)

	err := repo.Create(rule1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = repo.Create(rule2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := repo.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("List() returned %d rules, want at least 2", len(list))
	}
}
