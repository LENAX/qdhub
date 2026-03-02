package dao_test

import (
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/infrastructure/persistence/dao"
)

func TestDataTypeMappingRuleDAO_Create(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	// Add data_type_mapping_rules table
	_, err := db.Exec(`
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
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewDataTypeMappingRuleDAO(db.DB)

	rule := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)

	err = dao.Create(nil, rule)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if rule.ID.IsEmpty() {
		t.Error("DataTypeMappingRule ID should be set")
	}
}

func TestDataTypeMappingRuleDAO_GetByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewDataTypeMappingRuleDAO(db.DB)

	rule := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	err = dao.Create(nil, rule)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := dao.GetByID(nil, rule.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got == nil {
		t.Fatal("GetByID() returned nil")
	}

	if got.ID != rule.ID {
		t.Errorf("GetByID() ID = %s, want %s", got.ID, rule.ID)
	}
}

func TestDataTypeMappingRuleDAO_GetBySourceAndTarget(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewDataTypeMappingRuleDAO(db.DB)

	rule1 := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	rule2 := datastore.NewDataTypeMappingRule("tushare", "float", "duckdb", "DOUBLE", 100, true)

	err = dao.Create(nil, rule1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = dao.Create(nil, rule2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	rules, err := dao.GetBySourceAndTarget(nil, "tushare", "duckdb")
	if err != nil {
		t.Fatalf("GetBySourceAndTarget() error = %v", err)
	}

	if len(rules) < 2 {
		t.Errorf("GetBySourceAndTarget() returned %d rules, want at least 2", len(rules))
	}
}

func TestDataTypeMappingRuleDAO_SaveBatch(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewDataTypeMappingRuleDAO(db.DB)

	rules := []*datastore.DataTypeMappingRule{
		datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true),
		datastore.NewDataTypeMappingRule("tushare", "float", "duckdb", "DOUBLE", 100, true),
		datastore.NewDataTypeMappingRule("tushare", "int", "duckdb", "INTEGER", 100, true),
	}

	err = dao.SaveBatch(nil, rules)
	if err != nil {
		t.Fatalf("SaveBatch() error = %v", err)
	}

	// Verify all rules were created
	list, err := dao.ListAll(nil)
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}

	if len(list) < 3 {
		t.Errorf("ListAll() returned %d rules, want at least 3", len(list))
	}
}

func TestDataTypeMappingRuleDAO_Update(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewDataTypeMappingRuleDAO(db.DB)

	rule := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	err = dao.Create(nil, rule)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	rule.TargetType = "TEXT"
	rule.Priority = 200
	err = dao.Update(nil, rule)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := dao.GetByID(nil, rule.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.TargetType != "TEXT" {
		t.Errorf("Update() TargetType = %s, want TEXT", got.TargetType)
	}

	if got.Priority != 200 {
		t.Errorf("Update() Priority = %d, want 200", got.Priority)
	}
}

func TestDataTypeMappingRuleDAO_DeleteByID(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewDataTypeMappingRuleDAO(db.DB)

	rule := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	err = dao.Create(nil, rule)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = dao.DeleteByID(nil, rule.ID)
	if err != nil {
		t.Fatalf("DeleteByID() error = %v", err)
	}

	got, err := dao.GetByID(nil, rule.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got != nil {
		t.Error("DeleteByID() should remove the rule")
	}
}

func TestDataTypeMappingRuleDAO_ListAll(t *testing.T) {
	db, cleanup := setupDAOTestDB(t)
	defer cleanup()

	_, err := db.Exec(`
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
		t.Fatalf("Failed to create table: %v", err)
	}

	dao := dao.NewDataTypeMappingRuleDAO(db.DB)

	rule1 := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	rule2 := datastore.NewDataTypeMappingRule("tushare", "float", "duckdb", "DOUBLE", 100, true)

	err = dao.Create(nil, rule1)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	err = dao.Create(nil, rule2)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	list, err := dao.ListAll(nil)
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}

	if len(list) < 2 {
		t.Errorf("ListAll() returned %d rules, want at least 2", len(list))
	}
}
