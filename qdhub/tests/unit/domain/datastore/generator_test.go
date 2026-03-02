package datastore_test

import (
	"strings"
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

func TestSchemaGenerator_GenerateDDL(t *testing.T) {
	generator := datastore.NewSchemaGenerator()

	validSchema := func() *datastore.TableSchema {
		return &datastore.TableSchema{
			ID:            shared.NewID(),
			DataStoreID:   shared.NewID(),
			APIMetadataID: shared.NewID(),
			TableName:     "daily_prices",
			Columns: []datastore.ColumnDef{
				{Name: "ts_code", SourceType: "str", TargetType: "VARCHAR(20)", Nullable: false},
				{Name: "trade_date", SourceType: "str", TargetType: "DATE", Nullable: false},
				{Name: "close", SourceType: "float", TargetType: "DOUBLE", Nullable: true},
			},
			PrimaryKeys: []string{"ts_code", "trade_date"},
			Status:      datastore.SchemaStatusPending,
		}
	}

	t.Run("generate DDL for DuckDB", func(t *testing.T) {
		schema := validSchema()
		ddl, err := generator.GenerateDDL(schema, datastore.DataStoreTypeDuckDB)

		if err != nil {
			t.Fatalf("GenerateDDL() error = %v", err)
		}

		// Check DDL contains expected elements
		if !strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS daily_prices") {
			t.Error("DDL should contain CREATE TABLE statement")
		}

		if !strings.Contains(ddl, "ts_code VARCHAR(20)") {
			t.Error("DDL should contain ts_code column")
		}

		if !strings.Contains(ddl, "NOT NULL") {
			t.Error("DDL should contain NOT NULL for non-nullable columns")
		}

		if !strings.Contains(ddl, "PRIMARY KEY (ts_code, trade_date)") {
			t.Error("DDL should contain PRIMARY KEY constraint")
		}
	})

	t.Run("generate DDL without primary keys", func(t *testing.T) {
		schema := validSchema()
		schema.PrimaryKeys = []string{}

		ddl, err := generator.GenerateDDL(schema, datastore.DataStoreTypeDuckDB)

		if err != nil {
			t.Fatalf("GenerateDDL() error = %v", err)
		}

		if strings.Contains(ddl, "PRIMARY KEY") {
			t.Error("DDL should not contain PRIMARY KEY when none specified")
		}
	})

	t.Run("generate DDL with default value", func(t *testing.T) {
		schema := validSchema()
		defaultVal := "0.0"
		schema.Columns[2].Default = &defaultVal

		ddl, err := generator.GenerateDDL(schema, datastore.DataStoreTypeDuckDB)

		if err != nil {
			t.Fatalf("GenerateDDL() error = %v", err)
		}

		if !strings.Contains(ddl, "DEFAULT 0.0") {
			t.Error("DDL should contain DEFAULT value")
		}
	})

	t.Run("nil schema returns error", func(t *testing.T) {
		_, err := generator.GenerateDDL(nil, datastore.DataStoreTypeDuckDB)

		if err == nil {
			t.Error("GenerateDDL() should return error for nil schema")
		}
	})

	t.Run("generate DDL for different DB types", func(t *testing.T) {
		schema := validSchema()
		dbTypes := []datastore.DataStoreType{
			datastore.DataStoreTypeDuckDB,
			datastore.DataStoreTypeClickHouse,
			datastore.DataStoreTypePostgreSQL,
		}

		for _, dbType := range dbTypes {
			t.Run(string(dbType), func(t *testing.T) {
				ddl, err := generator.GenerateDDL(schema, dbType)

				if err != nil {
					t.Errorf("GenerateDDL() error = %v for %s", err, dbType)
				}

				if ddl == "" {
					t.Errorf("GenerateDDL() returned empty DDL for %s", dbType)
				}
			})
		}
	})
}

func TestSchemaGenerator_GenerateDropDDL(t *testing.T) {
	generator := datastore.NewSchemaGenerator()

	tests := []struct {
		name      string
		tableName string
		dbType    datastore.DataStoreType
		expected  string
	}{
		{
			name:      "DuckDB",
			tableName: "daily_prices",
			dbType:    datastore.DataStoreTypeDuckDB,
			expected:  "DROP TABLE IF EXISTS daily_prices",
		},
		{
			name:      "PostgreSQL",
			tableName: "stock_data",
			dbType:    datastore.DataStoreTypePostgreSQL,
			expected:  "DROP TABLE IF EXISTS stock_data",
		},
		{
			name:      "ClickHouse",
			tableName: "trades",
			dbType:    datastore.DataStoreTypeClickHouse,
			expected:  "DROP TABLE IF EXISTS trades",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddl := generator.GenerateDropDDL(tt.tableName, tt.dbType)

			if ddl != tt.expected {
				t.Errorf("GenerateDropDDL() = %v, expected %v", ddl, tt.expected)
			}
		})
	}
}

func TestSchemaGenerator_ValidateDDL(t *testing.T) {
	generator := datastore.NewSchemaGenerator()

	tests := []struct {
		name    string
		ddl     string
		wantErr bool
	}{
		{
			name:    "valid CREATE TABLE",
			ddl:     "CREATE TABLE test (id INT)",
			wantErr: false,
		},
		{
			name:    "valid DROP TABLE",
			ddl:     "DROP TABLE IF EXISTS test",
			wantErr: false,
		},
		{
			name:    "valid ALTER TABLE",
			ddl:     "ALTER TABLE test ADD COLUMN name VARCHAR",
			wantErr: false,
		},
		{
			name:    "valid CREATE INDEX",
			ddl:     "CREATE INDEX idx_name ON test(name)",
			wantErr: false,
		},
		{
			name:    "empty DDL",
			ddl:     "",
			wantErr: true,
		},
		{
			name:    "whitespace only DDL",
			ddl:     "   ",
			wantErr: true,
		},
		{
			name:    "invalid DDL - SELECT",
			ddl:     "SELECT * FROM test",
			wantErr: true,
		},
		{
			name:    "invalid DDL - INSERT",
			ddl:     "INSERT INTO test VALUES (1)",
			wantErr: true,
		},
		{
			name:    "case insensitive - lowercase",
			ddl:     "create table test (id int)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := generator.ValidateDDL(tt.ddl)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateDDL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
