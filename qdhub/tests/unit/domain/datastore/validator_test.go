package datastore_test

import (
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

func TestSchemaValidator_ValidateTableSchema(t *testing.T) {
	validator := datastore.NewSchemaValidator()

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
			Indexes: []datastore.IndexDef{
				{Name: "idx_trade_date", Columns: []string{"trade_date"}, Unique: false},
			},
			Status: datastore.SchemaStatusPending,
		}
	}

	tests := []struct {
		name    string
		modify  func(*datastore.TableSchema)
		wantErr bool
	}{
		{
			name:    "valid schema",
			modify:  func(s *datastore.TableSchema) {},
			wantErr: false,
		},
		{
			name:    "nil schema",
			modify:  func(s *datastore.TableSchema) {},
			wantErr: true,
		},
		{
			name:    "empty ID",
			modify:  func(s *datastore.TableSchema) { s.ID = "" },
			wantErr: true,
		},
		{
			name:    "empty DataStoreID",
			modify:  func(s *datastore.TableSchema) { s.DataStoreID = "" },
			wantErr: true,
		},
		{
			name:    "empty APIMetadataID",
			modify:  func(s *datastore.TableSchema) { s.APIMetadataID = "" },
			wantErr: true,
		},
		{
			name:    "empty table name",
			modify:  func(s *datastore.TableSchema) { s.TableName = "" },
			wantErr: true,
		},
		{
			name:    "invalid table name - starts with digit",
			modify:  func(s *datastore.TableSchema) { s.TableName = "123table" },
			wantErr: true,
		},
		{
			name:    "invalid table name - contains special char",
			modify:  func(s *datastore.TableSchema) { s.TableName = "table-name" },
			wantErr: true,
		},
		{
			name:    "valid table name with underscore",
			modify:  func(s *datastore.TableSchema) { s.TableName = "daily_prices_2024" },
			wantErr: false,
		},
		{
			name:    "empty columns",
			modify:  func(s *datastore.TableSchema) { s.Columns = []datastore.ColumnDef{} },
			wantErr: true,
		},
		{
			name: "invalid column - empty name",
			modify: func(s *datastore.TableSchema) {
				s.Columns[0].Name = ""
			},
			wantErr: true,
		},
		{
			name: "invalid column - empty target type",
			modify: func(s *datastore.TableSchema) {
				s.Columns[0].TargetType = ""
			},
			wantErr: true,
		},
		{
			name: "invalid primary key - column not found",
			modify: func(s *datastore.TableSchema) {
				s.PrimaryKeys = []string{"non_existent_column"}
			},
			wantErr: true,
		},
		{
			name: "empty index name",
			modify: func(s *datastore.TableSchema) {
				s.Indexes[0].Name = ""
			},
			wantErr: true,
		},
		{
			name: "empty index columns",
			modify: func(s *datastore.TableSchema) {
				s.Indexes[0].Columns = []string{}
			},
			wantErr: true,
		},
		{
			name:    "empty primary keys is valid",
			modify:  func(s *datastore.TableSchema) { s.PrimaryKeys = []string{} },
			wantErr: false,
		},
		{
			name:    "empty indexes is valid",
			modify:  func(s *datastore.TableSchema) { s.Indexes = []datastore.IndexDef{} },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var schema *datastore.TableSchema
			if tt.name == "nil schema" {
				schema = nil
			} else {
				schema = validSchema()
				tt.modify(schema)
			}

			err := validator.ValidateTableSchema(schema)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTableSchema() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSchemaValidator_ValidateDataStore(t *testing.T) {
	validator := datastore.NewSchemaValidator()

	validDataStore := func() *datastore.QuantDataStore {
		return &datastore.QuantDataStore{
			ID:          shared.NewID(),
			Name:        "DuckDB Local",
			Description: "本地 DuckDB 数据库",
			Type:        datastore.DataStoreTypeDuckDB,
			DSN:         "",
			StoragePath: "/data/quant.duckdb",
			Status:      shared.StatusActive,
		}
	}

	tests := []struct {
		name    string
		modify  func(*datastore.QuantDataStore)
		wantErr bool
	}{
		{
			name:    "valid DuckDB data store",
			modify:  func(ds *datastore.QuantDataStore) {},
			wantErr: false,
		},
		{
			name:    "nil data store",
			modify:  func(ds *datastore.QuantDataStore) {},
			wantErr: true,
		},
		{
			name:    "empty ID",
			modify:  func(ds *datastore.QuantDataStore) { ds.ID = "" },
			wantErr: true,
		},
		{
			name:    "empty name",
			modify:  func(ds *datastore.QuantDataStore) { ds.Name = "" },
			wantErr: true,
		},
		{
			name:    "invalid type",
			modify:  func(ds *datastore.QuantDataStore) { ds.Type = "invalid" },
			wantErr: true,
		},
		{
			name: "DuckDB without storage path",
			modify: func(ds *datastore.QuantDataStore) {
				ds.Type = datastore.DataStoreTypeDuckDB
				ds.StoragePath = ""
			},
			wantErr: true,
		},
		{
			name: "ClickHouse without DSN",
			modify: func(ds *datastore.QuantDataStore) {
				ds.Type = datastore.DataStoreTypeClickHouse
				ds.DSN = ""
			},
			wantErr: true,
		},
		{
			name: "valid ClickHouse data store",
			modify: func(ds *datastore.QuantDataStore) {
				ds.Type = datastore.DataStoreTypeClickHouse
				ds.DSN = "tcp://localhost:9000/default"
				ds.StoragePath = ""
			},
			wantErr: false,
		},
		{
			name: "valid PostgreSQL data store",
			modify: func(ds *datastore.QuantDataStore) {
				ds.Type = datastore.DataStoreTypePostgreSQL
				ds.DSN = "postgres://user:pass@localhost:5432/db"
				ds.StoragePath = ""
			},
			wantErr: false,
		},
		{
			name:    "invalid status",
			modify:  func(ds *datastore.QuantDataStore) { ds.Status = "invalid" },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ds *datastore.QuantDataStore
			if tt.name == "nil data store" {
				ds = nil
			} else {
				ds = validDataStore()
				tt.modify(ds)
			}

			err := validator.ValidateDataStore(ds)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateDataStore() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSchemaValidator_ValidateColumnDef(t *testing.T) {
	validator := datastore.NewSchemaValidator()

	tests := []struct {
		name    string
		column  *datastore.ColumnDef
		wantErr bool
	}{
		{
			name: "valid column",
			column: &datastore.ColumnDef{
				Name:       "ts_code",
				SourceType: "str",
				TargetType: "VARCHAR(20)",
				Nullable:   false,
			},
			wantErr: false,
		},
		{
			name:    "nil column",
			column:  nil,
			wantErr: true,
		},
		{
			name: "empty name",
			column: &datastore.ColumnDef{
				Name:       "",
				SourceType: "str",
				TargetType: "VARCHAR",
			},
			wantErr: true,
		},
		{
			name: "invalid column name",
			column: &datastore.ColumnDef{
				Name:       "column-name",
				SourceType: "str",
				TargetType: "VARCHAR",
			},
			wantErr: true,
		},
		{
			name: "empty target type",
			column: &datastore.ColumnDef{
				Name:       "ts_code",
				SourceType: "str",
				TargetType: "",
			},
			wantErr: true,
		},
		{
			name: "empty source type is valid",
			column: &datastore.ColumnDef{
				Name:       "ts_code",
				SourceType: "",
				TargetType: "VARCHAR",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateColumnDef(tt.column)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateColumnDef() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
