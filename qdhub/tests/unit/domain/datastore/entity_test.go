package datastore_test

import (
	"encoding/json"
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

func TestNewQuantDataStore(t *testing.T) {
	ds := datastore.NewQuantDataStore("DuckDB Local", "本地数据库", datastore.DataStoreTypeDuckDB, "", "/data/quant.duckdb")

	if ds.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if ds.Name != "DuckDB Local" {
		t.Errorf("Name = %s, expected DuckDB Local", ds.Name)
	}
	if ds.Type != datastore.DataStoreTypeDuckDB {
		t.Errorf("Type = %s, expected duckdb", ds.Type)
	}
	if ds.Status != shared.StatusActive {
		t.Errorf("Status = %s, expected active", ds.Status)
	}
}

func TestQuantDataStore_ActivateDeactivate(t *testing.T) {
	ds := datastore.NewQuantDataStore("Test", "", datastore.DataStoreTypeDuckDB, "", "/test")

	ds.Deactivate()
	if ds.Status != shared.StatusInactive {
		t.Errorf("Status after Deactivate = %s, expected inactive", ds.Status)
	}

	ds.Activate()
	if ds.Status != shared.StatusActive {
		t.Errorf("Status after Activate = %s, expected active", ds.Status)
	}
}

func TestQuantDataStore_UpdateConnection(t *testing.T) {
	ds := datastore.NewQuantDataStore("Test", "", datastore.DataStoreTypeDuckDB, "", "/old/path")
	originalUpdatedAt := ds.UpdatedAt

	ds.UpdateConnection("new_dsn", "/new/path")

	if ds.DSN != "new_dsn" {
		t.Errorf("DSN = %s, expected new_dsn", ds.DSN)
	}
	if ds.StoragePath != "/new/path" {
		t.Errorf("StoragePath = %s, expected /new/path", ds.StoragePath)
	}
	if ds.UpdatedAt == originalUpdatedAt {
		t.Error("UpdatedAt should be updated")
	}
}

func TestNewTableSchema(t *testing.T) {
	dsID := shared.NewID()
	apiID := shared.NewID()

	schema := datastore.NewTableSchema(dsID, apiID, "daily_prices")

	if schema.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if schema.DataStoreID != dsID {
		t.Error("DataStoreID mismatch")
	}
	if schema.APIMetadataID != apiID {
		t.Error("APIMetadataID mismatch")
	}
	if schema.TableName != "daily_prices" {
		t.Errorf("TableName = %s, expected daily_prices", schema.TableName)
	}
	if schema.Status != datastore.SchemaStatusPending {
		t.Errorf("Status = %s, expected pending", schema.Status)
	}
	if len(schema.Columns) != 0 {
		t.Error("Columns should be empty")
	}
}

func TestTableSchema_SetColumns(t *testing.T) {
	schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test")

	columns := []datastore.ColumnDef{
		{Name: "ts_code", SourceType: "str", TargetType: "VARCHAR(20)"},
		{Name: "close", SourceType: "float", TargetType: "DOUBLE"},
	}
	schema.SetColumns(columns)

	if len(schema.Columns) != 2 {
		t.Errorf("Columns length = %d, expected 2", len(schema.Columns))
	}
}

func TestTableSchema_SetPrimaryKeys(t *testing.T) {
	schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test")

	schema.SetPrimaryKeys([]string{"ts_code", "trade_date"})

	if len(schema.PrimaryKeys) != 2 {
		t.Errorf("PrimaryKeys length = %d, expected 2", len(schema.PrimaryKeys))
	}
}

func TestTableSchema_AddIndex(t *testing.T) {
	schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test")

	schema.AddIndex(datastore.IndexDef{Name: "idx_date", Columns: []string{"trade_date"}, Unique: false})
	schema.AddIndex(datastore.IndexDef{Name: "idx_code", Columns: []string{"ts_code"}, Unique: true})

	if len(schema.Indexes) != 2 {
		t.Errorf("Indexes length = %d, expected 2", len(schema.Indexes))
	}
}

func TestTableSchema_MarkCreated(t *testing.T) {
	schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test")
	errMsg := "previous error"
	schema.ErrorMessage = &errMsg

	schema.MarkCreated()

	if schema.Status != datastore.SchemaStatusCreated {
		t.Errorf("Status = %s, expected created", schema.Status)
	}
	if schema.ErrorMessage != nil {
		t.Error("ErrorMessage should be nil after MarkCreated")
	}
}

func TestTableSchema_MarkFailed(t *testing.T) {
	schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test")

	schema.MarkFailed("connection timeout")

	if schema.Status != datastore.SchemaStatusFailed {
		t.Errorf("Status = %s, expected failed", schema.Status)
	}
	if schema.ErrorMessage == nil {
		t.Fatal("ErrorMessage should not be nil")
	}
	if *schema.ErrorMessage != "connection timeout" {
		t.Errorf("ErrorMessage = %s, expected connection timeout", *schema.ErrorMessage)
	}
}

func TestTableSchema_JSONMarshaling(t *testing.T) {
	schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test")
	schema.SetColumns([]datastore.ColumnDef{
		{Name: "ts_code", SourceType: "str", TargetType: "VARCHAR(20)", Nullable: false},
	})
	schema.AddIndex(datastore.IndexDef{Name: "idx_code", Columns: []string{"ts_code"}, Unique: true})

	// Test Columns JSON
	columnsJSON, err := schema.MarshalColumnsJSON()
	if err != nil {
		t.Fatalf("MarshalColumnsJSON error: %v", err)
	}

	schema2 := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test2")
	if err := schema2.UnmarshalColumnsJSON(columnsJSON); err != nil {
		t.Fatalf("UnmarshalColumnsJSON error: %v", err)
	}
	if len(schema2.Columns) != 1 {
		t.Errorf("Unmarshaled Columns length = %d, expected 1", len(schema2.Columns))
	}

	// Test Indexes JSON
	indexesJSON, err := schema.MarshalIndexesJSON()
	if err != nil {
		t.Fatalf("MarshalIndexesJSON error: %v", err)
	}

	if err := schema2.UnmarshalIndexesJSON(indexesJSON); err != nil {
		t.Fatalf("UnmarshalIndexesJSON error: %v", err)
	}
	if len(schema2.Indexes) != 1 {
		t.Errorf("Unmarshaled Indexes length = %d, expected 1", len(schema2.Indexes))
	}
}

func TestDataStoreType(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		if datastore.DataStoreTypeDuckDB.String() != "duckdb" {
			t.Error("DuckDB String mismatch")
		}
		if datastore.DataStoreTypeClickHouse.String() != "clickhouse" {
			t.Error("ClickHouse String mismatch")
		}
		if datastore.DataStoreTypePostgreSQL.String() != "postgres" {
			t.Error("PostgreSQL String mismatch")
		}
	})

	t.Run("IsValid", func(t *testing.T) {
		validTypes := []datastore.DataStoreType{
			datastore.DataStoreTypeDuckDB,
			datastore.DataStoreTypeClickHouse,
			datastore.DataStoreTypePostgreSQL,
		}

		for _, dt := range validTypes {
			if !dt.IsValid() {
				t.Errorf("%s should be valid", dt)
			}
		}

		invalidType := datastore.DataStoreType("invalid")
		if invalidType.IsValid() {
			t.Error("invalid type should not be valid")
		}
	})
}

func TestSchemaStatus(t *testing.T) {
	if datastore.SchemaStatusPending.String() != "pending" {
		t.Error("Pending String mismatch")
	}
	if datastore.SchemaStatusCreated.String() != "created" {
		t.Error("Created String mismatch")
	}
	if datastore.SchemaStatusFailed.String() != "failed" {
		t.Error("Failed String mismatch")
	}
}

func TestValueObjects_JSON(t *testing.T) {
	t.Run("ColumnDef", func(t *testing.T) {
		defaultVal := "0"
		col := datastore.ColumnDef{
			Name:       "amount",
			SourceType: "float",
			TargetType: "DOUBLE",
			Nullable:   true,
			Default:    &defaultVal,
			Comment:    "金额",
		}

		data, err := json.Marshal(col)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}

		var unmarshaled datastore.ColumnDef
		if err := json.Unmarshal(data, &unmarshaled); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}

		if unmarshaled.Name != col.Name {
			t.Errorf("Name mismatch")
		}
		if unmarshaled.Nullable != col.Nullable {
			t.Errorf("Nullable mismatch")
		}
	})

	t.Run("IndexDef", func(t *testing.T) {
		idx := datastore.IndexDef{
			Name:    "idx_composite",
			Columns: []string{"ts_code", "trade_date"},
			Unique:  true,
		}

		data, err := json.Marshal(idx)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}

		var unmarshaled datastore.IndexDef
		if err := json.Unmarshal(data, &unmarshaled); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}

		if len(unmarshaled.Columns) != 2 {
			t.Errorf("Columns length = %d, expected 2", len(unmarshaled.Columns))
		}
		if unmarshaled.Unique != idx.Unique {
			t.Errorf("Unique mismatch")
		}
	})
}
