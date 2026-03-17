package typemap_test

import (
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/pkg/typemap"
)

func TestDefaultTypeMapper_MapFieldType(t *testing.T) {
	mapper := typemap.NewDefaultTypeMapper()

	tests := []struct {
		name           string
		field          metadata.FieldMeta
		dataSourceType string
		targetDB       datastore.DataStoreType
		expected       string
	}{
		{
			name:           "str to VARCHAR for DuckDB",
			field:          metadata.FieldMeta{Name: "test_field", Type: "str"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "VARCHAR",
		},
		{
			name:           "float to DOUBLE for DuckDB",
			field:          metadata.FieldMeta{Name: "price", Type: "float"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DOUBLE",
		},
		{
			name:           "int to BIGINT for DuckDB",
			field:          metadata.FieldMeta{Name: "count", Type: "int"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "BIGINT",
		},
		{
			name:           "date to DATE for DuckDB",
			field:          metadata.FieldMeta{Name: "created", Type: "date"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DATE",
		},
		{
			name:           "datetime to TIMESTAMP for DuckDB",
			field:          metadata.FieldMeta{Name: "timestamp", Type: "datetime"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "TIMESTAMP",
		},
		{
			name:           "str to String for ClickHouse",
			field:          metadata.FieldMeta{Name: "name", Type: "str"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeClickHouse,
			expected:       "String",
		},
		{
			name:           "float to Float64 for ClickHouse",
			field:          metadata.FieldMeta{Name: "value", Type: "float"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeClickHouse,
			expected:       "Float64",
		},
		{
			name:           "int to BIGINT for PostgreSQL",
			field:          metadata.FieldMeta{Name: "id", Type: "int"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypePostgreSQL,
			expected:       "BIGINT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapper.MapFieldType(tt.field, tt.dataSourceType, tt.targetDB)
			if result != tt.expected {
				t.Errorf("MapFieldType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDefaultTypeMapper_FieldPatternRules(t *testing.T) {
	mapper := typemap.NewDefaultTypeMapper()

	tests := []struct {
		name           string
		field          metadata.FieldMeta
		dataSourceType string
		targetDB       datastore.DataStoreType
		expected       string
	}{
		{
			name:           "ts_code to VARCHAR(16) for DuckDB",
			field:          metadata.FieldMeta{Name: "ts_code", Type: "str"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "VARCHAR(16)",
		},
		{
			name:           "stock_code to VARCHAR(16) for DuckDB",
			field:          metadata.FieldMeta{Name: "stock_code", Type: "str"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "VARCHAR(16)",
		},
		{
			name:           "trade_date to DATE for DuckDB",
			field:          metadata.FieldMeta{Name: "trade_date", Type: "str"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DATE",
		},
		{
			name:           "ann_date to DATE for DuckDB",
			field:          metadata.FieldMeta{Name: "ann_date", Type: "str"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DATE",
		},
		{
			name:           "vol to DECIMAL(20,2) for DuckDB",
			field:          metadata.FieldMeta{Name: "vol", Type: "float"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DECIMAL(20,2)",
		},
		{
			name:           "amount to DECIMAL(20,2) for DuckDB",
			field:          metadata.FieldMeta{Name: "amount", Type: "float"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DECIMAL(20,2)",
		},
		{
			name:           "pct_chg to DECIMAL(10,4) for DuckDB",
			field:          metadata.FieldMeta{Name: "pct_chg", Type: "float"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DECIMAL(10,4)",
		},
		{
			name:           "change_rate to DECIMAL(10,4) for DuckDB",
			field:          metadata.FieldMeta{Name: "change_rate", Type: "float"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DECIMAL(10,4)",
		},
		{
			name:           "open to DECIMAL(10,2) for DuckDB",
			field:          metadata.FieldMeta{Name: "open", Type: "float"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DECIMAL(10,2)",
		},
		{
			name:           "close to DECIMAL(10,2) for DuckDB",
			field:          metadata.FieldMeta{Name: "close", Type: "float"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DECIMAL(10,2)",
		},
		{
			name:           "high to DECIMAL(10,2) for DuckDB",
			field:          metadata.FieldMeta{Name: "high", Type: "float"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "DECIMAL(10,2)",
		},
		{
			name:           "ts_code to FixedString(16) for ClickHouse",
			field:          metadata.FieldMeta{Name: "ts_code", Type: "str"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeClickHouse,
			expected:       "FixedString(16)",
		},
		{
			name:           "vol to Decimal(20,2) for ClickHouse",
			field:          metadata.FieldMeta{Name: "vol", Type: "float"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeClickHouse,
			expected:       "Decimal(20,2)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapper.MapFieldType(tt.field, tt.dataSourceType, tt.targetDB)
			if result != tt.expected {
				t.Errorf("MapFieldType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDefaultTypeMapper_MapAllFields(t *testing.T) {
	mapper := typemap.NewDefaultTypeMapper()

	fields := []metadata.FieldMeta{
		{Name: "ts_code", Type: "str", IsPrimary: true, Description: "股票代码"},
		{Name: "trade_date", Type: "str", IsPrimary: true, Description: "交易日期"},
		{Name: "open", Type: "float", Description: "开盘价"},
		{Name: "close", Type: "float", Description: "收盘价"},
		{Name: "vol", Type: "float", Description: "成交量"},
		{Name: "pct_chg", Type: "float", Description: "涨跌幅"},
	}

	columns := mapper.MapAllFields(fields, "tushare", datastore.DataStoreTypeDuckDB)

	if len(columns) != 6 {
		t.Fatalf("Expected 6 columns, got %d", len(columns))
	}

	// Verify ts_code column
	if columns[0].Name != "ts_code" {
		t.Errorf("Expected column name 'ts_code', got '%s'", columns[0].Name)
	}
	if columns[0].TargetType != "VARCHAR(16)" {
		t.Errorf("Expected target type 'VARCHAR(16)', got '%s'", columns[0].TargetType)
	}
	if columns[0].Nullable != false {
		t.Error("Primary key column should not be nullable")
	}

	// Verify vol column
	if columns[4].TargetType != "DECIMAL(20,2)" {
		t.Errorf("Expected target type 'DECIMAL(20,2)' for vol, got '%s'", columns[4].TargetType)
	}

	// Verify pct_chg column
	if columns[5].TargetType != "DECIMAL(10,4)" {
		t.Errorf("Expected target type 'DECIMAL(10,4)' for pct_chg, got '%s'", columns[5].TargetType)
	}
}

func TestRuleBasedTypeMapper_MapFieldType(t *testing.T) {
	// Create custom rules
	pattern := `^custom_.*$`
	rules := []*datastore.DataTypeMappingRule{
		{
			DataSourceType: "tushare",
			SourceType:     "str",
			TargetDBType:   "duckdb",
			TargetType:     "VARCHAR(100)",
			FieldPattern:   &pattern,
			Priority:       100,
		},
		{
			DataSourceType: "tushare",
			SourceType:     "str",
			TargetDBType:   "duckdb",
			TargetType:     "VARCHAR(50)",
			Priority:       50,
		},
	}

	typeMappingSvc := datastore.NewTypeMappingService()
	mapper := typemap.NewRuleBasedTypeMapper(rules, typeMappingSvc)

	tests := []struct {
		name           string
		field          metadata.FieldMeta
		dataSourceType string
		targetDB       datastore.DataStoreType
		expected       string
	}{
		{
			name:           "custom field with pattern match",
			field:          metadata.FieldMeta{Name: "custom_field", Type: "str"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "VARCHAR(100)",
		},
		{
			name:           "regular field without pattern match",
			field:          metadata.FieldMeta{Name: "regular_field", Type: "str"},
			dataSourceType: "tushare",
			targetDB:       datastore.DataStoreTypeDuckDB,
			expected:       "VARCHAR(50)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapper.MapFieldType(tt.field, tt.dataSourceType, tt.targetDB)
			if result != tt.expected {
				t.Errorf("MapFieldType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestRuleBasedTypeMapper_SetRules(t *testing.T) {
	// Start with empty rules
	typeMappingSvc := datastore.NewTypeMappingService()
	mapper := typemap.NewRuleBasedTypeMapper(nil, typeMappingSvc)

	// Initially should use default mapping
	field := metadata.FieldMeta{Name: "test", Type: "str"}
	result := mapper.MapFieldType(field, "tushare", datastore.DataStoreTypeDuckDB)
	if result != "VARCHAR" {
		t.Errorf("Expected default 'VARCHAR', got '%s'", result)
	}

	// Add custom rule
	rules := []*datastore.DataTypeMappingRule{
		{
			DataSourceType: "tushare",
			SourceType:     "str",
			TargetDBType:   "duckdb",
			TargetType:     "TEXT",
			Priority:       100,
		},
	}
	mapper.SetRules(rules)

	// Now should use custom rule
	result = mapper.MapFieldType(field, "tushare", datastore.DataStoreTypeDuckDB)
	if result != "TEXT" {
		t.Errorf("Expected 'TEXT' after SetRules, got '%s'", result)
	}
}

func TestNewTypeMapper_WithRules(t *testing.T) {
	rules := []*datastore.DataTypeMappingRule{
		{
			DataSourceType: "tushare",
			SourceType:     "str",
			TargetDBType:   "duckdb",
			TargetType:     "TEXT",
		},
	}

	mapper := typemap.NewTypeMapper(rules, nil)
	if _, ok := mapper.(*typemap.RuleBasedTypeMapper); !ok {
		t.Error("Expected RuleBasedTypeMapper when rules are provided")
	}
}

func TestNewTypeMapper_WithoutRules(t *testing.T) {
	mapper := typemap.NewTypeMapper(nil, nil)
	if _, ok := mapper.(*typemap.DefaultTypeMapper); !ok {
		t.Error("Expected DefaultTypeMapper when no rules are provided")
	}
}

func TestDefaultTypeMapper_UnknownType(t *testing.T) {
	mapper := typemap.NewDefaultTypeMapper()

	field := metadata.FieldMeta{Name: "unknown", Type: "unknown_type"}

	// Should fallback to VARCHAR for DuckDB
	result := mapper.MapFieldType(field, "tushare", datastore.DataStoreTypeDuckDB)
	if result != "VARCHAR" {
		t.Errorf("Expected fallback 'VARCHAR' for DuckDB, got '%s'", result)
	}

	// Should fallback to String for ClickHouse
	result = mapper.MapFieldType(field, "tushare", datastore.DataStoreTypeClickHouse)
	if result != "String" {
		t.Errorf("Expected fallback 'String' for ClickHouse, got '%s'", result)
	}
}
