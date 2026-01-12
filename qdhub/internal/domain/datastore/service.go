// Package datastore contains the datastore domain services.
package datastore

// ==================== 领域服务接口（纯业务逻辑）====================

// SchemaValidator defines domain service for schema validation.
// Implementation: datastore/service_impl.go
type SchemaValidator interface {
	// ValidateTableSchema validates table schema definition.
	ValidateTableSchema(schema *TableSchema) error

	// ValidateDataStore validates data store configuration.
	ValidateDataStore(dataStore *QuantDataStore) error

	// ValidateColumnDef validates column definition.
	ValidateColumnDef(column *ColumnDef) error
}

// SchemaGenerator defines domain service for schema generation.
// Implementation: datastore/service_impl.go
type SchemaGenerator interface {
	// GenerateDDL generates DDL statement for creating table.
	GenerateDDL(schema *TableSchema, dbType DataStoreType) (string, error)

	// GenerateDropDDL generates DDL statement for dropping table.
	GenerateDropDDL(tableName string, dbType DataStoreType) string

	// ValidateDDL validates DDL statement syntax (basic check).
	ValidateDDL(ddl string) error
}

// ==================== 外部依赖接口（领域定义，基础设施实现）====================

// QuantDBAdapter defines the interface for quantitative database operations.
// Implementation: infrastructure/quantdb/duckdb/, infrastructure/quantdb/clickhouse/
type QuantDBAdapter interface {
	// Connect establishes a connection to the database.
	Connect(dsn string) error

	// Close closes the database connection.
	Close() error

	// CreateTable creates a table based on schema definition.
	CreateTable(schema *TableSchema) error

	// DropTable drops a table.
	DropTable(tableName string) error

	// TableExists checks if a table exists.
	TableExists(tableName string) (bool, error)

	// GetTableInfo retrieves table information.
	GetTableInfo(tableName string) (*TableSchema, error)

	// Execute executes a SQL statement.
	Execute(sql string) error

	// Query executes a query and returns results.
	Query(sql string) ([]map[string]interface{}, error)

	// GetType returns the database type.
	GetType() DataStoreType
}

// QuantDBAdapterFactory defines the interface for creating QuantDB adapters.
// Implementation: infrastructure/quantdb/
type QuantDBAdapterFactory interface {
	// GetAdapter returns an adapter for the given data store type.
	GetAdapter(storeType DataStoreType) (QuantDBAdapter, error)

	// RegisterAdapter registers an adapter.
	RegisterAdapter(storeType DataStoreType, adapter QuantDBAdapter)
}
