// Package datastore contains the datastore domain service implementations.
package datastore

import (
	"fmt"
	"strings"

	"qdhub/internal/domain/shared"
)

// ==================== SchemaValidator 实现 ====================

type schemaValidatorImpl struct{}

// NewSchemaValidator creates a new SchemaValidator.
func NewSchemaValidator() SchemaValidator {
	return &schemaValidatorImpl{}
}

// ValidateTableSchema validates table schema definition.
func (v *schemaValidatorImpl) ValidateTableSchema(schema *TableSchema) error {
	if schema == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "table schema cannot be nil", nil)
	}

	if schema.ID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "schema ID cannot be empty", nil)
	}

	if schema.DataStoreID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "data store ID cannot be empty", nil)
	}

	if schema.APIMetadataID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "API metadata ID cannot be empty", nil)
	}

	if strings.TrimSpace(schema.TableName) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "table name cannot be empty", nil)
	}

	// Validate table name format (alphanumeric and underscores only)
	if !isValidTableName(schema.TableName) {
		return shared.NewDomainError(shared.ErrCodeValidation, "table name must contain only alphanumeric characters and underscores", nil)
	}

	// Must have at least one column
	if len(schema.Columns) == 0 {
		return shared.NewDomainError(shared.ErrCodeValidation, "table must have at least one column", nil)
	}

	// Validate each column
	for i, col := range schema.Columns {
		if err := v.ValidateColumnDef(&col); err != nil {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("column[%d] validation failed", i), err)
		}
	}

	// Validate primary keys
	if len(schema.PrimaryKeys) > 0 {
		columnNames := make(map[string]bool)
		for _, col := range schema.Columns {
			columnNames[col.Name] = true
		}
		for _, pkCol := range schema.PrimaryKeys {
			if !columnNames[pkCol] {
				return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("primary key column '%s' not found in columns", pkCol), nil)
			}
		}
	}

	// Validate indexes
	for i, idx := range schema.Indexes {
		if strings.TrimSpace(idx.Name) == "" {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("index[%d] name cannot be empty", i), nil)
		}
		if len(idx.Columns) == 0 {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("index[%d] must have at least one column", i), nil)
		}
	}

	return nil
}

// ValidateDataStore validates data store configuration.
func (v *schemaValidatorImpl) ValidateDataStore(dataStore *QuantDataStore) error {
	if dataStore == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "data store cannot be nil", nil)
	}

	if dataStore.ID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "data store ID cannot be empty", nil)
	}

	if strings.TrimSpace(dataStore.Name) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "data store name cannot be empty", nil)
	}

	if !dataStore.Type.IsValid() {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid data store type: %s", dataStore.Type), nil)
	}

	// For file-based databases, storage path is required
	if dataStore.Type == DataStoreTypeDuckDB && strings.TrimSpace(dataStore.StoragePath) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "storage path is required for DuckDB", nil)
	}

	// For network databases, DSN is required
	if (dataStore.Type == DataStoreTypeClickHouse || dataStore.Type == DataStoreTypePostgreSQL) && strings.TrimSpace(dataStore.DSN) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "DSN is required for network databases", nil)
	}

	if !dataStore.Status.IsValid() {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid status: %s", dataStore.Status), nil)
	}

	return nil
}

// ValidateColumnDef validates column definition.
func (v *schemaValidatorImpl) ValidateColumnDef(column *ColumnDef) error {
	if column == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "column definition cannot be nil", nil)
	}

	if strings.TrimSpace(column.Name) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "column name cannot be empty", nil)
	}

	// Validate column name format
	if !isValidColumnName(column.Name) {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid column name: %s", column.Name), nil)
	}

	if strings.TrimSpace(column.TargetType) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "column target type cannot be empty", nil)
	}

	return nil
}

// ==================== SchemaGenerator 实现 ====================

type schemaGeneratorImpl struct{}

// NewSchemaGenerator creates a new SchemaGenerator.
func NewSchemaGenerator() SchemaGenerator {
	return &schemaGeneratorImpl{}
}

// GenerateDDL generates DDL statement for creating table.
func (g *schemaGeneratorImpl) GenerateDDL(schema *TableSchema, dbType DataStoreType) (string, error) {
	if schema == nil {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "schema cannot be nil", nil)
	}

	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", schema.TableName))

	// Add columns
	for i, col := range schema.Columns {
		if i > 0 {
			ddl.WriteString(",\n")
		}
		ddl.WriteString(fmt.Sprintf("  %s %s", col.Name, col.TargetType))
		if !col.Nullable {
			ddl.WriteString(" NOT NULL")
		}
		if col.Default != nil {
			ddl.WriteString(fmt.Sprintf(" DEFAULT %s", *col.Default))
		}
	}

	// Add primary key constraint
	if len(schema.PrimaryKeys) > 0 {
		ddl.WriteString(",\n")
		ddl.WriteString(fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(schema.PrimaryKeys, ", ")))
	}

	ddl.WriteString("\n)")

	// Add table comment (if supported by db type)
	if dbType == DataStoreTypePostgreSQL || dbType == DataStoreTypeClickHouse {
		// These databases support table comments
		ddl.WriteString(";")
	}

	return ddl.String(), nil
}

// GenerateDropDDL generates DDL statement for dropping table.
func (g *schemaGeneratorImpl) GenerateDropDDL(tableName string, dbType DataStoreType) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
}

// ValidateDDL validates DDL statement syntax (basic check).
func (g *schemaGeneratorImpl) ValidateDDL(ddl string) error {
	if strings.TrimSpace(ddl) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "DDL statement cannot be empty", nil)
	}

	upperDDL := strings.ToUpper(strings.TrimSpace(ddl))
	
	// Check if it's a valid DDL statement
	validPrefixes := []string{"CREATE TABLE", "DROP TABLE", "ALTER TABLE", "CREATE INDEX"}
	isValid := false
	for _, prefix := range validPrefixes {
		if strings.HasPrefix(upperDDL, prefix) {
			isValid = true
			break
		}
	}

	if !isValid {
		return shared.NewDomainError(shared.ErrCodeValidation, "invalid DDL statement", nil)
	}

	return nil
}

// ==================== 工具函数 ====================

// isValidTableName checks if table name is valid (alphanumeric and underscores only).
func isValidTableName(name string) bool {
	if name == "" {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	// Must not start with a digit
	if name[0] >= '0' && name[0] <= '9' {
		return false
	}
	return true
}

// isValidColumnName checks if column name is valid.
func isValidColumnName(name string) bool {
	return isValidTableName(name) // Same rules as table name
}
