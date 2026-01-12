// Package datastore contains the datastore domain services.
package datastore

import "regexp"

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

// TypeMappingService defines domain service for type mapping.
// Implementation: datastore/service_impl.go
type TypeMappingService interface {
	// FindBestMatchingRule finds the best matching type mapping rule.
	// Priority: 1. Field pattern match 2. Source type match
	FindBestMatchingRule(rules []*DataTypeMappingRule, fieldName, sourceType string) *DataTypeMappingRule

	// ValidateMappingRule validates a mapping rule.
	ValidateMappingRule(rule *DataTypeMappingRule) error
}

// ==================== 内部工具函数 ====================

// matchFieldPattern checks if field name matches the pattern.
func matchFieldPattern(pattern, fieldName string) bool {
	if pattern == "" {
		return false
	}
	matched, err := regexp.MatchString(pattern, fieldName)
	if err != nil {
		return false
	}
	return matched
}