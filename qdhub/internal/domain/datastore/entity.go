// Package datastore contains the datastore domain entities.
package datastore

import (
	"encoding/json"

	"qdhub/internal/domain/shared"
)

// ==================== 聚合根 ====================

// QuantDataStore represents a quant data store aggregate root.
// Responsibilities:
//   - Manage data store connection configuration
//   - Manage table schema definitions
type QuantDataStore struct {
	ID          shared.ID
	Name        string
	Description string
	Type        DataStoreType
	DSN         string // encrypted
	StoragePath string
	Status      shared.Status
	CreatedAt   shared.Timestamp
	UpdatedAt   shared.Timestamp

	// Aggregated entities (lazy loaded)
	Schemas []TableSchema
}

// NewQuantDataStore creates a new QuantDataStore aggregate.
func NewQuantDataStore(name, description string, storeType DataStoreType, dsn, storagePath string) *QuantDataStore {
	now := shared.Now()
	return &QuantDataStore{
		ID:          shared.NewID(),
		Name:        name,
		Description: description,
		Type:        storeType,
		DSN:         dsn,
		StoragePath: storagePath,
		Status:      shared.StatusActive,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// Activate activates the data store.
func (ds *QuantDataStore) Activate() {
	ds.Status = shared.StatusActive
	ds.UpdatedAt = shared.Now()
}

// Deactivate deactivates the data store.
func (ds *QuantDataStore) Deactivate() {
	ds.Status = shared.StatusInactive
	ds.UpdatedAt = shared.Now()
}

// UpdateConnection updates the connection information.
func (ds *QuantDataStore) UpdateConnection(dsn, storagePath string) {
	ds.DSN = dsn
	ds.StoragePath = storagePath
	ds.UpdatedAt = shared.Now()
}

// ==================== 聚合内实体 ====================

// TableSchema represents a table schema entity.
// Belongs to: QuantDataStore aggregate
type TableSchema struct {
	ID            shared.ID
	DataStoreID   shared.ID
	APIMetadataID shared.ID
	TableName     string
	Columns       []ColumnDef
	PrimaryKeys   []string
	Indexes       []IndexDef
	Status        SchemaStatus
	CreatedAt     shared.Timestamp
	UpdatedAt     shared.Timestamp
	ErrorMessage  *string
}

// NewTableSchema creates a new TableSchema.
func NewTableSchema(dataStoreID, apiMetadataID shared.ID, tableName string) *TableSchema {
	now := shared.Now()
	return &TableSchema{
		ID:            shared.NewID(),
		DataStoreID:   dataStoreID,
		APIMetadataID: apiMetadataID,
		TableName:     tableName,
		Columns:       []ColumnDef{},
		PrimaryKeys:   []string{},
		Indexes:       []IndexDef{},
		Status:        SchemaStatusPending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

// SetColumns sets the column definitions.
func (ts *TableSchema) SetColumns(columns []ColumnDef) {
	ts.Columns = columns
	ts.UpdatedAt = shared.Now()
}

// SetPrimaryKeys sets the primary keys.
func (ts *TableSchema) SetPrimaryKeys(keys []string) {
	ts.PrimaryKeys = keys
	ts.UpdatedAt = shared.Now()
}

// AddIndex adds an index definition.
func (ts *TableSchema) AddIndex(index IndexDef) {
	ts.Indexes = append(ts.Indexes, index)
	ts.UpdatedAt = shared.Now()
}

// MarkCreated marks the schema as created.
func (ts *TableSchema) MarkCreated() {
	ts.Status = SchemaStatusCreated
	ts.ErrorMessage = nil
	ts.UpdatedAt = shared.Now()
}

// MarkFailed marks the schema as failed.
func (ts *TableSchema) MarkFailed(errorMsg string) {
	ts.Status = SchemaStatusFailed
	ts.ErrorMessage = &errorMsg
	ts.UpdatedAt = shared.Now()
}

// MarshalColumnsJSON marshals columns to JSON string.
func (ts *TableSchema) MarshalColumnsJSON() (string, error) {
	data, err := json.Marshal(ts.Columns)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalColumnsJSON unmarshals columns from JSON string.
func (ts *TableSchema) UnmarshalColumnsJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), &ts.Columns)
}

// MarshalIndexesJSON marshals indexes to JSON string.
func (ts *TableSchema) MarshalIndexesJSON() (string, error) {
	data, err := json.Marshal(ts.Indexes)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalIndexesJSON unmarshals indexes from JSON string.
func (ts *TableSchema) UnmarshalIndexesJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), &ts.Indexes)
}

// ==================== 值对象 ====================

// ColumnDef represents column definition (value object).
type ColumnDef struct {
	Name       string  `json:"name"`
	SourceType string  `json:"source_type"`
	TargetType string  `json:"target_type"`
	Nullable   bool    `json:"nullable"`
	Default    *string `json:"default,omitempty"`
	Comment    string  `json:"comment"`
}

// IndexDef represents index definition (value object).
type IndexDef struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// ==================== 枚举类型 ====================

// DataStoreType represents data store type.
type DataStoreType string

const (
	DataStoreTypeDuckDB     DataStoreType = "duckdb"
	DataStoreTypeClickHouse DataStoreType = "clickhouse"
	DataStoreTypePostgreSQL DataStoreType = "postgres"
)

// String returns the string representation of the data store type.
func (dst DataStoreType) String() string {
	return string(dst)
}

// IsValid checks if the data store type is valid.
func (dst DataStoreType) IsValid() bool {
	switch dst {
	case DataStoreTypeDuckDB, DataStoreTypeClickHouse, DataStoreTypePostgreSQL:
		return true
	default:
		return false
	}
}

// SchemaStatus represents table schema status.
type SchemaStatus string

const (
	SchemaStatusPending SchemaStatus = "pending"
	SchemaStatusCreated SchemaStatus = "created"
	SchemaStatusFailed  SchemaStatus = "failed"
)

// String returns the string representation of the schema status.
func (ss SchemaStatus) String() string {
	return string(ss)
}
