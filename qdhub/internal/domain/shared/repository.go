// Package shared provides shared repository interfaces.
package shared

import (
	"fmt"
	"regexp"
)

// ==================== 分页类型 ====================

// Pagination defines pagination parameters.
type Pagination struct {
	Page     int // Page number, starting from 1
	PageSize int // Number of items per page
}

// NewPagination creates a new Pagination with defaults.
func NewPagination(page, pageSize int) Pagination {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	if pageSize > 100 {
		pageSize = 100 // Maximum page size limit
	}
	return Pagination{Page: page, PageSize: pageSize}
}

// Offset returns the offset for database query.
func (p Pagination) Offset() int {
	return (p.Page - 1) * p.PageSize
}

// Limit returns the limit for database query.
func (p Pagination) Limit() int {
	return p.PageSize
}

// PageResult represents a paginated result set.
type PageResult[T any] struct {
	Items      []*T  // Items in current page
	Total      int64 // Total number of items
	Page       int   // Current page number
	PageSize   int   // Items per page
	TotalPages int   // Total number of pages
}

// NewPageResult creates a new PageResult.
func NewPageResult[T any](items []*T, total int64, pagination Pagination) *PageResult[T] {
	totalPages := int(total) / pagination.PageSize
	if int(total)%pagination.PageSize > 0 {
		totalPages++
	}
	return &PageResult[T]{
		Items:      items,
		Total:      total,
		Page:       pagination.Page,
		PageSize:   pagination.PageSize,
		TotalPages: totalPages,
	}
}

// ==================== 查询条件类型 ====================

// QueryOperator defines the comparison operator for query conditions.
type QueryOperator string

const (
	OpEqual       QueryOperator = "="
	OpNotEqual    QueryOperator = "!="
	OpGreater     QueryOperator = ">"
	OpGreaterOrEq QueryOperator = ">="
	OpLess        QueryOperator = "<"
	OpLessOrEq    QueryOperator = "<="
	OpLike        QueryOperator = "LIKE"
	OpIn          QueryOperator = "IN"
	OpIsNull      QueryOperator = "IS NULL"
	OpIsNotNull   QueryOperator = "IS NOT NULL"
)

// QueryCondition defines a single query condition.
type QueryCondition struct {
	Field    string        // Field name (database column name)
	Operator QueryOperator // Comparison operator
	Value    interface{}   // Value to compare (nil for IS NULL/IS NOT NULL)
}

// Eq creates an equality condition.
func Eq(field string, value interface{}) QueryCondition {
	return QueryCondition{Field: field, Operator: OpEqual, Value: value}
}

// NotEq creates a not-equal condition.
func NotEq(field string, value interface{}) QueryCondition {
	return QueryCondition{Field: field, Operator: OpNotEqual, Value: value}
}

// Gt creates a greater-than condition.
func Gt(field string, value interface{}) QueryCondition {
	return QueryCondition{Field: field, Operator: OpGreater, Value: value}
}

// Gte creates a greater-than-or-equal condition.
func Gte(field string, value interface{}) QueryCondition {
	return QueryCondition{Field: field, Operator: OpGreaterOrEq, Value: value}
}

// Lt creates a less-than condition.
func Lt(field string, value interface{}) QueryCondition {
	return QueryCondition{Field: field, Operator: OpLess, Value: value}
}

// Lte creates a less-than-or-equal condition.
func Lte(field string, value interface{}) QueryCondition {
	return QueryCondition{Field: field, Operator: OpLessOrEq, Value: value}
}

// Like creates a LIKE condition.
func Like(field string, pattern string) QueryCondition {
	return QueryCondition{Field: field, Operator: OpLike, Value: pattern}
}

// In creates an IN condition.
func In(field string, values interface{}) QueryCondition {
	return QueryCondition{Field: field, Operator: OpIn, Value: values}
}

// IsNull creates an IS NULL condition.
func IsNull(field string) QueryCondition {
	return QueryCondition{Field: field, Operator: OpIsNull, Value: nil}
}

// IsNotNull creates an IS NOT NULL condition.
func IsNotNull(field string) QueryCondition {
	return QueryCondition{Field: field, Operator: OpIsNotNull, Value: nil}
}

// ==================== 排序类型 ====================

// SortOrder defines the sort direction.
type SortOrder string

const (
	SortAsc  SortOrder = "ASC"
	SortDesc SortOrder = "DESC"
)

// OrderBy defines a sort specification.
type OrderBy struct {
	Field string
	Order SortOrder
}

// Asc creates an ascending order specification.
func Asc(field string) OrderBy {
	return OrderBy{Field: field, Order: SortAsc}
}

// Desc creates a descending order specification.
func Desc(field string) OrderBy {
	return OrderBy{Field: field, Order: SortDesc}
}

// ==================== SQL 注入防护 ====================

// FieldWhitelist defines allowed field names for a specific entity.
// Used to prevent SQL injection by validating field names against a whitelist.
type FieldWhitelist struct {
	allowedFields map[string]bool
	tableName     string
}

// NewFieldWhitelist creates a new FieldWhitelist.
func NewFieldWhitelist(tableName string, fields ...string) *FieldWhitelist {
	allowedFields := make(map[string]bool)
	for _, f := range fields {
		allowedFields[f] = true
	}
	return &FieldWhitelist{
		allowedFields: allowedFields,
		tableName:     tableName,
	}
}

// IsAllowed checks if a field name is in the whitelist.
func (w *FieldWhitelist) IsAllowed(field string) bool {
	return w.allowedFields[field]
}

// ValidateConditions validates all conditions against the whitelist.
// Returns an error if any field is not in the whitelist.
func (w *FieldWhitelist) ValidateConditions(conditions ...QueryCondition) error {
	for _, cond := range conditions {
		if !w.IsAllowed(cond.Field) {
			return fmt.Errorf("invalid field name '%s' for table '%s': not in whitelist", cond.Field, w.tableName)
		}
	}
	return nil
}

// ValidateOrderBy validates all order by fields against the whitelist.
func (w *FieldWhitelist) ValidateOrderBy(orderBy []OrderBy) error {
	for _, o := range orderBy {
		if !w.IsAllowed(o.Field) {
			return fmt.Errorf("invalid order by field '%s' for table '%s': not in whitelist", o.Field, w.tableName)
		}
	}
	return nil
}

// safeFieldNameRegex matches valid SQL field names (alphanumeric and underscore only).
var safeFieldNameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// IsSafeFieldName checks if a field name is safe (no SQL injection risk).
// This is a fallback when whitelist is not available.
func IsSafeFieldName(field string) bool {
	return safeFieldNameRegex.MatchString(field) && len(field) <= 64
}

// ValidateFieldNamesSafe validates field names using regex (fallback method).
func ValidateFieldNamesSafe(fields ...string) error {
	for _, f := range fields {
		if !IsSafeFieldName(f) {
			return fmt.Errorf("invalid field name '%s': must be alphanumeric with underscores", f)
		}
	}
	return nil
}

// ==================== Repository 接口 ====================

// Repository defines the base repository interface for CRUD operations.
// All domain repositories should embed this interface to inherit common operations.
type Repository[T any] interface {
	// ==================== 基础 CRUD 操作 ====================

	// Create creates a new entity (幂等: 如果 ID 已存在则返回错误).
	Create(entity *T) error

	// Get retrieves an entity by ID.
	Get(id ID) (*T, error)

	// Update updates an existing entity (幂等: 多次调用结果相同).
	Update(entity *T) error

	// Delete deletes an entity by ID (幂等: 删除不存在的实体不报错).
	Delete(id ID) error

	// List retrieves all entities.
	List() ([]*T, error)

	// ==================== 扩展查询操作 ====================

	// FindBy retrieves entities matching the given conditions.
	// Multiple conditions are combined with AND.
	// Field names are validated to prevent SQL injection.
	FindBy(conditions ...QueryCondition) ([]*T, error)

	// FindByWithOrder retrieves entities matching conditions with ordering.
	FindByWithOrder(orderBy []OrderBy, conditions ...QueryCondition) ([]*T, error)

	// ListWithPagination retrieves entities with pagination.
	ListWithPagination(pagination Pagination) (*PageResult[T], error)

	// FindByWithPagination retrieves entities matching conditions with pagination.
	FindByWithPagination(pagination Pagination, conditions ...QueryCondition) (*PageResult[T], error)

	// Count returns the total count of entities matching conditions.
	// If no conditions provided, returns total count of all entities.
	Count(conditions ...QueryCondition) (int64, error)

	// Exists checks if any entity matching conditions exists.
	Exists(conditions ...QueryCondition) (bool, error)
}

// StringIDRepository defines the base repository interface for entities with string ID.
// Used for repositories that integrate with external systems using string IDs (e.g., Task Engine).
type StringIDRepository[T any] interface {
	// ==================== 基础 CRUD 操作 ====================

	// Create creates a new entity (幂等: 如果 ID 已存在则返回错误).
	Create(entity *T) error

	// Get retrieves an entity by ID.
	Get(id string) (*T, error)

	// Update updates an existing entity (幂等: 多次调用结果相同).
	Update(entity *T) error

	// Delete deletes an entity by ID (幂等: 删除不存在的实体不报错).
	Delete(id string) error

	// List retrieves all entities.
	List() ([]*T, error)

	// ==================== 扩展查询操作 ====================

	// FindBy retrieves entities matching the given conditions.
	// Field names are validated to prevent SQL injection.
	FindBy(conditions ...QueryCondition) ([]*T, error)

	// FindByWithOrder retrieves entities matching conditions with ordering.
	FindByWithOrder(orderBy []OrderBy, conditions ...QueryCondition) ([]*T, error)

	// ListWithPagination retrieves entities with pagination.
	ListWithPagination(pagination Pagination) (*PageResult[T], error)

	// FindByWithPagination retrieves entities matching conditions with pagination.
	FindByWithPagination(pagination Pagination, conditions ...QueryCondition) (*PageResult[T], error)

	// Count returns the total count of entities matching conditions.
	Count(conditions ...QueryCondition) (int64, error)

	// Exists checks if any entity matching conditions exists.
	Exists(conditions ...QueryCondition) (bool, error)
}
