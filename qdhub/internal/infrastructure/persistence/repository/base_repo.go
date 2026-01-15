// Package repository provides base repository implementations with common CRUD operations.
package repository

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
)

// EntityDAO defines the interface that DAO must implement for BaseRepository to work.
type EntityDAO[T any] interface {
	Create(tx *sqlx.Tx, entity *T) error
	GetByID(tx *sqlx.Tx, id shared.ID) (*T, error)
	Update(tx *sqlx.Tx, entity *T) error
	DeleteByID(tx *sqlx.Tx, id shared.ID) error
	ListAll(tx *sqlx.Tx) ([]*T, error)
	TableName() string
	DB() *sqlx.DB
}

// BaseRepository provides common repository operations.
type BaseRepository[T any] struct {
	db             *persistence.DB
	dao            EntityDAO[T]
	tableName      string
	idColumn       string
	fieldWhitelist *shared.FieldWhitelist
}

// NewBaseRepository creates a new BaseRepository.
func NewBaseRepository[T any](db *persistence.DB, dao EntityDAO[T], tableName, idColumn string) *BaseRepository[T] {
	return &BaseRepository[T]{
		db:        db,
		dao:       dao,
		tableName: tableName,
		idColumn:  idColumn,
	}
}

// NewBaseRepositoryWithWhitelist creates a new BaseRepository with field whitelist.
func NewBaseRepositoryWithWhitelist[T any](db *persistence.DB, dao EntityDAO[T], tableName, idColumn string, allowedFields ...string) *BaseRepository[T] {
	return &BaseRepository[T]{
		db:             db,
		dao:            dao,
		tableName:      tableName,
		idColumn:       idColumn,
		fieldWhitelist: shared.NewFieldWhitelist(tableName, allowedFields...),
	}
}

// Create creates a new entity.
func (r *BaseRepository[T]) Create(entity *T) error {
	return r.dao.Create(nil, entity)
}

// Get retrieves an entity by ID.
func (r *BaseRepository[T]) Get(id shared.ID) (*T, error) {
	return r.dao.GetByID(nil, id)
}

// Update updates an existing entity.
func (r *BaseRepository[T]) Update(entity *T) error {
	return r.dao.Update(nil, entity)
}

// Delete deletes an entity by ID.
func (r *BaseRepository[T]) Delete(id shared.ID) error {
	return r.dao.DeleteByID(nil, id)
}

// List retrieves all entities.
func (r *BaseRepository[T]) List() ([]*T, error) {
	return r.dao.ListAll(nil)
}

// FindBy retrieves entities matching the given conditions.
func (r *BaseRepository[T]) FindBy(conditions ...shared.QueryCondition) ([]*T, error) {
	return r.findByInternal(nil, nil, conditions...)
}

// FindByWithOrder retrieves entities matching conditions with ordering.
func (r *BaseRepository[T]) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*T, error) {
	return r.findByInternal(orderBy, nil, conditions...)
}

// ListWithPagination retrieves entities with pagination.
func (r *BaseRepository[T]) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[T], error) {
	return r.FindByWithPagination(pagination)
}

// FindByWithPagination retrieves entities matching conditions with pagination.
func (r *BaseRepository[T]) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[T], error) {
	total, err := r.Count(conditions...)
	if err != nil {
		return nil, fmt.Errorf("failed to count entities: %w", err)
	}

	items, err := r.findByInternal(nil, &pagination, conditions...)
	if err != nil {
		return nil, err
	}

	return shared.NewPageResult(items, total, pagination), nil
}

// Count returns the total count of entities matching conditions.
func (r *BaseRepository[T]) Count(conditions ...shared.QueryCondition) (int64, error) {
	qb := newQueryBuilder(r.tableName, r.fieldWhitelist)
	qb.addConditions(conditions...)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", r.tableName, qb.whereClause())

	// 使用 sqlx.Named 转换命名参数
	namedQuery, args, err := sqlx.Named(query, qb.params)
	if err != nil {
		return 0, fmt.Errorf("failed to build named query: %w", err)
	}
	// 使用 sqlx.In 展开 IN 子句的参数
	namedQuery, args, err = sqlx.In(namedQuery, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to expand IN clause: %w", err)
	}
	// 重新绑定占位符（适配不同数据库）
	namedQuery = r.dao.DB().Rebind(namedQuery)

	var count int64
	if err := r.dao.DB().Get(&count, namedQuery, args...); err != nil {
		return 0, fmt.Errorf("failed to count %s: %w", r.tableName, err)
	}
	return count, nil
}

// Exists checks if any entity matching conditions exists.
func (r *BaseRepository[T]) Exists(conditions ...shared.QueryCondition) (bool, error) {
	count, err := r.Count(conditions...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// findByInternal is the internal implementation for FindBy variants.
func (r *BaseRepository[T]) findByInternal(orderBy []shared.OrderBy, pagination *shared.Pagination, conditions ...shared.QueryCondition) ([]*T, error) {
	qb := newQueryBuilder(r.tableName, r.fieldWhitelist)
	qb.addConditions(conditions...)
	qb.addOrderBy(orderBy)
	qb.addPagination(pagination)

	query := fmt.Sprintf("SELECT * FROM %s%s%s%s", r.tableName, qb.whereClause(), qb.orderClause(), qb.limitClause())

	// 使用 sqlx.Named 转换命名参数
	namedQuery, args, err := sqlx.Named(query, qb.params)
	if err != nil {
		return nil, fmt.Errorf("failed to build named query: %w", err)
	}
	// 使用 sqlx.In 展开 IN 子句的参数
	namedQuery, args, err = sqlx.In(namedQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to expand IN clause: %w", err)
	}
	// 重新绑定占位符
	namedQuery = r.dao.DB().Rebind(namedQuery)

	var entities []*T
	if err := r.dao.DB().Select(&entities, namedQuery, args...); err != nil {
		if err == sql.ErrNoRows {
			return []*T{}, nil
		}
		return nil, fmt.Errorf("failed to find %s: %w", r.tableName, err)
	}
	return entities, nil
}

// ==================== Query Builder ====================

// queryBuilder builds SQL queries with named parameters to prevent SQL injection.
type queryBuilder struct {
	tableName      string
	fieldWhitelist *shared.FieldWhitelist
	conditions     []string
	orders         []string
	params         map[string]interface{}
	paramIndex     int
	limitOffset    *shared.Pagination
}

// newQueryBuilder creates a new query builder.
func newQueryBuilder(tableName string, whitelist *shared.FieldWhitelist) *queryBuilder {
	return &queryBuilder{
		tableName:      tableName,
		fieldWhitelist: whitelist,
		conditions:     make([]string, 0),
		orders:         make([]string, 0),
		params:         make(map[string]interface{}),
		paramIndex:     0,
	}
}

// isFieldAllowed checks if a field name is allowed.
func (qb *queryBuilder) isFieldAllowed(field string) bool {
	if qb.fieldWhitelist != nil {
		return qb.fieldWhitelist.IsAllowed(field)
	}
	return shared.IsSafeFieldName(field)
}

// nextParamName generates a unique parameter name.
func (qb *queryBuilder) nextParamName(prefix string) string {
	qb.paramIndex++
	return fmt.Sprintf("%s_%d", prefix, qb.paramIndex)
}

// addConditions adds WHERE conditions.
func (qb *queryBuilder) addConditions(conditions ...shared.QueryCondition) {
	for _, cond := range conditions {
		if !qb.isFieldAllowed(cond.Field) {
			continue // 跳过非法字段
		}
		qb.addCondition(cond)
	}
}

// addCondition adds a single condition.
func (qb *queryBuilder) addCondition(cond shared.QueryCondition) {
	paramName := qb.nextParamName(cond.Field)

	switch cond.Operator {
	case shared.OpEqual:
		qb.conditions = append(qb.conditions, fmt.Sprintf("%s = :%s", cond.Field, paramName))
		qb.params[paramName] = cond.Value
	case shared.OpNotEqual:
		qb.conditions = append(qb.conditions, fmt.Sprintf("%s != :%s", cond.Field, paramName))
		qb.params[paramName] = cond.Value
	case shared.OpGreater:
		qb.conditions = append(qb.conditions, fmt.Sprintf("%s > :%s", cond.Field, paramName))
		qb.params[paramName] = cond.Value
	case shared.OpGreaterOrEq:
		qb.conditions = append(qb.conditions, fmt.Sprintf("%s >= :%s", cond.Field, paramName))
		qb.params[paramName] = cond.Value
	case shared.OpLess:
		qb.conditions = append(qb.conditions, fmt.Sprintf("%s < :%s", cond.Field, paramName))
		qb.params[paramName] = cond.Value
	case shared.OpLessOrEq:
		qb.conditions = append(qb.conditions, fmt.Sprintf("%s <= :%s", cond.Field, paramName))
		qb.params[paramName] = cond.Value
	case shared.OpLike:
		qb.conditions = append(qb.conditions, fmt.Sprintf("%s LIKE :%s", cond.Field, paramName))
		qb.params[paramName] = cond.Value
	case shared.OpIn:
		qb.addInCondition(cond.Field, cond.Value, paramName)
	case shared.OpIsNull:
		qb.conditions = append(qb.conditions, fmt.Sprintf("%s IS NULL", cond.Field))
	case shared.OpIsNotNull:
		qb.conditions = append(qb.conditions, fmt.Sprintf("%s IS NOT NULL", cond.Field))
	}
}

// addInCondition adds an IN condition with proper handling of slices.
func (qb *queryBuilder) addInCondition(field string, value interface{}, paramName string) {
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		qb.conditions = append(qb.conditions, fmt.Sprintf("%s = :%s", field, paramName))
		qb.params[paramName] = value
		return
	}

	if v.Len() == 0 {
		qb.conditions = append(qb.conditions, "1 = 0") // 空 IN 子句始终为 false
		return
	}

	// 使用 sqlx.In 支持的格式
	qb.conditions = append(qb.conditions, fmt.Sprintf("%s IN (:%s)", field, paramName))
	qb.params[paramName] = value
}

// addOrderBy adds ORDER BY clauses.
func (qb *queryBuilder) addOrderBy(orderBy []shared.OrderBy) {
	for _, o := range orderBy {
		if !qb.isFieldAllowed(o.Field) {
			continue
		}
		// 只允许 ASC 和 DESC
		if o.Order != shared.SortAsc && o.Order != shared.SortDesc {
			continue
		}
		qb.orders = append(qb.orders, fmt.Sprintf("%s %s", o.Field, o.Order))
	}
}

// addPagination adds LIMIT/OFFSET.
func (qb *queryBuilder) addPagination(pagination *shared.Pagination) {
	qb.limitOffset = pagination
}

// whereClause returns the WHERE clause.
func (qb *queryBuilder) whereClause() string {
	if len(qb.conditions) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(qb.conditions, " AND ")
}

// orderClause returns the ORDER BY clause.
func (qb *queryBuilder) orderClause() string {
	if len(qb.orders) == 0 {
		return ""
	}
	return " ORDER BY " + strings.Join(qb.orders, ", ")
}

// limitClause returns the LIMIT/OFFSET clause.
func (qb *queryBuilder) limitClause() string {
	if qb.limitOffset == nil {
		return ""
	}
	return fmt.Sprintf(" LIMIT %d OFFSET %d", qb.limitOffset.Limit(), qb.limitOffset.Offset())
}

// ==================== String ID Base Repository ====================

// StringIDEntityDAO defines the interface for DAO with string ID.
type StringIDEntityDAO[T any] interface {
	Create(tx *sqlx.Tx, entity *T) error
	GetByID(tx *sqlx.Tx, id string) (*T, error)
	Update(tx *sqlx.Tx, entity *T) error
	DeleteByID(tx *sqlx.Tx, id string) error
	ListAll(tx *sqlx.Tx) ([]*T, error)
	TableName() string
	DB() *sqlx.DB
}

// StringIDBaseRepository provides common repository operations for string ID entities.
type StringIDBaseRepository[T any] struct {
	db             *persistence.DB
	dao            StringIDEntityDAO[T]
	tableName      string
	idColumn       string
	fieldWhitelist *shared.FieldWhitelist
}

// NewStringIDBaseRepository creates a new StringIDBaseRepository.
func NewStringIDBaseRepository[T any](db *persistence.DB, dao StringIDEntityDAO[T], tableName, idColumn string) *StringIDBaseRepository[T] {
	return &StringIDBaseRepository[T]{
		db:        db,
		dao:       dao,
		tableName: tableName,
		idColumn:  idColumn,
	}
}

// NewStringIDBaseRepositoryWithWhitelist creates a new StringIDBaseRepository with field whitelist.
func NewStringIDBaseRepositoryWithWhitelist[T any](db *persistence.DB, dao StringIDEntityDAO[T], tableName, idColumn string, allowedFields ...string) *StringIDBaseRepository[T] {
	return &StringIDBaseRepository[T]{
		db:             db,
		dao:            dao,
		tableName:      tableName,
		idColumn:       idColumn,
		fieldWhitelist: shared.NewFieldWhitelist(tableName, allowedFields...),
	}
}

// Create creates a new entity.
func (r *StringIDBaseRepository[T]) Create(entity *T) error {
	return r.dao.Create(nil, entity)
}

// Get retrieves an entity by ID.
func (r *StringIDBaseRepository[T]) Get(id string) (*T, error) {
	return r.dao.GetByID(nil, id)
}

// Update updates an existing entity.
func (r *StringIDBaseRepository[T]) Update(entity *T) error {
	return r.dao.Update(nil, entity)
}

// Delete deletes an entity by ID.
func (r *StringIDBaseRepository[T]) Delete(id string) error {
	return r.dao.DeleteByID(nil, id)
}

// List retrieves all entities.
func (r *StringIDBaseRepository[T]) List() ([]*T, error) {
	return r.dao.ListAll(nil)
}

// FindBy retrieves entities matching the given conditions.
func (r *StringIDBaseRepository[T]) FindBy(conditions ...shared.QueryCondition) ([]*T, error) {
	return r.findByInternal(nil, nil, conditions...)
}

// FindByWithOrder retrieves entities matching conditions with ordering.
func (r *StringIDBaseRepository[T]) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*T, error) {
	return r.findByInternal(orderBy, nil, conditions...)
}

// ListWithPagination retrieves entities with pagination.
func (r *StringIDBaseRepository[T]) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[T], error) {
	return r.FindByWithPagination(pagination)
}

// FindByWithPagination retrieves entities matching conditions with pagination.
func (r *StringIDBaseRepository[T]) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[T], error) {
	total, err := r.Count(conditions...)
	if err != nil {
		return nil, fmt.Errorf("failed to count entities: %w", err)
	}

	items, err := r.findByInternal(nil, &pagination, conditions...)
	if err != nil {
		return nil, err
	}

	return shared.NewPageResult(items, total, pagination), nil
}

// Count returns the total count of entities matching conditions.
func (r *StringIDBaseRepository[T]) Count(conditions ...shared.QueryCondition) (int64, error) {
	qb := newQueryBuilder(r.tableName, r.fieldWhitelist)
	qb.addConditions(conditions...)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", r.tableName, qb.whereClause())

	namedQuery, args, err := sqlx.Named(query, qb.params)
	if err != nil {
		return 0, fmt.Errorf("failed to build named query: %w", err)
	}
	namedQuery, args, err = sqlx.In(namedQuery, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to expand IN clause: %w", err)
	}
	namedQuery = r.dao.DB().Rebind(namedQuery)

	var count int64
	if err := r.dao.DB().Get(&count, namedQuery, args...); err != nil {
		return 0, fmt.Errorf("failed to count %s: %w", r.tableName, err)
	}
	return count, nil
}

// Exists checks if any entity matching conditions exists.
func (r *StringIDBaseRepository[T]) Exists(conditions ...shared.QueryCondition) (bool, error) {
	count, err := r.Count(conditions...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *StringIDBaseRepository[T]) findByInternal(orderBy []shared.OrderBy, pagination *shared.Pagination, conditions ...shared.QueryCondition) ([]*T, error) {
	qb := newQueryBuilder(r.tableName, r.fieldWhitelist)
	qb.addConditions(conditions...)
	qb.addOrderBy(orderBy)
	qb.addPagination(pagination)

	query := fmt.Sprintf("SELECT * FROM %s%s%s%s", r.tableName, qb.whereClause(), qb.orderClause(), qb.limitClause())

	namedQuery, args, err := sqlx.Named(query, qb.params)
	if err != nil {
		return nil, fmt.Errorf("failed to build named query: %w", err)
	}
	namedQuery, args, err = sqlx.In(namedQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to expand IN clause: %w", err)
	}
	namedQuery = r.dao.DB().Rebind(namedQuery)

	var entities []*T
	if err := r.dao.DB().Select(&entities, namedQuery, args...); err != nil {
		if err == sql.ErrNoRows {
			return []*T{}, nil
		}
		return nil, fmt.Errorf("failed to find %s: %w", r.tableName, err)
	}
	return entities, nil
}

// ==================== Shared Helper Functions (for backward compatibility) ====================

func buildWhereClause(conditions ...shared.QueryCondition) (string, []interface{}) {
	qb := newQueryBuilder("", nil)
	qb.addConditions(conditions...)

	if len(qb.conditions) == 0 {
		return "", nil
	}

	query := " WHERE " + strings.Join(qb.conditions, " AND ")
	namedQuery, args, _ := sqlx.Named(query, qb.params)
	namedQuery, args, _ = sqlx.In(namedQuery, args...)

	return namedQuery, args
}

func buildOrderClause(orderBy []shared.OrderBy) string {
	qb := newQueryBuilder("", nil)
	qb.addOrderBy(orderBy)
	return qb.orderClause()
}

func buildLimitClause(pagination *shared.Pagination) string {
	qb := newQueryBuilder("", nil)
	qb.addPagination(pagination)
	return qb.limitClause()
}
