package repository

import (
	"fmt"
	"reflect"
	"sort"

	"qdhub/internal/domain/realtime"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
)

// RealtimeSourceRepositoryImpl implements realtime.RealtimeSourceRepository.
type RealtimeSourceRepositoryImpl struct {
	db  *persistence.DB
	dao *dao.RealtimeSourceDAO
}

// NewRealtimeSourceRepository creates a new RealtimeSourceRepositoryImpl.
func NewRealtimeSourceRepository(db *persistence.DB) *RealtimeSourceRepositoryImpl {
	return &RealtimeSourceRepositoryImpl{
		db:  db,
		dao: dao.NewRealtimeSourceDAO(db.DB),
	}
}

// Create creates a new RealtimeSource.
func (r *RealtimeSourceRepositoryImpl) Create(entity *realtime.RealtimeSource) error {
	return r.dao.Create(nil, entity)
}

// Get retrieves a RealtimeSource by ID.
func (r *RealtimeSourceRepositoryImpl) Get(id shared.ID) (*realtime.RealtimeSource, error) {
	return r.dao.GetByID(nil, id)
}

// Update updates a RealtimeSource.
func (r *RealtimeSourceRepositoryImpl) Update(entity *realtime.RealtimeSource) error {
	return r.dao.Update(nil, entity)
}

// Delete deletes a RealtimeSource by ID.
func (r *RealtimeSourceRepositoryImpl) Delete(id shared.ID) error {
	return r.dao.DeleteByID(nil, id)
}

// List returns all RealtimeSources.
func (r *RealtimeSourceRepositoryImpl) List() ([]*realtime.RealtimeSource, error) {
	return r.dao.ListAll(nil)
}

// ListEnabledForHealthCheck returns enabled sources with health_check_on_startup=true.
func (r *RealtimeSourceRepositoryImpl) ListEnabledForHealthCheck() ([]*realtime.RealtimeSource, error) {
	return r.dao.ListEnabledForHealthCheck(nil)
}

// GetOrderedByPurpose returns enabled sources for the given purpose, ordered by priority ascending.
func (r *RealtimeSourceRepositoryImpl) GetOrderedByPurpose(purpose string) ([]*realtime.RealtimeSource, error) {
	return r.dao.GetOrderedByPurpose(nil, purpose)
}

// FindBy returns entities matching the given conditions (in-memory filter over List).
func (r *RealtimeSourceRepositoryImpl) FindBy(conditions ...shared.QueryCondition) ([]*realtime.RealtimeSource, error) {
	return r.findByInternal(nil, nil, conditions...)
}

// FindByWithOrder returns entities matching conditions with ordering.
func (r *RealtimeSourceRepositoryImpl) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*realtime.RealtimeSource, error) {
	return r.findByInternal(orderBy, nil, conditions...)
}

// ListWithPagination returns a page of entities.
func (r *RealtimeSourceRepositoryImpl) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[realtime.RealtimeSource], error) {
	return r.FindByWithPagination(pagination)
}

// FindByWithPagination returns entities matching conditions with pagination.
func (r *RealtimeSourceRepositoryImpl) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[realtime.RealtimeSource], error) {
	total, err := r.Count(conditions...)
	if err != nil {
		return nil, fmt.Errorf("failed to count realtime_sources: %w", err)
	}
	items, err := r.findByInternal(nil, &pagination, conditions...)
	if err != nil {
		return nil, err
	}
	return shared.NewPageResult(items, total, pagination), nil
}

// Count returns the number of entities matching conditions.
func (r *RealtimeSourceRepositoryImpl) Count(conditions ...shared.QueryCondition) (int64, error) {
	list, err := r.findByInternal(nil, nil, conditions...)
	if err != nil {
		return 0, err
	}
	return int64(len(list)), nil
}

// Exists returns true if any entity matches the conditions.
func (r *RealtimeSourceRepositoryImpl) Exists(conditions ...shared.QueryCondition) (bool, error) {
	n, err := r.Count(conditions...)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (r *RealtimeSourceRepositoryImpl) findByInternal(orderBy []shared.OrderBy, pagination *shared.Pagination, conditions ...shared.QueryCondition) ([]*realtime.RealtimeSource, error) {
	all, err := r.dao.ListAll(nil)
	if err != nil {
		return nil, err
	}
	var out []*realtime.RealtimeSource
	for _, e := range all {
		if matchRealtimeSource(e, conditions) {
			out = append(out, e)
		}
	}
	if len(orderBy) > 0 {
		sort.Slice(out, func(i, j int) bool {
			for _, o := range orderBy {
				vi := getRealtimeSourceField(out[i], o.Field)
				vj := getRealtimeSourceField(out[j], o.Field)
				cmp := compareValues(vi, vj)
				if cmp != 0 {
					if o.Order == shared.SortDesc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})
	}
	if pagination != nil {
		offset := pagination.Offset()
		limit := pagination.Limit()
		if offset >= len(out) {
			return []*realtime.RealtimeSource{}, nil
		}
		end := offset + limit
		if end > len(out) {
			end = len(out)
		}
		out = out[offset:end]
	}
	return out, nil
}

func matchRealtimeSource(e *realtime.RealtimeSource, conditions []shared.QueryCondition) bool {
	for _, c := range conditions {
		val := getRealtimeSourceField(e, c.Field)
		if !matchCondition(val, c) {
			return false
		}
	}
	return true
}

func getRealtimeSourceField(e *realtime.RealtimeSource, field string) interface{} {
	switch field {
	case "id":
		return e.ID.String()
	case "name":
		return e.Name
	case "type":
		return e.Type
	case "config":
		return e.Config
	case "priority":
		return e.Priority
	case "is_primary":
		return e.IsPrimary
	case "health_check_on_startup":
		return e.HealthCheckOnStartup
	case "enabled":
		return e.Enabled
	case "last_health_status":
		return e.LastHealthStatus
	case "last_health_at":
		return e.LastHealthAt.ToTime()
	case "last_health_error":
		return e.LastHealthError
	case "created_at":
		return e.CreatedAt.ToTime()
	case "updated_at":
		return e.UpdatedAt.ToTime()
	default:
		return nil
	}
}

func matchCondition(fieldVal interface{}, c shared.QueryCondition) bool {
	switch c.Operator {
	case shared.OpEqual:
		return reflect.DeepEqual(fieldVal, c.Value)
	case shared.OpNotEqual:
		return !reflect.DeepEqual(fieldVal, c.Value)
	case shared.OpIn:
		return sliceContains(c.Value, fieldVal)
	case shared.OpIsNull:
		return fieldVal == nil || isZero(fieldVal)
	case shared.OpIsNotNull:
		return fieldVal != nil && !isZero(fieldVal)
	default:
		return false
	}
}

func sliceContains(slice interface{}, val interface{}) bool {
	v := reflect.ValueOf(slice)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return reflect.DeepEqual(slice, val)
	}
	for i := 0; i < v.Len(); i++ {
		if reflect.DeepEqual(v.Index(i).Interface(), val) {
			return true
		}
	}
	return false
}

func isZero(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return rv.String() == ""
	case reflect.Bool:
		return !rv.Bool()
	case reflect.Int, reflect.Int64:
		return rv.Int() == 0
	default:
		return reflect.DeepEqual(v, reflect.Zero(reflect.TypeOf(v)).Interface())
	}
}

func compareValues(a, b interface{}) int {
	// string
	as, okA := a.(string)
	bs, okB := b.(string)
	if okA && okB {
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}
	// int
	ai, okA := a.(int)
	bi, okB := b.(int)
	if okA && okB {
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
		return 0
	}
	// bool: false < true
	ab, okA := a.(bool)
	bb, okB := b.(bool)
	if okA && okB {
		if !ab && bb {
			return -1
		}
		if ab && !bb {
			return 1
		}
		return 0
	}
	return 0
}
