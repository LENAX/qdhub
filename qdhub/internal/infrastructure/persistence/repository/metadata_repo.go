// Package repository provides aggregate repository implementations.
package repository

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
)

// MetadataRepositoryImpl implements metadata.Repository interface.
// This is the aggregated repository used by task engine job functions.
type MetadataRepositoryImpl struct {
	db                 *persistence.DB
	tx                 *sqlx.Tx // External transaction (nil if not in transaction)
	dataSourceDAO      *dao.DataSourceDAO
	categoryDAO        *dao.APICategoryDAO
	apiMetadataDAO     *dao.APIMetadataDAO
	tokenDAO           *dao.TokenDAO
	apiSyncStrategyDAO *dao.APISyncStrategyDAO
}

// NewMetadataRepository creates a new MetadataRepositoryImpl.
func NewMetadataRepository(db *persistence.DB) *MetadataRepositoryImpl {
	return &MetadataRepositoryImpl{
		db:                 db,
		tx:                 nil,
		dataSourceDAO:      dao.NewDataSourceDAO(db.DB),
		categoryDAO:        dao.NewAPICategoryDAO(db.DB),
		apiMetadataDAO:     dao.NewAPIMetadataDAO(db.DB),
		tokenDAO:           dao.NewTokenDAO(db.DB),
		apiSyncStrategyDAO: dao.NewAPISyncStrategyDAO(db.DB),
	}
}

// NewMetadataRepositoryWithTx creates a new MetadataRepositoryImpl bound to an external transaction.
func NewMetadataRepositoryWithTx(db *persistence.DB, tx *sqlx.Tx) *MetadataRepositoryImpl {
	return &MetadataRepositoryImpl{
		db:                 db,
		tx:                 tx,
		dataSourceDAO:      dao.NewDataSourceDAO(db.DB),
		categoryDAO:        dao.NewAPICategoryDAO(db.DB),
		apiMetadataDAO:     dao.NewAPIMetadataDAO(db.DB),
		tokenDAO:           dao.NewTokenDAO(db.DB),
		apiSyncStrategyDAO: dao.NewAPISyncStrategyDAO(db.DB),
	}
}

// ==================== Category 操作 ====================

// SaveCategories batch saves API categories.
// Uses upsert logic: if category exists (by ID), update it; otherwise insert.
func (r *MetadataRepositoryImpl) SaveCategories(ctx context.Context, categories []metadata.APICategory) error {
	if r.tx != nil {
		return r.saveCategoriesInTx(r.tx, categories)
	}
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.saveCategoriesInTx(tx, categories)
	})
}

func (r *MetadataRepositoryImpl) saveCategoriesInTx(tx *sqlx.Tx, categories []metadata.APICategory) error {
	for i, cat := range categories {
		// Check if exists
		existing, err := r.categoryDAO.GetByID(tx, cat.ID)
		if err != nil {
			return fmt.Errorf("failed to check category existence (index %d, id=%s): %w", i, cat.ID, err)
		}

		if existing != nil {
			// Update
			if err := r.categoryDAO.Update(tx, &cat); err != nil {
				return fmt.Errorf("failed to update category (index %d, id=%s, name=%s): %w", i, cat.ID, cat.Name, err)
			}
		} else {
			// Insert
			if err := r.categoryDAO.Create(tx, &cat); err != nil {
				return fmt.Errorf("failed to create category (index %d, id=%s, name=%s, data_source_id=%s, parent_id=%v): %w",
					i, cat.ID, cat.Name, cat.DataSourceID, cat.ParentID, err)
			}
		}
	}
	return nil
}

// DeleteCategoriesByDataSource deletes all categories for a data source.
// Used for SAGA compensation to rollback SaveCategories.
func (r *MetadataRepositoryImpl) DeleteCategoriesByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	return r.categoryDAO.DeleteByDataSource(r.tx, dataSourceID)
}

// ListCategoriesByDataSource returns all categories for a data source.
func (r *MetadataRepositoryImpl) ListCategoriesByDataSource(ctx context.Context, dataSourceID shared.ID) ([]metadata.APICategory, error) {
	categories, err := r.categoryDAO.ListByDataSource(r.tx, dataSourceID)
	if err != nil {
		return nil, err
	}

	result := make([]metadata.APICategory, len(categories))
	for i, cat := range categories {
		result[i] = *cat
	}
	return result, nil
}

// ListCategoriesByDataSourceWithAPIs returns only categories that have at least one api_metadata.
func (r *MetadataRepositoryImpl) ListCategoriesByDataSourceWithAPIs(ctx context.Context, dataSourceID shared.ID) ([]metadata.APICategory, error) {
	categories, err := r.categoryDAO.ListByDataSourceWithAPIs(r.tx, dataSourceID)
	if err != nil {
		return nil, err
	}
	result := make([]metadata.APICategory, len(categories))
	for i, cat := range categories {
		result[i] = *cat
	}
	return result, nil
}

// ==================== API Metadata 操作 ====================

// SaveAPIMetadata saves a single API metadata.
// Uses upsert logic: if metadata exists (by ID), update it; otherwise insert.
func (r *MetadataRepositoryImpl) SaveAPIMetadata(ctx context.Context, meta *metadata.APIMetadata) error {
	// Check if exists
	existing, err := r.apiMetadataDAO.GetByID(r.tx, meta.ID)
	if err != nil {
		return fmt.Errorf("failed to check API metadata existence: %w", err)
	}

	if existing != nil {
		// Update
		return r.apiMetadataDAO.Update(r.tx, meta)
	}
	// Insert
	return r.apiMetadataDAO.Create(r.tx, meta)
}

// SaveAPIMetadataBatch batch saves API metadata.
// Uses upsert logic based on (data_source_id, name) unique constraint:
// - If metadata exists with same data_source_id and name, update it
// - Otherwise insert new record
func (r *MetadataRepositoryImpl) SaveAPIMetadataBatch(ctx context.Context, metas []metadata.APIMetadata) error {
	if r.tx != nil {
		return r.saveAPIMetadataBatchInTx(r.tx, metas)
	}
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.saveAPIMetadataBatchInTx(tx, metas)
	})
}

func (r *MetadataRepositoryImpl) saveAPIMetadataBatchInTx(tx *sqlx.Tx, metas []metadata.APIMetadata) error {
	for _, meta := range metas {
		// Check if exists by (data_source_id, name) unique constraint
		existing, err := r.apiMetadataDAO.GetByDataSourceAndName(tx, meta.DataSourceID, meta.Name)
		if err != nil {
			return fmt.Errorf("failed to check API metadata existence: %w", err)
		}

		if existing != nil {
			// Update: use existing ID to avoid creating duplicate
			meta.ID = existing.ID
			if err := r.apiMetadataDAO.Update(tx, &meta); err != nil {
				return fmt.Errorf("failed to update API metadata: %w", err)
			}
		} else {
			// Insert
			if err := r.apiMetadataDAO.Create(tx, &meta); err != nil {
				return fmt.Errorf("failed to create API metadata: %w", err)
			}
		}
	}
	return nil
}

// DeleteAPIMetadata deletes a single API metadata by ID.
// Used for SAGA compensation to rollback SaveAPIMetadata.
func (r *MetadataRepositoryImpl) DeleteAPIMetadata(ctx context.Context, id shared.ID) error {
	return r.apiMetadataDAO.DeleteByID(r.tx, id)
}

// DeleteAPIMetadataByDataSource deletes all API metadata for a data source.
// Used for SAGA compensation to rollback batch operations.
func (r *MetadataRepositoryImpl) DeleteAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	return r.apiMetadataDAO.DeleteByDataSource(r.tx, dataSourceID)
}

// GetAPIMetadata returns API metadata by ID.
func (r *MetadataRepositoryImpl) GetAPIMetadata(ctx context.Context, id shared.ID) (*metadata.APIMetadata, error) {
	return r.apiMetadataDAO.GetByID(r.tx, id)
}

// ListAPIMetadataByDataSource returns all API metadata for a data source.
func (r *MetadataRepositoryImpl) ListAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) ([]metadata.APIMetadata, error) {
	apis, err := r.apiMetadataDAO.ListByDataSource(r.tx, dataSourceID)
	if err != nil {
		return nil, err
	}

	result := make([]metadata.APIMetadata, len(apis))
	for i, api := range apis {
		result[i] = *api
	}
	return result, nil
}

// ListAPIMetadataByDataSourcePaginated returns a paginated list of API metadata for a data source with optional filters.
func (r *MetadataRepositoryImpl) ListAPIMetadataByDataSourcePaginated(ctx context.Context, dataSourceID shared.ID, idFilter *shared.ID, nameFilter string, categoryIDFilter *shared.ID, page, pageSize int) ([]metadata.APIMetadata, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	offset := (page - 1) * pageSize
	apis, err := r.apiMetadataDAO.ListByDataSourceFiltered(r.tx, dataSourceID, idFilter, nameFilter, categoryIDFilter, pageSize, offset)
	if err != nil {
		return nil, 0, err
	}
	total, err := r.apiMetadataDAO.CountByDataSourceFiltered(r.tx, dataSourceID, idFilter, nameFilter, categoryIDFilter)
	if err != nil {
		return nil, 0, err
	}
	result := make([]metadata.APIMetadata, len(apis))
	for i, api := range apis {
		result[i] = *api
	}
	return result, total, nil
}

// ==================== 查询操作 ====================

// GetDataSource returns a data source by ID (with aggregated entities).
func (r *MetadataRepositoryImpl) GetDataSource(ctx context.Context, id shared.ID) (*metadata.DataSource, error) {
	ds, err := r.dataSourceDAO.GetByID(r.tx, id)
	if err != nil {
		return nil, err
	}
	if ds == nil {
		return nil, nil
	}

	// Load categories
	categories, err := r.categoryDAO.ListByDataSource(r.tx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load categories: %w", err)
	}
	ds.Categories = make([]metadata.APICategory, len(categories))
	for i, cat := range categories {
		ds.Categories[i] = *cat
	}

	// Load API metadata
	apis, err := r.apiMetadataDAO.ListByDataSource(r.tx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load API metadata: %w", err)
	}
	ds.APIs = make([]metadata.APIMetadata, len(apis))
	for i, api := range apis {
		ds.APIs[i] = *api
	}

	// Load token
	ds.Token, err = r.tokenDAO.GetByDataSource(r.tx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load token: %w", err)
	}

	return ds, nil
}

// GetToken returns a token by data source ID.
func (r *MetadataRepositoryImpl) GetToken(ctx context.Context, dataSourceID shared.ID) (*metadata.Token, error) {
	return r.tokenDAO.GetByDataSource(r.tx, dataSourceID)
}

// GetDataSourceByName returns a data source by name.
// Used by compensation handlers to find data source ID when only name is available.
func (r *MetadataRepositoryImpl) GetDataSourceByName(ctx context.Context, name string) (*metadata.DataSource, error) {
	ds, err := r.dataSourceDAO.GetByName(r.tx, name)
	if err != nil {
		return nil, err
	}
	if ds == nil {
		return nil, nil
	}
	// Note: Not loading aggregated entities for efficiency (only ID is needed for compensation)
	return ds, nil
}

// ==================== API Sync Strategy 操作 ====================

// SaveAPISyncStrategy saves or updates an API sync strategy.
func (r *MetadataRepositoryImpl) SaveAPISyncStrategy(ctx context.Context, strategy *metadata.APISyncStrategy) error {
	// Check if exists by unique constraint (data_source_id, api_name)
	existing, err := r.apiSyncStrategyDAO.GetByDataSourceAndAPIName(r.tx, strategy.DataSourceID, strategy.APIName)
	if err != nil {
		return fmt.Errorf("failed to check strategy existence: %w", err)
	}

	if existing != nil {
		// Update: use existing ID
		strategy.ID = existing.ID
		return r.apiSyncStrategyDAO.Update(r.tx, strategy)
	}
	// Insert
	return r.apiSyncStrategyDAO.Create(r.tx, strategy)
}

// SaveAPISyncStrategyBatch batch saves API sync strategies.
func (r *MetadataRepositoryImpl) SaveAPISyncStrategyBatch(ctx context.Context, strategies []*metadata.APISyncStrategy) error {
	if r.tx != nil {
		return r.saveAPISyncStrategyBatchInTx(r.tx, strategies)
	}
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.saveAPISyncStrategyBatchInTx(tx, strategies)
	})
}

func (r *MetadataRepositoryImpl) saveAPISyncStrategyBatchInTx(tx *sqlx.Tx, strategies []*metadata.APISyncStrategy) error {
	for _, strategy := range strategies {
		// Check if exists by unique constraint (data_source_id, api_name)
		existing, err := r.apiSyncStrategyDAO.GetByDataSourceAndAPIName(tx, strategy.DataSourceID, strategy.APIName)
		if err != nil {
			return fmt.Errorf("failed to check strategy existence: %w", err)
		}

		if existing != nil {
			// Update: use existing ID
			strategy.ID = existing.ID
			if err := r.apiSyncStrategyDAO.Update(tx, strategy); err != nil {
				return fmt.Errorf("failed to update strategy: %w", err)
			}
		} else {
			// Insert
			if err := r.apiSyncStrategyDAO.Create(tx, strategy); err != nil {
				return fmt.Errorf("failed to create strategy: %w", err)
			}
		}
	}
	return nil
}

// GetAPISyncStrategyByID retrieves a sync strategy by ID.
func (r *MetadataRepositoryImpl) GetAPISyncStrategyByID(ctx context.Context, id shared.ID) (*metadata.APISyncStrategy, error) {
	return r.apiSyncStrategyDAO.GetByID(r.tx, id)
}

// GetAPISyncStrategyByAPIName retrieves a sync strategy by data source ID and API name.
func (r *MetadataRepositoryImpl) GetAPISyncStrategyByAPIName(ctx context.Context, dataSourceID shared.ID, apiName string) (*metadata.APISyncStrategy, error) {
	return r.apiSyncStrategyDAO.GetByDataSourceAndAPIName(r.tx, dataSourceID, apiName)
}

// ListAPISyncStrategiesByDataSource retrieves all sync strategies for a data source.
func (r *MetadataRepositoryImpl) ListAPISyncStrategiesByDataSource(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APISyncStrategy, error) {
	return r.apiSyncStrategyDAO.ListByDataSource(r.tx, dataSourceID)
}

// ListAPISyncStrategiesByAPINames retrieves sync strategies for specific API names.
func (r *MetadataRepositoryImpl) ListAPISyncStrategiesByAPINames(ctx context.Context, dataSourceID shared.ID, apiNames []string) ([]*metadata.APISyncStrategy, error) {
	return r.apiSyncStrategyDAO.ListByAPINames(r.tx, dataSourceID, apiNames)
}

// DeleteAPISyncStrategy deletes a sync strategy by ID.
func (r *MetadataRepositoryImpl) DeleteAPISyncStrategy(ctx context.Context, id shared.ID) error {
	return r.apiSyncStrategyDAO.DeleteByID(r.tx, id)
}

// DeleteAPISyncStrategiesByDataSource deletes all sync strategies for a data source.
func (r *MetadataRepositoryImpl) DeleteAPISyncStrategiesByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	return r.apiSyncStrategyDAO.DeleteByDataSource(r.tx, dataSourceID)
}

// ==================== 辅助方法 ====================

// DB returns the underlying database connection (for testing).
func (r *MetadataRepositoryImpl) DB() *persistence.DB {
	return r.db
}
