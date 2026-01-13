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
	db             *persistence.DB
	dataSourceDAO  *dao.DataSourceDAO
	categoryDAO    *dao.APICategoryDAO
	apiMetadataDAO *dao.APIMetadataDAO
	tokenDAO       *dao.TokenDAO
}

// NewMetadataRepository creates a new MetadataRepositoryImpl.
func NewMetadataRepository(db *persistence.DB) *MetadataRepositoryImpl {
	return &MetadataRepositoryImpl{
		db:             db,
		dataSourceDAO:  dao.NewDataSourceDAO(db.DB),
		categoryDAO:    dao.NewAPICategoryDAO(db.DB),
		apiMetadataDAO: dao.NewAPIMetadataDAO(db.DB),
		tokenDAO:       dao.NewTokenDAO(db.DB),
	}
}

// ==================== Category 操作 ====================

// SaveCategories batch saves API categories.
// Uses upsert logic: if category exists (by ID), update it; otherwise insert.
func (r *MetadataRepositoryImpl) SaveCategories(ctx context.Context, categories []metadata.APICategory) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		for _, cat := range categories {
			// Check if exists
			existing, err := r.categoryDAO.GetByID(tx, cat.ID)
			if err != nil {
				return fmt.Errorf("failed to check category existence: %w", err)
			}

			if existing != nil {
				// Update
				if err := r.categoryDAO.Update(tx, &cat); err != nil {
					return fmt.Errorf("failed to update category: %w", err)
				}
			} else {
				// Insert
				if err := r.categoryDAO.Create(tx, &cat); err != nil {
					return fmt.Errorf("failed to create category: %w", err)
				}
			}
		}
		return nil
	})
}

// DeleteCategoriesByDataSource deletes all categories for a data source.
// Used for SAGA compensation to rollback SaveCategories.
func (r *MetadataRepositoryImpl) DeleteCategoriesByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	return r.categoryDAO.DeleteByDataSource(nil, dataSourceID)
}

// ListCategoriesByDataSource returns all categories for a data source.
func (r *MetadataRepositoryImpl) ListCategoriesByDataSource(ctx context.Context, dataSourceID shared.ID) ([]metadata.APICategory, error) {
	categories, err := r.categoryDAO.ListByDataSource(nil, dataSourceID)
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
	existing, err := r.apiMetadataDAO.GetByID(nil, meta.ID)
	if err != nil {
		return fmt.Errorf("failed to check API metadata existence: %w", err)
	}

	if existing != nil {
		// Update
		return r.apiMetadataDAO.Update(nil, meta)
	}
	// Insert
	return r.apiMetadataDAO.Create(nil, meta)
}

// SaveAPIMetadataBatch batch saves API metadata.
func (r *MetadataRepositoryImpl) SaveAPIMetadataBatch(ctx context.Context, metas []metadata.APIMetadata) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		for _, meta := range metas {
			// Check if exists
			existing, err := r.apiMetadataDAO.GetByID(tx, meta.ID)
			if err != nil {
				return fmt.Errorf("failed to check API metadata existence: %w", err)
			}

			if existing != nil {
				// Update
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
	})
}

// DeleteAPIMetadata deletes a single API metadata by ID.
// Used for SAGA compensation to rollback SaveAPIMetadata.
func (r *MetadataRepositoryImpl) DeleteAPIMetadata(ctx context.Context, id shared.ID) error {
	return r.apiMetadataDAO.DeleteByID(nil, id)
}

// DeleteAPIMetadataByDataSource deletes all API metadata for a data source.
// Used for SAGA compensation to rollback batch operations.
func (r *MetadataRepositoryImpl) DeleteAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	return r.apiMetadataDAO.DeleteByDataSource(nil, dataSourceID)
}

// GetAPIMetadata returns API metadata by ID.
func (r *MetadataRepositoryImpl) GetAPIMetadata(ctx context.Context, id shared.ID) (*metadata.APIMetadata, error) {
	return r.apiMetadataDAO.GetByID(nil, id)
}

// ListAPIMetadataByDataSource returns all API metadata for a data source.
func (r *MetadataRepositoryImpl) ListAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) ([]metadata.APIMetadata, error) {
	apis, err := r.apiMetadataDAO.ListByDataSource(nil, dataSourceID)
	if err != nil {
		return nil, err
	}

	result := make([]metadata.APIMetadata, len(apis))
	for i, api := range apis {
		result[i] = *api
	}
	return result, nil
}

// ==================== 查询操作 ====================

// GetDataSource returns a data source by ID (with aggregated entities).
func (r *MetadataRepositoryImpl) GetDataSource(ctx context.Context, id shared.ID) (*metadata.DataSource, error) {
	ds, err := r.dataSourceDAO.GetByID(nil, id)
	if err != nil {
		return nil, err
	}
	if ds == nil {
		return nil, nil
	}

	// Load categories
	categories, err := r.categoryDAO.ListByDataSource(nil, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load categories: %w", err)
	}
	ds.Categories = make([]metadata.APICategory, len(categories))
	for i, cat := range categories {
		ds.Categories[i] = *cat
	}

	// Load API metadata
	apis, err := r.apiMetadataDAO.ListByDataSource(nil, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load API metadata: %w", err)
	}
	ds.APIs = make([]metadata.APIMetadata, len(apis))
	for i, api := range apis {
		ds.APIs[i] = *api
	}

	// Load token
	ds.Token, err = r.tokenDAO.GetByDataSource(nil, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load token: %w", err)
	}

	return ds, nil
}

// GetToken returns a token by data source ID.
func (r *MetadataRepositoryImpl) GetToken(ctx context.Context, dataSourceID shared.ID) (*metadata.Token, error) {
	return r.tokenDAO.GetByDataSource(nil, dataSourceID)
}

// ==================== 辅助方法 ====================

// DB returns the underlying database connection (for testing).
func (r *MetadataRepositoryImpl) DB() *persistence.DB {
	return r.db
}
