// Package repository provides aggregate repository implementations.
package repository

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
)

// DataSourceRepositoryImpl implements metadata.DataSourceRepository.
type DataSourceRepositoryImpl struct {
	db             *persistence.DB
	dataSourceDAO  *dao.DataSourceDAO
	categoryDAO    *dao.APICategoryDAO
	apiMetadataDAO *dao.APIMetadataDAO
	tokenDAO       *dao.TokenDAO
}

// NewDataSourceRepository creates a new DataSourceRepositoryImpl.
func NewDataSourceRepository(db *persistence.DB) *DataSourceRepositoryImpl {
	return &DataSourceRepositoryImpl{
		db:             db,
		dataSourceDAO:  dao.NewDataSourceDAO(db.DB),
		categoryDAO:    dao.NewAPICategoryDAO(db.DB),
		apiMetadataDAO: dao.NewAPIMetadataDAO(db.DB),
		tokenDAO:       dao.NewTokenDAO(db.DB),
	}
}

// Create creates a new data source with its aggregated entities.
func (r *DataSourceRepositoryImpl) Create(ds *metadata.DataSource) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Create data source
		if err := r.dataSourceDAO.Create(tx, ds); err != nil {
			return err
		}

		// Create categories
		for _, cat := range ds.Categories {
			if err := r.categoryDAO.Create(tx, &cat); err != nil {
				return err
			}
		}

		// Create API metadata
		for _, api := range ds.APIs {
			if err := r.apiMetadataDAO.Create(tx, &api); err != nil {
				return err
			}
		}

		// Create token if exists
		if ds.Token != nil {
			if err := r.tokenDAO.Create(tx, ds.Token); err != nil {
				return err
			}
		}

		return nil
	})
}

// Get retrieves a data source by ID with its aggregated entities.
func (r *DataSourceRepositoryImpl) Get(id shared.ID) (*metadata.DataSource, error) {
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

// Update updates a data source and its aggregated entities.
func (r *DataSourceRepositoryImpl) Update(ds *metadata.DataSource) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Update data source
		if err := r.dataSourceDAO.Update(tx, ds); err != nil {
			return err
		}
		return nil
	})
}

// Delete deletes a data source and its aggregated entities.
func (r *DataSourceRepositoryImpl) Delete(id shared.ID) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Delete token first (no FK constraint issue)
		if err := r.tokenDAO.DeleteByDataSource(tx, id); err != nil {
			return err
		}

		// Delete API metadata
		if err := r.apiMetadataDAO.DeleteByDataSource(tx, id); err != nil {
			return err
		}

		// Delete categories
		if err := r.categoryDAO.DeleteByDataSource(tx, id); err != nil {
			return err
		}

		// Delete data source
		if err := r.dataSourceDAO.DeleteByID(tx, id); err != nil {
			return err
		}

		return nil
	})
}

// List retrieves all data sources (without aggregated entities for performance).
func (r *DataSourceRepositoryImpl) List() ([]*metadata.DataSource, error) {
	return r.dataSourceDAO.ListAll(nil)
}

// ==================== Child Entity Operations (APICategory) ====================

// AddCategory adds a new APICategory to a DataSource.
func (r *DataSourceRepositoryImpl) AddCategory(cat *metadata.APICategory) error {
	return r.categoryDAO.Create(nil, cat)
}

// GetCategory retrieves an APICategory by ID.
func (r *DataSourceRepositoryImpl) GetCategory(id shared.ID) (*metadata.APICategory, error) {
	return r.categoryDAO.GetByID(nil, id)
}

// ListCategoriesByDataSource retrieves all APICategories for a DataSource.
func (r *DataSourceRepositoryImpl) ListCategoriesByDataSource(dataSourceID shared.ID) ([]*metadata.APICategory, error) {
	return r.categoryDAO.ListByDataSource(nil, dataSourceID)
}

// UpdateCategory updates an APICategory.
func (r *DataSourceRepositoryImpl) UpdateCategory(cat *metadata.APICategory) error {
	return r.categoryDAO.Update(nil, cat)
}

// DeleteCategory deletes an APICategory by ID.
func (r *DataSourceRepositoryImpl) DeleteCategory(id shared.ID) error {
	return r.categoryDAO.DeleteByID(nil, id)
}

// ==================== Child Entity Operations (APIMetadata) ====================

// AddAPIMetadata adds a new APIMetadata to a DataSource.
func (r *DataSourceRepositoryImpl) AddAPIMetadata(meta *metadata.APIMetadata) error {
	return r.apiMetadataDAO.Create(nil, meta)
}

// GetAPIMetadata retrieves an APIMetadata by ID.
func (r *DataSourceRepositoryImpl) GetAPIMetadata(id shared.ID) (*metadata.APIMetadata, error) {
	return r.apiMetadataDAO.GetByID(nil, id)
}

// ListAPIMetadataByDataSource retrieves all APIMetadata for a DataSource.
func (r *DataSourceRepositoryImpl) ListAPIMetadataByDataSource(dataSourceID shared.ID) ([]*metadata.APIMetadata, error) {
	return r.apiMetadataDAO.ListByDataSource(nil, dataSourceID)
}

// ListAPIMetadataByCategory retrieves all APIMetadata for a category.
func (r *DataSourceRepositoryImpl) ListAPIMetadataByCategory(categoryID shared.ID) ([]*metadata.APIMetadata, error) {
	return r.apiMetadataDAO.ListByCategory(nil, categoryID)
}

// UpdateAPIMetadata updates an APIMetadata.
func (r *DataSourceRepositoryImpl) UpdateAPIMetadata(meta *metadata.APIMetadata) error {
	return r.apiMetadataDAO.Update(nil, meta)
}

// DeleteAPIMetadata deletes an APIMetadata by ID.
func (r *DataSourceRepositoryImpl) DeleteAPIMetadata(id shared.ID) error {
	return r.apiMetadataDAO.DeleteByID(nil, id)
}

// ==================== Child Entity Operations (Token) ====================

// SetToken sets the token for a DataSource (creates or updates).
func (r *DataSourceRepositoryImpl) SetToken(token *metadata.Token) error {
	// Check if token already exists for this data source
	existing, err := r.tokenDAO.GetByDataSource(nil, token.DataSourceID)
	if err != nil {
		return err
	}
	if existing != nil {
		// Update existing token
		token.ID = existing.ID
		return r.tokenDAO.Update(nil, token)
	}
	// Create new token
	return r.tokenDAO.Create(nil, token)
}

// GetToken retrieves a Token by ID.
func (r *DataSourceRepositoryImpl) GetToken(id shared.ID) (*metadata.Token, error) {
	return r.tokenDAO.GetByID(nil, id)
}

// GetTokenByDataSource retrieves the Token for a DataSource.
func (r *DataSourceRepositoryImpl) GetTokenByDataSource(dataSourceID shared.ID) (*metadata.Token, error) {
	return r.tokenDAO.GetByDataSource(nil, dataSourceID)
}

// DeleteToken deletes a Token by ID.
func (r *DataSourceRepositoryImpl) DeleteToken(id shared.ID) error {
	return r.tokenDAO.DeleteByID(nil, id)
}

// ==================== Extended Query Operations ====================

// FindBy retrieves entities matching the given conditions.
func (r *DataSourceRepositoryImpl) FindBy(conditions ...shared.QueryCondition) ([]*metadata.DataSource, error) {
	return r.findByInternal(nil, nil, conditions...)
}

// FindByWithOrder retrieves entities matching conditions with ordering.
func (r *DataSourceRepositoryImpl) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*metadata.DataSource, error) {
	return r.findByInternal(orderBy, nil, conditions...)
}

// ListWithPagination retrieves entities with pagination.
func (r *DataSourceRepositoryImpl) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[metadata.DataSource], error) {
	return r.FindByWithPagination(pagination)
}

// FindByWithPagination retrieves entities matching conditions with pagination.
func (r *DataSourceRepositoryImpl) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[metadata.DataSource], error) {
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
func (r *DataSourceRepositoryImpl) Count(conditions ...shared.QueryCondition) (int64, error) {
	whereClause, args := buildWhereClause(conditions...)
	query := fmt.Sprintf("SELECT COUNT(*) FROM data_sources%s", whereClause)
	query = r.db.DB.Rebind(query)

	var count int64
	err := r.db.DB.Get(&count, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to count data_sources: %w", err)
	}
	return count, nil
}

// Exists checks if any entity matching conditions exists.
func (r *DataSourceRepositoryImpl) Exists(conditions ...shared.QueryCondition) (bool, error) {
	count, err := r.Count(conditions...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *DataSourceRepositoryImpl) findByInternal(orderBy []shared.OrderBy, pagination *shared.Pagination, conditions ...shared.QueryCondition) ([]*metadata.DataSource, error) {
	whereClause, args := buildWhereClause(conditions...)
	orderClause := buildOrderClause(orderBy)
	limitClause := buildLimitClause(pagination)

	query := fmt.Sprintf("SELECT * FROM data_sources%s%s%s", whereClause, orderClause, limitClause)
	query = r.db.DB.Rebind(query)

	var rows []dao.DataSourceRow
	err := r.db.DB.Select(&rows, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []*metadata.DataSource{}, nil
		}
		return nil, fmt.Errorf("failed to find data_sources: %w", err)
	}

	entities := make([]*metadata.DataSource, len(rows))
	for i, row := range rows {
		entities[i] = &metadata.DataSource{
			ID:          shared.ID(row.ID),
			Name:        row.Name,
			Description: row.Description,
			BaseURL:     row.BaseURL,
			DocURL:      row.DocURL,
			Status:      shared.Status(row.Status),
			CreatedAt:   shared.Timestamp(row.CreatedAt),
			UpdatedAt:   shared.Timestamp(row.UpdatedAt),
		}
	}
	return entities, nil
}
