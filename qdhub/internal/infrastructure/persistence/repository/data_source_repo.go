// Package repository provides aggregate repository implementations.
package repository

import (
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

// APICategoryRepositoryImpl implements metadata.APICategoryRepository.
type APICategoryRepositoryImpl struct {
	db          *persistence.DB
	categoryDAO *dao.APICategoryDAO
}

// NewAPICategoryRepository creates a new APICategoryRepositoryImpl.
func NewAPICategoryRepository(db *persistence.DB) *APICategoryRepositoryImpl {
	return &APICategoryRepositoryImpl{
		db:          db,
		categoryDAO: dao.NewAPICategoryDAO(db.DB),
	}
}

// Create creates a new API category.
func (r *APICategoryRepositoryImpl) Create(cat *metadata.APICategory) error {
	return r.categoryDAO.Create(nil, cat)
}

// Get retrieves an API category by ID.
func (r *APICategoryRepositoryImpl) Get(id shared.ID) (*metadata.APICategory, error) {
	return r.categoryDAO.GetByID(nil, id)
}

// Update updates an API category.
func (r *APICategoryRepositoryImpl) Update(cat *metadata.APICategory) error {
	return r.categoryDAO.Update(nil, cat)
}

// Delete deletes an API category.
func (r *APICategoryRepositoryImpl) Delete(id shared.ID) error {
	return r.categoryDAO.DeleteByID(nil, id)
}

// ListByDataSource retrieves all API categories for a data source.
func (r *APICategoryRepositoryImpl) ListByDataSource(dataSourceID shared.ID) ([]*metadata.APICategory, error) {
	return r.categoryDAO.ListByDataSource(nil, dataSourceID)
}

// APIMetadataRepositoryImpl implements metadata.APIMetadataRepository.
type APIMetadataRepositoryImpl struct {
	db             *persistence.DB
	apiMetadataDAO *dao.APIMetadataDAO
}

// NewAPIMetadataRepository creates a new APIMetadataRepositoryImpl.
func NewAPIMetadataRepository(db *persistence.DB) *APIMetadataRepositoryImpl {
	return &APIMetadataRepositoryImpl{
		db:             db,
		apiMetadataDAO: dao.NewAPIMetadataDAO(db.DB),
	}
}

// Create creates a new API metadata.
func (r *APIMetadataRepositoryImpl) Create(meta *metadata.APIMetadata) error {
	return r.apiMetadataDAO.Create(nil, meta)
}

// Get retrieves an API metadata by ID.
func (r *APIMetadataRepositoryImpl) Get(id shared.ID) (*metadata.APIMetadata, error) {
	return r.apiMetadataDAO.GetByID(nil, id)
}

// Update updates an API metadata.
func (r *APIMetadataRepositoryImpl) Update(meta *metadata.APIMetadata) error {
	return r.apiMetadataDAO.Update(nil, meta)
}

// Delete deletes an API metadata.
func (r *APIMetadataRepositoryImpl) Delete(id shared.ID) error {
	return r.apiMetadataDAO.DeleteByID(nil, id)
}

// ListByDataSource retrieves all API metadata for a data source.
func (r *APIMetadataRepositoryImpl) ListByDataSource(dataSourceID shared.ID) ([]*metadata.APIMetadata, error) {
	return r.apiMetadataDAO.ListByDataSource(nil, dataSourceID)
}

// ListByCategory retrieves all API metadata for a category.
func (r *APIMetadataRepositoryImpl) ListByCategory(categoryID shared.ID) ([]*metadata.APIMetadata, error) {
	return r.apiMetadataDAO.ListByCategory(nil, categoryID)
}

// TokenRepositoryImpl implements metadata.TokenRepository.
type TokenRepositoryImpl struct {
	db       *persistence.DB
	tokenDAO *dao.TokenDAO
}

// NewTokenRepository creates a new TokenRepositoryImpl.
func NewTokenRepository(db *persistence.DB) *TokenRepositoryImpl {
	return &TokenRepositoryImpl{
		db:       db,
		tokenDAO: dao.NewTokenDAO(db.DB),
	}
}

// Create creates a new token.
func (r *TokenRepositoryImpl) Create(token *metadata.Token) error {
	return r.tokenDAO.Create(nil, token)
}

// Get retrieves a token by ID.
func (r *TokenRepositoryImpl) Get(id shared.ID) (*metadata.Token, error) {
	return r.tokenDAO.GetByID(nil, id)
}

// GetByDataSource retrieves a token by data source ID.
func (r *TokenRepositoryImpl) GetByDataSource(dataSourceID shared.ID) (*metadata.Token, error) {
	return r.tokenDAO.GetByDataSource(nil, dataSourceID)
}

// Update updates a token.
func (r *TokenRepositoryImpl) Update(token *metadata.Token) error {
	return r.tokenDAO.Update(nil, token)
}

// Delete deletes a token.
func (r *TokenRepositoryImpl) Delete(id shared.ID) error {
	return r.tokenDAO.DeleteByID(nil, id)
}
