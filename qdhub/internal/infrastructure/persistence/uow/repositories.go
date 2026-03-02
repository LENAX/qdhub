// Package uow provides Unit of Work implementation for transaction management.
package uow

import (
	"github.com/jmoiron/sqlx"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
)

// transactionalRepositories implements contracts.Repositories
// All repositories returned by this struct share the same transaction context
type transactionalRepositories struct {
	syncPlanRepo   sync.SyncPlanRepository
	dataSourceRepo metadata.DataSourceRepository
	dataStoreRepo  datastore.QuantDataStoreRepository
	metadataRepo   metadata.Repository
}

// newTransactionalRepositories creates a new transactionalRepositories instance
// with all repositories bound to the given transaction
func newTransactionalRepositories(db *persistence.DB, tx *sqlx.Tx) contracts.Repositories {
	return &transactionalRepositories{
		syncPlanRepo:   repository.NewSyncPlanRepositoryWithTx(db, tx),
		dataSourceRepo: repository.NewDataSourceRepositoryWithTx(db, tx),
		dataStoreRepo:  repository.NewQuantDataStoreRepositoryWithTx(db, tx),
		metadataRepo:   repository.NewMetadataRepositoryWithTx(db, tx),
	}
}

// SyncPlanRepo returns the sync plan repository bound to the transaction
func (r *transactionalRepositories) SyncPlanRepo() sync.SyncPlanRepository {
	return r.syncPlanRepo
}

// DataSourceRepo returns the data source repository bound to the transaction
func (r *transactionalRepositories) DataSourceRepo() metadata.DataSourceRepository {
	return r.dataSourceRepo
}

// DataStoreRepo returns the data store repository bound to the transaction
func (r *transactionalRepositories) DataStoreRepo() datastore.QuantDataStoreRepository {
	return r.dataStoreRepo
}

// MetadataRepo returns the metadata repository bound to the transaction
func (r *transactionalRepositories) MetadataRepo() metadata.Repository {
	return r.metadataRepo
}
