// Package uow provides Unit of Work implementation for transaction management.
package uow

import (
	"context"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/application/contracts"
	"qdhub/internal/infrastructure/persistence"
)

// UnitOfWorkImpl implements contracts.UnitOfWork
type UnitOfWorkImpl struct {
	db *persistence.DB
}

// NewUnitOfWork creates a new UnitOfWork instance
func NewUnitOfWork(db *persistence.DB) contracts.UnitOfWork {
	return &UnitOfWorkImpl{
		db: db,
	}
}

// Do executes the given function within a transaction.
// If the function returns an error, the transaction is rolled back.
// Otherwise, the transaction is committed.
func (u *UnitOfWorkImpl) Do(ctx context.Context, fn func(repos contracts.Repositories) error) error {
	return u.db.ExecInTx(func(tx *sqlx.Tx) error {
		repos := newTransactionalRepositories(u.db, tx)
		return fn(repos)
	})
}
