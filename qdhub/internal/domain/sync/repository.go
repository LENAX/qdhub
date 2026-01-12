// Package sync contains the sync domain repository interfaces.
package sync

import "qdhub/internal/domain/shared"

// SyncJobRepository defines the repository interface for SyncJob aggregate.
type SyncJobRepository interface {
	Create(job *SyncJob) error
	Get(id shared.ID) (*SyncJob, error)
	Update(job *SyncJob) error
	Delete(id shared.ID) error
	List() ([]*SyncJob, error)
}

// SyncExecutionRepository defines the repository interface for SyncExecution.
type SyncExecutionRepository interface {
	Create(exec *SyncExecution) error
	Get(id shared.ID) (*SyncExecution, error)
	GetBySyncJob(syncJobID shared.ID) ([]*SyncExecution, error)
	Update(exec *SyncExecution) error
	Delete(id shared.ID) error
}
