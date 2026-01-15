// Package sync contains the sync domain repository interfaces.
package sync

import "qdhub/internal/domain/shared"

// SyncJobRepository defines the repository interface for SyncJob aggregate.
// Following DDD principles, this repository handles both the aggregate root (SyncJob)
// and its child entities (SyncExecution) to maintain aggregate boundaries.
//
// Embeds shared.Repository[SyncJob] to inherit common CRUD operations.
type SyncJobRepository interface {
	// Embed base repository for common CRUD operations
	shared.Repository[SyncJob]

	// ==================== Child Entity Operations (SyncExecution) ====================

	// AddExecution adds a new SyncExecution to a SyncJob.
	AddExecution(exec *SyncExecution) error

	// GetExecution retrieves a SyncExecution by ID.
	GetExecution(id shared.ID) (*SyncExecution, error)

	// GetExecutionsByJob retrieves all SyncExecutions for a SyncJob.
	GetExecutionsByJob(jobID shared.ID) ([]*SyncExecution, error)

	// UpdateExecution updates a SyncExecution.
	UpdateExecution(exec *SyncExecution) error
}
