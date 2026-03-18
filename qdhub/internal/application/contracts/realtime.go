// Package contracts defines application service interfaces and DTOs.
package contracts

import (
	"context"

	"qdhub/internal/domain/realtime"
	"qdhub/internal/domain/shared"
)

// RealtimeSourceApplicationService defines application service for realtime data source management.
type RealtimeSourceApplicationService interface {
	List(ctx context.Context) ([]*realtime.RealtimeSource, error)
	Get(ctx context.Context, id shared.ID) (*realtime.RealtimeSource, error)
	Create(ctx context.Context, req CreateRealtimeSourceRequest) (*realtime.RealtimeSource, error)
	Update(ctx context.Context, id shared.ID, req UpdateRealtimeSourceRequest) (*realtime.RealtimeSource, error)
	Delete(ctx context.Context, id shared.ID) error
	// TriggerHealthCheck triggers a one-off health check for the given source and returns the result (optional).
	TriggerHealthCheck(ctx context.Context, id shared.ID) (status string, errMsg string, err error)
}

// CreateRealtimeSourceRequest is the request to create a realtime source.
type CreateRealtimeSourceRequest struct {
	Name                  string
	Type                  string
	Config                string // JSON: type-specific (ws_url, rsa_public_key_path, etc.)
	Priority              int
	IsPrimary             bool
	HealthCheckOnStartup  bool
	Enabled               bool
}

// UpdateRealtimeSourceRequest is the request to update a realtime source (partial fields).
type UpdateRealtimeSourceRequest struct {
	Name                  *string
	Config                *string
	Priority              *int
	IsPrimary             *bool
	HealthCheckOnStartup  *bool
	Enabled               *bool
}
