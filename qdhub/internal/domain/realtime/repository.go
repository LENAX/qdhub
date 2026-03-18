// Package realtime contains realtime data source repository interface.
package realtime

import (
	"qdhub/internal/domain/shared"
)

// RealtimeSourceRepository defines the repository for RealtimeSource.
type RealtimeSourceRepository interface {
	shared.Repository[RealtimeSource]

	// ListEnabledForHealthCheck returns enabled sources with health_check_on_startup=true.
	ListEnabledForHealthCheck() ([]*RealtimeSource, error)

	// GetOrderedByPurpose returns enabled sources for the given purpose, ordered by priority ascending.
	// purpose: PurposeTsRealtimeMktTick -> types tushare_forward, tushare_ws; PurposeRealtimeQuote -> sina, eastmoney; PurposeRealtimeTick -> eastmoney.
	GetOrderedByPurpose(purpose string) ([]*RealtimeSource, error)
}
