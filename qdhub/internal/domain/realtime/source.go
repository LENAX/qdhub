// Package realtime contains realtime data source domain entities.
package realtime

import (
	"encoding/json"
	"time"

	"qdhub/internal/domain/shared"
)

// RealtimeSourceType represents the type of realtime data source.
const (
	TypeTushareForward = "tushare_forward" // ts_proxy
	TypeTushareWS      = "tushare_ws"      // direct Tushare WS
	TypeSina           = "sina"
	TypeEastmoney      = "eastmoney" // dc
)

// Purpose identifies the use-case for selecting sources (ts_realtime_mkt_tick, realtime_quote, realtime_tick).
const (
	PurposeTsRealtimeMktTick = "ts_realtime_mkt_tick"
	PurposeRealtimeQuote    = "realtime_quote"
	PurposeRealtimeTick     = "realtime_tick"
)

// RealtimeSource represents a realtime data source (ts_proxy, tushare_ws, sina, eastmoney).
type RealtimeSource struct {
	ID                    shared.ID        `json:"id"`
	Name                  string           `json:"name"`
	Type                  string           `json:"type"`
	Config                string           `json:"config"` // JSON: type-specific (ws_url, rsa_public_key_path, endpoint, token, etc.)
	Priority              int              `json:"priority"`
	IsPrimary             bool             `json:"is_primary"`
	HealthCheckOnStartup  bool             `json:"health_check_on_startup"`
	Enabled               bool             `json:"enabled"`
	LastHealthStatus      string           `json:"last_health_status,omitempty"`
	LastHealthAt          shared.Timestamp `json:"last_health_at,omitempty"`
	LastHealthError       string           `json:"last_health_error,omitempty"`
	CreatedAt             shared.Timestamp `json:"created_at"`
	UpdatedAt             shared.Timestamp `json:"updated_at"`
}

// NewRealtimeSource creates a new RealtimeSource.
func NewRealtimeSource(name, typ, config string, priority int, isPrimary, healthCheckOnStartup, enabled bool) *RealtimeSource {
	now := shared.Now()
	return &RealtimeSource{
		ID:                   shared.NewID(),
		Name:                 name,
		Type:                 typ,
		Config:               config,
		Priority:             priority,
		IsPrimary:            isPrimary,
		HealthCheckOnStartup: healthCheckOnStartup,
		Enabled:              enabled,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
}

// UpdateHealth updates last health check result.
func (s *RealtimeSource) UpdateHealth(status, errMsg string) {
	s.LastHealthStatus = status
	s.LastHealthAt = shared.Timestamp(time.Now())
	s.LastHealthError = errMsg
	s.UpdatedAt = shared.Now()
}

// ConfigMap returns config as map (for type-specific fields like ws_url, rsa_public_key_path).
func (s *RealtimeSource) ConfigMap() (map[string]interface{}, error) {
	if s.Config == "" {
		return make(map[string]interface{}), nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(s.Config), &m); err != nil {
		return nil, err
	}
	if m == nil {
		return make(map[string]interface{}), nil
	}
	return m, nil
}

// SetConfigFromMap sets Config from a map (JSON serialized).
func (s *RealtimeSource) SetConfigFromMap(m map[string]interface{}) error {
	if len(m) == 0 {
		s.Config = "{}"
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.Config = string(b)
	return nil
}

// UpdateInfo updates name, config, priority, is_primary, enabled, health_check_on_startup.
func (s *RealtimeSource) UpdateInfo(name, config string, priority int, isPrimary, enabled, healthCheckOnStartup bool) {
	s.Name = name
	s.Config = config
	s.Priority = priority
	s.IsPrimary = isPrimary
	s.Enabled = enabled
	s.HealthCheckOnStartup = healthCheckOnStartup
	s.UpdatedAt = shared.Now()
}
