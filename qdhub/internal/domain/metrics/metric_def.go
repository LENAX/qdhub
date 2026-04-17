package metrics

import (
	"fmt"
	"strings"
	"time"

	"qdhub/internal/domain/shared"
)

type MetricKind string

const (
	MetricKindFactor   MetricKind = "factor"
	MetricKindSignal   MetricKind = "signal"
	MetricKindUniverse MetricKind = "universe"
)

type MetricStatus string

const (
	MetricStatusDraft      MetricStatus = "draft"
	MetricStatusActive     MetricStatus = "active"
	MetricStatusPaused     MetricStatus = "paused"
	MetricStatusDeprecated MetricStatus = "deprecated"
	MetricStatusArchived   MetricStatus = "archived"
)

type Frequency string

const (
	FrequencyDaily Frequency = "1d"
)

type MetricDef struct {
	ID               string        `json:"metric_id"`
	DisplayNameCN    string        `json:"display_name_cn"`
	Kind             MetricKind    `json:"kind"`
	Category         string        `json:"category,omitempty"`
	Expression       string        `json:"expression"`
	Frequency        Frequency     `json:"frequency"`
	SourceResolution string        `json:"source_resolution,omitempty"`
	Status           MetricStatus  `json:"status"`
	Version          int           `json:"version"`
	DependsOn        []string      `json:"depends_on,omitempty"`
	FactorSpec       *FactorSpec   `json:"factor_spec,omitempty"`
	SignalSpec       *SignalSpec   `json:"signal_spec,omitempty"`
	UniverseSpec     *UniverseSpec `json:"universe_spec,omitempty"`
	CreatedAt        time.Time     `json:"created_at"`
	UpdatedAt        time.Time     `json:"updated_at"`
}

func (m *MetricDef) Normalize() {
	m.ID = strings.TrimSpace(m.ID)
	m.DisplayNameCN = strings.TrimSpace(m.DisplayNameCN)
	m.Category = strings.TrimSpace(m.Category)
	m.Expression = strings.TrimSpace(m.Expression)
	m.SourceResolution = strings.TrimSpace(m.SourceResolution)
	if m.Frequency == "" {
		m.Frequency = FrequencyDaily
	}
	if m.Status == "" {
		m.Status = MetricStatusDraft
	}
	if m.Version <= 0 {
		m.Version = 1
	}
	normalized := make([]string, 0, len(m.DependsOn))
	seen := make(map[string]struct{}, len(m.DependsOn))
	for _, item := range m.DependsOn {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		normalized = append(normalized, item)
	}
	m.DependsOn = normalized
	if m.UniverseSpec != nil {
		m.UniverseSpec.Normalize()
	}
}

func (m *MetricDef) Validate() error {
	if m == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "metric definition is required", nil)
	}
	m.Normalize()
	if m.ID == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "metric_id is required", nil)
	}
	if m.DisplayNameCN == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "display_name_cn is required", nil)
	}
	if m.Expression == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "expression is required", nil)
	}
	switch m.Kind {
	case MetricKindFactor:
		if m.FactorSpec == nil {
			return shared.NewDomainError(shared.ErrCodeValidation, "factor metric requires factor_spec", nil)
		}
		if err := m.FactorSpec.Validate(); err != nil {
			return err
		}
	case MetricKindSignal:
		if m.SignalSpec == nil {
			return shared.NewDomainError(shared.ErrCodeValidation, "signal metric requires signal_spec", nil)
		}
		if err := m.SignalSpec.Validate(); err != nil {
			return err
		}
	case MetricKindUniverse:
		if m.UniverseSpec == nil {
			return shared.NewDomainError(shared.ErrCodeValidation, "universe metric requires universe_spec", nil)
		}
	default:
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported metric kind: %s", m.Kind), nil)
	}
	if m.Frequency != FrequencyDaily {
		return shared.NewDomainError(shared.ErrCodeValidation, "only 1d frequency is supported in phase 1", nil)
	}
	switch m.Status {
	case MetricStatusDraft, MetricStatusActive, MetricStatusPaused, MetricStatusDeprecated, MetricStatusArchived:
	default:
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported metric status: %s", m.Status), nil)
	}
	return nil
}
