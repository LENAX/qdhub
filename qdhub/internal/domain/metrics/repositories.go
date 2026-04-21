package metrics

import "context"

type MetricFilter struct {
	Kind      MetricKind `json:"kind,omitempty"`
	Status    string     `json:"status,omitempty"`
	Category  string     `json:"category,omitempty"`
	Query     string     `json:"query,omitempty"`
	Frequency Frequency  `json:"frequency,omitempty"`
}

type FactorValue struct {
	MetricID  string  `json:"metric_id"`
	EntityID  string  `json:"entity_id"`
	TradeDate string  `json:"trade_date"`
	Frequency string  `json:"frequency"`
	Version   int     `json:"version"`
	Value     float64 `json:"value"`
}

type SignalValue struct {
	MetricID  string `json:"metric_id"`
	EntityID  string `json:"entity_id"`
	TradeDate string `json:"trade_date"`
	Frequency string `json:"frequency"`
	Version   int    `json:"version"`
	BoolValue *bool  `json:"bool_value,omitempty"`
	TextValue string `json:"text_value,omitempty"`
}

type UniverseMembership struct {
	UniverseID string `json:"universe_id"`
	EntityID   string `json:"entity_id"`
	TradeDate  string `json:"trade_date"`
	Frequency  string `json:"frequency"`
	Version    int    `json:"version"`
}

type FactorPanelQuery struct {
	MetricIDs  []string `json:"metric_ids"`
	UniverseID string   `json:"universe_id,omitempty"`
	StartDate  string   `json:"start_date"`
	EndDate    string   `json:"end_date"`
	Frequency  string   `json:"frequency"`
}

type SignalSeriesQuery struct {
	MetricIDs []string `json:"metric_ids"`
	EntityIDs []string `json:"entity_ids,omitempty"`
	StartDate string   `json:"start_date"`
	EndDate   string   `json:"end_date"`
	Frequency string   `json:"frequency"`
}

type UniverseMembersQuery struct {
	UniverseID string `json:"universe_id"`
	TradeDate  string `json:"trade_date,omitempty"`
	StartDate  string `json:"start_date,omitempty"`
	EndDate    string `json:"end_date,omitempty"`
	Frequency  string `json:"frequency"`
}

type MetricDefRepository interface {
	Save(ctx context.Context, metric *MetricDef) error
	Get(ctx context.Context, metricID string) (*MetricDef, error)
	List(ctx context.Context, filter MetricFilter) ([]*MetricDef, error)
}

type FactorValueRepository interface {
	Replace(ctx context.Context, metricID string, startDate, endDate string, values []FactorValue) error
	QueryPanel(ctx context.Context, req FactorPanelQuery) ([]FactorValue, error)
	LoadByMetric(ctx context.Context, metricID string, startDate, endDate string) ([]FactorValue, error)
}

type SignalValueRepository interface {
	Replace(ctx context.Context, metricID string, startDate, endDate string, values []SignalValue) error
	QuerySeries(ctx context.Context, req SignalSeriesQuery) ([]SignalValue, error)
	LoadByMetric(ctx context.Context, metricID string, startDate, endDate string) ([]SignalValue, error)
}

type UniverseMembershipRepository interface {
	Replace(ctx context.Context, universeID string, startDate, endDate string, values []UniverseMembership) error
	QueryMembers(ctx context.Context, req UniverseMembersQuery) ([]UniverseMembership, error)
}

type ComputeJobRepository interface {
	Save(ctx context.Context, job *ComputeJob) error
	Get(ctx context.Context, jobID string) (*ComputeJob, error)
}
