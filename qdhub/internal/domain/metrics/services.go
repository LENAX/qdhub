package metrics

import "context"

type DailyObservation struct {
	EntityID  string
	TradeDate string
	Features  map[string]Value
}

type Dataset struct {
	SeriesByEntity map[string][]DailyObservation
	EntitiesByDate map[string][]string
}

type ExpressionParser interface {
	Parse(expression string) (Node, error)
	CollectIdentifiers(node Node) []string
	Validate(metric *MetricDef) error
}

type RegistryService interface {
	RegisterMetric(ctx context.Context, metric *MetricDef) (*MetricDef, error)
	ListMetrics(ctx context.Context, filter MetricFilter) ([]*MetricDef, error)
	GetMetric(ctx context.Context, metricID string) (*MetricDef, error)
	GetFactorPanel(ctx context.Context, req FactorPanelQuery) ([]FactorValue, error)
	GetSignalSeries(ctx context.Context, req SignalSeriesQuery) ([]SignalValue, error)
	GetUniverseMembers(ctx context.Context, req UniverseMembersQuery) ([]UniverseMembership, error)
}

type JobExecutor interface {
	Execute(ctx context.Context, job *ComputeJob) error
}
