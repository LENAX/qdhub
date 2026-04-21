package contracts

import (
	"context"

	"qdhub/internal/domain/metrics"
)

type CreateMetricRequest struct {
	Metric metrics.MetricDef `json:"metric"`
}

type SubmitMetricJobRequest struct {
	JobType       metrics.ComputeJobType   `json:"job_type"`
	TargetIDs     []string                 `json:"target_ids"`
	RangeType     metrics.ComputeRangeType `json:"range_type"`
	StartTime     string                   `json:"start_time,omitempty"`
	EndTime       string                   `json:"end_time,omitempty"`
	TriggerReason string                   `json:"trigger_reason,omitempty"`
	Priority      int                      `json:"priority,omitempty"`
}

type MetricsApplicationService interface {
	CreateMetric(ctx context.Context, req CreateMetricRequest) (*metrics.MetricDef, error)
	ListMetrics(ctx context.Context, filter metrics.MetricFilter) ([]*metrics.MetricDef, error)
	GetMetricDetail(ctx context.Context, metricID string) (*metrics.MetricDef, error)
	GetFactorPanel(ctx context.Context, req metrics.FactorPanelQuery) ([]metrics.FactorValue, error)
	GetSignalSeries(ctx context.Context, req metrics.SignalSeriesQuery) ([]metrics.SignalValue, error)
	GetUniverseMembers(ctx context.Context, req metrics.UniverseMembersQuery) ([]metrics.UniverseMembership, error)
	SubmitMetricJob(ctx context.Context, req SubmitMetricJobRequest) (*metrics.ComputeJob, error)
	GetMetricJobStatus(ctx context.Context, jobID string) (*metrics.ComputeJob, error)
}
