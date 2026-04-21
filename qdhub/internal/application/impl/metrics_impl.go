package impl

import (
	"context"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/metrics"
)

type MetricsApplicationServiceImpl struct {
	registry contractsMetricsRegistry
	executor metrics.JobExecutor
	jobRepo  metrics.ComputeJobRepository
}

type contractsMetricsRegistry interface {
	RegisterMetric(ctx context.Context, metric *metrics.MetricDef) (*metrics.MetricDef, error)
	ListMetrics(ctx context.Context, filter metrics.MetricFilter) ([]*metrics.MetricDef, error)
	GetMetric(ctx context.Context, metricID string) (*metrics.MetricDef, error)
	GetFactorPanel(ctx context.Context, req metrics.FactorPanelQuery) ([]metrics.FactorValue, error)
	GetSignalSeries(ctx context.Context, req metrics.SignalSeriesQuery) ([]metrics.SignalValue, error)
	GetUniverseMembers(ctx context.Context, req metrics.UniverseMembersQuery) ([]metrics.UniverseMembership, error)
}

func NewMetricsApplicationService(registry contractsMetricsRegistry, executor metrics.JobExecutor, jobRepo metrics.ComputeJobRepository) contracts.MetricsApplicationService {
	return &MetricsApplicationServiceImpl{
		registry: registry,
		executor: executor,
		jobRepo:  jobRepo,
	}
}

func (s *MetricsApplicationServiceImpl) CreateMetric(ctx context.Context, req contracts.CreateMetricRequest) (*metrics.MetricDef, error) {
	return s.registry.RegisterMetric(ctx, &req.Metric)
}

func (s *MetricsApplicationServiceImpl) ListMetrics(ctx context.Context, filter metrics.MetricFilter) ([]*metrics.MetricDef, error) {
	return s.registry.ListMetrics(ctx, filter)
}

func (s *MetricsApplicationServiceImpl) GetMetricDetail(ctx context.Context, metricID string) (*metrics.MetricDef, error) {
	return s.registry.GetMetric(ctx, metricID)
}

func (s *MetricsApplicationServiceImpl) GetFactorPanel(ctx context.Context, req metrics.FactorPanelQuery) ([]metrics.FactorValue, error) {
	return s.registry.GetFactorPanel(ctx, req)
}

func (s *MetricsApplicationServiceImpl) GetSignalSeries(ctx context.Context, req metrics.SignalSeriesQuery) ([]metrics.SignalValue, error) {
	return s.registry.GetSignalSeries(ctx, req)
}

func (s *MetricsApplicationServiceImpl) GetUniverseMembers(ctx context.Context, req metrics.UniverseMembersQuery) ([]metrics.UniverseMembership, error) {
	return s.registry.GetUniverseMembers(ctx, req)
}

func (s *MetricsApplicationServiceImpl) SubmitMetricJob(ctx context.Context, req contracts.SubmitMetricJobRequest) (*metrics.ComputeJob, error) {
	job := metrics.NewComputeJob(req.JobType, req.TargetIDs, req.RangeType)
	job.StartTime = req.StartTime
	job.EndTime = req.EndTime
	job.TriggerReason = req.TriggerReason
	job.Priority = req.Priority
	job.MarkQueued()
	if err := s.jobRepo.Save(ctx, job); err != nil {
		return nil, err
	}
	go s.runJob(job)
	return job, nil
}

func (s *MetricsApplicationServiceImpl) runJob(job *metrics.ComputeJob) {
	ctx := context.Background()
	job.MarkRunning()
	if err := s.jobRepo.Save(ctx, job); err != nil {
		return
	}
	if err := s.executor.Execute(ctx, job); err != nil {
		job.MarkFailed(err)
		_ = s.jobRepo.Save(ctx, job)
		return
	}
	job.MarkSucceeded("materialization finished")
	_ = s.jobRepo.Save(ctx, job)
}

func (s *MetricsApplicationServiceImpl) GetMetricJobStatus(ctx context.Context, jobID string) (*metrics.ComputeJob, error) {
	return s.jobRepo.Get(ctx, jobID)
}
