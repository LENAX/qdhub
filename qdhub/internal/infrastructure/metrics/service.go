package metrics

import (
	"context"
	"fmt"
	"strings"

	"qdhub/internal/domain/datastore"
	domain "qdhub/internal/domain/metrics"
	"qdhub/internal/domain/shared"
)

// Service is the infrastructure-layer orchestrator for metric computation.
// It owns the SQL compiler, residual runner, and repositories; the evaluator
// AST interpreter has been fully replaced by the DuckDB compiler.
type Service struct {
	db             datastore.QuantDB
	parser         domain.ExpressionParser
	metricRepo     domain.MetricDefRepository
	factorRepo     domain.FactorValueRepository
	signalRepo     domain.SignalValueRepository
	universeRepo   domain.UniverseMembershipRepository
	compiler       *SQLCompiler
	residualRunner *ResidualRunner
}

func NewService(
	db datastore.QuantDB,
	parser domain.ExpressionParser,
	metricRepo domain.MetricDefRepository,
	factorRepo domain.FactorValueRepository,
	signalRepo domain.SignalValueRepository,
	universeRepo domain.UniverseMembershipRepository,
) *Service {
	compiler := NewCachedSQLCompiler(db, parser, metricRepo)
	return &Service{
		db:             db,
		parser:         parser,
		metricRepo:     metricRepo,
		factorRepo:     factorRepo,
		signalRepo:     signalRepo,
		universeRepo:   universeRepo,
		compiler:       compiler,
		residualRunner: NewResidualRunner(db, factorRepo),
	}
}

var _ domain.RegistryService = (*Service)(nil)
var _ domain.JobExecutor = (*Service)(nil)

func (s *Service) RegisterMetric(ctx context.Context, metric *domain.MetricDef) (*domain.MetricDef, error) {
	if err := metric.Validate(); err != nil {
		return nil, err
	}
	if err := s.parser.Validate(metric); err != nil {
		return nil, err
	}
	allMetrics, err := s.metricRepo.List(ctx, domain.MetricFilter{})
	if err != nil {
		return nil, err
	}
	existing := make(map[string]*domain.MetricDef, len(allMetrics))
	for _, item := range allMetrics {
		existing[item.ID] = item
	}
	node, err := s.parser.Parse(metric.Expression)
	if err != nil {
		return nil, err
	}
	identifiers := s.parser.CollectIdentifiers(node)
	if len(metric.DependsOn) == 0 {
		for _, ident := range identifiers {
			if ident == metric.ID {
				return nil, shared.NewDomainError(shared.ErrCodeValidation, "metric cannot depend on itself", nil)
			}
			if _, ok := existing[ident]; ok {
				metric.DependsOn = append(metric.DependsOn, ident)
			}
		}
	}
	for _, depID := range metric.DependsOn {
		dep, ok := existing[depID]
		if !ok {
			return nil, shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("depends_on metric not found: %s", depID), nil)
		}
		if metric.Kind == domain.MetricKindSignal && dep.Kind == domain.MetricKindSignal {
			return nil, shared.NewDomainError(shared.ErrCodeValidation, "signal -> signal dependency is not supported in phase 1", nil)
		}
	}
	if err := ensureNoCycle(existing, metric); err != nil {
		return nil, err
	}
	if err := s.metricRepo.Save(ctx, metric); err != nil {
		return nil, err
	}
	return s.metricRepo.Get(ctx, metric.ID)
}

func ensureNoCycle(existing map[string]*domain.MetricDef, candidate *domain.MetricDef) error {
	graph := make(map[string][]string, len(existing)+1)
	for id, metric := range existing {
		graph[id] = append([]string(nil), metric.DependsOn...)
	}
	graph[candidate.ID] = append([]string(nil), candidate.DependsOn...)
	visiting := make(map[string]bool)
	visited := make(map[string]bool)
	var walk func(string) error
	walk = func(id string) error {
		if visiting[id] {
			return shared.NewDomainError(shared.ErrCodeValidation, "metric dependency cycle detected", nil)
		}
		if visited[id] {
			return nil
		}
		visiting[id] = true
		for _, dep := range graph[id] {
			if err := walk(dep); err != nil {
				return err
			}
		}
		visiting[id] = false
		visited[id] = true
		return nil
	}
	return walk(candidate.ID)
}

func (s *Service) ListMetrics(ctx context.Context, filter domain.MetricFilter) ([]*domain.MetricDef, error) {
	return s.metricRepo.List(ctx, filter)
}

func (s *Service) GetMetric(ctx context.Context, metricID string) (*domain.MetricDef, error) {
	return s.metricRepo.Get(ctx, metricID)
}

func (s *Service) GetFactorPanel(ctx context.Context, req domain.FactorPanelQuery) ([]domain.FactorValue, error) {
	return s.factorRepo.QueryPanel(ctx, req)
}

func (s *Service) GetSignalSeries(ctx context.Context, req domain.SignalSeriesQuery) ([]domain.SignalValue, error) {
	return s.signalRepo.QuerySeries(ctx, req)
}

func (s *Service) GetUniverseMembers(ctx context.Context, req domain.UniverseMembersQuery) ([]domain.UniverseMembership, error) {
	return s.universeRepo.QueryMembers(ctx, req)
}

func (s *Service) Execute(ctx context.Context, job *domain.ComputeJob) error {
	if err := job.Validate(); err != nil {
		return err
	}
	startDate, endDate, err := s.resolveDateRange(ctx, job.StartTime, job.EndTime)
	if err != nil {
		return err
	}
	completed := make(map[string]bool)
	for _, targetID := range job.TargetIDs {
		if err := s.executeMetric(ctx, targetID, startDate, endDate, completed); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) executeMetric(ctx context.Context, metricID, startDate, endDate string, completed map[string]bool) error {
	if completed[metricID] {
		return nil
	}
	metric, err := s.metricRepo.Get(ctx, metricID)
	if err != nil {
		return err
	}
	for _, depID := range metric.DependsOn {
		if err := s.executeMetric(ctx, depID, startDate, endDate, completed); err != nil {
			return err
		}
	}
	if err := s.materialize(ctx, metric, startDate, endDate); err != nil {
		return err
	}
	completed[metricID] = true
	return nil
}

// materialize compiles the metric to a LogicalPlan and executes it. Residual
// plans bypass the SQL transaction path and use the gonum runner.
func (s *Service) materialize(ctx context.Context, metric *domain.MetricDef, startDate, endDate string) error {
	plan, err := s.compiler.Compile(ctx, metric, startDate, endDate)
	if err != nil {
		return fmt.Errorf("compile metric %s: %w", metric.ID, err)
	}
	if plan.Residual != nil {
		return s.residualRunner.Run(ctx, metric, startDate, endDate, plan.Residual)
	}
	return s.executePureSQLPlan(ctx, metric, plan, startDate, endDate)
}

// executePureSQLPlan runs DELETE + INSERT inside a single transaction so the
// rewrite is atomic. Any failure rolls the DELETE back.
func (s *Service) executePureSQLPlan(ctx context.Context, metric *domain.MetricDef, plan *LogicalPlan, startDate, endDate string) error {
	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin tx for %s: %w", metric.ID, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	deleteSQL, err := deleteStatementForKind(metric.Kind)
	if err != nil {
		return err
	}
	if _, err := tx.Execute(ctx, deleteSQL, metric.ID, startDate, endDate); err != nil {
		return fmt.Errorf("delete existing rows for %s: %w", metric.ID, err)
	}
	if _, err := tx.Execute(ctx, plan.SQL, plan.Args...); err != nil {
		return fmt.Errorf("insert compiled rows for %s: %w", metric.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", metric.ID, err)
	}
	committed = true
	return nil
}

func deleteStatementForKind(kind domain.MetricKind) (string, error) {
	switch kind {
	case domain.MetricKindFactor:
		return `DELETE FROM factor_value WHERE metric_id = ? AND trade_date >= ? AND trade_date <= ?`, nil
	case domain.MetricKindSignal:
		return `DELETE FROM signal_value WHERE metric_id = ? AND trade_date >= ? AND trade_date <= ?`, nil
	case domain.MetricKindUniverse:
		return `DELETE FROM universe_membership WHERE universe_id = ? AND trade_date >= ? AND trade_date <= ?`, nil
	}
	return "", shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported metric kind %s", kind), nil)
}

func (s *Service) resolveDateRange(ctx context.Context, startDate, endDate string) (string, string, error) {
	if strings.TrimSpace(startDate) != "" && strings.TrimSpace(endDate) != "" {
		return startDate, endDate, nil
	}
	rows, err := s.db.Query(ctx, `SELECT MIN(trade_date) AS min_td, MAX(trade_date) AS max_td FROM daily`)
	if err != nil {
		return "", "", fmt.Errorf("resolve date range: %w", err)
	}
	if len(rows) == 0 {
		return "", "", shared.NewDomainError(shared.ErrCodeValidation, "daily table is empty", nil)
	}
	minTD := toString(rows[0]["min_td"])
	maxTD := toString(rows[0]["max_td"])
	if strings.TrimSpace(startDate) == "" {
		startDate = minTD
	}
	if strings.TrimSpace(endDate) == "" {
		endDate = maxTD
	}
	if startDate == "" || endDate == "" {
		return "", "", shared.NewDomainError(shared.ErrCodeValidation, "unable to resolve date range from daily table", nil)
	}
	return startDate, endDate, nil
}
