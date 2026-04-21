package metrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"qdhub/internal/domain/datastore"
	domain "qdhub/internal/domain/metrics"
	"qdhub/internal/domain/shared"
)

type MetricDefRepoDuckDB struct {
	db datastore.QuantDB
}

func NewMetricDefRepoDuckDB(db datastore.QuantDB) *MetricDefRepoDuckDB {
	return &MetricDefRepoDuckDB{db: db}
}

func (r *MetricDefRepoDuckDB) Save(ctx context.Context, metric *domain.MetricDef) error {
	if err := metric.Validate(); err != nil {
		return err
	}
	now := time.Now().UTC()
	if metric.CreatedAt.IsZero() {
		metric.CreatedAt = now
	}
	metric.UpdatedAt = now
	sql := `INSERT INTO metric_def (
		metric_id, display_name_cn, kind, category, expression, frequency, source_resolution, status, version,
		depends_on_json, factor_spec_json, signal_spec_json, universe_spec_json, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(metric_id) DO UPDATE SET
		display_name_cn = EXCLUDED.display_name_cn,
		kind = EXCLUDED.kind,
		category = EXCLUDED.category,
		expression = EXCLUDED.expression,
		frequency = EXCLUDED.frequency,
		source_resolution = EXCLUDED.source_resolution,
		status = EXCLUDED.status,
		version = EXCLUDED.version,
		depends_on_json = EXCLUDED.depends_on_json,
		factor_spec_json = EXCLUDED.factor_spec_json,
		signal_spec_json = EXCLUDED.signal_spec_json,
		universe_spec_json = EXCLUDED.universe_spec_json,
		updated_at = EXCLUDED.updated_at`
	_, err := r.db.Execute(ctx, sql,
		metric.ID,
		metric.DisplayNameCN,
		string(metric.Kind),
		metric.Category,
		metric.Expression,
		string(metric.Frequency),
		metric.SourceResolution,
		string(metric.Status),
		metric.Version,
		mustJSON(metric.DependsOn),
		mustJSON(metric.FactorSpec),
		mustJSON(metric.SignalSpec),
		mustJSON(metric.UniverseSpec),
		metric.CreatedAt.Format(time.RFC3339Nano),
		metric.UpdatedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("save metric_def: %w", err)
	}
	return nil
}

func (r *MetricDefRepoDuckDB) Get(ctx context.Context, metricID string) (*domain.MetricDef, error) {
	rows, err := r.db.Query(ctx, `SELECT * FROM metric_def WHERE metric_id = ?`, metricID)
	if err != nil {
		return nil, fmt.Errorf("get metric_def: %w", err)
	}
	if len(rows) == 0 {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "metric not found", nil)
	}
	return mapMetricDef(rows[0])
}

func (r *MetricDefRepoDuckDB) List(ctx context.Context, filter domain.MetricFilter) ([]*domain.MetricDef, error) {
	sql := `SELECT * FROM metric_def WHERE 1=1`
	args := make([]any, 0)
	if filter.Kind != "" {
		sql += ` AND kind = ?`
		args = append(args, string(filter.Kind))
	}
	if strings.TrimSpace(filter.Status) != "" {
		sql += ` AND status = ?`
		args = append(args, strings.TrimSpace(filter.Status))
	}
	if strings.TrimSpace(filter.Category) != "" {
		sql += ` AND category = ?`
		args = append(args, strings.TrimSpace(filter.Category))
	}
	if filter.Frequency != "" {
		sql += ` AND frequency = ?`
		args = append(args, string(filter.Frequency))
	}
	if strings.TrimSpace(filter.Query) != "" {
		sql += ` AND (metric_id ILIKE ? OR display_name_cn ILIKE ?)`
		q := "%" + strings.TrimSpace(filter.Query) + "%"
		args = append(args, q, q)
	}
	sql += ` ORDER BY metric_id`
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("list metric_def: %w", err)
	}
	out := make([]*domain.MetricDef, 0, len(rows))
	for _, row := range rows {
		item, err := mapMetricDef(row)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func mapMetricDef(row map[string]any) (*domain.MetricDef, error) {
	item := &domain.MetricDef{
		ID:               toString(row["metric_id"]),
		DisplayNameCN:    toString(row["display_name_cn"]),
		Kind:             domain.MetricKind(toString(row["kind"])),
		Category:         toString(row["category"]),
		Expression:       toString(row["expression"]),
		Frequency:        domain.Frequency(toString(row["frequency"])),
		SourceResolution: toString(row["source_resolution"]),
		Status:           domain.MetricStatus(toString(row["status"])),
		Version:          toInt(row["version"]),
	}
	if err := decodeJSON(row["depends_on_json"], &item.DependsOn); err != nil {
		return nil, fmt.Errorf("decode depends_on_json: %w", err)
	}
	if err := decodeJSON(row["factor_spec_json"], &item.FactorSpec); err != nil {
		return nil, fmt.Errorf("decode factor_spec_json: %w", err)
	}
	if err := decodeJSON(row["signal_spec_json"], &item.SignalSpec); err != nil {
		return nil, fmt.Errorf("decode signal_spec_json: %w", err)
	}
	if err := decodeJSON(row["universe_spec_json"], &item.UniverseSpec); err != nil {
		return nil, fmt.Errorf("decode universe_spec_json: %w", err)
	}
	if created := toTimePtr(row["created_at"]); created != nil {
		item.CreatedAt = created.UTC()
	}
	if updated := toTimePtr(row["updated_at"]); updated != nil {
		item.UpdatedAt = updated.UTC()
	}
	item.Normalize()
	return item, nil
}
