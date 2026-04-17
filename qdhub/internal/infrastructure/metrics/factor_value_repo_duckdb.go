package metrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"qdhub/internal/domain/datastore"
	domain "qdhub/internal/domain/metrics"
)

type FactorValueRepoDuckDB struct {
	db datastore.QuantDB
}

func NewFactorValueRepoDuckDB(db datastore.QuantDB) *FactorValueRepoDuckDB {
	return &FactorValueRepoDuckDB{db: db}
}

func (r *FactorValueRepoDuckDB) Replace(ctx context.Context, metricID string, startDate, endDate string, values []domain.FactorValue) error {
	if _, err := r.db.Execute(ctx, `DELETE FROM factor_value WHERE metric_id = ? AND trade_date >= ? AND trade_date <= ?`, metricID, startDate, endDate); err != nil {
		return fmt.Errorf("delete factor_value range: %w", err)
	}
	if len(values) == 0 {
		return nil
	}
	rows := make([]map[string]any, 0, len(values))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, value := range values {
		rows = append(rows, map[string]any{
			"metric_id":  value.MetricID,
			"entity_id":  value.EntityID,
			"trade_date": value.TradeDate,
			"frequency":  value.Frequency,
			"version":    value.Version,
			"value":      value.Value,
			"created_at": now,
		})
	}
	if _, err := r.db.BulkInsert(ctx, "factor_value", rows); err != nil {
		return fmt.Errorf("insert factor_value rows: %w", err)
	}
	return nil
}

func (r *FactorValueRepoDuckDB) QueryPanel(ctx context.Context, req domain.FactorPanelQuery) ([]domain.FactorValue, error) {
	if len(req.MetricIDs) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(req.MetricIDs)+4)
	sql := fmt.Sprintf(`SELECT fv.metric_id, fv.entity_id, fv.trade_date, fv.frequency, fv.version, fv.value
FROM factor_value fv
WHERE fv.metric_id IN (%s) AND fv.trade_date >= ? AND fv.trade_date <= ?`, placeholders(len(req.MetricIDs)))
	for _, metricID := range req.MetricIDs {
		args = append(args, metricID)
	}
	args = append(args, req.StartDate, req.EndDate)
	if strings.TrimSpace(req.Frequency) != "" {
		sql += ` AND fv.frequency = ?`
		args = append(args, req.Frequency)
	}
	if strings.TrimSpace(req.UniverseID) != "" {
		sql += ` AND EXISTS (
			SELECT 1 FROM universe_membership um
			WHERE um.universe_id = ? AND um.trade_date = fv.trade_date AND um.entity_id = fv.entity_id
		)`
		args = append(args, req.UniverseID)
	}
	sql += ` ORDER BY fv.trade_date, fv.entity_id, fv.metric_id`
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query factor panel: %w", err)
	}
	out := make([]domain.FactorValue, 0, len(rows))
	for _, row := range rows {
		out = append(out, domain.FactorValue{
			MetricID:  toString(row["metric_id"]),
			EntityID:  toString(row["entity_id"]),
			TradeDate: toString(row["trade_date"]),
			Frequency: toString(row["frequency"]),
			Version:   toInt(row["version"]),
			Value:     toFloat(row["value"]),
		})
	}
	return out, nil
}

func (r *FactorValueRepoDuckDB) LoadByMetric(ctx context.Context, metricID string, startDate, endDate string) ([]domain.FactorValue, error) {
	req := domain.FactorPanelQuery{
		MetricIDs: []string{metricID},
		StartDate: startDate,
		EndDate:   endDate,
	}
	return r.QueryPanel(ctx, req)
}
