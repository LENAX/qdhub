package metrics

import (
	domain "qdhub/internal/domain/metrics"
)

// LogicalPlan is the compiled form of a MetricDef expression handed to the
// execution layer. It is the only channel between compiler and service; the
// service must not build SQL itself.
//
// Exactly one of (SQL) or (Residual) is populated:
//   - SQL path   : a complete INSERT...SELECT statement ready for tx.Execute.
//   - Residual   : metadata for a gonum-backed post-processing path.
type LogicalPlan struct {
	Kind     domain.MetricKind
	SQL      string
	Args     []any
	Residual *ResidualPlan
}

// ResidualPlan describes a compute job that cannot be expressed in pure DuckDB
// SQL (currently only spearman_corr). InputSQL must produce the columns
// (entity_id, trade_date, x, y) ordered by (entity_id, trade_date).
type ResidualPlan struct {
	InputSQL  string
	InputArgs []any
	Fn        string
	Window    int
}
