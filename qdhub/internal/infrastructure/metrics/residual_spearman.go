package metrics

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"

	"qdhub/internal/domain/datastore"
	domain "qdhub/internal/domain/metrics"
)

// ResidualCalculator is the numeric back-end used for any DSL operator that
// cannot be expressed as pure DuckDB SQL. A single implementation is currently
// enough (gonumCalculator) but the interface keeps the door open for swapping
// in SIMD/BLAS variants later without touching callers.
type ResidualCalculator interface {
	// RollingSpearman computes, for each position i, the Spearman rank
	// correlation of x and y over the window [max(0,i-window+1), i]. NaN is
	// returned where the window has fewer than 2 valid pairs or the pairwise
	// variance collapses to zero. Ties are broken by stable ordering to match
	// the legacy evaluator semantics (non-average ranks, 1-based).
	RollingSpearman(x, y []float64, window int) []float64
}

var defaultResidualCalculator ResidualCalculator = gonumCalculator{}

// NewGonumCalculator returns the default pure-Go residual calculator. Exported
// so tests (and future alternate pipelines) can exercise it without reaching
// into internals.
func NewGonumCalculator() ResidualCalculator { return gonumCalculator{} }

// gonumCalculator implements ResidualCalculator with pure-Go gonum (stat +
// floats). No CGO; portable across macOS dev and Alibaba Cloud Linux prod.
type gonumCalculator struct{}

func (gonumCalculator) RollingSpearman(x, y []float64, window int) []float64 {
	n := len(x)
	out := make([]float64, n)
	if window < 2 || n < 2 {
		for i := range out {
			out[i] = math.NaN()
		}
		return out
	}
	xs := make([]float64, 0, window)
	ys := make([]float64, 0, window)
	for i := 0; i < n; i++ {
		start := i - window + 1
		if start < 0 {
			start = 0
		}
		xs = xs[:0]
		ys = ys[:0]
		for j := start; j <= i; j++ {
			if math.IsNaN(x[j]) || math.IsNaN(y[j]) {
				continue
			}
			xs = append(xs, x[j])
			ys = append(ys, y[j])
		}
		if len(xs) < 2 {
			out[i] = math.NaN()
			continue
		}
		rx := rankFloats(xs)
		ry := rankFloats(ys)
		corr := stat.Correlation(rx, ry, nil)
		out[i] = corr
	}
	return out
}

// rankFloats returns 1-based ranks using a stable ordering that matches the
// legacy evaluator's rankValues: equal values get distinct (non-averaged) ranks
// based on their original position.
func rankFloats(values []float64) []float64 {
	n := len(values)
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(i, j int) bool {
		return values[idx[i]] < values[idx[j]]
	})
	ranks := make([]float64, n)
	for rank, original := range idx {
		ranks[original] = float64(rank + 1)
	}
	return ranks
}

// _ keeps the floats import used even if we simplify elsewhere.
var _ = floats.Sum

// ResidualRunner executes a ResidualPlan: loads (entity_id, trade_date, x, y)
// tuples from DuckDB, computes rolling spearman per entity, then writes into
// factor_value inside a transaction.
type ResidualRunner struct {
	db         datastore.QuantDB
	factorRepo domain.FactorValueRepository
	calculator ResidualCalculator
}

// NewResidualRunner wires a runner with the default (gonum) backend.
func NewResidualRunner(db datastore.QuantDB, factorRepo domain.FactorValueRepository) *ResidualRunner {
	return &ResidualRunner{db: db, factorRepo: factorRepo, calculator: defaultResidualCalculator}
}

// WithCalculator returns a copy of the runner with a custom calculator (tests).
func (r *ResidualRunner) WithCalculator(c ResidualCalculator) *ResidualRunner {
	cp := *r
	cp.calculator = c
	return &cp
}

// Run executes the plan and persists factor values.
func (r *ResidualRunner) Run(ctx context.Context, metric *domain.MetricDef, startDate, endDate string, plan *ResidualPlan) error {
	if plan == nil {
		return fmt.Errorf("residual plan is nil")
	}
	rows, err := r.db.Query(ctx, plan.InputSQL, plan.InputArgs...)
	if err != nil {
		return fmt.Errorf("load residual input: %w", err)
	}

	type pair struct {
		trade string
		x, y  float64
	}
	grouped := make(map[string][]pair, 64)
	entityOrder := make([]string, 0, 64)
	for _, row := range rows {
		entityID := toString(row["entity_id"])
		p := pair{
			trade: toString(row["trade_date"]),
			x:     coerceFloatOrNaN(row["x"]),
			y:     coerceFloatOrNaN(row["y"]),
		}
		if _, seen := grouped[entityID]; !seen {
			entityOrder = append(entityOrder, entityID)
		}
		grouped[entityID] = append(grouped[entityID], p)
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	values := make([]map[string]any, 0, len(rows))
	factorValues := make([]domain.FactorValue, 0, len(rows))
	for _, entityID := range entityOrder {
		series := grouped[entityID]
		xs := make([]float64, len(series))
		ys := make([]float64, len(series))
		for i, p := range series {
			xs[i] = p.x
			ys[i] = p.y
		}
		out := r.calculator.RollingSpearman(xs, ys, plan.Window)
		for i, v := range out {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			factorValues = append(factorValues, domain.FactorValue{
				MetricID:  metric.ID,
				EntityID:  entityID,
				TradeDate: series[i].trade,
				Frequency: string(metric.Frequency),
				Version:   metric.Version,
				Value:     v,
			})
			values = append(values, map[string]any{
				"metric_id":  metric.ID,
				"entity_id":  entityID,
				"trade_date": series[i].trade,
				"frequency":  string(metric.Frequency),
				"version":    metric.Version,
				"value":      v,
				"created_at": now,
			})
		}
	}

	return r.factorRepo.Replace(ctx, metric.ID, startDate, endDate, factorValues)
}

func coerceFloatOrNaN(v any) float64 {
	if v == nil {
		return math.NaN()
	}
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	}
	return toFloat(v)
}
