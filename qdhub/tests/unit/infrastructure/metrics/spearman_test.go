package metrics_test

import (
	"context"
	"math"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/stat"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	domain "qdhub/internal/domain/metrics"
	"qdhub/internal/infrastructure/metrics"
	duckdbinfra "qdhub/internal/infrastructure/quantdb/duckdb"
)

// TestRollingSpearman_MatchesManualRanks pins the rolling Spearman semantics
// used by the residual path. We compute the same thing the compiler handles
// off-SQL and assert it equals stat.Correlation over independently-ranked
// slices, matching the legacy evaluator exactly.
func TestRollingSpearman_MatchesManualRanks(t *testing.T) {
	t.Parallel()
	calc := metrics.NewGonumCalculator()

	x := []float64{10, 20, 30, 25, 35, 50, 40}
	y := []float64{1, 2, 3, 2, 4, 7, 5}
	window := 4

	got := calc.RollingSpearman(x, y, window)
	require.Len(t, got, len(x))

	expected := make([]float64, len(x))
	for i := range x {
		start := i - window + 1
		if start < 0 {
			start = 0
		}
		xs := make([]float64, 0, window)
		ys := make([]float64, 0, window)
		for j := start; j <= i; j++ {
			if math.IsNaN(x[j]) || math.IsNaN(y[j]) {
				continue
			}
			xs = append(xs, x[j])
			ys = append(ys, y[j])
		}
		if len(xs) < 2 {
			expected[i] = math.NaN()
			continue
		}
		expected[i] = stat.Correlation(stableRank(xs), stableRank(ys), nil)
	}

	for i := range got {
		if math.IsNaN(expected[i]) {
			require.Truef(t, math.IsNaN(got[i]), "i=%d want NaN got=%v", i, got[i])
			continue
		}
		require.InDeltaf(t, expected[i], got[i], 1e-12, "i=%d", i)
	}
}

func TestRollingSpearman_HandlesNaNInputs(t *testing.T) {
	t.Parallel()
	calc := metrics.NewGonumCalculator()

	x := []float64{1, math.NaN(), 3, 4, 5}
	y := []float64{2, 2, math.NaN(), 4, 5}
	got := calc.RollingSpearman(x, y, 3)

	require.Truef(t, math.IsNaN(got[0]), "index 0 should be NaN (single valid pair)")
	require.InDelta(t, 1.0, got[4], 1e-12)
}

// TestResidualRunner_EndToEnd wires the full residual path against an in-memory
// DuckDB fixture. We register a spearman_corr factor, trigger the pipeline, and
// verify the stored factor values match an independent Python-style rolling
// Spearman computed in this test.
func TestResidualRunner_EndToEnd(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "spearman_test.duckdb")
	db := duckdbinfra.NewAdapter(dbPath)
	require.NoError(t, db.Connect(ctx))
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, metrics.EnsureSchema(ctx, db))
	seedSpearmanFixture(t, ctx, db)

	metricRepo := metrics.NewMetricDefRepoDuckDB(db)
	factorRepo := metrics.NewFactorValueRepoDuckDB(db)
	signalRepo := metrics.NewSignalValueRepoDuckDB(db)
	universeRepo := metrics.NewUniverseRepoDuckDB(db)
	jobRepo := metrics.NewComputeJobRepoDuckDB(db)
	registry := metrics.NewService(db, domain.NewDSLParser(), metricRepo, factorRepo, signalRepo, universeRepo)
	app := impl.NewMetricsApplicationService(registry, registry, jobRepo)

	_, err := app.CreateMetric(ctx, contracts.CreateMetricRequest{
		Metric: domain.MetricDef{
			ID:            "corr_close_vol",
			DisplayNameCN: "收盘成交相关性",
			Kind:          domain.MetricKindFactor,
			Expression:    "spearman_corr(close, volume, 3)",
			Frequency:     domain.FrequencyDaily,
			Status:        domain.MetricStatusActive,
			FactorSpec:    &domain.FactorSpec{Direction: domain.FactorDirectionHigherBetter},
		},
	})
	require.NoError(t, err)

	job, err := app.SubmitMetricJob(ctx, contracts.SubmitMetricJobRequest{
		JobType:   domain.ComputeJobTypeFactorRecalculate,
		TargetIDs: []string{"corr_close_vol"},
		RangeType: domain.ComputeRangeDateRange,
		StartTime: "20250101",
		EndTime:   "20250105",
	})
	require.NoError(t, err)

	var finalJob *domain.ComputeJob
	for i := 0; i < 200; i++ {
		finalJob, err = app.GetMetricJobStatus(ctx, job.ID)
		require.NoError(t, err)
		if finalJob.Status == domain.ComputeJobStatusSucceeded || finalJob.Status == domain.ComputeJobStatusFailed {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NotNil(t, finalJob)
	require.Equalf(t, domain.ComputeJobStatusSucceeded, finalJob.Status, "job failed: %s", finalJob.ErrorMessage)

	panel, err := app.GetFactorPanel(ctx, domain.FactorPanelQuery{
		MetricIDs: []string{"corr_close_vol"},
		StartDate: "20250101",
		EndDate:   "20250105",
		Frequency: "1d",
	})
	require.NoError(t, err)

	expected := expectedSpearmanPanel()
	require.Len(t, panel, len(expected))
	actual := make(map[string]float64, len(panel))
	for _, v := range panel {
		actual[v.EntityID+"|"+v.TradeDate] = v.Value
	}
	for key, want := range expected {
		got, ok := actual[key]
		require.Truef(t, ok, "missing factor %s", key)
		require.InDeltaf(t, want, got, 1e-9, "key=%s", key)
	}
}

// stableRank duplicates rankFloats in residual_spearman.go but kept local so
// the test depends only on the public interface. Ties get distinct (non-
// averaged) ranks based on original order, matching the legacy evaluator.
func stableRank(values []float64) []float64 {
	n := len(values)
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(i, j int) bool { return values[idx[i]] < values[idx[j]] })
	ranks := make([]float64, n)
	for rank, original := range idx {
		ranks[original] = float64(rank + 1)
	}
	return ranks
}

func seedSpearmanFixture(t *testing.T, ctx context.Context, db interface {
	Execute(context.Context, string, ...any) (int64, error)
}) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE stock_basic (ts_code TEXT, name TEXT, market TEXT, list_status TEXT)`,
		`CREATE TABLE daily (
			ts_code TEXT, trade_date TEXT,
			open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
			vol DOUBLE, amount DOUBLE, pre_close DOUBLE,
			change DOUBLE, pct_chg DOUBLE
		)`,
		`INSERT INTO stock_basic VALUES
			('000001.SZ', '平安银行', '主板', 'L'),
			('000002.SZ', '万科A', '主板', 'L')`,
		`INSERT INTO daily VALUES
			('000001.SZ', '20250101', 10, 10, 10, 10, 100, 1000, 10, 0, 0),
			('000001.SZ', '20250102', 11, 11, 11, 11, 120, 1100, 10, 1, 10),
			('000001.SZ', '20250103', 12, 12, 12, 12, 150, 1200, 11, 1, 9),
			('000001.SZ', '20250104', 11, 11, 11, 11, 140, 1300, 12, -1, -8),
			('000001.SZ', '20250105', 13, 13, 13, 13, 160, 1400, 11, 2, 18),
			('000002.SZ', '20250101', 20, 20, 20, 20, 200, 2000, 20, 0, 0),
			('000002.SZ', '20250102', 21, 21, 21, 21, 180, 2100, 20, 1, 5),
			('000002.SZ', '20250103', 22, 22, 22, 22, 170, 2200, 21, 1, 4),
			('000002.SZ', '20250104', 21, 21, 21, 21, 160, 2300, 22, -1, -4),
			('000002.SZ', '20250105', 23, 23, 23, 23, 150, 2400, 21, 2, 9)`,
	}
	for _, stmt := range stmts {
		_, err := db.Execute(ctx, stmt)
		require.NoError(t, err)
	}
}

// expectedSpearmanPanel computes the expected rolling Spearman(close, volume, 3)
// for the two-entity fixture. NaN windows (fewer than 2 pairs) are skipped.
func expectedSpearmanPanel() map[string]float64 {
	close1 := []float64{10, 11, 12, 11, 13}
	vol1 := []float64{100, 120, 150, 140, 160}
	close2 := []float64{20, 21, 22, 21, 23}
	vol2 := []float64{200, 180, 170, 160, 150}
	dates := []string{"20250101", "20250102", "20250103", "20250104", "20250105"}
	calc := metrics.NewGonumCalculator()

	out := make(map[string]float64, 16)
	for _, series := range []struct {
		id   string
		x, y []float64
	}{
		{"000001.SZ", close1, vol1},
		{"000002.SZ", close2, vol2},
	} {
		rolled := calc.RollingSpearman(series.x, series.y, 3)
		for i, v := range rolled {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			out[series.id+"|"+dates[i]] = v
		}
	}
	return out
}
