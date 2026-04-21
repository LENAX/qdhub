package metrics_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	domain "qdhub/internal/domain/metrics"
	"qdhub/internal/infrastructure/metrics"
	duckdbinfra "qdhub/internal/infrastructure/quantdb/duckdb"
)

func TestMetricsPipeline_EndToEnd(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "metrics_test.duckdb")
	db := duckdbinfra.NewAdapter(dbPath)
	require.NoError(t, db.Connect(ctx))
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, metrics.EnsureSchema(ctx, db))
	seedFeatureTables(t, ctx, db)

	metricRepo := metrics.NewMetricDefRepoDuckDB(db)
	factorRepo := metrics.NewFactorValueRepoDuckDB(db)
	signalRepo := metrics.NewSignalValueRepoDuckDB(db)
	universeRepo := metrics.NewUniverseRepoDuckDB(db)
	jobRepo := metrics.NewComputeJobRepoDuckDB(db)
	registry := metrics.NewService(
		db,
		domain.NewDSLParser(),
		metricRepo,
		factorRepo,
		signalRepo,
		universeRepo,
	)
	app := impl.NewMetricsApplicationService(registry, registry, jobRepo)

	_, err := app.CreateMetric(ctx, contracts.CreateMetricRequest{
		Metric: domain.MetricDef{
			ID:            "vol_ratio",
			DisplayNameCN: "放量比率",
			Kind:          domain.MetricKindFactor,
			Expression:    "div(volume, ma(volume, 2))",
			Frequency:     domain.FrequencyDaily,
			Status:        domain.MetricStatusActive,
			FactorSpec:    &domain.FactorSpec{Direction: domain.FactorDirectionHigherBetter},
		},
	})
	require.NoError(t, err)

	_, err = app.CreateMetric(ctx, contracts.CreateMetricRequest{
		Metric: domain.MetricDef{
			ID:            "hot_signal",
			DisplayNameCN: "放量信号",
			Kind:          domain.MetricKindSignal,
			Expression:    "gte(vol_ratio, 1.1)",
			Frequency:     domain.FrequencyDaily,
			Status:        domain.MetricStatusActive,
			DependsOn:     []string{"vol_ratio"},
			SignalSpec:    &domain.SignalSpec{OutputKind: domain.SignalOutputBool},
		},
	})
	require.NoError(t, err)

	_, err = app.CreateMetric(ctx, contracts.CreateMetricRequest{
		Metric: domain.MetricDef{
			ID:            "growth_universe",
			DisplayNameCN: "成长股池",
			Kind:          domain.MetricKindUniverse,
			Expression:    "and(hot_signal, lt(pe_ttm, 30))",
			Frequency:     domain.FrequencyDaily,
			Status:        domain.MetricStatusActive,
			DependsOn:     []string{"hot_signal"},
			UniverseSpec:  &domain.UniverseSpec{},
		},
	})
	require.NoError(t, err)

	job, err := app.SubmitMetricJob(ctx, contracts.SubmitMetricJobRequest{
		JobType:   domain.ComputeJobTypeUniverseMaterial,
		TargetIDs: []string{"growth_universe"},
		RangeType: domain.ComputeRangeDateRange,
		StartTime: "20250101",
		EndTime:   "20250103",
	})
	require.NoError(t, err)

	var finalJob *domain.ComputeJob
	for i := 0; i < 100; i++ {
		finalJob, err = app.GetMetricJobStatus(ctx, job.ID)
		require.NoError(t, err)
		if finalJob.Status == domain.ComputeJobStatusSucceeded {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.NotNil(t, finalJob)
	require.Equal(t, domain.ComputeJobStatusSucceeded, finalJob.Status)

	factorValues, err := app.GetFactorPanel(ctx, domain.FactorPanelQuery{
		MetricIDs: []string{"vol_ratio"},
		StartDate: "20250103",
		EndDate:   "20250103",
		Frequency: "1d",
	})
	require.NoError(t, err)
	require.Len(t, factorValues, 2)

	signalValues, err := app.GetSignalSeries(ctx, domain.SignalSeriesQuery{
		MetricIDs: []string{"hot_signal"},
		StartDate: "20250103",
		EndDate:   "20250103",
		Frequency: "1d",
	})
	require.NoError(t, err)
	require.Len(t, signalValues, 2)

	members, err := app.GetUniverseMembers(ctx, domain.UniverseMembersQuery{
		UniverseID: "growth_universe",
		TradeDate:  "20250103",
		Frequency:  "1d",
	})
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "000001.SZ", members[0].EntityID)
}

func seedFeatureTables(t *testing.T, ctx context.Context, db interface {
	Execute(context.Context, string, ...any) (int64, error)
}) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE stock_basic (
			ts_code TEXT,
			name TEXT,
			market TEXT,
			list_status TEXT
		)`,
		`CREATE TABLE daily (
			ts_code TEXT,
			trade_date TEXT,
			open DOUBLE,
			high DOUBLE,
			low DOUBLE,
			close DOUBLE,
			vol DOUBLE,
			amount DOUBLE,
			pre_close DOUBLE,
			change DOUBLE,
			pct_chg DOUBLE
		)`,
		`CREATE TABLE daily_basic (
			ts_code TEXT,
			trade_date TEXT,
			turnover_rate DOUBLE,
			pe DOUBLE,
			pe_ttm DOUBLE
		)`,
		`INSERT INTO stock_basic VALUES
			('000001.SZ', '平安银行', '主板', 'L'),
			('000002.SZ', '万科A', '主板', 'L')`,
		`INSERT INTO daily VALUES
			('000001.SZ', '20250101', 10, 10.5, 9.8, 10.2, 100, 1000, 10, 0.2, 2.0),
			('000001.SZ', '20250102', 10.2, 10.8, 10.1, 10.7, 200, 2200, 10.2, 0.5, 4.9),
			('000001.SZ', '20250103', 10.7, 11.2, 10.6, 11.1, 300, 3300, 10.7, 0.4, 3.7),
			('000002.SZ', '20250101', 20, 20.2, 19.8, 20.1, 100, 2000, 20, 0.1, 0.5),
			('000002.SZ', '20250102', 20.1, 20.2, 19.7, 19.9, 90, 1800, 20.1, -0.2, -1.0),
			('000002.SZ', '20250103', 19.9, 20.0, 19.5, 19.7, 80, 1600, 19.9, -0.2, -1.0)`,
		`INSERT INTO daily_basic VALUES
			('000001.SZ', '20250101', 5, 20, 20),
			('000001.SZ', '20250102', 6, 20, 20),
			('000001.SZ', '20250103', 9, 20, 20),
			('000002.SZ', '20250101', 4, 40, 40),
			('000002.SZ', '20250102', 3, 40, 40),
			('000002.SZ', '20250103', 2, 40, 40)`,
	}
	for _, stmt := range stmts {
		_, err := db.Execute(ctx, stmt)
		require.NoError(t, err)
	}
}
