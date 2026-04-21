//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/impl"
	domainmetrics "qdhub/internal/domain/metrics"
	"qdhub/internal/domain/shared"
	authinfra "qdhub/internal/infrastructure/auth"
	metricsinfra "qdhub/internal/infrastructure/metrics"
	"qdhub/internal/infrastructure/persistence"
	duckdbinfra "qdhub/internal/infrastructure/quantdb/duckdb"
	httpapi "qdhub/internal/interfaces/http"
)

// metricsE2EDateRange 特征表抽样后的 job / 查询日期范围（YYYYMMDD）。
type metricsE2EDateRange struct {
	Start string
	End   string
}

type metricsE2ESampledSource struct {
	Path   string
	Dates  metricsE2EDateRange
	Closer func()
}

func TestE2E_HTTPAPI_MetricsPipeline(t *testing.T) {
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	ts, token, cleanup := setupMetricsE2EHTTPServer(t, db)
	defer cleanup()

	postJSONWithToken(t, ts.URL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "vol_ratio",
			"display_name_cn": "放量比率",
			"kind":            "factor",
			"expression":      "div(volume, ma(volume, 2))",
			"frequency":       "1d",
			"status":          "active",
			"factor_spec": map[string]any{
				"direction": "higher_better",
			},
		},
	}, http.StatusCreated)

	postJSONWithToken(t, ts.URL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "hot_signal",
			"display_name_cn": "放量信号",
			"kind":            "signal",
			"expression":      "gte(vol_ratio, 1.1)",
			"frequency":       "1d",
			"status":          "active",
			"depends_on":      []string{"vol_ratio"},
			"signal_spec": map[string]any{
				"output_kind": "bool",
			},
		},
	}, http.StatusCreated)

	postJSONWithToken(t, ts.URL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "growth_universe",
			"display_name_cn": "成长股池",
			"kind":            "universe",
			"expression":      "and(hot_signal, lt(pe_ttm, 30))",
			"frequency":       "1d",
			"status":          "active",
			"depends_on":      []string{"hot_signal"},
			"universe_spec":   map[string]any{},
		},
	}, http.StatusCreated)

	resp := postJSONWithToken(t, ts.URL+"/api/v1/metrics/jobs?token="+token, map[string]any{
		"job_type":   "universe_materialize",
		"target_ids": []string{"growth_universe"},
		"range_type": "date_range",
		"start_time": "20250101",
		"end_time":   "20250103",
	}, http.StatusCreated)

	jobID := parseJobID(t, resp)
	waitJobSucceeded(t, ts.URL, token, jobID)

	factorResp := getJSON(t, ts.URL+"/api/v1/metrics/factor-panel?token="+token+"&metric_ids=vol_ratio&start_date=20250103&end_date=20250103&frequency=1d", http.StatusOK)
	require.Equal(t, float64(2), factorResp["data"].(map[string]any)["total"])

	signalResp := getJSON(t, ts.URL+"/api/v1/metrics/signal-series?token="+token+"&metric_ids=hot_signal&start_date=20250103&end_date=20250103&frequency=1d", http.StatusOK)
	require.Equal(t, float64(2), signalResp["data"].(map[string]any)["total"])

	universeResp := getJSON(t, ts.URL+"/api/v1/metrics/universe-members?token="+token+"&universe_id=growth_universe&trade_date=20250103&frequency=1d", http.StatusOK)
	data := universeResp["data"].(map[string]any)
	require.Equal(t, float64(1), data["total"])
	items := data["items"].([]any)
	require.Len(t, items, 1)
	item := items[0].(map[string]any)
	require.Equal(t, "000001.SZ", item["entity_id"])
}

func TestE2E_HTTPAPI_MetricsPipeline_WithRealSampledData(t *testing.T) {
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	sourceAbsPath := resolveQDHubDuckDBPathForMetricsE2E(t)
	sampled := buildMetricsSampleDuckDBFromQDHub(t, sourceAbsPath)
	defer sampled.Closer()

	ts, token, metricsDB, cleanup, dates := setupMetricsE2EHTTPServerWithSampledSource(t, db, sampled)
	defer cleanup()

	postJSONWithToken(t, ts.URL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "close_ma_5",
			"display_name_cn": "5日均线",
			"kind":            "factor",
			"expression":      "ma(close, 5)",
			"frequency":       "1d",
			"status":          "active",
			"factor_spec": map[string]any{
				"direction": "higher_better",
			},
		},
	}, http.StatusCreated)

	postJSONWithToken(t, ts.URL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "close_above_ma_5",
			"display_name_cn": "收盘站上5日均线",
			"kind":            "signal",
			"expression":      "gt(close, close_ma_5)",
			"frequency":       "1d",
			"status":          "active",
			"depends_on":      []string{"close_ma_5"},
			"signal_spec": map[string]any{
				"output_kind": "bool",
			},
		},
	}, http.StatusCreated)

	resp := postJSONWithToken(t, ts.URL+"/api/v1/metrics/jobs?token="+token, map[string]any{
		"job_type":   "signal_recalculate",
		"target_ids": []string{"close_above_ma_5"},
		"range_type": "date_range",
		"start_time": dates.Start,
		"end_time":   dates.End,
	}, http.StatusCreated)

	jobID := parseJobID(t, resp)
	waitJobSucceededWithin(t, ts.URL, token, jobID, 5*time.Minute, 500*time.Millisecond)

	expected := loadExpectedCloseMA5Signals(t, context.Background(), metricsDB, dates.End)
	require.NotEmpty(t, expected)

	factorResp := getJSON(t, ts.URL+"/api/v1/metrics/factor-panel?token="+token+"&metric_ids=close_ma_5&start_date="+dates.End+"&end_date="+dates.End+"&frequency=1d", http.StatusOK)
	factorData := factorResp["data"].(map[string]any)
	require.Equal(t, float64(len(expected)), factorData["total"])
	factorItems := factorData["items"].([]any)
	actualFactors := make(map[string]float64, len(factorItems))
	for _, raw := range factorItems {
		item := raw.(map[string]any)
		entityID := item["entity_id"].(string)
		actualFactors[entityID] = item["value"].(float64)
		require.Equal(t, dates.End, item["trade_date"])
	}

	signalResp := getJSON(t, ts.URL+"/api/v1/metrics/signal-series?token="+token+"&metric_ids=close_above_ma_5&start_date="+dates.End+"&end_date="+dates.End+"&frequency=1d", http.StatusOK)
	signalData := signalResp["data"].(map[string]any)
	require.Equal(t, float64(len(expected)), signalData["total"])
	signalItems := signalData["items"].([]any)
	actualSignals := make(map[string]bool, len(signalItems))
	for _, raw := range signalItems {
		item := raw.(map[string]any)
		entityID := item["entity_id"].(string)
		boolValue, ok := item["bool_value"].(bool)
		require.True(t, ok, "signal bool_value missing for %s", entityID)
		actualSignals[entityID] = boolValue
		require.Equal(t, dates.End, item["trade_date"])
	}

	require.Len(t, actualFactors, len(expected))
	require.Len(t, actualSignals, len(expected))
	for entityID, exp := range expected {
		actualFactor, ok := actualFactors[entityID]
		require.True(t, ok, "missing factor value for %s", entityID)
		require.InDelta(t, exp.CloseMA5, actualFactor, 1e-9)

		actualSignal, ok := actualSignals[entityID]
		require.True(t, ok, "missing signal value for %s", entityID)
		require.Equal(t, exp.CloseAboveMA5, actualSignal)
	}
}

func TestE2E_HTTPAPI_MetricsPipeline_WithRealMoneyflowSampledData(t *testing.T) {
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	sourceAbsPath := resolveQDHubDuckDBPathForMetricsE2E(t)
	sampled := buildMetricsSampleDuckDBFromQDHub(t, sourceAbsPath)
	defer sampled.Closer()

	ts, token, metricsDB, cleanup, dates := setupMetricsE2EHTTPServerWithSampledSource(t, db, sampled)
	defer cleanup()

	postJSONWithToken(t, ts.URL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "net_mf_ratio",
			"display_name_cn": "资金净流入占比",
			"kind":            "factor",
			"expression":      "div(net_mf_amount, amount)",
			"frequency":       "1d",
			"status":          "active",
			"factor_spec": map[string]any{
				"direction": "higher_better",
			},
		},
	}, http.StatusCreated)

	postJSONWithToken(t, ts.URL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "positive_net_mf_ratio",
			"display_name_cn": "资金净流入为正",
			"kind":            "signal",
			"expression":      "gt(net_mf_ratio, 0)",
			"frequency":       "1d",
			"status":          "active",
			"depends_on":      []string{"net_mf_ratio"},
			"signal_spec": map[string]any{
				"output_kind": "bool",
			},
		},
	}, http.StatusCreated)

	resp := postJSONWithToken(t, ts.URL+"/api/v1/metrics/jobs?token="+token, map[string]any{
		"job_type":   "signal_recalculate",
		"target_ids": []string{"positive_net_mf_ratio"},
		"range_type": "date_range",
		"start_time": dates.Start,
		"end_time":   dates.End,
	}, http.StatusCreated)

	jobID := parseJobID(t, resp)
	waitJobSucceededWithin(t, ts.URL, token, jobID, 5*time.Minute, 500*time.Millisecond)

	expected := loadExpectedNetMfRatioSignals(t, context.Background(), metricsDB, dates.End)
	require.NotEmpty(t, expected)

	factorResp := getJSON(t, ts.URL+"/api/v1/metrics/factor-panel?token="+token+"&metric_ids=net_mf_ratio&start_date="+dates.End+"&end_date="+dates.End+"&frequency=1d", http.StatusOK)
	factorData := factorResp["data"].(map[string]any)
	require.Equal(t, float64(len(expected)), factorData["total"])
	factorItems := factorData["items"].([]any)
	actualFactors := make(map[string]float64, len(factorItems))
	for _, raw := range factorItems {
		item := raw.(map[string]any)
		entityID := item["entity_id"].(string)
		actualFactors[entityID] = item["value"].(float64)
		require.Equal(t, dates.End, item["trade_date"])
	}

	signalResp := getJSON(t, ts.URL+"/api/v1/metrics/signal-series?token="+token+"&metric_ids=positive_net_mf_ratio&start_date="+dates.End+"&end_date="+dates.End+"&frequency=1d", http.StatusOK)
	signalData := signalResp["data"].(map[string]any)
	require.Equal(t, float64(len(expected)), signalData["total"])
	signalItems := signalData["items"].([]any)
	actualSignals := make(map[string]bool, len(signalItems))
	for _, raw := range signalItems {
		item := raw.(map[string]any)
		entityID := item["entity_id"].(string)
		boolValue, ok := item["bool_value"].(bool)
		require.True(t, ok, "signal bool_value missing for %s", entityID)
		actualSignals[entityID] = boolValue
		require.Equal(t, dates.End, item["trade_date"])
	}

	require.Len(t, actualFactors, len(expected))
	require.Len(t, actualSignals, len(expected))
	for entityID, exp := range expected {
		actualFactor, ok := actualFactors[entityID]
		require.True(t, ok, "missing factor value for %s", entityID)
		require.InDelta(t, exp.NetMfRatio, actualFactor, 1e-12)

		actualSignal, ok := actualSignals[entityID]
		require.True(t, ok, "missing signal value for %s", entityID)
		require.Equal(t, exp.PositiveNetMf, actualSignal)
	}
}

func TestE2E_HTTPAPI_MetricsPipeline_WithRealLimitUpSampledData(t *testing.T) {
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	sourceAbsPath := resolveQDHubDuckDBPathForMetricsE2E(t)
	sampled := buildMetricsSampleDuckDBFromQDHub(t, sourceAbsPath)
	defer sampled.Closer()

	ts, token, metricsDB, cleanup, dates := setupMetricsE2EHTTPServerWithSampledSource(t, db, sampled)
	defer cleanup()

	registerMetricsLimitUpScenario(t, ts.URL, token)

	resp := postJSONWithToken(t, ts.URL+"/api/v1/metrics/jobs?token="+token, map[string]any{
		"job_type":   "signal_recalculate",
		"target_ids": []string{"has_limit_up_in_5d"},
		"range_type": "date_range",
		"start_time": dates.Start,
		"end_time":   dates.End,
	}, http.StatusCreated)

	jobID := parseJobID(t, resp)
	waitJobSucceededWithin(t, ts.URL, token, jobID, 5*time.Minute, 500*time.Millisecond)

	expected := loadExpectedLimitUpSignals(t, context.Background(), metricsDB, dates.End)
	require.NotEmpty(t, expected)

	factorResp := getJSON(t, ts.URL+"/api/v1/metrics/factor-panel?token="+token+"&metric_ids=limit_up_days_5&start_date="+dates.End+"&end_date="+dates.End+"&frequency=1d", http.StatusOK)
	factorData := factorResp["data"].(map[string]any)
	require.Equal(t, float64(len(expected)), factorData["total"])
	factorItems := factorData["items"].([]any)
	actualFactors := make(map[string]float64, len(factorItems))
	for _, raw := range factorItems {
		item := raw.(map[string]any)
		entityID := item["entity_id"].(string)
		actualFactors[entityID] = item["value"].(float64)
		require.Equal(t, dates.End, item["trade_date"])
	}

	signalResp := getJSON(t, ts.URL+"/api/v1/metrics/signal-series?token="+token+"&metric_ids=has_limit_up_in_5d&start_date="+dates.End+"&end_date="+dates.End+"&frequency=1d", http.StatusOK)
	signalData := signalResp["data"].(map[string]any)
	require.Equal(t, float64(len(expected)), signalData["total"])
	signalItems := signalData["items"].([]any)
	actualSignals := make(map[string]bool, len(signalItems))
	for _, raw := range signalItems {
		item := raw.(map[string]any)
		entityID := item["entity_id"].(string)
		boolValue, ok := item["bool_value"].(bool)
		require.True(t, ok, "signal bool_value missing for %s", entityID)
		actualSignals[entityID] = boolValue
		require.Equal(t, dates.End, item["trade_date"])
	}

	require.Len(t, actualFactors, len(expected))
	require.Len(t, actualSignals, len(expected))
	for entityID, exp := range expected {
		actualFactor, ok := actualFactors[entityID]
		require.True(t, ok, "missing factor value for %s", entityID)
		require.InDeltaf(t, exp.LimitUpDays5, actualFactor, 1e-12, "entity=%s expected=%v actual=%v", entityID, exp.LimitUpDays5, actualFactor)

		actualSignal, ok := actualSignals[entityID]
		require.True(t, ok, "missing signal value for %s", entityID)
		require.Equalf(t, exp.HasLimitUpIn5, actualSignal, "entity=%s", entityID)
	}
}

func TestE2E_HTTPAPI_MetricsPipeline_WriteFactorSignalResultsReport(t *testing.T) {
	db, cleanupDB := setupTestDB(t)
	defer cleanupDB()

	sourceAbsPath := resolveQDHubDuckDBPathForMetricsE2E(t)
	sampled := buildMetricsSampleDuckDBFromQDHub(t, sourceAbsPath)
	defer sampled.Closer()

	ts, token, metricsDB, cleanup, dates := setupMetricsE2EHTTPServerWithSampledSource(t, db, sampled)
	defer cleanup()

	registerMetricsCloseMA5Scenario(t, ts.URL, token)
	registerMetricsMoneyflowScenario(t, ts.URL, token)
	registerMetricsLimitUpScenario(t, ts.URL, token)

	resp := postJSONWithToken(t, ts.URL+"/api/v1/metrics/jobs?token="+token, map[string]any{
		"job_type":   "signal_recalculate",
		"target_ids": []string{"close_above_ma_5", "positive_net_mf_ratio", "has_limit_up_in_5d"},
		"range_type": "date_range",
		"start_time": dates.Start,
		"end_time":   dates.End,
	}, http.StatusCreated)

	jobID := parseJobID(t, resp)
	waitJobSucceededWithin(t, ts.URL, token, jobID, 5*time.Minute, 500*time.Millisecond)

	reportPath := resolveMetricsResultsReportPath(t)
	writeMetricsResultsReport(
		t,
		context.Background(),
		metricsDB,
		reportPath,
		dates,
		map[string]string{
			"close_ma_5":            "ma(close, 5)",
			"close_above_ma_5":      "gt(close, close_ma_5)",
			"net_mf_ratio":          "div(net_mf_amount, amount)",
			"positive_net_mf_ratio": "gt(net_mf_ratio, 0)",
			"limit_up_days_5":       "ts_count(or(limit10(universe, 1, 1, 1), limit20(universe, 1, 1, 1), limit30(universe, 1, 1, 1)), 5)",
			"has_limit_up_in_5d":    "gte(limit_up_days_5, 1)",
		},
		[]string{"close_ma_5", "net_mf_ratio", "limit_up_days_5"},
		[]string{"close_above_ma_5", "positive_net_mf_ratio", "has_limit_up_in_5d"},
	)
}

func TestE2E_MetricsFeatureSeed_FromRealDuckDB_IncludesRequestedDatasets(t *testing.T) {
	sourceAbsPath := resolveQDHubDuckDBPathForMetricsE2E(t)
	sampled := buildMetricsSampleDuckDBFromQDHub(t, sourceAbsPath)
	defer sampled.Closer()
	ctx := context.Background()
	sampledDB := duckdbinfra.NewAdapter(sampled.Path)
	require.NoError(t, sampledDB.Connect(ctx))
	defer func() { _ = sampledDB.Close() }()

	for _, tableName := range []string{
		"moneyflow",
		"limit_list_d",
		"limit_list_ths",
		"limit_cpt_list",
		"index_basic",
		"index_daily",
	} {
		assertMetricsSampledTableHasRows(t, ctx, sampledDB, tableName)
	}
}

func setupMetricsE2EHTTPServer(t *testing.T, db *persistence.DB) (*httptest.Server, string, func()) {
	ts, token, _, cleanup, _ := setupMetricsE2EHTTPServerWithFeatureSeed(t, db, func(t *testing.T, ctx context.Context, metricsDB *duckdbinfra.Adapter) metricsE2EDateRange {
		seedMetricsFeatureTables(t, ctx, metricsDB)
		return metricsE2EDateRange{Start: "20250101", End: "20250103"}
	})
	return ts, token, cleanup
}

// setupMetricsE2EHTTPServerWithFeatureSeed 在 EnsureSchema 之后调用 seed 写入 daily / stock_basic / daily_basic，并返回抽样日期范围。
func setupMetricsE2EHTTPServerWithFeatureSeed(
	t *testing.T,
	db *persistence.DB,
	seed func(*testing.T, context.Context, *duckdbinfra.Adapter) metricsE2EDateRange,
) (*httptest.Server, string, *duckdbinfra.Adapter, func(), metricsE2EDateRange) {
	t.Helper()
	ctx := context.Background()

	metricsDBPath := filepath.Join(t.TempDir(), "metrics_e2e.duckdb")
	metricsDB := duckdbinfra.NewAdapter(metricsDBPath)
	require.NoError(t, metricsDB.Connect(ctx))
	require.NoError(t, metricsinfra.EnsureSchema(ctx, metricsDB))
	dates := seed(t, ctx, metricsDB)

	metricRepo := metricsinfra.NewMetricDefRepoDuckDB(metricsDB)
	factorRepo := metricsinfra.NewFactorValueRepoDuckDB(metricsDB)
	signalRepo := metricsinfra.NewSignalValueRepoDuckDB(metricsDB)
	universeRepo := metricsinfra.NewUniverseRepoDuckDB(metricsDB)
	jobRepo := metricsinfra.NewComputeJobRepoDuckDB(metricsDB)
	metricsService := metricsinfra.NewService(
		metricsDB,
		domainmetrics.NewDSLParser(),
		metricRepo,
		factorRepo,
		signalRepo,
		universeRepo,
	)
	metricsSvc := impl.NewMetricsApplicationService(metricsService, metricsService, jobRepo)

	jwtManager := authinfra.NewJWTManager("metrics_e2e_secret_1234567890", time.Hour, 24*time.Hour)
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS casbin_rule (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		ptype TEXT,
		v0 TEXT,
		v1 TEXT,
		v2 TEXT,
		v3 TEXT,
		v4 TEXT,
		v5 TEXT
	)`)
	require.NoError(t, err)
	enforcer, err := authinfra.NewCasbinEnforcer(db.DB, persistence.DBTypeSQLite)
	require.NoError(t, err)
	require.NoError(t, authinfra.InitializeDefaultPolicies(enforcer))
	token, err := jwtManager.GenerateAccessToken(shared.NewID(), "e2e-admin", []string{"admin"})
	require.NoError(t, err)

	config := httpapi.DefaultServerConfig()
	config.Mode = gin.TestMode
	server := httpapi.NewServer(config, nil, nil, nil, nil, nil, nil, nil, metricsSvc, nil, nil, nil, nil, jwtManager, enforcer, "")
	ts := httptest.NewServer(server.Engine())

	cleanup := func() {
		ts.Close()
		_ = metricsDB.Close()
	}
	return ts, token, metricsDB, cleanup, dates
}

func setupMetricsE2EHTTPServerWithSampledSource(
	t *testing.T,
	db *persistence.DB,
	sampled metricsE2ESampledSource,
) (*httptest.Server, string, *duckdbinfra.Adapter, func(), metricsE2EDateRange) {
	t.Helper()
	return setupMetricsE2EHTTPServerWithFeatureSeed(t, db, func(t *testing.T, ctx context.Context, metricsDB *duckdbinfra.Adapter) metricsE2EDateRange {
		importMetricsSampledTablesFromDuckDB(t, ctx, metricsDB, sampled.Path)
		return sampled.Dates
	})
}

func registerMetricsCloseMA5Scenario(t *testing.T, baseURL, token string) {
	t.Helper()
	postJSONWithToken(t, baseURL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "close_ma_5",
			"display_name_cn": "5日均线",
			"kind":            "factor",
			"expression":      "ma(close, 5)",
			"frequency":       "1d",
			"status":          "active",
			"factor_spec": map[string]any{
				"direction": "higher_better",
			},
		},
	}, http.StatusCreated)

	postJSONWithToken(t, baseURL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "close_above_ma_5",
			"display_name_cn": "收盘站上5日均线",
			"kind":            "signal",
			"expression":      "gt(close, close_ma_5)",
			"frequency":       "1d",
			"status":          "active",
			"depends_on":      []string{"close_ma_5"},
			"signal_spec": map[string]any{
				"output_kind": "bool",
			},
		},
	}, http.StatusCreated)
}

func registerMetricsMoneyflowScenario(t *testing.T, baseURL, token string) {
	t.Helper()
	postJSONWithToken(t, baseURL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "net_mf_ratio",
			"display_name_cn": "资金净流入占比",
			"kind":            "factor",
			"expression":      "div(net_mf_amount, amount)",
			"frequency":       "1d",
			"status":          "active",
			"factor_spec": map[string]any{
				"direction": "higher_better",
			},
		},
	}, http.StatusCreated)

	postJSONWithToken(t, baseURL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "positive_net_mf_ratio",
			"display_name_cn": "资金净流入为正",
			"kind":            "signal",
			"expression":      "gt(net_mf_ratio, 0)",
			"frequency":       "1d",
			"status":          "active",
			"depends_on":      []string{"net_mf_ratio"},
			"signal_spec": map[string]any{
				"output_kind": "bool",
			},
		},
	}, http.StatusCreated)
}

func registerMetricsLimitUpScenario(t *testing.T, baseURL, token string) {
	t.Helper()
	postJSONWithToken(t, baseURL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "limit_up_days_5",
			"display_name_cn": "近5日涨停次数",
			"kind":            "factor",
			"expression":      "ts_count(or(limit10(universe, 1, 1, 1), limit20(universe, 1, 1, 1), limit30(universe, 1, 1, 1)), 5)",
			"frequency":       "1d",
			"status":          "active",
			"factor_spec": map[string]any{
				"direction": "higher_better",
			},
		},
	}, http.StatusCreated)

	postJSONWithToken(t, baseURL+"/api/v1/metrics/definitions?token="+token, map[string]any{
		"metric": map[string]any{
			"metric_id":       "has_limit_up_in_5d",
			"display_name_cn": "近5日至少一次涨停",
			"kind":            "signal",
			"expression":      "gte(limit_up_days_5, 1)",
			"frequency":       "1d",
			"status":          "active",
			"depends_on":      []string{"limit_up_days_5"},
			"signal_spec": map[string]any{
				"output_kind": "bool",
			},
		},
	}, http.StatusCreated)
}

// resolveQDHubDuckDBPathForMetricsE2E 用于从真实 DuckDB 取样；可通过环境变量 QDHub_METRICS_E2E_SOURCE_DUCKDB 覆盖。
func resolveQDHubDuckDBPathForMetricsE2E(t *testing.T) string {
	t.Helper()
	if p := strings.TrimSpace(os.Getenv("QDHub_METRICS_E2E_SOURCE_DUCKDB")); p != "" {
		abs, err := filepath.Abs(p)
		require.NoError(t, err)
		return abs
	}
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	e2eDir := filepath.Dir(file)
	modRoot := filepath.Clean(filepath.Join(e2eDir, "..", ".."))
	dataPath := filepath.Join(modRoot, "data", "qdhub.duckdb")
	if _, err := os.Stat(dataPath); err == nil {
		return dataPath
	}
	return filepath.Join(modRoot, "qdhub.duckdb")
}

func resolveMetricsResultsReportPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	e2eDir := filepath.Dir(file)
	return filepath.Join(e2eDir, "logs", "metrics_factor_signal_results.txt")
}

func buildMetricsSampleDuckDBFromQDHub(t *testing.T, sourceAbsPath string) metricsE2ESampledSource {
	t.Helper()
	ctx := context.Background()
	sampledDBPath := filepath.Join(t.TempDir(), "metrics_sample_source.duckdb")
	sampledDB := duckdbinfra.NewAdapter(sampledDBPath)
	require.NoError(t, sampledDB.Connect(ctx))

	startDate, endDate := seedMetricsFeatureTablesFromQDHubDuckDB(t, ctx, sampledDB, sourceAbsPath)
	require.NoError(t, sampledDB.Close())
	return metricsE2ESampledSource{
		Path:   sampledDBPath,
		Dates:  metricsE2EDateRange{Start: startDate, End: endDate},
		Closer: func() {},
	}
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func duckdbCellString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}

func duckdbCellFloat64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	default:
		return 0
	}
}

func duckdbCellBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int:
		return x != 0
	case int32:
		return x != 0
	case int64:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x == "true" || x == "1"
	case []byte:
		s := string(x)
		return s == "true" || s == "1"
	default:
		return false
	}
}

type metricsE2EExpectedValue struct {
	CloseMA5      float64
	CloseAboveMA5 bool
}

type metricsE2EMoneyflowExpectedValue struct {
	NetMfRatio    float64
	PositiveNetMf bool
}

type metricsE2ELimitUpExpectedValue struct {
	LimitUpDays5  float64
	HasLimitUpIn5 bool
}

func loadExpectedCloseMA5Signals(t *testing.T, ctx context.Context, db *duckdbinfra.Adapter, tradeDate string) map[string]metricsE2EExpectedValue {
	t.Helper()
	rows, err := db.Query(ctx, `
WITH calc AS (
	SELECT
		ts_code,
		trade_date,
		AVG(close) OVER (
			PARTITION BY ts_code
			ORDER BY trade_date
			ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
		) AS close_ma_5,
		close > AVG(close) OVER (
			PARTITION BY ts_code
			ORDER BY trade_date
			ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
		) AS close_above_ma_5,
		COUNT(*) OVER (
			PARTITION BY ts_code
			ORDER BY trade_date
			ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
		) AS window_size
	FROM daily
)
SELECT ts_code, close_ma_5, close_above_ma_5
FROM calc
WHERE trade_date = ? AND window_size = 5
ORDER BY ts_code
`, tradeDate)
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	out := make(map[string]metricsE2EExpectedValue, len(rows))
	for _, row := range rows {
		entityID := duckdbCellString(row["ts_code"])
		out[entityID] = metricsE2EExpectedValue{
			CloseMA5:      duckdbCellFloat64(row["close_ma_5"]),
			CloseAboveMA5: duckdbCellBool(row["close_above_ma_5"]),
		}
	}
	return out
}

func loadExpectedNetMfRatioSignals(t *testing.T, ctx context.Context, db *duckdbinfra.Adapter, tradeDate string) map[string]metricsE2EMoneyflowExpectedValue {
	t.Helper()
	rows, err := db.Query(ctx, `
SELECT
	d.ts_code,
	CASE
		WHEN COALESCE(d.amount, 0) = 0 THEN NULL
		ELSE COALESCE(m.net_mf_amount, 0) / d.amount
	END AS net_mf_ratio,
	CASE
		WHEN COALESCE(d.amount, 0) = 0 THEN FALSE
		ELSE COALESCE(m.net_mf_amount, 0) / d.amount > 0
	END AS positive_net_mf
FROM daily d
LEFT JOIN moneyflow m ON m.ts_code = d.ts_code AND m.trade_date = d.trade_date
WHERE d.trade_date = ? AND COALESCE(d.amount, 0) > 0
ORDER BY d.ts_code
`, tradeDate)
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	out := make(map[string]metricsE2EMoneyflowExpectedValue, len(rows))
	for _, row := range rows {
		entityID := duckdbCellString(row["ts_code"])
		out[entityID] = metricsE2EMoneyflowExpectedValue{
			NetMfRatio:    duckdbCellFloat64(row["net_mf_ratio"]),
			PositiveNetMf: duckdbCellBool(row["positive_net_mf"]),
		}
	}
	return out
}

func loadExpectedLimitUpSignals(t *testing.T, ctx context.Context, db *duckdbinfra.Adapter, tradeDate string) map[string]metricsE2ELimitUpExpectedValue {
	t.Helper()
	rows, err := db.Query(ctx, `
WITH calc AS (
	SELECT
		d.ts_code,
		d.trade_date,
		CAST(SUM(
			CASE
				WHEN (
					(COALESCE(s.market, '') NOT IN ('科创板', '北交所') AND COALESCE(d.pct_chg, 0) >= 9.8) OR
					(COALESCE(s.market, '') = '科创板' AND COALESCE(d.pct_chg, 0) >= 19.8) OR
					(COALESCE(s.market, '') = '北交所' AND COALESCE(d.pct_chg, 0) >= 29.8)
				) THEN 1 ELSE 0
			END
		) OVER (
			PARTITION BY d.ts_code
			ORDER BY d.trade_date
			ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
		) AS DOUBLE) AS limit_up_days_5
	FROM daily d
	JOIN stock_basic s ON s.ts_code = d.ts_code
)
SELECT
	ts_code,
	limit_up_days_5,
	limit_up_days_5 >= 1 AS has_limit_up_in_5d
FROM calc
WHERE trade_date = ?
ORDER BY ts_code
`, tradeDate)
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	out := make(map[string]metricsE2ELimitUpExpectedValue, len(rows))
	for _, row := range rows {
		entityID := duckdbCellString(row["ts_code"])
		out[entityID] = metricsE2ELimitUpExpectedValue{
			LimitUpDays5:  duckdbCellFloat64(row["limit_up_days_5"]),
			HasLimitUpIn5: duckdbCellBool(row["has_limit_up_in_5d"]),
		}
	}
	return out
}

func assertMetricsSampledTableHasRows(t *testing.T, ctx context.Context, db *duckdbinfra.Adapter, tableName string) {
	t.Helper()
	rows, err := db.Query(ctx, fmt.Sprintf(`SELECT COUNT(*) AS cnt FROM %s`, tableName))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Greater(t, duckdbCellFloat64(rows[0]["cnt"]), 0.0, "sampled table %s should not be empty", tableName)
}

func writeMetricsResultsReport(
	t *testing.T,
	ctx context.Context,
	db *duckdbinfra.Adapter,
	reportPath string,
	dates metricsE2EDateRange,
	expressions map[string]string,
	factorMetricIDs []string,
	signalMetricIDs []string,
) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(reportPath), 0755))

	factorInList := makeSQLInList(factorMetricIDs)
	signalInList := makeSQLInList(signalMetricIDs)

	factorRows, err := db.Query(ctx, fmt.Sprintf(`
SELECT metric_id, trade_date, entity_id, value
FROM factor_value
WHERE metric_id IN (%s)
ORDER BY metric_id, trade_date, entity_id
`, factorInList))
	require.NoError(t, err)
	require.NotEmpty(t, factorRows)

	signalRows, err := db.Query(ctx, fmt.Sprintf(`
SELECT metric_id, trade_date, entity_id, bool_value, text_value
FROM signal_value
WHERE metric_id IN (%s)
ORDER BY metric_id, trade_date, entity_id
`, signalInList))
	require.NoError(t, err)
	require.NotEmpty(t, signalRows)

	var b strings.Builder
	b.WriteString("Metrics Factor/Signal Results Report\n")
	b.WriteString(fmt.Sprintf("Date Range: %s - %s\n\n", dates.Start, dates.End))

	b.WriteString("Definitions\n")
	for _, metricID := range append(append([]string(nil), factorMetricIDs...), signalMetricIDs...) {
		b.WriteString(fmt.Sprintf("- %s = %s\n", metricID, expressions[metricID]))
	}
	b.WriteString("\n")

	b.WriteString("Factors\n")
	b.WriteString("metric_id\ttrade_date\tentity_id\tvalue\n")
	for _, row := range factorRows {
		b.WriteString(fmt.Sprintf(
			"%s\t%s\t%s\t%.12f\n",
			duckdbCellString(row["metric_id"]),
			duckdbCellString(row["trade_date"]),
			duckdbCellString(row["entity_id"]),
			duckdbCellFloat64(row["value"]),
		))
	}
	b.WriteString("\n")

	b.WriteString("Signals\n")
	b.WriteString("metric_id\ttrade_date\tentity_id\tbool_value\ttext_value\n")
	for _, row := range signalRows {
		b.WriteString(fmt.Sprintf(
			"%s\t%s\t%s\t%t\t%s\n",
			duckdbCellString(row["metric_id"]),
			duckdbCellString(row["trade_date"]),
			duckdbCellString(row["entity_id"]),
			duckdbCellBool(row["bool_value"]),
			duckdbCellString(row["text_value"]),
		))
	}

	require.NoError(t, os.WriteFile(reportPath, []byte(b.String()), 0644))
	t.Logf("metrics factor/signal report written to %s", reportPath)
}

func makeSQLInList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, fmt.Sprintf("'%s'", escapeSQLString(value)))
	}
	return strings.Join(quoted, ", ")
}

func importMetricsSampledTablesFromDuckDB(t *testing.T, ctx context.Context, dest *duckdbinfra.Adapter, sampledPath string) {
	t.Helper()
	require.FileExists(t, sampledPath)
	_, err := dest.Execute(ctx, fmt.Sprintf(`ATTACH '%s' AS sampled (READ_ONLY)`, escapeSQLString(sampledPath)))
	require.NoError(t, err)
	defer func() {
		_, detachErr := dest.Execute(ctx, "DETACH sampled")
		require.NoError(t, detachErr)
	}()

	for _, stmt := range []string{
		"DROP TABLE IF EXISTS daily",
		"DROP TABLE IF EXISTS stock_basic",
		"DROP TABLE IF EXISTS daily_basic",
		"DROP TABLE IF EXISTS moneyflow",
		"DROP TABLE IF EXISTS limit_list_d",
		"DROP TABLE IF EXISTS limit_list_ths",
		"DROP TABLE IF EXISTS limit_cpt_list",
		"DROP TABLE IF EXISTS index_basic",
		"DROP TABLE IF EXISTS index_daily",
		"CREATE TABLE daily AS SELECT * FROM sampled.daily",
		"CREATE TABLE stock_basic AS SELECT * FROM sampled.stock_basic",
		"CREATE TABLE daily_basic AS SELECT * FROM sampled.daily_basic",
		"CREATE TABLE moneyflow AS SELECT * FROM sampled.moneyflow",
		"CREATE TABLE limit_list_d AS SELECT * FROM sampled.limit_list_d",
		"CREATE TABLE limit_list_ths AS SELECT * FROM sampled.limit_list_ths",
		"CREATE TABLE limit_cpt_list AS SELECT * FROM sampled.limit_cpt_list",
		"CREATE TABLE index_basic AS SELECT * FROM sampled.index_basic",
		"CREATE TABLE index_daily AS SELECT * FROM sampled.index_daily",
	} {
		_, err := dest.Execute(ctx, stmt)
		require.NoError(t, err, "stmt failed: %s", stmt)
	}
}

// seedMetricsFeatureTablesFromQDHubDuckDB 从项目 qdhub.duckdb（或 QDHub_METRICS_E2E_SOURCE_DUCKDB）抽取最近若干交易日与部分股票，写入 metrics 用 DuckDB。
func seedMetricsFeatureTablesFromQDHubDuckDB(t *testing.T, ctx context.Context, dest *duckdbinfra.Adapter, sourceAbsPath string) (startDate, endDate string) {
	t.Helper()
	require.FileExists(t, sourceAbsPath)
	_, err := dest.Execute(ctx, fmt.Sprintf(`ATTACH '%s' AS src (READ_ONLY)`, escapeSQLString(sourceAbsPath)))
	require.NoError(t, err)
	defer func() {
		_, detachErr := dest.Execute(ctx, "DETACH src")
		require.NoError(t, detachErr)
	}()

	const requiredTradingDays = 40

	boundsSQL := `
WITH per_table_dates AS (
  SELECT 'daily' AS tbl, trade_date FROM src.daily GROUP BY trade_date
  UNION ALL
  SELECT 'daily_basic' AS tbl, trade_date FROM src.daily_basic GROUP BY trade_date
  UNION ALL
  SELECT 'moneyflow' AS tbl, trade_date FROM src.moneyflow GROUP BY trade_date
  UNION ALL
  SELECT 'limit_list_d' AS tbl, trade_date FROM src.limit_list_d GROUP BY trade_date
  UNION ALL
  SELECT 'limit_cpt_list' AS tbl, trade_date FROM src.limit_cpt_list GROUP BY trade_date
  UNION ALL
  SELECT 'index_daily' AS tbl, trade_date FROM src.index_daily GROUP BY trade_date
),
common_dates AS (
  SELECT trade_date
  FROM per_table_dates
  GROUP BY trade_date
  HAVING COUNT(DISTINCT tbl) = 6
),
date_window AS (
  SELECT trade_date FROM common_dates ORDER BY trade_date DESC LIMIT ?
),
bounds AS (
  SELECT min(trade_date) AS start_td, max(trade_date) AS end_td FROM date_window
)
SELECT start_td, end_td FROM bounds`
	brows, err := dest.Query(ctx, boundsSQL, requiredTradingDays)
	require.NoError(t, err)
	require.NotEmpty(t, brows)
	startDate = brows[0]["start_td"].(string)
	endDate = brows[0]["end_td"].(string)
	require.NotEmpty(t, startDate)
	require.NotEmpty(t, endDate)

	calendarRows, err := dest.Query(ctx, `
SELECT COUNT(*) AS cnt
FROM src.trade_cal
WHERE cal_date BETWEEN ? AND ? AND is_open = 1
`, startDate, endDate)
	require.NoError(t, err)
	require.Len(t, calendarRows, 1)
	require.Equal(t, float64(requiredTradingDays), duckdbCellFloat64(calendarRows[0]["cnt"]), "采样窗口必须覆盖连续的 %d 个开市交易日", requiredTradingDays)

	stmts := []string{
		"DROP TABLE IF EXISTS daily",
		"DROP TABLE IF EXISTS stock_basic",
		"DROP TABLE IF EXISTS daily_basic",
		"DROP TABLE IF EXISTS moneyflow",
		"DROP TABLE IF EXISTS limit_list_d",
		"DROP TABLE IF EXISTS limit_list_ths",
		"DROP TABLE IF EXISTS limit_cpt_list",
		"DROP TABLE IF EXISTS index_basic",
		"DROP TABLE IF EXISTS index_daily",
		fmt.Sprintf(`CREATE TABLE daily AS
SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close, d.vol, d.amount, d.pre_close, d.change, d.pct_chg
FROM src.daily d
JOIN src.stock_basic s ON s.ts_code = d.ts_code
WHERE d.trade_date BETWEEN '%s' AND '%s'
  AND COALESCE(d.vol, 0) > 0
  AND COALESCE(s.list_status, 'L') = 'L'
  AND NOT (trim(COALESCE(s.name, '')) LIKE 'ST%%' OR trim(COALESCE(s.name, '')) LIKE '*ST%%')`, escapeSQLString(startDate), escapeSQLString(endDate)),
		fmt.Sprintf(`CREATE TABLE stock_basic AS
SELECT DISTINCT s.ts_code, s.name, s.market, s.list_status
FROM src.stock_basic s
JOIN daily d ON d.ts_code = s.ts_code`),
		fmt.Sprintf(`CREATE TABLE daily_basic AS
SELECT db.ts_code, db.trade_date, db.turnover_rate, db.pe, db.pe_ttm
FROM src.daily_basic db
JOIN daily d ON d.ts_code = db.ts_code AND d.trade_date = db.trade_date
WHERE db.trade_date BETWEEN '%s' AND '%s'`, escapeSQLString(startDate), escapeSQLString(endDate)),
		fmt.Sprintf(`CREATE TABLE moneyflow AS
SELECT
	m.ts_code,
	m.trade_date,
	m.buy_sm_vol,
	m.buy_sm_amount,
	m.sell_sm_vol,
	m.sell_sm_amount,
	m.buy_md_vol,
	m.buy_md_amount,
	m.sell_md_vol,
	m.sell_md_amount,
	m.buy_lg_vol,
	m.buy_lg_amount,
	m.sell_lg_vol,
	m.sell_lg_amount,
	m.buy_elg_vol,
	m.buy_elg_amount,
	m.sell_elg_vol,
	m.sell_elg_amount,
	m.net_mf_vol,
	m.net_mf_amount
FROM src.moneyflow m
JOIN daily d ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
WHERE m.trade_date BETWEEN '%s' AND '%s'`, escapeSQLString(startDate), escapeSQLString(endDate)),
		fmt.Sprintf(`CREATE TABLE limit_list_d AS
SELECT
	l.trade_date,
	l.ts_code,
	l.industry,
	l.name,
	l.close,
	l.pct_chg,
	l.amount,
	l.limit_amount,
	l.float_mv,
	l.total_mv,
	l.turnover_ratio,
	l.fd_amount,
	l.first_time,
	l.last_time,
	l.open_times,
	l.up_stat,
	l.limit_times,
	l."limit"
FROM src.limit_list_d l
WHERE l.trade_date BETWEEN '%s' AND '%s'`, escapeSQLString(startDate), escapeSQLString(endDate)),
		fmt.Sprintf(`CREATE TABLE limit_list_ths AS
SELECT
	l.trade_date,
	l.ts_code,
	l.name,
	l.price,
	l.pct_chg,
	l.turnover_rate,
	l.turnover,
	l.free_float,
	l.open_num,
	l.limit_type,
	l.first_lu_time,
	l.last_lu_time,
	l.lu_desc
FROM src.limit_list_ths l
WHERE l.trade_date BETWEEN '%s' AND '%s'`, escapeSQLString(startDate), escapeSQLString(endDate)),
		fmt.Sprintf(`CREATE TABLE limit_cpt_list AS
SELECT
	l.ts_code,
	l.name,
	l.trade_date,
	l.days,
	l.up_stat,
	l.cons_nums,
	l.up_nums,
	l.pct_chg,
	l.rank
FROM src.limit_cpt_list l
WHERE l.trade_date BETWEEN '%s' AND '%s'`, escapeSQLString(startDate), escapeSQLString(endDate)),
		fmt.Sprintf(`CREATE TABLE index_basic AS
WITH active_dates AS (
	SELECT DISTINCT trade_date
	FROM src.index_daily
	WHERE trade_date BETWEEN '%s' AND '%s'
),
active_indices AS (
	SELECT DISTINCT d.ts_code
	FROM src.index_daily d
	JOIN active_dates ad ON ad.trade_date = d.trade_date
)
SELECT
	i.ts_code,
	i.name,
	i.fullname,
	i.market,
	i.publisher,
	i.index_type,
	i.category,
	i.base_date,
	i.base_point,
	i.list_date,
	i.weight_rule,
	i."desc",
	i.exp_date
FROM src.index_basic i
JOIN active_indices a ON a.ts_code = i.ts_code`, escapeSQLString(startDate), escapeSQLString(endDate)),
		fmt.Sprintf(`CREATE TABLE index_daily AS
WITH active_dates AS (
	SELECT DISTINCT trade_date
	FROM src.index_daily
	WHERE trade_date BETWEEN '%s' AND '%s'
),
ranked_indices AS (
	SELECT
		d.ts_code,
		AVG(ABS(COALESCE(d.amount, 0))) AS avg_amount
	FROM src.index_daily d
	JOIN active_dates ad ON ad.trade_date = d.trade_date
	GROUP BY d.ts_code
	ORDER BY avg_amount DESC, d.ts_code
	LIMIT 20
)
SELECT
	d.ts_code,
	d.trade_date,
	d.close,
	d.open,
	d.high,
	d.low,
	d.pre_close,
	d.change,
	d.pct_chg,
	d.vol,
	d.amount
FROM src.index_daily d
JOIN ranked_indices r ON r.ts_code = d.ts_code
WHERE d.trade_date BETWEEN '%s' AND '%s'`, escapeSQLString(startDate), escapeSQLString(endDate), escapeSQLString(startDate), escapeSQLString(endDate)),
	}
	for _, stmt := range stmts {
		_, err := dest.Execute(ctx, stmt)
		require.NoError(t, err, "stmt failed: %s", stmt)
	}
	return startDate, endDate
}

func seedMetricsFeatureTables(t *testing.T, ctx context.Context, db interface {
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

func postJSONWithToken(t *testing.T, url string, body any, expectedStatus int) map[string]any {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, expectedStatus, resp.StatusCode)
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func getJSON(t *testing.T, url string, expectedStatus int) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, expectedStatus, resp.StatusCode)
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

func parseJobID(t *testing.T, resp map[string]any) string {
	t.Helper()
	data := resp["data"].(map[string]any)
	jobID, ok := data["job_id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, jobID)
	return jobID
}

func waitJobSucceeded(t *testing.T, baseURL, token, jobID string) {
	waitJobSucceededWithin(t, baseURL, token, jobID, 2*time.Second, 20*time.Millisecond)
}

func waitJobSucceededWithin(t *testing.T, baseURL, token, jobID string, timeout, pollInterval time.Duration) {
	t.Helper()
	var resp map[string]any
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp = getJSON(t, baseURL+"/api/v1/metrics/jobs/"+jobID+"?token="+token, http.StatusOK)
		data := resp["data"].(map[string]any)
		status, _ := data["status"].(string)
		if status == "succeeded" {
			return
		}
		require.NotEqual(t, "failed", status)
		time.Sleep(pollInterval)
	}
	t.Fatalf("job %s did not succeed in time: %+v", jobID, resp)
}
