package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	domain "qdhub/internal/domain/metrics"
	metricsinfra "qdhub/internal/infrastructure/metrics"
	duckdbinfra "qdhub/internal/infrastructure/quantdb/duckdb"
	httpapi "qdhub/internal/interfaces/http"
)

func TestMetricsHandler_CreateAndList(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := newMetricsTestService(t)
	r := gin.New()
	httpapi.NewMetricsHandler(svc).RegisterRoutes(r.Group("/api/v1"))

	body := `{"metric":{"metric_id":"vol_ratio","display_name_cn":"放量比率","kind":"factor","expression":"div(volume, ma(volume, 2))","frequency":"1d","status":"active","factor_spec":{"direction":"higher_better"}}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/metrics/definitions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code)

	listReq := httptest.NewRequest(http.MethodGet, "/api/v1/metrics?kind=factor", nil)
	listW := httptest.NewRecorder()
	r.ServeHTTP(listW, listReq)
	require.Equal(t, http.StatusOK, listW.Code)

	var resp struct {
		Code int `json:"code"`
		Data struct {
			Items []domain.MetricDef `json:"items"`
			Total int                `json:"total"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(listW.Body.Bytes(), &resp))
	require.Equal(t, 0, resp.Code)
	require.Equal(t, 1, resp.Data.Total)
	require.Len(t, resp.Data.Items, 1)
	require.Equal(t, "vol_ratio", resp.Data.Items[0].ID)
}

func newMetricsTestService(t *testing.T) contracts.MetricsApplicationService {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "metrics_http_test.duckdb")
	db := duckdbinfra.NewAdapter(dbPath)
	require.NoError(t, db.Connect(ctx))
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, metricsinfra.EnsureSchema(ctx, db))

	metricRepo := metricsinfra.NewMetricDefRepoDuckDB(db)
	factorRepo := metricsinfra.NewFactorValueRepoDuckDB(db)
	signalRepo := metricsinfra.NewSignalValueRepoDuckDB(db)
	universeRepo := metricsinfra.NewUniverseRepoDuckDB(db)
	jobRepo := metricsinfra.NewComputeJobRepoDuckDB(db)
	registry := metricsinfra.NewService(
		db,
		domain.NewDSLParser(),
		metricRepo,
		factorRepo,
		signalRepo,
		universeRepo,
	)
	return impl.NewMetricsApplicationService(registry, registry, jobRepo)
}
