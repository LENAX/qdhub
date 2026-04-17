package http

import (
	"strings"

	"github.com/gin-gonic/gin"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/metrics"
)

type MetricsHandler struct {
	svc contracts.MetricsApplicationService
}

// NewMetricsHandler creates a new MetricsHandler.
func NewMetricsHandler(svc contracts.MetricsApplicationService) *MetricsHandler {
	return &MetricsHandler{svc: svc}
}

// RegisterRoutes registers metrics routes.
func (h *MetricsHandler) RegisterRoutes(rg *gin.RouterGroup) {
	g := rg.Group("/metrics")
	{
		g.POST("/definitions", h.CreateMetric)
		g.GET("", h.ListMetrics)
		g.GET("/factor-panel", h.GetFactorPanel)
		g.GET("/signal-series", h.GetSignalSeries)
		g.GET("/universe-members", h.GetUniverseMembers)
		g.POST("/jobs", h.SubmitMetricJob)
		g.GET("/jobs/:job_id", h.GetMetricJobStatus)
		g.GET("/:metric_id", h.GetMetricDetail)
	}
}

// CreateMetric handles POST /api/v1/metrics/definitions
// @Summary      Create metric definition
// @Description  Register a factor, signal or universe metric definition
// @Tags         Metrics
// @Accept       json
// @Produce      json
// @Param        request  body      contracts.CreateMetricRequest  true  "Metric definition"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Security     BearerAuth
// @Router       /metrics/definitions [post]
func (h *MetricsHandler) CreateMetric(c *gin.Context) {
	var req contracts.CreateMetricRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}
	item, err := h.svc.CreateMetric(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, item)
}

// ListMetrics handles GET /api/v1/metrics
// @Summary      List metric definitions
// @Description  List registered metrics with optional kind/status/category/query filters
// @Tags         Metrics
// @Accept       json
// @Produce      json
// @Param        kind       query     string  false  "Metric kind: factor/signal/universe"
// @Param        status     query     string  false  "Metric status"
// @Param        category   query     string  false  "Metric category"
// @Param        query      query     string  false  "Query by metric_id/display_name_cn"
// @Param        frequency  query     string  false  "Frequency" default(1d)
// @Success      200        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /metrics [get]
func (h *MetricsHandler) ListMetrics(c *gin.Context) {
	filter := metrics.MetricFilter{
		Kind:      metrics.MetricKind(strings.TrimSpace(c.Query("kind"))),
		Status:    strings.TrimSpace(c.Query("status")),
		Category:  strings.TrimSpace(c.Query("category")),
		Query:     strings.TrimSpace(c.Query("query")),
		Frequency: metrics.Frequency(strings.TrimSpace(c.DefaultQuery("frequency", "1d"))),
	}
	items, err := h.svc.ListMetrics(c.Request.Context(), filter)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"items": items, "total": len(items)})
}

// GetMetricDetail handles GET /api/v1/metrics/:metric_id
// @Summary      Get metric detail
// @Description  Get one metric definition by metric_id
// @Tags         Metrics
// @Accept       json
// @Produce      json
// @Param        metric_id  path      string  true  "Metric ID"
// @Success      200        {object}  Response
// @Failure      404        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /metrics/{metric_id} [get]
func (h *MetricsHandler) GetMetricDetail(c *gin.Context) {
	item, err := h.svc.GetMetricDetail(c.Request.Context(), c.Param("metric_id"))
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, item)
}

// GetFactorPanel handles GET /api/v1/metrics/factor-panel
// @Summary      Get factor panel
// @Description  Read factor values by metric_ids/date range and optional universe filter
// @Tags         Metrics
// @Accept       json
// @Produce      json
// @Param        metric_ids   query     string  true   "Comma separated metric IDs"
// @Param        universe_id  query     string  false  "Universe ID"
// @Param        start_date   query     string  true   "Start date YYYYMMDD"
// @Param        end_date     query     string  true   "End date YYYYMMDD"
// @Param        frequency    query     string  false  "Frequency" default(1d)
// @Success      200          {object}  Response
// @Failure      500          {object}  Response
// @Security     BearerAuth
// @Router       /metrics/factor-panel [get]
func (h *MetricsHandler) GetFactorPanel(c *gin.Context) {
	req := metrics.FactorPanelQuery{
		MetricIDs:  splitCSV(c.Query("metric_ids")),
		UniverseID: strings.TrimSpace(c.Query("universe_id")),
		StartDate:  strings.TrimSpace(c.Query("start_date")),
		EndDate:    strings.TrimSpace(c.Query("end_date")),
		Frequency:  strings.TrimSpace(c.DefaultQuery("frequency", "1d")),
	}
	items, err := h.svc.GetFactorPanel(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"items": items, "total": len(items)})
}

// GetSignalSeries handles GET /api/v1/metrics/signal-series
// @Summary      Get signal series
// @Description  Read signal results by metric_ids/date range and optional entity_ids filter
// @Tags         Metrics
// @Accept       json
// @Produce      json
// @Param        metric_ids  query     string  true   "Comma separated metric IDs"
// @Param        entity_ids  query     string  false  "Comma separated entity IDs"
// @Param        start_date  query     string  true   "Start date YYYYMMDD"
// @Param        end_date    query     string  true   "End date YYYYMMDD"
// @Param        frequency   query     string  false  "Frequency" default(1d)
// @Success      200         {object}  Response
// @Failure      500         {object}  Response
// @Security     BearerAuth
// @Router       /metrics/signal-series [get]
func (h *MetricsHandler) GetSignalSeries(c *gin.Context) {
	req := metrics.SignalSeriesQuery{
		MetricIDs: splitCSV(c.Query("metric_ids")),
		EntityIDs: splitCSV(c.Query("entity_ids")),
		StartDate: strings.TrimSpace(c.Query("start_date")),
		EndDate:   strings.TrimSpace(c.Query("end_date")),
		Frequency: strings.TrimSpace(c.DefaultQuery("frequency", "1d")),
	}
	items, err := h.svc.GetSignalSeries(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"items": items, "total": len(items)})
}

// GetUniverseMembers handles GET /api/v1/metrics/universe-members
// @Summary      Get universe members
// @Description  Read universe membership by universe_id and trade_date/date range
// @Tags         Metrics
// @Accept       json
// @Produce      json
// @Param        universe_id  query     string  true   "Universe ID"
// @Param        trade_date   query     string  false  "Trade date YYYYMMDD"
// @Param        start_date   query     string  false  "Start date YYYYMMDD"
// @Param        end_date     query     string  false  "End date YYYYMMDD"
// @Param        frequency    query     string  false  "Frequency" default(1d)
// @Success      200          {object}  Response
// @Failure      500          {object}  Response
// @Security     BearerAuth
// @Router       /metrics/universe-members [get]
func (h *MetricsHandler) GetUniverseMembers(c *gin.Context) {
	req := metrics.UniverseMembersQuery{
		UniverseID: strings.TrimSpace(c.Query("universe_id")),
		TradeDate:  strings.TrimSpace(c.Query("trade_date")),
		StartDate:  strings.TrimSpace(c.Query("start_date")),
		EndDate:    strings.TrimSpace(c.Query("end_date")),
		Frequency:  strings.TrimSpace(c.DefaultQuery("frequency", "1d")),
	}
	items, err := h.svc.GetUniverseMembers(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"items": items, "total": len(items)})
}

// SubmitMetricJob handles POST /api/v1/metrics/jobs
// @Summary      Submit metric compute job
// @Description  Submit an asynchronous factor/signal/universe materialization job
// @Tags         Metrics
// @Accept       json
// @Produce      json
// @Param        request  body      contracts.SubmitMetricJobRequest  true  "Job request"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Security     BearerAuth
// @Router       /metrics/jobs [post]
func (h *MetricsHandler) SubmitMetricJob(c *gin.Context) {
	var req contracts.SubmitMetricJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}
	job, err := h.svc.SubmitMetricJob(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, job)
}

// GetMetricJobStatus handles GET /api/v1/metrics/jobs/:job_id
// @Summary      Get metric job status
// @Description  Query async metric compute job status by job_id
// @Tags         Metrics
// @Accept       json
// @Produce      json
// @Param        job_id  path      string  true  "Job ID"
// @Success      200     {object}  Response
// @Failure      404     {object}  Response
// @Failure      500     {object}  Response
// @Security     BearerAuth
// @Router       /metrics/jobs/{job_id} [get]
func (h *MetricsHandler) GetMetricJobStatus(c *gin.Context) {
	job, err := h.svc.GetMetricJobStatus(c.Request.Context(), c.Param("job_id"))
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, job)
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
