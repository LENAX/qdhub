package http

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/analysis"
)

// AnalysisHandler 分析模块 HTTP Handler
type AnalysisHandler struct {
	svc         contracts.AnalysisApplicationService
	newsNotifier NewsUpdateNotifier // 可选：非 nil 时新闻写入后主动通知 SSE 推送
}

// NewAnalysisHandler 创建 AnalysisHandler。newsNotifier 可选，用于新闻 SSE 写入即推。
func NewAnalysisHandler(svc contracts.AnalysisApplicationService, newsNotifier NewsUpdateNotifier) *AnalysisHandler {
	return &AnalysisHandler{svc: svc, newsNotifier: newsNotifier}
}

// RegisterRoutes 注册 /analysis 下所有路由（挂在 protected 组下，需 JWT + RBAC）
func (h *AnalysisHandler) RegisterRoutes(rg *gin.RouterGroup) {
	g := rg.Group("/analysis")
	{
		g.GET("/kline", h.GetKLine)
		g.GET("/trade-cal", h.GetTradeCalendar)
		g.GET("/stocks", h.ListStocks)
		g.GET("/stocks/indicators", h.GetStockIndicators)
		g.GET("/stocks/snapshot", h.GetStockSnapshot)
		g.GET("/stocks/:ts_code/basic", h.GetStockBasic)
		g.GET("/indices", h.ListIndices)
		g.GET("/concepts", h.ListConcepts)
		g.GET("/financial/indicators", h.GetFinancialIndicators)
		g.GET("/financial/income", h.GetFinancialIncome)
		g.GET("/financial/balancesheet", h.GetFinancialBalanceSheet)
		g.GET("/financial/cashflow", h.GetFinancialCashFlow)
		g.GET("/limit-stats", h.GetLimitStats)
		g.GET("/limit-stocks", h.GetLimitStockList)
		g.GET("/limit-up-ladder", h.GetLimitUpLadder)
		g.GET("/limit-up-comparison", h.GetLimitUpComparison)
		g.GET("/limit-up-list", h.GetLimitUpList)
		g.GET("/sector-limit-up-stats", h.GetSectorLimitStats)
		g.GET("/sector-limit-up-stocks", h.GetSectorLimitStocks)
		g.GET("/concept-heat", h.GetConceptHeat)
		g.GET("/concept-stocks", h.GetConceptStocks)
		g.GET("/concept-rotation", h.GetConceptRotation)
		g.GET("/dragon-tiger", h.GetDragonTigerList)
		g.GET("/money-flow", h.GetMoneyFlow)
		g.GET("/moneyflow-concept", h.GetMoneyFlowConcept)
		g.GET("/popularity-rank", h.GetPopularityRank)
		g.GET("/news", h.ListNews)
		g.GET("/news/stream", h.StreamNews)
		g.GET("/realtime-tick", h.GetRealtimeTicks)
		g.GET("/intraday-ticks", h.GetIntradayTicks)
		g.GET("/intraday-kline", h.GetIntradayKline)
		g.POST("/factors", h.CalculateFactors)
		g.POST("/custom-query/query", h.ExecuteReadOnlyQuery)
	}
}

func defaultInt(s string, def int) int {
	if s == "" {
		return def
	}
	v, _ := strconv.Atoi(s)
	if v <= 0 {
		return def
	}
	return v
}

// GetKLine handles GET /api/v1/analysis/kline
// @Summary      Get K-line data
// @Description  Get daily/weekly/monthly K-line data for a stock
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        ts_code     query     string  true   "Stock code (e.g. 000001.SZ)"
// @Param        start_date  query     string  true   "Start date YYYYMMDD"
// @Param        end_date    query     string  true   "End date YYYYMMDD"
// @Param        adjust_type query     string  false  "Adjust type: none, qfq, hfq" default(none)
// @Param        period      query     string  false  "Period: D, W, M" default(D)
// @Success      200        {object}  Response
// @Failure      400        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/kline [get]
func (h *AnalysisHandler) GetKLine(c *gin.Context) {
	tsCode := c.Query("ts_code")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	adjust := c.DefaultQuery("adjust_type", "none")
	period := c.DefaultQuery("period", "D")
	if tsCode == "" || startDate == "" || endDate == "" {
		BadRequest(c, "ts_code, start_date, end_date required")
		return
	}
	req := analysis.KLineRequest{
		TsCode:     tsCode,
		StartDate:  startDate,
		EndDate:    endDate,
		AdjustType: analysis.AdjustType(adjust),
		Period:     period,
	}
	data, err := h.svc.GetKLine(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, data)
}

// ListStocks handles GET /api/v1/analysis/stocks
// @Summary      List stocks
// @Description  List stocks with optional market/industry/list_status/query filter. query searches by name, ts_code, symbol.
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        market      query     string  false  "Market filter"
// @Param        industry    query     string  false  "Industry filter"
// @Param        list_status query     string  false  "List status filter"
// @Param        query       query     string  false  "Search by name or code (fuzzy)"
// @Param        limit       query     int     false  "Limit" default(100)
// @Param        offset      query     int     false  "Offset" default(0)
// @Success      200        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/stocks [get]
func (h *AnalysisHandler) ListStocks(c *gin.Context) {
	var market, industry, listStatus, query *string
	if v := c.Query("market"); v != "" {
		market = &v
	}
	if v := c.Query("industry"); v != "" {
		industry = &v
	}
	if v := c.Query("list_status"); v != "" {
		listStatus = &v
	}
	if v := c.Query("query"); v != "" {
		query = &v
	}
	var searchType *string
	if v := c.Query("search_type"); v != "" {
		searchType = &v
	}
	req := analysis.StockListRequest{
		Market: market, Industry: industry, ListStatus: listStatus, Query: query, SearchType: searchType,
		Limit: defaultInt(c.Query("limit"), 100), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.ListStocks(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"total": len(list), "items": list})
}

// GetStockSnapshot handles GET /api/v1/analysis/stocks/snapshot
// @Summary      Get stock snapshot
// @Description  Get latest adjusted close price and change for given stocks on a trade date
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        trade_date  query     string  true  "Trade date YYYYMMDD"
// @Param        adjust_type query     string  false "Adjust type: none, qfq, hfq" default(qfq)
// @Param        ts_codes    query     string  true  "Comma separated ts_code list"
// @Success      200        {object}  Response
// @Failure      400        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/stocks/snapshot [get]
func (h *AnalysisHandler) GetStockSnapshot(c *gin.Context) {
	tradeDate := c.Query("trade_date")
	adjust := c.DefaultQuery("adjust_type", "qfq")
	rawCodes := c.Query("ts_codes")
	if tradeDate == "" || rawCodes == "" {
		BadRequest(c, "trade_date, ts_codes required")
		return
	}
	parts := strings.Split(rawCodes, ",")
	tsCodes := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			tsCodes = append(tsCodes, v)
		}
	}
	if len(tsCodes) == 0 {
		BadRequest(c, "ts_codes required")
		return
	}
	list, err := h.svc.GetStockSnapshot(c.Request.Context(), tradeDate, analysis.AdjustType(adjust), tsCodes)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"trade_date": tradeDate, "items": list})
}

// GetTradeCalendar handles GET /api/v1/analysis/trade-cal
// @Summary      Get trading calendar
// @Description  Returns list of trading dates (cal_date where is_open=1) from trade_cal table for the given range
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        start_date query string true "Start date YYYYMMDD"
// @Param        end_date   query string true "End date YYYYMMDD"
// @Success      200 {object} Response
// @Failure      400 {object} Response
// @Failure      500 {object} Response
// @Security     BearerAuth
// @Router       /analysis/trade-cal [get]
func (h *AnalysisHandler) GetTradeCalendar(c *gin.Context) {
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	if startDate == "" || endDate == "" {
		BadRequest(c, "start_date, end_date required")
		return
	}
	dates, err := h.svc.GetTradeCalendar(c.Request.Context(), startDate, endDate)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"dates": dates})
}

// GetStockBasic handles GET /api/v1/analysis/stocks/:ts_code/basic
// @Summary      Get stock basic info
// @Description  Get basic information for a stock by ts_code
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        ts_code  path      string  true  "Stock code (e.g. 000001.SZ)"
// @Success      200     {object}  Response
// @Failure      400     {object}  Response
// @Failure      500     {object}  Response
// @Security     BearerAuth
// @Router       /analysis/stocks/{ts_code}/basic [get]
func (h *AnalysisHandler) GetStockBasic(c *gin.Context) {
	tsCode := c.Param("ts_code")
	if tsCode == "" {
		BadRequest(c, "ts_code required")
		return
	}
	info, err := h.svc.GetStockBasicInfo(c.Request.Context(), tsCode)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, info)
}

// ListIndices handles GET /api/v1/analysis/indices
// @Summary      List indices
// @Description  List market indices with optional market/category filter
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        market    query     string  false  "Market filter"
// @Param        category  query     string  false  "Category filter"
// @Param        limit     query     int     false  "Limit" default(100)
// @Param        offset    query     int     false  "Offset" default(0)
// @Success      200       {object}  Response
// @Failure      500       {object}  Response
// @Security     BearerAuth
// @Router       /analysis/indices [get]
func (h *AnalysisHandler) ListIndices(c *gin.Context) {
	var market, category *string
	if v := c.Query("market"); v != "" {
		market = &v
	}
	if v := c.Query("category"); v != "" {
		category = &v
	}
	req := analysis.IndexListRequest{
		Market: market, Category: category,
		Limit: defaultInt(c.Query("limit"), 100), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.ListIndices(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"total": len(list), "items": list})
}

// ListConcepts handles GET /api/v1/analysis/concepts
// @Summary      List concepts
// @Description  List concept themes with optional source filter
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        source  query     string  false  "Source filter"
// @Param        limit   query     int     false  "Limit" default(100)
// @Param        offset  query     int     false  "Offset" default(0)
// @Success      200    {object}  Response
// @Failure      500    {object}  Response
// @Security     BearerAuth
// @Router       /analysis/concepts [get]
func (h *AnalysisHandler) ListConcepts(c *gin.Context) {
	var source *string
	if v := c.Query("source"); v != "" {
		source = &v
	}
	req := analysis.ConceptListRequest{
		Source: source,
		Limit:  defaultInt(c.Query("limit"), 100), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.ListConcepts(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"total": len(list), "items": list})
}

// GetFinancialIndicators handles GET /api/v1/analysis/financial/indicators
// @Summary      Get financial indicators
// @Description  Get financial indicators for a stock
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        ts_code     query     string  true   "Stock code"
// @Param        start_date  query     string  false  "Start date YYYYMMDD"
// @Param        end_date    query     string  false  "End date YYYYMMDD"
// @Param        limit       query     int     false  "Limit" default(50)
// @Param        offset      query     int     false  "Offset" default(0)
// @Success      200        {object}  Response
// @Failure      400        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/financial/indicators [get]
func (h *AnalysisHandler) GetFinancialIndicators(c *gin.Context) {
	tsCode := c.Query("ts_code")
	if tsCode == "" {
		BadRequest(c, "ts_code required")
		return
	}
	var startDate, endDate *string
	if v := c.Query("start_date"); v != "" {
		startDate = &v
	}
	if v := c.Query("end_date"); v != "" {
		endDate = &v
	}
	req := analysis.FinancialIndicatorRequest{
		TsCode: tsCode, StartDate: startDate, EndDate: endDate,
		Limit: defaultInt(c.Query("limit"), 50), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.GetFinancialIndicators(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"total": len(list), "items": list})
}

// GetStockIndicators handles GET /api/v1/analysis/stocks/indicators
// @Summary      Get technical indicators for a stock
// @Description  Calculate MA/RSI/MACD indicators for given stock and date range, with same adjust type & period as K line
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        ts_code     query     string  true   "Stock code (e.g. 000001.SZ)"
// @Param        start_date  query     string  true   "Start date YYYYMMDD"
// @Param        end_date    query     string  true   "End date YYYYMMDD"
// @Param        adjust_type query     string  false  "Adjust type: none, qfq, hfq" default(qfq)
// @Param        period      query     string  false  "Period: D, W, M" default(D)
// @Param        indicators  query     string  true   "Comma separated indicator names, e.g. MA5,MA10,MA20,RSI,MACD"
// @Success      200        {object}  Response
// @Failure      400        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/stocks/indicators [get]
func (h *AnalysisHandler) GetStockIndicators(c *gin.Context) {
	tsCode := c.Query("ts_code")
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	adjust := c.DefaultQuery("adjust_type", "qfq")
	period := c.DefaultQuery("period", "D")
	rawIndicators := c.Query("indicators")
	if tsCode == "" || startDate == "" || endDate == "" || rawIndicators == "" {
		BadRequest(c, "ts_code, start_date, end_date, indicators required")
		return
	}
	parts := strings.Split(rawIndicators, ",")
	names := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			names = append(names, v)
		}
	}
	if len(names) == 0 {
		BadRequest(c, "indicators required")
		return
	}
	req := analysis.TechnicalIndicatorCalcRequest{
		TsCode:     tsCode,
		StartDate:  startDate,
		EndDate:    endDate,
		AdjustType: analysis.AdjustType(adjust),
		Period:     period,
		Indicators: names,
	}
	list, err := h.svc.GetTechnicalIndicators(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, list)
}

// GetRealtimeTicks handles GET /api/v1/analysis/realtime-tick
func (h *AnalysisHandler) GetRealtimeTicks(c *gin.Context) {
	tsCode := c.Query("ts_code")
	if tsCode == "" {
		BadRequest(c, "ts_code required")
		return
	}
	limit := defaultInt(c.Query("limit"), 500)
	list, err := h.svc.GetRealtimeTicks(c.Request.Context(), tsCode, limit)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"items": list})
}

// GetIntradayTicks handles GET /api/v1/analysis/intraday-ticks
// 历史分时数据：参数 ts_code、date（或 trade_date），返回当日已有分笔数据，按 trade_time 升序
func (h *AnalysisHandler) GetIntradayTicks(c *gin.Context) {
	tsCode := c.Query("ts_code")
	tradeDate := c.Query("trade_date")
	if tradeDate == "" {
		tradeDate = c.Query("date")
	}
	if tsCode == "" || tradeDate == "" {
		BadRequest(c, "ts_code and date (or trade_date) required")
		return
	}
	list, err := h.svc.GetIntradayTicks(c.Request.Context(), tsCode, tradeDate)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"ts_code": tsCode, "date": tradeDate, "items": list})
}

// GetIntradayKline handles GET /api/v1/analysis/intraday-kline
func (h *AnalysisHandler) GetIntradayKline(c *gin.Context) {
	tsCode := c.Query("ts_code")
	tradeDate := c.Query("trade_date")
	period := c.DefaultQuery("period", "1m")
	if tsCode == "" || tradeDate == "" {
		BadRequest(c, "ts_code and trade_date required")
		return
	}
	list, err := h.svc.GetIntradayKline(c.Request.Context(), tsCode, tradeDate, period)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"items": list})
}

// getFinancialTableData 通用财报查询，table 由调用方指定
func (h *AnalysisHandler) getFinancialTableData(c *gin.Context, table string) {
	tsCode := c.Query("ts_code")
	if tsCode == "" {
		BadRequest(c, "ts_code required")
		return
	}
	var startDate, endDate, reportType *string
	if v := c.Query("start_date"); v != "" {
		startDate = &v
	}
	if v := c.Query("end_date"); v != "" {
		endDate = &v
	}
	if v := c.Query("report_type"); v != "" {
		reportType = &v
	}
	req := analysis.FinancialReportRequest{
		TsCode: tsCode, StartDate: startDate, EndDate: endDate, ReportType: reportType,
		Limit: defaultInt(c.Query("limit"), 50), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.GetFinancialTableData(c.Request.Context(), table, req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"total": len(list), "items": list})
}

// GetFinancialIncome handles GET /api/v1/analysis/financial/income
// @Router /analysis/financial/income [get]
func (h *AnalysisHandler) GetFinancialIncome(c *gin.Context) {
	h.getFinancialTableData(c, "income")
}

// GetFinancialBalanceSheet handles GET /api/v1/analysis/financial/balancesheet
// @Router /analysis/financial/balancesheet [get]
func (h *AnalysisHandler) GetFinancialBalanceSheet(c *gin.Context) {
	h.getFinancialTableData(c, "balancesheet")
}

// GetFinancialCashFlow handles GET /api/v1/analysis/financial/cashflow
// @Router /analysis/financial/cashflow [get]
func (h *AnalysisHandler) GetFinancialCashFlow(c *gin.Context) {
	h.getFinancialTableData(c, "cashflow")
}

// GetLimitStats handles GET /api/v1/analysis/limit-stats
// @Summary      Get limit up/down stats
// @Description  Get daily limit statistics for a date range
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        start_date  query     string  true  "Start date YYYYMMDD"
// @Param        end_date    query     string  true  "End date YYYYMMDD"
// @Success      200        {object}  Response
// @Failure      400        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/limit-stats [get]
func (h *AnalysisHandler) GetLimitStats(c *gin.Context) {
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	if startDate == "" || endDate == "" {
		BadRequest(c, "start_date, end_date required")
		return
	}
	list, err := h.svc.GetLimitStats(c.Request.Context(), startDate, endDate)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, list)
}

// GetLimitStockList handles GET /api/v1/analysis/limit-stocks
// @Summary      Get limit up/down stock list
// @Description  Get list of limit up or limit down stocks for a trade date
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        trade_date  query     string  true   "Trade date YYYYMMDD"
// @Param        limit_type  query     string  false  "up or down" default(up)
// @Success      200        {object}  Response
// @Failure      400        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/limit-stocks [get]
func (h *AnalysisHandler) GetLimitStockList(c *gin.Context) {
	tradeDate := c.Query("trade_date")
	limitType := c.DefaultQuery("limit_type", "up")
	if tradeDate == "" {
		BadRequest(c, "trade_date required")
		return
	}
	list, err := h.svc.GetLimitStockList(c.Request.Context(), tradeDate, limitType)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"trade_date": tradeDate, "items": list})
}

// GetLimitUpLadder handles GET /api/v1/analysis/limit-up-ladder
// @Summary      Get limit up ladder
// @Description  Get limit up ladder stats for a trade date
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        trade_date  query     string  true  "Trade date YYYYMMDD"
// @Success      200         {object}  Response
// @Failure      400         {object}  Response
// @Failure      500         {object}  Response
// @Security     BearerAuth
// @Router       /analysis/limit-up-ladder [get]
func (h *AnalysisHandler) GetLimitUpLadder(c *gin.Context) {
	tradeDate := c.Query("trade_date")
	if tradeDate == "" {
		BadRequest(c, "trade_date required")
		return
	}
	list, err := h.svc.GetLimitUpLadder(c.Request.Context(), tradeDate)
	if err != nil {
		HandleError(c, err)
		return
	}
	FirstLimitUp, _ := h.svc.GetFirstLimitUpStocks(c.Request.Context(), tradeDate)
	if FirstLimitUp == nil {
		FirstLimitUp = []analysis.LimitStock{}
	}
	Success(c, gin.H{"trade_date": tradeDate, "ladders": list, "first_board_stocks": FirstLimitUp})
}

// GetLimitUpComparison handles GET /api/v1/analysis/limit-up-comparison
// @Summary      Get limit up comparison
// @Description  Get limit up comparison for today vs previous days
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        today_date  query     string  true  "Today date YYYYMMDD"
// @Success      200         {object}  Response
// @Failure      400         {object}  Response
// @Failure      500         {object}  Response
// @Security     BearerAuth
// @Router       /analysis/limit-up-comparison [get]
func (h *AnalysisHandler) GetLimitUpComparison(c *gin.Context) {
	todayDate := c.Query("today_date")
	if todayDate == "" {
		BadRequest(c, "today_date required")
		return
	}
	data, err := h.svc.GetLimitUpComparison(c.Request.Context(), todayDate)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, data)
}

// GetLimitUpList handles GET /api/v1/analysis/limit-up-list
// @Summary      Get limit up list
// @Description  Get paginated limit up stock list for a trade date
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        trade_date  query     string  true   "Trade date YYYYMMDD"
// @Param        limit       query     int     false  "Limit" default(100)
// @Param        offset      query     int     false  "Offset" default(0)
// @Success      200        {object}  Response
// @Failure      400        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/limit-up-list [get]
func (h *AnalysisHandler) GetLimitUpList(c *gin.Context) {
	tradeDate := c.Query("trade_date")
	if tradeDate == "" {
		BadRequest(c, "trade_date required")
		return
	}
	req := analysis.LimitUpListRequest{
		TradeDate: tradeDate,
		Limit:     defaultInt(c.Query("limit"), 100), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.GetLimitUpList(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"total": len(list), "items": list})
}

// GetSectorLimitStats handles GET /api/v1/analysis/sector-limit-up-stats
// @Summary      Get sector limit up stats
// @Description  Get limit up statistics by sector (industry/concept) for a trade date
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        trade_date   query     string  true   "Trade date YYYYMMDD"
// @Param        sector_type  query     string  false  "industry or concept" default(industry)
// @Success      200         {object}  Response
// @Failure      400         {object}  Response
// @Failure      500         {object}  Response
// @Security     BearerAuth
// @Router       /analysis/sector-limit-up-stats [get]
func (h *AnalysisHandler) GetSectorLimitStats(c *gin.Context) {
	tradeDate := c.Query("trade_date")
	sectorType := c.DefaultQuery("sector_type", "industry")
	if tradeDate == "" {
		BadRequest(c, "trade_date required")
		return
	}
	list, err := h.svc.GetLimitUpBySector(c.Request.Context(), tradeDate, sectorType)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"trade_date": tradeDate, "sector_type": sectorType, "stats": list})
}

// GetSectorLimitStocks handles GET /api/v1/analysis/sector-limit-up-stocks
// @Summary      Get sector limit up stocks
// @Description  Get limit up stocks for a specific sector on a trade date
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        sector_code  query     string  true   "Sector code"
// @Param        sector_type  query     string  false  "industry or concept" default(industry)
// @Param        trade_date   query     string  true   "Trade date YYYYMMDD"
// @Success      200         {object}  Response
// @Failure      400         {object}  Response
// @Failure      500         {object}  Response
// @Security     BearerAuth
// @Router       /analysis/sector-limit-up-stocks [get]
func (h *AnalysisHandler) GetSectorLimitStocks(c *gin.Context) {
	sectorCode := c.Query("sector_code")
	sectorType := c.DefaultQuery("sector_type", "industry")
	tradeDate := c.Query("trade_date")
	if sectorCode == "" || tradeDate == "" {
		BadRequest(c, "sector_code, trade_date required")
		return
	}
	list, err := h.svc.GetLimitUpStocksBySector(c.Request.Context(), sectorCode, sectorType, tradeDate)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"sector_code": sectorCode, "sector_type": sectorType, "trade_date": tradeDate, "stocks": list})
}

// GetConceptHeat handles GET /api/v1/analysis/concept-heat
// @Summary      Get concept heat
// @Description  Get concept theme heat for a trade date
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        trade_date  query     string  true  "Trade date YYYYMMDD"
// @Success      200         {object}  Response
// @Failure      400         {object}  Response
// @Failure      500         {object}  Response
// @Security     BearerAuth
// @Router       /analysis/concept-heat [get]
func (h *AnalysisHandler) GetConceptHeat(c *gin.Context) {
	tradeDate := c.Query("trade_date")
	if tradeDate == "" {
		BadRequest(c, "trade_date required")
		return
	}
	list, err := h.svc.GetConceptHeat(c.Request.Context(), tradeDate)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, list)
}

// GetConceptStocks handles GET /api/v1/analysis/concept-stocks
// @Summary      Get concept stocks
// @Description  Get stocks under a concept for a trade date
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        concept_code  query     string  true  "Concept code"
// @Param        trade_date    query     string  true  "Trade date YYYYMMDD"
// @Success      200           {object}  Response
// @Failure      400           {object}  Response
// @Failure      500           {object}  Response
// @Security     BearerAuth
// @Router       /analysis/concept-stocks [get]
func (h *AnalysisHandler) GetConceptStocks(c *gin.Context) {
	conceptCode := c.Query("concept_code")
	tradeDate := c.Query("trade_date")
	if conceptCode == "" || tradeDate == "" {
		BadRequest(c, "concept_code, trade_date required")
		return
	}
	list, err := h.svc.GetConceptStocks(c.Request.Context(), conceptCode, tradeDate)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"concept_code": conceptCode, "trade_date": tradeDate, "items": list})
}

// GetConceptRotation handles GET /api/v1/analysis/concept-rotation
// @Summary      Get concept rotation
// @Description  Get concept rotation stats for a date range
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        start_date  query     string  true   "Start date YYYYMMDD"
// @Param        end_date    query     string  true   "End date YYYYMMDD"
// @Param        rank_by     query     string  false  "Rank by field" default(pct_chg)
// @Param        top_n       query     int     false  "Top N"
// @Success      200        {object}  Response
// @Failure      400        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/concept-rotation [get]
func (h *AnalysisHandler) GetConceptRotation(c *gin.Context) {
	startDate := c.Query("start_date")
	endDate := c.Query("end_date")
	rankBy := c.DefaultQuery("rank_by", "pct_chg")
	if startDate == "" || endDate == "" {
		BadRequest(c, "start_date, end_date required")
		return
	}
	req := analysis.ConceptRotationRequest{
		StartDate: startDate, EndDate: endDate, RankBy: rankBy,
		TopN: defaultInt(c.Query("top_n"), 0),
	}
	data, err := h.svc.GetConceptRotationStats(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, data)
}

// GetDragonTigerList handles GET /api/v1/analysis/dragon-tiger
// @Summary      Get dragon-tiger list
// @Description  Get dragon-tiger (龙虎榜) list with optional trade_date/ts_code filter
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        trade_date  query     string  false  "Trade date YYYYMMDD"
// @Param        ts_code    query     string  false  "Stock code"
// @Param        limit      query     int     false  "Limit" default(100)
// @Param        offset     query     int     false  "Offset" default(0)
// @Success      200        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/dragon-tiger [get]
func (h *AnalysisHandler) GetDragonTigerList(c *gin.Context) {
	var tradeDate, tsCode *string
	if v := c.Query("trade_date"); v != "" {
		tradeDate = &v
	}
	if v := c.Query("ts_code"); v != "" {
		tsCode = &v
	}
	if (tradeDate == nil || *tradeDate == "") && (tsCode == nil || *tsCode == "") {
		BadRequest(c, "trade_date or ts_code required (at least one)")
		return
	}
	req := analysis.DragonTigerRequest{
		TradeDate: tradeDate, TsCode: tsCode,
		Limit: defaultInt(c.Query("limit"), 100), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.GetDragonTigerList(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"items": list})
}

// GetMoneyFlow handles GET /api/v1/analysis/money-flow
// @Summary      Get money flow
// @Description  Get main/net money flow by date, date range, or stock code
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        trade_date  query     string  false  "Trade date YYYYMMDD (single day)"
// @Param        start_date  query     string  false  "Start date YYYYMMDD (range query, use with end_date)"
// @Param        end_date    query     string  false  "End date YYYYMMDD (range query, use with start_date)"
// @Param        ts_code     query     string  false  "Stock code"
// @Param        market      query     string  false  "Market"
// @Param        limit       query     int     false  "Limit" default(100)
// @Param        offset      query     int     false  "Offset" default(0)
// @Success      200        {object}  Response
// @Failure      400        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/money-flow [get]
func (h *AnalysisHandler) GetMoneyFlow(c *gin.Context) {
	var tradeDate, tsCode, startDate, endDate *string
	if v := c.Query("trade_date"); v != "" {
		tradeDate = &v
	}
	if v := c.Query("start_date"); v != "" {
		startDate = &v
	}
	if v := c.Query("end_date"); v != "" {
		endDate = &v
	}
	if v := c.Query("ts_code"); v != "" {
		tsCode = &v
	}

	hasDate := tradeDate != nil
	hasRange := startDate != nil && endDate != nil
	hasTsCode := tsCode != nil

	if !hasDate && !hasRange && !hasTsCode {
		BadRequest(c, "trade_date, start_date+end_date, or ts_code required (at least one)")
		return
	}

	var market *string
	if v := c.Query("market"); v != "" {
		market = &v
	}
	req := analysis.MoneyFlowRequest{
		TradeDate: tradeDate, StartDate: startDate, EndDate: endDate,
		TsCode: tsCode, Market: market,
		Limit: defaultInt(c.Query("limit"), 5000), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.GetMoneyFlow(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	respTradeDate := ""
	if tradeDate != nil {
		respTradeDate = *tradeDate
	}
	Success(c, gin.H{"trade_date": respTradeDate, "items": list})
}

// GetMoneyFlowConcept handles GET /api/v1/analysis/moneyflow-concept
func (h *AnalysisHandler) GetMoneyFlowConcept(c *gin.Context) {
	tradeDate := c.Query("trade_date")
	if tradeDate == "" {
		BadRequest(c, "trade_date required")
		return
	}
	var concept *string
	if v := c.Query("concept"); v != "" {
		concept = &v
	}
	req := analysis.MoneyFlowConceptRequest{
		TradeDate: tradeDate, Concept: concept,
		Limit: defaultInt(c.Query("limit"), 100), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.GetMoneyFlowConcept(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"trade_date": tradeDate, "items": list})
}

// GetPopularityRank handles GET /api/v1/analysis/popularity-rank
// @Summary      Get popularity rank
// @Description  Get stock popularity rank by volume or other metric
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        rank_type  query     string  false  "Rank type (e.g. volume)" default(volume)
// @Param        limit     query     int     false  "Limit" default(20)
// @Success      200       {object}  Response
// @Failure      500       {object}  Response
// @Security     BearerAuth
// @Router       /analysis/popularity-rank [get]
func (h *AnalysisHandler) GetPopularityRank(c *gin.Context) {
	rankType := c.DefaultQuery("rank_type", "volume")
	req := analysis.PopularityRankRequest{
		RankType: rankType, Limit: defaultInt(c.Query("limit"), 20),
	}
	list, err := h.svc.GetPopularityRank(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"rank_type": rankType, "items": list})
}

// ListNews handles GET /api/v1/analysis/news
// @Summary      List news
// @Description  List news with optional ts_code, category, date range
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        ts_code     query     string  false  "Stock code"
// @Param        category    query     string  false  "News category"
// @Param        start_date  query     string  false  "Start date YYYYMMDD"
// @Param        end_date    query     string  false  "End date YYYYMMDD"
// @Param        limit       query     int     false  "Limit" default(20)
// @Param        offset      query     int     false  "Offset" default(0)
// @Success      200        {object}  Response
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /analysis/news [get]
func (h *AnalysisHandler) ListNews(c *gin.Context) {
	var tsCode, category, startDate, endDate, sources *string
	if v := c.Query("ts_code"); v != "" {
		tsCode = &v
	}
	if v := c.Query("category"); v != "" {
		category = &v
	}
	if v := c.Query("start_date"); v != "" {
		startDate = &v
	}
	if v := c.Query("end_date"); v != "" {
		endDate = &v
	}
	if v := c.Query("sources"); v != "" {
		sources = &v
	}
	order := c.DefaultQuery("order", "time_desc")
	req := analysis.NewsListRequest{
		TsCode: tsCode, Category: category, StartDate: startDate, EndDate: endDate,
		Order: order, Sources: sources,
		Limit: defaultInt(c.Query("limit"), 50), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.ListNews(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"total": len(list), "items": list})
}

// StreamNews handles GET /api/v1/analysis/news/stream (SSE)
func (h *AnalysisHandler) StreamNews(c *gin.Context) {
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		BadRequest(c, "streaming not supported")
		return
	}

	intervalSec := defaultInt(c.Query("interval_sec"), 5)
	if intervalSec < 1 {
		intervalSec = 5
	}
	limit := defaultInt(c.Query("limit"), 20)
	var sources *string
	if v := c.Query("sources"); v != "" {
		sources = &v
	}
	since := c.Query("since")
	if since == "" {
		if lastID := c.GetHeader("Last-Event-ID"); lastID != "" {
			since = lastID
		}
	}

	c.Writer.Write([]byte("retry: 3000\n\n"))
	flusher.Flush()

	ctx := c.Request.Context()
	keepaliveTicker := time.NewTicker(15 * time.Second)
	defer keepaliveTicker.Stop()
	pollTicker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer pollTicker.Stop()

	var notifyCh <-chan struct{}
	var unsubscribe func()
	if h.newsNotifier != nil {
		notifyCh, unsubscribe = h.newsNotifier.Subscribe()
		defer unsubscribe()
	}

	var lastLatest string

	sendNews := func(reason string) {
		req := analysis.NewsListRequest{
			Order: "time_desc", Sources: sources,
			Limit: limit, Offset: 0,
		}
		list, err := h.svc.ListNewsFromRealtime(ctx, req)
		if err != nil {
			logrus.Warnf("[NewsSSE] sendNews reason=%s ListNewsFromRealtime err=%v", reason, err)
			c.Writer.Write([]byte("event: error\ndata: {\"error\":\"" + strings.ReplaceAll(err.Error(), "\"", "\\\"") + "\"}\n\n"))
			flusher.Flush()
			return
		}
		if since != "" {
			filtered := list[:0]
			for _, item := range list {
				if item.PublishTime >= since {
					filtered = append(filtered, item)
				}
			}
			list = filtered
		}
		if len(list) == 0 {
			logrus.Debugf("[NewsSSE] sendNews reason=%s, list empty, skip push", reason)
			return
		}
		latest := list[0].PublishTime
		if reason != "initial" && latest == lastLatest {
			return
		}
		lastLatest = latest
		logrus.Infof("[NewsSSE] sendNews reason=%s, items=%d, latest=%s", reason, len(list), latest)
		data, _ := json.Marshal(list)
		c.Writer.Write([]byte("id: " + latest + "\nevent: news\ndata: "))
		c.Writer.Write(data)
		c.Writer.Write([]byte("\n\n"))
		flusher.Flush()
	}

	sendNews("initial")
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-notifyCh:
			if !ok {
				return
			}
			sendNews("notify")
		case <-keepaliveTicker.C:
			c.Writer.Write([]byte(": keepalive\n\n"))
			flusher.Flush()
		case <-pollTicker.C:
			sendNews("poll")
		}
	}
}

// CalculateFactors handles POST /api/v1/analysis/factors
// @Summary      Calculate factors
// @Description  Calculate factor values for given stocks and date range
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        request  body      object  true  "ts_codes, start_date, end_date, factors"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Security     BearerAuth
// @Router       /analysis/factors [post]
func (h *AnalysisHandler) CalculateFactors(c *gin.Context) {
	var body struct {
		TsCodes   []string                    `json:"ts_codes"`
		StartDate string                      `json:"start_date"`
		EndDate   string                      `json:"end_date"`
		Factors   []analysis.FactorExpression `json:"factors"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		BadRequest(c, "invalid body: "+err.Error())
		return
	}
	req := analysis.FactorRequest{
		TsCodes: body.TsCodes, StartDate: body.StartDate, EndDate: body.EndDate, Factors: body.Factors,
	}
	data, err := h.svc.CalculateFactors(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, data)
}

// ExecuteReadOnlyQuery handles POST /api/v1/analysis/custom-query/query
// @Summary      Execute read-only query
// @Description  Execute a read-only SQL query (max_rows, timeout_seconds apply)
// @Tags         Analysis
// @Accept       json
// @Produce      json
// @Param        request  body      object  true  "sql, max_rows, timeout_seconds"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Security     BearerAuth
// @Router       /analysis/custom-query/query [post]
func (h *AnalysisHandler) ExecuteReadOnlyQuery(c *gin.Context) {
	var body struct {
		SQL            string `json:"sql"`
		MaxRows        int    `json:"max_rows"`
		TimeoutSeconds int    `json:"timeout_seconds"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		BadRequest(c, "invalid body: "+err.Error())
		return
	}
	if body.MaxRows <= 0 {
		body.MaxRows = 10000
	}
	if body.TimeoutSeconds <= 0 {
		body.TimeoutSeconds = 30
	}
	req := analysis.CustomQueryRequest{
		SQL: body.SQL, MaxRows: body.MaxRows, TimeoutSeconds: body.TimeoutSeconds,
	}
	result, err := h.svc.ExecuteReadOnlyQuery(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, result)
}
