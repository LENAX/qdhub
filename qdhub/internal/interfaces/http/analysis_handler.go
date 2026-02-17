package http

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/analysis"
)

// AnalysisHandler 分析模块 HTTP Handler
type AnalysisHandler struct {
	svc contracts.AnalysisApplicationService
}

// NewAnalysisHandler 创建 AnalysisHandler
func NewAnalysisHandler(svc contracts.AnalysisApplicationService) *AnalysisHandler {
	return &AnalysisHandler{svc: svc}
}

// RegisterRoutes 注册 /analysis 下所有路由（挂在 protected 组下，需 JWT + RBAC）
func (h *AnalysisHandler) RegisterRoutes(rg *gin.RouterGroup) {
	g := rg.Group("/analysis")
	{
		g.GET("/kline", h.GetKLine)
		g.GET("/stocks", h.ListStocks)
		g.GET("/stocks/:ts_code/basic", h.GetStockBasic)
		g.GET("/indices", h.ListIndices)
		g.GET("/concepts", h.ListConcepts)
		g.GET("/financial/indicators", h.GetFinancialIndicators)
		g.GET("/financial/reports", h.GetFinancialReports)
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
		g.GET("/popularity-rank", h.GetPopularityRank)
		g.GET("/news", h.ListNews)
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

func (h *AnalysisHandler) ListStocks(c *gin.Context) {
	var market, industry, listStatus *string
	if v := c.Query("market"); v != "" {
		market = &v
	}
	if v := c.Query("industry"); v != "" {
		industry = &v
	}
	if v := c.Query("list_status"); v != "" {
		listStatus = &v
	}
	req := analysis.StockListRequest{
		Market: market, Industry: industry, ListStatus: listStatus,
		Limit: defaultInt(c.Query("limit"), 100), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.ListStocks(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"total": len(list), "items": list})
}

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

func (h *AnalysisHandler) GetFinancialReports(c *gin.Context) {
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
	req := analysis.FinancialReportRequest{
		TsCode: tsCode, StartDate: startDate, EndDate: endDate,
		Limit: defaultInt(c.Query("limit"), 50), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.GetFinancialReports(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"total": len(list), "items": list})
}

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
	Success(c, gin.H{"trade_date": tradeDate, "ladders": list})
}

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

func (h *AnalysisHandler) GetDragonTigerList(c *gin.Context) {
	var tradeDate, tsCode *string
	if v := c.Query("trade_date"); v != "" {
		tradeDate = &v
	}
	if v := c.Query("ts_code"); v != "" {
		tsCode = &v
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

func (h *AnalysisHandler) GetMoneyFlow(c *gin.Context) {
	tradeDate := c.Query("trade_date")
	if tradeDate == "" {
		BadRequest(c, "trade_date required")
		return
	}
	var tsCode, market *string
	if v := c.Query("ts_code"); v != "" {
		tsCode = &v
	}
	if v := c.Query("market"); v != "" {
		market = &v
	}
	req := analysis.MoneyFlowRequest{
		TradeDate: tradeDate, TsCode: tsCode, Market: market,
		Limit: defaultInt(c.Query("limit"), 100), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.GetMoneyFlow(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"trade_date": tradeDate, "items": list})
}

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

func (h *AnalysisHandler) ListNews(c *gin.Context) {
	var tsCode, category, startDate, endDate *string
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
	req := analysis.NewsListRequest{
		TsCode: tsCode, Category: category, StartDate: startDate, EndDate: endDate,
		Limit: defaultInt(c.Query("limit"), 20), Offset: defaultInt(c.Query("offset"), 0),
	}
	list, err := h.svc.ListNews(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"total": len(list), "items": list})
}

func (h *AnalysisHandler) CalculateFactors(c *gin.Context) {
	var body struct {
		TsCodes   []string               `json:"ts_codes"`
		StartDate string                 `json:"start_date"`
		EndDate   string                 `json:"end_date"`
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
