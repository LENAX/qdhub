package analysis

import "context"

// AnalysisService 分析领域服务接口
type AnalysisService interface {
	// K线数据
	GetKLine(ctx context.Context, req KLineRequest) ([]KLineData, error)

	// 涨跌停统计
	GetLimitStats(ctx context.Context, startDate, endDate string) ([]LimitStats, error)
	GetLimitStockList(ctx context.Context, tradeDate string, limitType string) ([]LimitStock, error)

	// 涨停天梯统计
	GetLimitLadder(ctx context.Context, tradeDate string) (*LimitLadderStats, error)
	GetLimitComparison(ctx context.Context, tradeDate string) (*LimitComparison, error)

	// 板块涨停统计
	GetSectorLimitStats(ctx context.Context, tradeDate string, sectorType string) ([]SectorLimitStats, error)
	GetSectorLimitStocks(ctx context.Context, tradeDate string, sectorCode string, sectorType string) (*SectorLimitStocks, error)

	// 题材分析
	GetConceptHeat(ctx context.Context, tradeDate string) ([]ConceptHeat, error)
	GetConceptStocks(ctx context.Context, conceptCode, tradeDate string) ([]StockInfo, error)

	// 因子计算
	CalculateFactors(ctx context.Context, req FactorRequest) ([]FactorValue, error)

	// 股票列表
	ListStocks(ctx context.Context, req StockListRequest) ([]StockInfo, error)

	// 股票快照：指定交易日、复权方式与 ts_code 列表，返回价格/涨跌幅等
	GetStockSnapshot(ctx context.Context, tradeDate string, adjustType AdjustType, tsCodes []string) ([]StockInfo, error)

	// 指数列表
	ListIndices(ctx context.Context, req IndexListRequest) ([]IndexInfo, error)

	// 题材概念列表
	ListConcepts(ctx context.Context, req ConceptListRequest) ([]ConceptInfo, error)

	// 龙虎榜
	GetDragonTigerList(ctx context.Context, req DragonTigerRequest) ([]DragonTigerList, error)

	// 资金流向
	GetMoneyFlow(ctx context.Context, req MoneyFlowRequest) ([]MoneyFlow, error)
	// 同花顺概念板块资金流入（moneyflow_cnt_ths）
	GetMoneyFlowConcept(ctx context.Context, req MoneyFlowConceptRequest) ([]MoneyFlowConcept, error)

	// 人气榜
	GetPopularityRank(ctx context.Context, req PopularityRankRequest) ([]PopularityRank, error)

	// 新闻列表
	ListNews(ctx context.Context, req NewsListRequest) ([]NewsItem, error)

	// 涨停天梯统计（按连板天数）
	GetLimitUpLadder(ctx context.Context, tradeDate string) ([]LimitUpLadder, error)
	// 首板列表（当日涨停且不在 limit_step 中的股票）
	GetFirstLimitUpStocks(ctx context.Context, tradeDate string) ([]LimitStock, error)

	// 涨停今日/昨日对比
	GetLimitUpComparison(ctx context.Context, todayDate string) (*LimitUpComparison, error)

	// 涨停列表（带连板天数）
	GetLimitUpList(ctx context.Context, req LimitUpListRequest) ([]LimitUpStock, error)

	// 涨停板块统计
	GetLimitUpBySector(ctx context.Context, tradeDate string, sectorType string) ([]LimitUpBySector, error)

	// 分板块涨停股列表
	GetLimitUpStocksBySector(ctx context.Context, sectorCode, sectorType, tradeDate string) ([]LimitUpStock, error)

	// 题材轮动统计（多天）
	GetConceptRotationStats(ctx context.Context, req ConceptRotationRequest) (*ConceptRotationStats, error)

	// 股票基本信息
	GetStockBasicInfo(ctx context.Context, tsCode string) (*StockBasicInfo, error)

	// 财务指标查询
	GetFinancialIndicators(ctx context.Context, req FinancialIndicatorRequest) ([]FinancialIndicator, error)

	// 财报数据查询（按表名：income / balancesheet / cashflow）
	GetFinancialTableData(ctx context.Context, table string, req FinancialReportRequest) ([]map[string]any, error)

	// 自定义只读 SQL 查询（高权限，仅 SELECT，受 max_rows/timeout 限制）
	ExecuteReadOnlyQuery(ctx context.Context, req CustomQueryRequest) (*CustomQueryResult, error)

	// 交易日历：从 trade_cal 表取 is_open=1 的 cal_date，供前端过滤非交易日
	GetTradeCalendar(ctx context.Context, startDate, endDate string) ([]string, error)

	// 技术指标：基于 K 线数据在内存中计算 MA/RSI/MACD 等
	GetTechnicalIndicators(ctx context.Context, req TechnicalIndicatorCalcRequest) ([]TechnicalIndicator, error)

	// 当日实时分笔（ts_realtime_mkt_tick，最近 N 条倒序）
	GetRealtimeTicks(ctx context.Context, tsCode string, limit int) ([]TickRow, error)
	// 按日分时+盘口回放（ts_realtime_mkt_tick，按 trade_time 升序）
	GetIntradayTicks(ctx context.Context, tsCode, tradeDate string) ([]TickRow, error)
	// 分钟 K 线（rt_min，按日）
	GetIntradayKline(ctx context.Context, tsCode, tradeDate, period string) ([]IntradayKlineRow, error)
}

// CustomQueryRequest 自定义查询请求（只读 SQL）
type CustomQueryRequest struct {
	SQL            string // 仅允许 SELECT
	MaxRows        int    // 最大返回行数，如 10000
	TimeoutSeconds int    // 查询超时秒数，如 30
}

// KLineRequest K线查询请求
type KLineRequest struct {
	TsCode     string     // 证券代码（股票/指数/板块）
	StartDate  string     // 开始日期
	EndDate    string     // 结束日期
	AdjustType AdjustType // 复权类型
	Period     string     // 周期：D/W/M
}

// FactorRequest 因子计算请求
type FactorRequest struct {
	TsCodes   []string           // 证券代码列表
	StartDate string             // 开始日期
	EndDate   string             // 结束日期
	Factors   []FactorExpression // 因子表达式列表
}

// StockListRequest 股票列表查询请求
type StockListRequest struct {
	Market     *string // 市场类型：主板/创业板/科创板
	Industry   *string // 行业
	Area       *string // 地域
	IsHS       *string // 是否沪深港通
	ListStatus *string // 上市状态：L上市/D退市/P暂停
	Query      *string // 关键词：按名称、ts_code、symbol、cnspell 模糊查询
	SearchType *string // 可选：cnspell 表示仅按拼音缩写匹配 query
	Limit      int     // 返回数量限制
	Offset     int     // 偏移量
}

// IndexListRequest 指数列表查询请求
type IndexListRequest struct {
	Market    *string // 交易市场
	Publisher *string // 发布方
	IndexType *string // 指数类型
	Category  *string // 指数类别
	Limit     int     // 返回数量限制
	Offset    int     // 偏移量
}

// ConceptListRequest 题材概念列表查询请求
type ConceptListRequest struct {
	Source  *string // 来源
	Keyword *string // 关键词搜索
	Limit   int     // 返回数量限制
	Offset  int     // 偏移量
}

// DragonTigerRequest 龙虎榜查询请求
type DragonTigerRequest struct {
	TradeDate *string // 交易日期（不填则查询最新）
	TsCode    *string // 股票代码（可选）
	Limit     int     // 返回数量限制
	Offset    int     // 偏移量
}

// MoneyFlowRequest 资金流向查询请求
// TradeDate / StartDate+EndDate / TsCode 至少填一个，不可同时为空
type MoneyFlowRequest struct {
	TradeDate *string // 单日查询（可选，与 StartDate/EndDate、TsCode 三选一或组合）
	StartDate *string // 范围起始日 YYYYMMDD（与 EndDate 配合使用）
	EndDate   *string // 范围截止日 YYYYMMDD（与 StartDate 配合使用）
	TsCode    *string // 证券代码（可选）
	Market    *string // 市场类型
	Limit     int     // 返回数量限制
	Offset    int     // 偏移量
}

// PopularityRankRequest 人气榜查询请求
type PopularityRankRequest struct {
	RankType string // 排名类型：volume/amount/turnover
	Limit    int    // 返回前N名
}

// NewsListRequest 新闻列表查询请求（支持财联社式电报流：按时间倒序、来源过滤）
type NewsListRequest struct {
	TsCode    *string // 关联股票代码（可选）
	Category  *string // 分类
	Keyword   *string // 关键词搜索
	StartDate *string // 开始日期
	EndDate   *string // 结束日期
	Order     string  // 排序：time_desc（默认）/ time_asc
	Sources   *string // 来源过滤，逗号分隔，如 "cls,sina"
	Limit     int     // 返回数量限制
	Offset    int     // 偏移量
}

// LimitUpListRequest 涨停列表查询请求
type LimitUpListRequest struct {
	TradeDate          string  // 交易日期
	MinConsecutiveDays *int    // 最小连板天数（可选）
	MaxConsecutiveDays *int    // 最大连板天数（可选）
	Industry           *string // 行业筛选（可选）
	Concept            *string // 概念筛选（可选）
	SortBy             string  // 排序字段：time/consecutive_days/pct_chg
	Order              string  // 排序方向：asc/desc
	Limit              int     // 返回数量限制
	Offset             int     // 偏移量
}

// ConceptRotationRequest 题材轮动统计查询请求
type ConceptRotationRequest struct {
	StartDate     string  // 开始日期
	EndDate       string  // 结束日期
	RankBy        string  // 排名依据：pct_chg/limit_up_count/volume/net_inflow
	TopN          int     // 返回前N名（可选，默认返回全部）
	ConceptSource *string // 概念来源筛选（可选）
}

// FinancialIndicatorRequest 财务指标查询请求
type FinancialIndicatorRequest struct {
	TsCode     string   // 股票代码
	StartDate  *string  // 开始日期（可选）
	EndDate    *string  // 结束日期（可选）
	ReportType *string  // 报告类型（可选）
	Fields     []string // 需要返回的字段列表（可选，默认返回全部）
	Limit      int      // 返回数量限制
	Offset     int      // 偏移量
}

// FinancialReportRequest 财报数据查询请求
type FinancialReportRequest struct {
	TsCode     string   // 股票代码
	StartDate  *string  // 开始日期（可选）
	EndDate    *string  // 结束日期（可选）
	ReportType *string  // 报告类型（可选）
	CompType   *string  // 公司类型（可选）：1一般工商业/2银行/3保险/4证券
	Fields     []string // 需要返回的字段列表（可选，默认返回全部）
	Limit      int      // 返回数量限制
	Offset     int      // 偏移量
}

// TechnicalIndicatorCalcRequest 技术指标计算请求
type TechnicalIndicatorCalcRequest struct {
	TsCode     string     // 股票代码（ts_code）
	StartDate  string     // 开始日期 YYYYMMDD
	EndDate    string     // 结束日期 YYYYMMDD
	AdjustType AdjustType // 复权类型：none/qfq/hfq
	Period     string     // 周期：D/W/M（目前仅 D 完全支持，其它按原始 K 线粒度计算）
	Indicators []string   // 需要计算的指标名：如 MA5/MA10/MA20/RSI/MACD
}
