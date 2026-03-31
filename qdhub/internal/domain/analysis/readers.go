package analysis

import "context"

// RawDailyRow 原始日线行（含复权因子），供 K 线复权计算使用
type RawDailyRow struct {
	TradeDate string  // YYYYMMDD
	Name      string  // 股票名称（来自 stock_basic，可为空）
	Open      float64 // 开盘价
	High      float64 // 最高价
	Low       float64 // 最低价
	Close     float64 // 收盘价
	Vol       float64 // 成交量
	Amount    float64 // 成交额
	PreClose  float64 // 昨收价
	Change    float64 // 涨跌额
	PctChg    float64 // 涨跌幅
	AdjFactor float64 // 复权因子
}

// KLineReader 获取原始 K 线/复权所需数据，由 infrastructure 实现并注入
type KLineReader interface {
	GetDailyWithAdjFactor(ctx context.Context, tsCode, startDate, endDate string) ([]RawDailyRow, error)
}

// LimitListReader 涨停列表历史，用于连板天数等计算
type LimitListReader interface {
	GetLimitDatesBefore(ctx context.Context, tsCode, beforeDate string) ([]string, error)
}

// ConceptRotationQuery 题材轮动查询条件
type ConceptRotationQuery struct {
	StartDate     string  // 开始日期
	EndDate       string  // 结束日期
	RankBy        string  // 排名依据：pct_chg/limit_up_count/volume/net_inflow
	TopN          int     // 返回前 N 名，0 表示全部
	ConceptSource *string // 概念来源筛选
}

// ConceptRotationRow 题材轮动单行（单日单概念）
type ConceptRotationRow struct {
	TradeDate    string  // 交易日期
	ConceptCode  string  // 概念代码
	ConceptName  string  // 概念名称
	Rank         int     // 排名
	AvgPctChg    float64 // 平均涨跌幅
	LimitUpCount int     // 涨停家数
	TotalVolume  float64 // 总成交量
	NetInflow    float64 // 资金净流入
	StockCount   int     // 成分股数量
	RankValue    float64 // 排名依据值
}

// ConceptRotationReader 题材轮动统计结果
type ConceptRotationReader interface {
	GetRankedConcepts(ctx context.Context, q ConceptRotationQuery) ([]ConceptRotationRow, error)
}

// CustomQueryExecutor 自定义只读 SQL 执行器，由应用/基础设施层实现
type CustomQueryExecutor interface {
	ExecuteReadOnlyQuery(ctx context.Context, req CustomQueryRequest) (*CustomQueryResult, error)
}

// LimitStatsReader 涨跌停统计
type LimitStatsReader interface {
	GetLimitStats(ctx context.Context, startDate, endDate string) ([]LimitStats, error)
}

// LimitStockListReader 涨跌停个股列表（指定日期与类型：up/down）
type LimitStockListReader interface {
	GetByDateAndType(ctx context.Context, tradeDate, limitType string) ([]LimitStock, error)
}

// LimitLadderReader 涨停天梯统计
type LimitLadderReader interface {
	GetByDate(ctx context.Context, tradeDate string) (*LimitLadderStats, error)
}

// LimitComparisonReader 涨停今日/昨日对比
type LimitComparisonReader interface {
	GetComparison(ctx context.Context, todayDate string) (*LimitComparison, error)
}

// SectorLimitStatsReader 板块涨停统计
type SectorLimitStatsReader interface {
	GetByDate(ctx context.Context, tradeDate, sectorType string) ([]SectorLimitStats, error)
}

// SectorLimitStocksReader 分板块涨停股列表
type SectorLimitStocksReader interface {
	GetBySectorAndDate(ctx context.Context, sectorCode, sectorType, tradeDate string) (*SectorLimitStocks, error)
}

// ConceptHeatReader 题材热度
type ConceptHeatReader interface {
	GetConceptHeat(ctx context.Context, tradeDate string) ([]ConceptHeat, error)
}

// ConceptStocksReader 题材成分股
type ConceptStocksReader interface {
	GetConceptStocks(ctx context.Context, conceptCode, tradeDate string) ([]StockInfo, error)
}

// StockListReader 股票列表
type StockListReader interface {
	List(ctx context.Context, req StockListRequest) ([]StockInfo, error)
}

// StockSnapshotReader 股票快照：按交易日与 ts_code 列表返回价格、涨跌幅等
type StockSnapshotReader interface {
	GetSnapshot(ctx context.Context, tradeDate string, tsCodes []string) ([]StockInfo, error)
}

// IndexListReader 指数列表
type IndexListReader interface {
	List(ctx context.Context, req IndexListRequest) ([]IndexInfo, error)
}

// ConceptListReader 题材概念列表
type ConceptListReader interface {
	List(ctx context.Context, req ConceptListRequest) ([]ConceptInfo, error)
}

// DragonTigerReader 龙虎榜
type DragonTigerReader interface {
	GetList(ctx context.Context, req DragonTigerRequest) ([]DragonTigerList, error)
}

// MoneyFlowReader 资金流向
type MoneyFlowReader interface {
	GetMoneyFlow(ctx context.Context, req MoneyFlowRequest) ([]MoneyFlow, error)
}

// MoneyFlowConceptRequest 概念板块资金流查询
type MoneyFlowConceptRequest struct {
	TradeDate string  // 交易日期
	Concept   *string // 概念名称或代码筛选（可选）
	Limit     int
	Offset    int
}

// MoneyFlowConceptReader 同花顺概念板块资金流入（moneyflow_cnt_ths）
type MoneyFlowConceptReader interface {
	GetMoneyFlowConcept(ctx context.Context, req MoneyFlowConceptRequest) ([]MoneyFlowConcept, error)
}

// MoneyFlowRankReader 资金流入排名（个股 + 概念）
type MoneyFlowRankReader interface {
	GetMoneyFlowRank(ctx context.Context, req MoneyFlowRankRequest) (*MoneyFlowRankResult, error)
}

// IndexOHLCVReader 指数日线 OHLCV（index_daily）
type IndexOHLCVReader interface {
	GetIndexOHLCV(ctx context.Context, req IndexOHLCVRequest) (*IndexOHLCVResult, error)
}

// PopularityRankReader 人气榜
type PopularityRankReader interface {
	GetRank(ctx context.Context, req PopularityRankRequest) ([]PopularityRank, error)
}

// NewsReader 新闻列表
type NewsReader interface {
	List(ctx context.Context, req NewsListRequest) ([]NewsItem, error)
}

// TickReader 分时 tick 数据读取（用于实时 DuckDB fallback）
type TickReader interface {
	GetRealtimeTicks(ctx context.Context, tsCode string, limit int) ([]TickRow, error)
	GetIntradayTicks(ctx context.Context, tsCode, tradeDate string) ([]TickRow, error)
}

// LimitUpListReader 涨停列表（带连板天数，按日）
type LimitUpListReader interface {
	GetByDate(ctx context.Context, tradeDate string) ([]LimitUpStock, error)
	GetList(ctx context.Context, req LimitUpListRequest) ([]LimitUpStock, error)
}

// LimitUpLadderReader 涨停天梯（按连板分组）
type LimitUpLadderReader interface {
	GetByDate(ctx context.Context, tradeDate string) ([]LimitUpLadder, error)
}

// FirstLimitUpReader 首板列表（当日涨停且不在 limit_step 中的股票）
type FirstLimitUpReader interface {
	GetByDate(ctx context.Context, tradeDate string) ([]LimitStock, error)
}

// LimitUpComparisonReader 涨停今日/昨日对比
type LimitUpComparisonReader interface {
	GetComparison(ctx context.Context, todayDate string) (*LimitUpComparison, error)
}

// LimitUpBySectorReader 板块涨停统计
type LimitUpBySectorReader interface {
	GetByDate(ctx context.Context, tradeDate, sectorType string) ([]LimitUpBySector, error)
}

// LimitUpStocksBySectorReader 分板块涨停股列表
type LimitUpStocksBySectorReader interface {
	GetStocks(ctx context.Context, sectorCode, sectorType, tradeDate string) ([]LimitUpStock, error)
}

// StockBasicReader 股票基本信息
type StockBasicReader interface {
	GetByTsCode(ctx context.Context, tsCode string) (*StockBasicInfo, error)
}

// FinancialIndicatorReader 财务指标
type FinancialIndicatorReader interface {
	GetIndicators(ctx context.Context, req FinancialIndicatorRequest) ([]FinancialIndicator, error)
}

// FinancialReportReader 财报数据（income / balancesheet / cashflow 三张独立表）
type FinancialReportReader interface {
	GetTableData(ctx context.Context, table string, req FinancialReportRequest) ([]map[string]any, error)
}

// TradeCalendarReader 交易日历（来自 trade_cal 表，cal_date + is_open）
type TradeCalendarReader interface {
	GetTradingDates(ctx context.Context, startDate, endDate string) ([]string, error)
}

// RealtimeTickReader 当日实时分笔（ts_realtime_mkt_tick，按 trade_time 倒序）
type RealtimeTickReader interface {
	GetRealtimeTicks(ctx context.Context, tsCode string, limit int) ([]TickRow, error)
}

// IntradayTickReader 按日分时+盘口回放（ts_realtime_mkt_tick，按 trade_time 升序）
type IntradayTickReader interface {
	GetIntradayTicks(ctx context.Context, tsCode, tradeDate string) ([]TickRow, error)
}

// IntradayKlineReader 分钟 K 线（rt_min，按日）
type IntradayKlineReader interface {
	GetIntradayKline(ctx context.Context, tsCode, tradeDate, period string) ([]IntradayKlineRow, error)
}
