// Package analysis contains the analysis domain entities and value objects.
package analysis

// TickRow 分笔/盘口单条（ts_realtime_mkt_tick 表一行，用于实时分笔与历史回放）
type TickRow struct {
	TradeTime  string  `json:"trade_time"`
	TsCode     string  `json:"ts_code"`
	Name       string  `json:"name,omitempty"`
	PrePrice   float64 `json:"pre_price"`
	Price      float64 `json:"price"`
	Open       float64 `json:"open"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Close      float64 `json:"close"`
	Volume     float64 `json:"volume"`
	Amount     float64 `json:"amount"`
	AskPrice1  float64 `json:"ask_price1"`
	AskVolume1 float64 `json:"ask_volume1"`
	BidPrice1  float64 `json:"bid_price1"`
	BidVolume1 float64 `json:"bid_volume1"`
	AskPrice2  float64 `json:"ask_price2"`
	AskVolume2 float64 `json:"ask_volume2"`
	BidPrice2  float64 `json:"bid_price2"`
	BidVolume2 float64 `json:"bid_volume2"`
	AskPrice3  float64 `json:"ask_price3"`
	AskVolume3 float64 `json:"ask_volume3"`
	BidPrice3  float64 `json:"bid_price3"`
	BidVolume3 float64 `json:"bid_volume3"`
	AskPrice4  float64 `json:"ask_price4"`
	AskVolume4 float64 `json:"ask_volume4"`
	BidPrice4  float64 `json:"bid_price4"`
	BidVolume4 float64 `json:"bid_volume4"`
	AskPrice5  float64 `json:"ask_price5"`
	AskVolume5 float64 `json:"ask_volume5"`
	BidPrice5  float64 `json:"bid_price5"`
	BidVolume5 float64 `json:"bid_volume5"`
}

// IntradayKlineRow 分钟 K 线单条（rt_min 表，用于当日分钟回放）
type IntradayKlineRow struct {
	Time   string  `json:"time"` // 时间 HH:MM 或 YYYY-MM-DD HH:MM:SS
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
	Amount float64 `json:"amount,omitempty"`
}

// KLineData K线数据（值对象）
type KLineData struct {
	TradeDate string  `json:"trade_date"`     // 交易日期 YYYYMMDD
	Name      string  `json:"name,omitempty"` // 股票名称
	Open      float64 `json:"open"`           // 开盘价
	High      float64 `json:"high"`           // 最高价
	Low       float64 `json:"low"`            // 最低价
	Close     float64 `json:"close"`          // 收盘价
	Volume    float64 `json:"volume"`         // 成交量
	Amount    float64 `json:"amount"`         // 成交额
	PreClose  float64 `json:"pre_close"`      // 昨收价
	Change    float64 `json:"change"`         // 涨跌额
	PctChg    float64 `json:"pct_chg"`        // 涨跌幅
}

// AdjustType 复权类型
type AdjustType string

const (
	AdjustNone AdjustType = "none" // 不复权
	AdjustQfq  AdjustType = "qfq"  // 前复权
	AdjustHfq  AdjustType = "hfq"  // 后复权
)

// LimitStats 涨跌停统计（值对象）
type LimitStats struct {
	TradeDate       string  `json:"trade_date"`
	LimitUpCount    int     `json:"limit_up_count"`    // 涨停家数
	LimitDownCount  int     `json:"limit_down_count"`  // 跌停家数
	LimitUpSealed   int     `json:"limit_up_sealed"`   // 涨停封板数（未打开）
	LimitUpOpened   int     `json:"limit_up_opened"`   // 涨停打开数（炸板）
	LimitDownSealed int     `json:"limit_down_sealed"` // 跌停封板数
	LimitDownOpened int     `json:"limit_down_opened"` // 跌停打开数
	UpCount         int     `json:"up_count"`          // 上涨家数
	DownCount       int     `json:"down_count"`        // 下跌家数
	FlatCount       int     `json:"flat_count"`        // 平盘家数
	LimitUpRatio    float64 `json:"limit_up_ratio"`    // 涨停比例
}

// LimitStock 涨停股票信息（值对象）
// 展示字段优先从 limit_list_ths（同花顺涨跌停榜单）获取，其次 limit_list_d
type LimitStock struct {
	TsCode          string   `json:"ts_code"`          // 股票代码
	Name            string   `json:"name"`             // 股票名称
	LimitTime       string   `json:"limit_time"`       // 首次封板时间（HH:MM:SS）
	LastLimitTime   string   `json:"last_limit_time"`  // 最后封板时间
	LimitReason     string   `json:"limit_reason"`     // 涨停原因（优先 limit_list_ths.lu_desc）
	LimitStatus     string   `json:"limit_status"`     // 涨停状态（N连板、一字板等）
	ConsecutiveDays int      `json:"consecutive_days"` // 连续涨停天数（连板数）
	FirstLimitDate  string   `json:"first_limit_date"` // 首次涨停日期
	Close           float64  `json:"close"`            // 收盘价
	PctChg          float64  `json:"pct_chg"`          // 涨跌幅
	TurnoverRate    float64  `json:"turnover_rate"`    // 换手率
	Amount          float64  `json:"amount"`           // 成交额
	FloatCap        float64  `json:"float_cap"`        // 流通市值（元）
	TotalCap        float64  `json:"total_cap"`        // 总市值（亿元）
	LimitAmount     float64  `json:"limit_amount"`     // 封单金额（元）
	OpenTimes       int      `json:"open_times"`       // 炸板次数
	Industry        string   `json:"industry"`         // 所属行业
	Concepts        []string `json:"concepts"`         // 关联概念列表
}

// LimitLadder 涨停天梯（值对象）
type LimitLadder struct {
	ConsecutiveDays int          `json:"consecutive_days"` // 连续涨停天数
	StockCount      int          `json:"stock_count"`      // 该连板数下的股票数量
	Stocks          []LimitStock `json:"stocks"`           // 股票列表（按涨停时间排序）
}

// LimitLadderStats 涨停天梯统计
type LimitLadderStats struct {
	TradeDate      string        `json:"trade_date"`      // 交易日期
	TotalLimitUp   int           `json:"total_limit_up"`  // 总涨停数
	Ladders        []LimitLadder `json:"ladders"`         // 天梯数据（按连板数降序）
	MaxConsecutive int           `json:"max_consecutive"` // 最高连板数
}

// LimitComparison 涨停对比（今日vs昨日）
type LimitComparison struct {
	TodayDate       string           `json:"today_date"`
	YesterdayDate   string           `json:"yesterday_date"`
	TodayCount      int              `json:"today_count"`      // 今日涨停数
	YesterdayCount  int              `json:"yesterday_count"`  // 昨日涨停数
	Change          int              `json:"change"`           // 变化数（正数表示增加）
	ChangeRatio     float64          `json:"change_ratio"`     // 变化比例
	TodayLadder     LimitLadderStats `json:"today_ladder"`     // 今日天梯
	YesterdayLadder LimitLadderStats `json:"yesterday_ladder"` // 昨日天梯
}

// SectorLimitStats 板块涨停统计（值对象）
type SectorLimitStats struct {
	SectorCode       string  `json:"sector_code"`        // 板块代码（行业或概念）
	SectorName       string  `json:"sector_name"`        // 板块名称
	SectorType       string  `json:"sector_type"`        // 板块类型：industry/concept
	LimitUpCount     int     `json:"limit_up_count"`     // 涨停家数
	TotalStocks      int     `json:"total_stocks"`       // 板块总股票数
	LimitUpRatio     float64 `json:"limit_up_ratio"`     // 涨停比例
	AvgPctChg        float64 `json:"avg_pct_chg"`        // 平均涨幅
	LeadingStock     string  `json:"leading_stock"`      // 龙头股代码
	LeadingStockName string  `json:"leading_stock_name"` // 龙头股名称
}

// SectorLimitStocks 板块涨停股列表
type SectorLimitStocks struct {
	SectorCode   string       `json:"sector_code"`
	SectorName   string       `json:"sector_name"`
	SectorType   string       `json:"sector_type"`
	LimitUpCount int          `json:"limit_up_count"`
	Stocks       []LimitStock `json:"stocks"` // 涨停股票列表
}

// ConceptHeat 题材热度（值对象）
type ConceptHeat struct {
	ConceptCode  string  `json:"concept_code"`
	ConceptName  string  `json:"concept_name"`
	StockCount   int     `json:"stock_count"`    // 成分股数量
	LimitUpCount int     `json:"limit_up_count"` // 涨停家数
	AvgPctChg    float64 `json:"avg_pct_chg"`    // 平均涨幅
	LeadingStock string  `json:"leading_stock"`  // 龙头股代码
}

// FactorValue 因子值（值对象）
type FactorValue struct {
	TsCode    string             `json:"ts_code"`
	TradeDate string             `json:"trade_date"`
	Values    map[string]float64 `json:"values"` // 因子名 -> 因子值
}

// FactorExpression 因子表达式
type FactorExpression struct {
	Name       string         `json:"name"`       // 因子名称
	Expression string         `json:"expression"` // 表达式，如 "MA(close, 5)"
	Params     map[string]any `json:"params,omitempty"`
}

// StockInfo 股票信息（值对象）
type StockInfo struct {
	TsCode   string   `json:"ts_code"`           // 股票代码
	Symbol   string   `json:"symbol"`            // 股票代码（6位）
	Name     string   `json:"name"`              // 股票名称
	Area     string   `json:"area"`              // 地域
	Industry string   `json:"industry"`          // 所属行业
	Market   string   `json:"market"`            // 市场类型（主板/创业板/科创板）
	ListDate string   `json:"list_date"`         // 上市日期
	IsHS     string   `json:"is_hs"`             // 是否沪深港通标的
	Price    *float64 `json:"price,omitempty"`   // 当前价
	PctChg   *float64 `json:"pct_chg,omitempty"` // 涨跌幅
	Change   *float64 `json:"change,omitempty"`  // 涨跌额
	Volume   *float64 `json:"volume,omitempty"`  // 成交量
	Amount   *float64 `json:"amount,omitempty"`  // 成交额
}

// StockBasicInfo 股票基本信息（详细）
type StockBasicInfo struct {
	TsCode       string  `json:"ts_code"`       // 股票代码
	Symbol       string  `json:"symbol"`        // 股票代码（6位）
	Name         string  `json:"name"`          // 股票名称
	Area         string  `json:"area"`          // 地域
	Industry     string  `json:"industry"`      // 所属行业
	Market       string  `json:"market"`        // 市场类型
	ListDate     string  `json:"list_date"`     // 上市日期
	ListStatus   string  `json:"list_status"`   // 上市状态：L上市/D退市/P暂停
	IsHS         string  `json:"is_hs"`         // 是否沪深港通标的
	Fullname     string  `json:"fullname"`      // 股票全称
	Enname       string  `json:"enname"`        // 英文全称
	Cnspell      string  `json:"cnspell"`       // 拼音缩写
	Exchange     string  `json:"exchange"`      // 交易所代码
	CurrType     string  `json:"curr_type"`     // 交易货币
	RegCapital   float64 `json:"reg_capital"`   // 注册资本（万元）
	Website      string  `json:"website"`       // 公司网站
	Email        string  `json:"email"`         // 公司邮箱
	Office       string  `json:"office"`        // 办公地址
	Employees    int     `json:"employees"`     // 员工人数
	Introduction string  `json:"introduction"`  // 公司简介
	Business     string  `json:"business"`      // 主营业务
	MainBusiness string  `json:"main_business"` // 主要产品及业务
}

// FinancialIndicator 财务指标（值对象）
type FinancialIndicator struct {
	TsCode               string   `json:"ts_code"`
	AnnDate              string   `json:"ann_date"`
	EndDate              string   `json:"end_date"`
	ReportType           string   `json:"report_type"`
	CompType             string   `json:"comp_type"`
	Roe                  *float64 `json:"roe,omitempty"`
	RoeAvg               *float64 `json:"roe_avg,omitempty"`
	RoeDiluted           *float64 `json:"roe_diluted,omitempty"`
	Roa                  *float64 `json:"roa,omitempty"`
	GrossProfitMargin    *float64 `json:"gross_profit_margin,omitempty"`
	NetProfitMargin      *float64 `json:"net_profit_margin,omitempty"`
	TotalAssetTurnover   *float64 `json:"total_asset_turnover,omitempty"`
	CurrentAssetTurnover *float64 `json:"current_asset_turnover,omitempty"`
	FixedAssetTurnover   *float64 `json:"fixed_asset_turnover,omitempty"`
	CurrentRatio         *float64 `json:"current_ratio,omitempty"`
	QuickRatio           *float64 `json:"quick_ratio,omitempty"`
	CashRatio            *float64 `json:"cash_ratio,omitempty"`
	DebtToAsset          *float64 `json:"debt_to_asset,omitempty"`
	EquityToAsset        *float64 `json:"equity_to_asset,omitempty"`
	RevenueYoy           *float64 `json:"revenue_yoy,omitempty"`
	ProfitYoy            *float64 `json:"profit_yoy,omitempty"`
	TotalAssetYoy        *float64 `json:"total_asset_yoy,omitempty"`
	EquityYoy            *float64 `json:"equity_yoy,omitempty"`
	Eps                  *float64 `json:"eps,omitempty"`
	Bps                  *float64 `json:"bps,omitempty"`
	Cps                  *float64 `json:"cps,omitempty"`
	Pe                   *float64 `json:"pe,omitempty"`
	Pb                   *float64 `json:"pb,omitempty"`
	Ps                   *float64 `json:"ps,omitempty"`
}

// FinancialReport 财报数据（值对象）
type FinancialReport struct {
	TsCode            string   `json:"ts_code"`
	AnnDate           string   `json:"ann_date"`
	FAnnDate          string   `json:"f_ann_date"`
	EndDate           string   `json:"end_date"`
	ReportType        string   `json:"report_type"`
	CompType          string   `json:"comp_type"`
	TotalAssets       *float64 `json:"total_assets,omitempty"`
	TotalLiab         *float64 `json:"total_liab,omitempty"`
	TotalEquity       *float64 `json:"total_equity,omitempty"`
	FixedAssets       *float64 `json:"fixed_assets,omitempty"`
	CurrentAssets     *float64 `json:"current_assets,omitempty"`
	CurrentLiab       *float64 `json:"current_liab,omitempty"`
	Revenue           *float64 `json:"revenue,omitempty"`
	OperatingProfit   *float64 `json:"operating_profit,omitempty"`
	TotalProfit       *float64 `json:"total_profit,omitempty"`
	NetProfit         *float64 `json:"net_profit,omitempty"`
	NetProfitAfter    *float64 `json:"net_profit_after,omitempty"`
	OperatingCashFlow *float64 `json:"operating_cash_flow,omitempty"`
	InvestingCashFlow *float64 `json:"investing_cash_flow,omitempty"`
	FinancingCashFlow *float64 `json:"financing_cash_flow,omitempty"`
	NetCashFlow       *float64 `json:"net_cash_flow,omitempty"`
}

// TechnicalIndicator 技术指标（值对象）
type TechnicalIndicator struct {
	Name   string    `json:"name"`   // 指标名称，如 MA5/RSI/MACD
	Values []float64 `json:"values"` // 与 K 线数据一一对应的数值序列
	Color  string    `json:"color"`  // 建议前端使用的颜色
}

// IndexInfo 指数信息（值对象）
type IndexInfo struct {
	TsCode    string   `json:"ts_code"`
	Name      string   `json:"name"`
	Market    string   `json:"market"`
	Publisher string   `json:"publisher"`
	IndexType string   `json:"index_type"`
	Category  string   `json:"category"`
	BaseDate  string   `json:"base_date"`
	BasePoint float64  `json:"base_point"`
	ListDate  string   `json:"list_date"`
	Close     *float64 `json:"close,omitempty"`
	PctChg    *float64 `json:"pct_chg,omitempty"`
}

// ConceptInfo 题材概念信息（值对象）
type ConceptInfo struct {
	Code         string   `json:"code"`
	Name         string   `json:"name"`
	Source       string   `json:"source"`
	StockCount   int      `json:"stock_count"`
	LimitUpCount *int     `json:"limit_up_count,omitempty"`
	AvgPctChg    *float64 `json:"avg_pct_chg,omitempty"`
}

// DragonTigerList 龙虎榜数据（值对象）
type DragonTigerList struct {
	TradeDate    string  `json:"trade_date"`
	TsCode       string  `json:"ts_code"`
	Name         string  `json:"name"`
	Close        float64 `json:"close"`
	PctChg       float64 `json:"pct_chg"`
	TurnoverRate float64 `json:"turnover_rate"`
	Amount       float64 `json:"amount"`
	Reason       string  `json:"reason"`
	BuyAmount    float64 `json:"buy_amount"`
	SellAmount   float64 `json:"sell_amount"`
	NetAmount    float64 `json:"net_amount"`
}

// MoneyFlow 资金流向（值对象）
type MoneyFlow struct {
	TradeDate     string  `json:"trade_date"`
	TsCode        string  `json:"ts_code"`
	Name          string  `json:"name"`
	BuySmAmount   float64 `json:"buy_sm_amount"`
	SellSmAmount  float64 `json:"sell_sm_amount"`
	BuyMdAmount   float64 `json:"buy_md_amount"`
	SellMdAmount  float64 `json:"sell_md_amount"`
	BuyLgAmount   float64 `json:"buy_lg_amount"`
	SellLgAmount  float64 `json:"sell_lg_amount"`
	BuyElgAmount  float64 `json:"buy_elg_amount"`
	SellElgAmount float64 `json:"sell_elg_amount"`
	NetMfAmount   float64 `json:"net_mf_amount"`
	NetMfRatio    float64 `json:"net_mf_ratio"`
}

// MoneyFlowConcept 同花顺概念板块资金流入（moneyflow_cnt_ths）
type MoneyFlowConcept struct {
	TradeDate      string  `json:"trade_date"`
	ConceptCode    string  `json:"concept_code"`
	ConceptName    string  `json:"concept_name"`
	NetInflow      float64 `json:"net_inflow"`
	NetInflowRatio float64 `json:"net_inflow_ratio,omitempty"`
}

// PopularityRank 人气榜（值对象）
type PopularityRank struct {
	Rank         int     `json:"rank"`
	TsCode       string  `json:"ts_code"`
	Name         string  `json:"name"`
	Score        float64 `json:"score"`
	Change       int     `json:"change"`
	PctChg       float64 `json:"pct_chg"`
	Volume       float64 `json:"volume"`
	TurnoverRate float64 `json:"turnover_rate"`
	UpdateTime   string  `json:"update_time"`
}

// NewsItem 新闻条目（值对象）
type NewsItem struct {
	ID           string   `json:"id"`
	Title        string   `json:"title"`
	Content      string   `json:"content"`
	Source       string   `json:"source"`
	Author       string   `json:"author"`
	PublishTime  string   `json:"publish_time"`
	RelateStocks []string `json:"relate_stocks"`
	Category     string   `json:"category"`
	Tags         []string `json:"tags"`
	URL          string   `json:"url"`
}

// LimitUpStock 涨停股票信息（值对象）
type LimitUpStock struct {
	TsCode          string   `json:"ts_code"`
	Name            string   `json:"name"`
	TradeDate       string   `json:"trade_date"`
	LimitTime       string   `json:"limit_time"`
	Reason          string   `json:"reason"`
	ConsecutiveDays int      `json:"consecutive_days"`
	Close           float64  `json:"close"`
	PctChg          float64  `json:"pct_chg"`
	Volume          float64  `json:"volume"`
	Amount          float64  `json:"amount"`
	TurnoverRate    float64  `json:"turnover_rate"`
	Industry        string   `json:"industry"`
	Concepts        []string `json:"concepts"`
}

// LimitUpLadder 涨停天梯（按连板天数分组）
type LimitUpLadder struct {
	ConsecutiveDays int            `json:"consecutive_days"`
	StockCount      int            `json:"stock_count"`
	Stocks          []LimitUpStock `json:"stocks"`
}

// LimitUpComparison 涨停今日/昨日对比
type LimitUpComparison struct {
	TodayDate       string          `json:"today_date"`
	YesterdayDate   string          `json:"yesterday_date"`
	TodayCount      int             `json:"today_count"`
	YesterdayCount  int             `json:"yesterday_count"`
	Change          int             `json:"change"`
	ChangeRatio     float64         `json:"change_ratio"`
	TodayLadder     []LimitUpLadder `json:"today_ladder"`
	YesterdayLadder []LimitUpLadder `json:"yesterday_ladder"`
}

// LimitUpBySector 分板块涨停统计
type LimitUpBySector struct {
	SectorCode      string         `json:"sector_code"`
	SectorName      string         `json:"sector_name"`
	SectorType      string         `json:"sector_type"`
	StockCount      int            `json:"stock_count"`    // 板块涨停家数
	LimitUpCount    int            `json:"limit_up_count"` // 与 StockCount 一致，供前端统一使用
	TotalStockCount int            `json:"total_stock_count"`
	LimitUpRatio    float64        `json:"limit_up_ratio"` // 保留字段，当前不再用于展示
	AvgPctChg       float64        `json:"avg_pct_chg"`
	Stocks          []LimitUpStock `json:"stocks"`

	// 来自 limit_cpt_list 的附加字段，供前端展示板块连板信息等
	Days     int    `json:"days,omitempty"`      // 上榜天数
	UpStat   string `json:"up_stat,omitempty"`   // 连板高度描述
	ConsNums int    `json:"cons_nums,omitempty"` // 连板家数
	UpNums   int    `json:"up_nums,omitempty"`   // 涨停家数（与 StockCount 一致）
	Rank     string `json:"rank,omitempty"`      // 板块热点排名
}

// ConceptRotationRank 题材轮动排名（单日）
type ConceptRotationRank struct {
	TradeDate    string  `json:"trade_date"`
	ConceptCode  string  `json:"concept_code"`
	ConceptName  string  `json:"concept_name"`
	Rank         int     `json:"rank"`
	AvgPctChg    float64 `json:"avg_pct_chg"`
	LimitUpCount int     `json:"limit_up_count"`
	TotalVolume  float64 `json:"total_volume"`
	NetInflow    float64 `json:"net_inflow"`
	StockCount   int     `json:"stock_count"`
	RankValue    float64 `json:"rank_value"`
}

// ConceptRotationStats 题材轮动统计（多天）
type ConceptRotationStats struct {
	StartDate  string             `json:"start_date"`
	EndDate    string             `json:"end_date"`
	RankBy     string             `json:"rank_by"`
	DailyRanks []ConceptDailyRank `json:"daily_ranks"`
}

// ConceptDailyRank 单日题材排名
type ConceptDailyRank struct {
	TradeDate string                `json:"trade_date"`
	Concepts  []ConceptRotationRank `json:"concepts"`
}

// MacroLiquidityFactor 宏观流动性因子（值对象）
type MacroLiquidityFactor struct {
	TradeDate  string   `json:"trade_date"`
	FactorType string   `json:"factor_type"` // money_supply/shibor/social_financing/rate/exchange_rate
	FactorCode string   `json:"factor_code"`
	FactorName string   `json:"factor_name"`
	Value      float64  `json:"value"`
	Unit       string   `json:"unit"`
	YoY        *float64 `json:"yoy,omitempty"`
	MoM        *float64 `json:"mom,omitempty"`
	Change     *float64 `json:"change,omitempty"`
	ChangePct  *float64 `json:"change_pct,omitempty"`
}

// MacroFactorSeries 宏观因子时间序列
type MacroFactorSeries struct {
	FactorType string                 `json:"factor_type"`
	FactorCode string                 `json:"factor_code"`
	FactorName string                 `json:"factor_name"`
	Unit       string                 `json:"unit"`
	StartDate  string                 `json:"start_date"`
	EndDate    string                 `json:"end_date"`
	Data       []MacroLiquidityFactor `json:"data"`
}

// MacroLiquidityDashboard 宏观流动性仪表盘数据
type MacroLiquidityDashboard struct {
	UpdateDate      string               `json:"update_date"`
	MoneySupply     *MoneySupplyData     `json:"money_supply,omitempty"`
	Shibor          *ShiborData          `json:"shibor,omitempty"`
	SocialFinancing *SocialFinancingData `json:"social_financing,omitempty"`
	InterestRate    *InterestRateData    `json:"interest_rate,omitempty"`
	ExchangeRate    *ExchangeRateData    `json:"exchange_rate,omitempty"`
}

// MoneySupplyData 货币供应量数据
type MoneySupplyData struct {
	M0        *float64 `json:"m0,omitempty"`
	M1        *float64 `json:"m1,omitempty"`
	M2        *float64 `json:"m2,omitempty"`
	M0YoY     *float64 `json:"m0_yoy,omitempty"`
	M1YoY     *float64 `json:"m1_yoy,omitempty"`
	M2YoY     *float64 `json:"m2_yoy,omitempty"`
	M1M2Ratio *float64 `json:"m1_m2_ratio,omitempty"`
	StatDate  string   `json:"stat_date"`
}

// ShiborData SHIBOR数据
type ShiborData struct {
	Overnight   *float64 `json:"overnight,omitempty"`
	OneWeek     *float64 `json:"one_week,omitempty"`
	TwoWeeks    *float64 `json:"two_weeks,omitempty"`
	OneMonth    *float64 `json:"one_month,omitempty"`
	ThreeMonths *float64 `json:"three_months,omitempty"`
	SixMonths   *float64 `json:"six_months,omitempty"`
	NineMonths  *float64 `json:"nine_months,omitempty"`
	OneYear     *float64 `json:"one_year,omitempty"`
	TradeDate   string   `json:"trade_date"`
}

// SocialFinancingData 社会融资规模数据
type SocialFinancingData struct {
	Total           *float64 `json:"total,omitempty"`
	RMBLoan         *float64 `json:"rmb_loan,omitempty"`
	ForeignLoan     *float64 `json:"foreign_loan,omitempty"`
	EntrustedLoan   *float64 `json:"entrusted_loan,omitempty"`
	TrustLoan       *float64 `json:"trust_loan,omitempty"`
	BankAcceptance  *float64 `json:"bank_acceptance,omitempty"`
	CorporateBond   *float64 `json:"corporate_bond,omitempty"`
	EquityFinancing *float64 `json:"equity_financing,omitempty"`
	YoY             *float64 `json:"yoy,omitempty"`
	StatDate        string   `json:"stat_date"`
}

// InterestRateData 利率数据
type InterestRateData struct {
	DepositRate1Y *float64 `json:"deposit_rate_1y,omitempty"`
	LoanRate1Y    *float64 `json:"loan_rate_1y,omitempty"`
	MLF           *float64 `json:"mlf,omitempty"`
	LPR1Y         *float64 `json:"lpr_1y,omitempty"`
	LPR5Y         *float64 `json:"lpr_5y,omitempty"`
	EffectiveDate string   `json:"effective_date"`
}

// ExchangeRateData 汇率数据
type ExchangeRateData struct {
	USDCNY       *float64 `json:"usd_cny,omitempty"`
	EURCNY       *float64 `json:"eur_cny,omitempty"`
	JPYCNY       *float64 `json:"jpy_cny,omitempty"`
	HKDCNY       *float64 `json:"hkd_cny,omitempty"`
	USDCNYChange *float64 `json:"usd_cny_change,omitempty"`
	TradeDate    string   `json:"trade_date"`
}

// CustomQueryResult 自定义查询结果
type CustomQueryResult struct {
	Columns  []string `json:"columns"`
	Rows     [][]any  `json:"rows"`
	RowCount int      `json:"row_count"`
}
