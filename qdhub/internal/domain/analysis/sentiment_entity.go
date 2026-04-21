package analysis

// PopularityRankSrc 人气榜数据源
type PopularityRankSrc string

const (
	PopularityRankSrcTHS       PopularityRankSrc = "ths"       // 同花顺
	PopularityRankSrcEastmoney PopularityRankSrc = "eastmoney" // 东方财富
	PopularityRankSrcKPL       PopularityRankSrc = "kpl"       // 开盘啦
)

// ValidPopularityRankSrc 校验人气榜数据源是否合法
func ValidPopularityRankSrc(s string) bool {
	switch PopularityRankSrc(s) {
	case PopularityRankSrcTHS, PopularityRankSrcEastmoney, PopularityRankSrcKPL:
		return true
	}
	return false
}

// SentimentStyle 情绪风格权重口径
type SentimentStyle string

const (
	SentimentStyleRelay    SentimentStyle = "relay"    // 接力优先（连板行情主导）
	SentimentStyleBalanced SentimentStyle = "balanced" // 均衡（默认）
	SentimentStyleTrend    SentimentStyle = "trend"    // 趋势优先（宽指行情主导）
)

// SentimentLevel 情绪操作等级
type SentimentLevel string

const (
	SentimentLevelStrong  SentimentLevel = "strong"  // 强势：可适当加仓，聚焦龙头
	SentimentLevelNeutral SentimentLevel = "neutral" // 中性：轻仓试错，快进快出
	SentimentLevelWeak    SentimentLevel = "weak"    // 低迷：空仓观望
)

// TemperatureLevel 市场温度
type TemperatureLevel string

const (
	TemperatureLevelBoiling  TemperatureLevel = "boiling"  // 沸点：极度亢奋
	TemperatureLevelNormal   TemperatureLevel = "normal"   // 正常
	TemperatureLevelFreezing TemperatureLevel = "freezing" // 冰点：极度低迷
)

// ---- Request types ----

// MarketSentimentRequest 当日市场情绪查询请求
type MarketSentimentRequest struct {
	TradeDate string         // YYYYMMDD，空则取最新可用交易日
	Style     SentimentStyle // relay|balanced|trend，默认 balanced
	Window    int            // 历史分位数计算窗口（交易日），默认 120
	HotSrc    string         // 热股数据源：ths|eastmoney|kpl，默认 ths
}

// SentimentHistoryRequest 情绪历史时序查询请求
type SentimentHistoryRequest struct {
	StartDate string         // YYYYMMDD
	EndDate   string         // YYYYMMDD
	Style     SentimentStyle // 默认 balanced
	Window    int            // 分位数窗口，默认 120
	HotSrc    string         // 默认 ths
}

// SentimentExtremesRequest 冰点/沸点统计请求
type SentimentExtremesRequest struct {
	EndDate           string         // YYYYMMDD，空则最新
	Style             SentimentStyle // 默认 balanced
	Window            int            // 默认 120
	FreezingThreshold float64        // 冰点分位阈值，默认 0.15
	BoilingThreshold  float64        // 沸点分位阈值，默认 0.85
	HotSrc            string         // 默认 ths
	ReversalHorizon   int            // 反转判断天数，默认 5
}

// SectorLeaderRequest 领涨/领跌板块统计请求
type SectorLeaderRequest struct {
	StartDate       string // YYYYMMDD，空则取近 5 交易日
	EndDate         string // YYYYMMDD，空则最新
	ConceptIndexSrc string // ths|eastmoney，默认 ths
	Limit           int    // 每个方向前 N 名，默认 20
}

// ---- Raw data types (reader → service) ----

// LayerPromotion 连板晋级率（单层）
type LayerPromotion struct {
	FromLayer int     `json:"from_layer"` // 起始连板数 n（昨日）
	ToLayer   int     `json:"to_layer"`   // 目标连板数 n+1（今日）
	Label     string  `json:"label"`      // 显示标签，如 "2→3"
	BaseCount int     `json:"base_count"` // 昨日 n 板数量
	Promoted  int     `json:"promoted"`   // 今日 n+1 板数量
	Rate      float64 `json:"rate"`       // Promoted / BaseCount（BaseCount=0时为0）
}

// LayerPremium 分层昨日涨停溢价（今日涨幅表现）
type LayerPremium struct {
	Layer     int     `json:"layer"`      // 连板层数
	Label     string  `json:"label"`      // 如 "2板"
	Count     int     `json:"count"`      // 昨日该层涨停家数
	AvgPct    float64 `json:"avg_pct"`    // 今日平均涨幅（%）
	MedianPct float64 `json:"median_pct"` // 今日中位涨幅（%）
}

// HighBoardWatch 7板及以上异动监管统计
type HighBoardWatch struct {
	Count       int     `json:"count"`        // 今日7板+数量
	PromotedTo7 int     `json:"promoted_to_7"` // 今日新晋7板数
	AvgPremium  float64 `json:"avg_premium"`  // 7板+昨日涨停平均溢价（%）
	MaxBoard    int     `json:"max_board"`    // 今日最高连板数
}

// RelayRawData 接力层原始数据（由 reader 提供）
type RelayRawData struct {
	TradeDate               string
	LimitUpCount            int
	LimitDownCount          int
	MaxBoard                int
	LadderComplete          float64          // [0,1] 天梯完整度
	BurstRate               float64          // [0,1] 炸板率（开板次数>0 / 涨停总数）
	LayerPromotions         []LayerPromotion // 各层晋级率（按层升序）
	YesterdayPremiumAvg     float64          // 综合昨日涨停溢价今日表现均值（%）
	YesterdayPremiumByLayer []LayerPremium   // 分层溢价
	HighBoardWatch          HighBoardWatch
}

// TrendRawData 趋势层原始数据
type TrendRawData struct {
	TradeDate         string
	UpCount           int
	DownCount         int
	TotalCount        int
	BullMACount       int      // MA5>MA10>MA20>MA60 的股票数
	BullMATotal       int      // 有足够历史数据的股票总数
	NewHighCount      int      // 20日新高股票数
	NewHighTotal      int      // 有足够历史数据的股票总数（20日）
	TodayHotCodes     []string // 今日热股代码列表（top 100）
	YesterdayHotCodes []string // 昨日热股代码列表
}

// MatrixPoint 单只个股的四象限数据点
type MatrixPoint struct {
	OpenGap   float64 // 隔夜溢价：(open - pre_close) / pre_close * 100
	DayReturn float64 // 日内收益：(close - open) / open * 100
}

// MatrixRawData 四象限层原始数据
type MatrixRawData struct {
	TradeDate  string
	FullMarket []MatrixPoint // 全市场
	HotTop100  []MatrixPoint // 热股前100
	HotSrc     string
}

// ---- Breakdown/output types ----

// RelayBreakdown 接力层详细指标（输出）
type RelayBreakdown struct {
	LimitUpCount            int              `json:"limit_up_count"`
	LimitDownCount          int              `json:"limit_down_count"`
	MaxBoard                int              `json:"max_board"`
	LadderComplete          float64          `json:"ladder_complete"`
	BurstRate               float64          `json:"burst_rate"`
	LayerPromotions         []LayerPromotion `json:"layer_promotions"`
	YesterdayPremiumAvg     float64          `json:"yesterday_premium_avg"`
	YesterdayPremiumByLayer []LayerPremium   `json:"yesterday_premium_by_layer"`
	HighBoardWatch          HighBoardWatch   `json:"high_board_watch"`
}

// TrendPersistence 热股趋势持续性
type TrendPersistence struct {
	SurvivorCount int     `json:"survivor_count"`  // 与昨日重叠数
	SurvivorRate  float64 `json:"survivor_rate"`   // 重叠率
	NewEntryCount int     `json:"new_entry_count"` // 新进
	ExitCount     int     `json:"exit_count"`      // 退出
	Direction     string  `json:"direction"`       // expanding|stable|contracting
}

// TrendBreakdown 趋势层详细指标（输出）
type TrendBreakdown struct {
	UpCount      int              `json:"up_count"`
	DownCount    int              `json:"down_count"`
	TotalCount   int              `json:"total_count"`
	RedBoardRate float64          `json:"red_board_rate"` // 上涨比例
	BullMACount  int              `json:"bull_ma_count"`
	BullMARate   float64          `json:"bull_ma_rate"`  // 多头排列比例
	NewHighCount int              `json:"new_high_count"`
	NewHighRate  float64          `json:"new_high_rate"` // 20日新高比例
	Persistence  TrendPersistence `json:"persistence"`
}

// MatrixQuadrant 四象限单格（输出）
type MatrixQuadrant struct {
	Label      string  `json:"label"`        // 如"高开高走"
	OpenSign   string  `json:"open_sign"`    // pos|neg
	DaySign    string  `json:"day_sign"`     // pos|neg
	Count      int     `json:"count"`
	Ratio      float64 `json:"ratio"`
	AvgOpenPct float64 `json:"avg_open_pct"` // 平均隔夜溢价（%）
	AvgDayPct  float64 `json:"avg_day_pct"`  // 平均日内收益（%）
}

// MatrixStats 四象限统计（输出）
type MatrixStats struct {
	TotalCount  int              `json:"total_count"`
	Quadrants   []MatrixQuadrant `json:"quadrants"`
	PosOpenAvg  float64          `json:"pos_open_avg"` // 正隔夜溢价平均值（%）
	NegOpenAvg  float64          `json:"neg_open_avg"` // 负隔夜溢价平均值（%）
	PosDayAvg   float64          `json:"pos_day_avg"`  // 正日内收益平均值（%）
	NegDayAvg   float64          `json:"neg_day_avg"`  // 负日内收益平均值（%）
}

// MatrixBreakdown 四象限层详细（输出）
type MatrixBreakdown struct {
	FullMarket MatrixStats `json:"full_market"`
	HotTop100  MatrixStats `json:"hot_top100"`
	HotSrc     string      `json:"hot_src"`
}

// DailySentimentResult 单日完整情绪结果
type DailySentimentResult struct {
	TradeDate             string           `json:"trade_date"`
	Style                 string           `json:"style"`
	RelayScoreRaw         float64          `json:"relay_score_raw"`         // [0,1] 接力层归一化原始分
	TrendScoreRaw         float64          `json:"trend_score_raw"`         // [0,1] 趋势层归一化原始分
	MatrixScoreRaw        float64          `json:"matrix_score_raw"`        // [0,1] 矩阵层归一化原始分
	RelayScore            float64          `json:"relay_score"`             // [0,100] 接力层分位分
	TrendScore            float64          `json:"trend_score"`             // [0,100] 趋势层分位分
	MatrixScore           float64          `json:"matrix_score"`            // [0,100] 矩阵层分位分
	CompositeScore        float64          `json:"composite_score"`         // [0,100] 加权综合分
	SentimentLevel        SentimentLevel   `json:"sentiment_level"`
	TemperatureLevel      TemperatureLevel `json:"temperature_level"`
	TemperaturePercentile float64          `json:"temperature_percentile"` // [0,1] 综合分在窗口内的分位数
	RelayBreakdown        RelayBreakdown   `json:"relay_breakdown"`
	TrendBreakdown        TrendBreakdown   `json:"trend_breakdown"`
	MatrixBreakdown       MatrixBreakdown  `json:"matrix_breakdown"`
}

// SentimentHistoryPoint 历史情绪序列单点
type SentimentHistoryPoint struct {
	TradeDate             string           `json:"trade_date"`
	RelayScore            float64          `json:"relay_score"`
	TrendScore            float64          `json:"trend_score"`
	MatrixScore           float64          `json:"matrix_score"`
	CompositeScore        float64          `json:"composite_score"`
	SentimentLevel        SentimentLevel   `json:"sentiment_level"`
	TemperatureLevel      TemperatureLevel `json:"temperature_level"`
	TemperaturePercentile float64          `json:"temperature_percentile"`
}

// SentimentHistoryResult 历史情绪结果
type SentimentHistoryResult struct {
	Style     string                  `json:"style"`
	Window    int                     `json:"window"`
	StartDate string                  `json:"start_date"`
	EndDate   string                  `json:"end_date"`
	Points    []SentimentHistoryPoint `json:"points"`
}

// ReversalProbability 极值后反转概率
type ReversalProbability struct {
	Horizon1 float64 `json:"horizon_1"` // 1日内反转概率
	Horizon3 float64 `json:"horizon_3"` // 3日内反转概率
	Horizon5 float64 `json:"horizon_5"` // 5日内反转概率
	SampleN  int     `json:"sample_n"`  // 样本量
}

// ExtremeSummary 极值（冰点/沸点）摘要
type ExtremeSummary struct {
	Count20d    int                 `json:"count_20d"`   // 近20日出现次数
	Count60d    int                 `json:"count_60d"`   // 近60日出现次数
	RecentDate  string              `json:"recent_date"` // 最近出现日期（空则无）
	ReversalProb ReversalProbability `json:"reversal_prob"`
}

// SentimentExtremesResult 冰点/沸点统计结果
type SentimentExtremesResult struct {
	Style             string         `json:"style"`
	Window            int            `json:"window"`
	EndDate           string         `json:"end_date"`
	FreezingThreshold float64        `json:"freezing_threshold"` // 如 0.15
	BoilingThreshold  float64        `json:"boiling_threshold"`  // 如 0.85
	Freezing          ExtremeSummary `json:"freezing"`
	Boiling           ExtremeSummary `json:"boiling"`
}

// ---- Sector leader types ----

// SectorLeaderStock 板块内龙头/弱势股
type SectorLeaderStock struct {
	TsCode          string  `json:"ts_code"`
	Name            string  `json:"name"`
	Price           float64 `json:"price"`            // 区间末日收盘价
	ReturnPct       float64 `json:"return_pct"`       // 区间累计涨幅（%）
	RepeatRankCount int     `json:"repeat_rank_count"` // 区间内进板块前5的次数
}

// SectorLeaderItem 领涨/领跌板块条目
type SectorLeaderItem struct {
	Code         string              `json:"code"`
	Name         string              `json:"name"`
	ReturnPct    float64             `json:"return_pct"`    // 区间累计涨幅（%）
	AvgDailyPct  float64             `json:"avg_daily_pct"` // 日均涨幅（%）
	Top5Count    int                 `json:"top5_count"`    // 区间内日涨幅进全市场前5次数
	LeaderStocks []SectorLeaderStock `json:"leader_stocks"` // 前5龙头/弱势股
}

// SurvivorPoint 热股榜持续性单日统计
type SurvivorPoint struct {
	TradeDate     string `json:"trade_date"`
	SurvivorCount int    `json:"survivor_count"`  // 持续上榜数
	NewEntryCount int    `json:"new_entry_count"` // 新进数
	ExitCount     int    `json:"exit_count"`      // 退出数
}

// SectorLeaderResult 领涨/领跌板块统计结果
type SectorLeaderResult struct {
	StartDate       string             `json:"start_date"`
	EndDate         string             `json:"end_date"`
	ConceptIndexSrc string             `json:"concept_index_src"`
	Leaders         []SectorLeaderItem `json:"leaders"`
	Laggards        []SectorLeaderItem `json:"laggards"`
	SurvivorHistory []SurvivorPoint    `json:"survivor_history"`
}

// sentimentWeights 三层情绪加权系数
type sentimentWeights struct {
	Relay  float64
	Trend  float64
	Matrix float64
}

// resolveWeights 根据风格返回加权系数（三者之和为1）
func resolveWeights(style SentimentStyle) sentimentWeights {
	switch style {
	case SentimentStyleRelay:
		return sentimentWeights{Relay: 0.55, Trend: 0.20, Matrix: 0.25}
	case SentimentStyleTrend:
		return sentimentWeights{Relay: 0.25, Trend: 0.50, Matrix: 0.25}
	default: // balanced
		return sentimentWeights{Relay: 0.40, Trend: 0.35, Matrix: 0.25}
	}
}
