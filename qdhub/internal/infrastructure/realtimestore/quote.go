package realtimestore

// Quote 表示 TsStkBndFnd 对应的强类型股票 tick 快照。
// 该结构仅服务 HQ_STK_TICK / ts_realtime_mkt_tick 主路径。
type Quote struct {
	TsCode    string `json:"ts_code"`
	Code      string `json:"code"`
	Name      string `json:"name,omitempty"`
	TradeTime string `json:"trade_time,omitempty"`

	PrePrice float64 `json:"pre_price,omitempty"`
	Price    float64 `json:"price,omitempty"`
	Open     float64 `json:"open,omitempty"`
	High     float64 `json:"high,omitempty"`
	Low      float64 `json:"low,omitempty"`
	Close    float64 `json:"close,omitempty"`

	OpenInt float64 `json:"open_int,omitempty"`
	Volume  float64 `json:"volume,omitempty"`
	Amount  float64 `json:"amount,omitempty"`
	Num     float64 `json:"num,omitempty"`

	AskPrice1  float64 `json:"ask_price1,omitempty"`
	AskVolume1 float64 `json:"ask_volume1,omitempty"`
	BidPrice1  float64 `json:"bid_price1,omitempty"`
	BidVolume1 float64 `json:"bid_volume1,omitempty"`

	AskPrice2  float64 `json:"ask_price2,omitempty"`
	AskVolume2 float64 `json:"ask_volume2,omitempty"`
	BidPrice2  float64 `json:"bid_price2,omitempty"`
	BidVolume2 float64 `json:"bid_volume2,omitempty"`

	AskPrice3  float64 `json:"ask_price3,omitempty"`
	AskVolume3 float64 `json:"ask_volume3,omitempty"`
	BidPrice3  float64 `json:"bid_price3,omitempty"`
	BidVolume3 float64 `json:"bid_volume3,omitempty"`

	AskPrice4  float64 `json:"ask_price4,omitempty"`
	AskVolume4 float64 `json:"ask_volume4,omitempty"`
	BidPrice4  float64 `json:"bid_price4,omitempty"`
	BidVolume4 float64 `json:"bid_volume4,omitempty"`

	AskPrice5  float64 `json:"ask_price5,omitempty"`
	AskVolume5 float64 `json:"ask_volume5,omitempty"`
	BidPrice5  float64 `json:"bid_price5,omitempty"`
	BidVolume5 float64 `json:"bid_volume5,omitempty"`
}

func (q Quote) normalizedCode() string {
	switch {
	case q.TsCode != "":
		return q.TsCode
	case q.Code != "":
		return q.Code
	default:
		return ""
	}
}

func (q Quote) normalizeIdentity() Quote {
	if q.TsCode == "" {
		q.TsCode = q.Code
	}
	if q.Code == "" {
		q.Code = q.TsCode
	}
	return q
}

func (q Quote) ToMap() map[string]interface{} {
	q = q.normalizeIdentity()
	out := map[string]interface{}{
		"ts_code": q.TsCode,
		"code":    q.Code,
	}
	addStringField(out, "name", q.Name)
	addStringField(out, "trade_time", q.TradeTime)

	addFloatField(out, "pre_price", q.PrePrice)
	addFloatField(out, "price", q.Price)
	addFloatField(out, "open", q.Open)
	addFloatField(out, "high", q.High)
	addFloatField(out, "low", q.Low)
	addFloatField(out, "close", q.Close)
	addFloatField(out, "open_int", q.OpenInt)
	addFloatField(out, "volume", q.Volume)
	addFloatField(out, "amount", q.Amount)
	addFloatField(out, "num", q.Num)

	addFloatField(out, "ask_price1", q.AskPrice1)
	addFloatField(out, "ask_volume1", q.AskVolume1)
	addFloatField(out, "bid_price1", q.BidPrice1)
	addFloatField(out, "bid_volume1", q.BidVolume1)
	addFloatField(out, "ask_price2", q.AskPrice2)
	addFloatField(out, "ask_volume2", q.AskVolume2)
	addFloatField(out, "bid_price2", q.BidPrice2)
	addFloatField(out, "bid_volume2", q.BidVolume2)
	addFloatField(out, "ask_price3", q.AskPrice3)
	addFloatField(out, "ask_volume3", q.AskVolume3)
	addFloatField(out, "bid_price3", q.BidPrice3)
	addFloatField(out, "bid_volume3", q.BidVolume3)
	addFloatField(out, "ask_price4", q.AskPrice4)
	addFloatField(out, "ask_volume4", q.AskVolume4)
	addFloatField(out, "bid_price4", q.BidPrice4)
	addFloatField(out, "bid_volume4", q.BidVolume4)
	addFloatField(out, "ask_price5", q.AskPrice5)
	addFloatField(out, "ask_volume5", q.AskVolume5)
	addFloatField(out, "bid_price5", q.BidPrice5)
	addFloatField(out, "bid_volume5", q.BidVolume5)

	return out
}

func addStringField(dst map[string]interface{}, key, value string) {
	if value != "" {
		dst[key] = value
	}
}

func addFloatField(dst map[string]interface{}, key string, value float64) {
	if value != 0 {
		dst[key] = value
	}
}
