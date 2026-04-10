package realtimestore

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// QuoteFromMap 将当前弱类型行情 map 归一化为 TsStkBndFnd 对应的强类型 Quote。
func QuoteFromMap(preferredCode string, raw map[string]interface{}) (Quote, bool) {
	if raw == nil {
		return Quote{}, false
	}
	q := Quote{
		TsCode:    trimString(preferredCode),
		Code:      trimString(preferredCode),
		Name:      getString(raw, "name"),
		TradeTime: getString(raw, "trade_time"),

		PrePrice: getFloat(raw, "pre_price"),
		Price:    getFloat(raw, "price"),
		Open:     getFloat(raw, "open"),
		High:     getFloat(raw, "high"),
		Low:      getFloat(raw, "low"),
		Close:    getFloat(raw, "close"),

		OpenInt: getFloat(raw, "open_int"),
		Volume:  getFloat(raw, "volume"),
		Amount:  getFloat(raw, "amount"),
		Num:     getFloat(raw, "num"),

		AskPrice1:  getFloat(raw, "ask_price1"),
		AskVolume1: getFloat(raw, "ask_volume1"),
		BidPrice1:  getFloat(raw, "bid_price1"),
		BidVolume1: getFloat(raw, "bid_volume1"),
		AskPrice2:  getFloat(raw, "ask_price2"),
		AskVolume2: getFloat(raw, "ask_volume2"),
		BidPrice2:  getFloat(raw, "bid_price2"),
		BidVolume2: getFloat(raw, "bid_volume2"),
		AskPrice3:  getFloat(raw, "ask_price3"),
		AskVolume3: getFloat(raw, "ask_volume3"),
		BidPrice3:  getFloat(raw, "bid_price3"),
		BidVolume3: getFloat(raw, "bid_volume3"),
		AskPrice4:  getFloat(raw, "ask_price4"),
		AskVolume4: getFloat(raw, "ask_volume4"),
		BidPrice4:  getFloat(raw, "bid_price4"),
		BidVolume4: getFloat(raw, "bid_volume4"),
		AskPrice5:  getFloat(raw, "ask_price5"),
		AskVolume5: getFloat(raw, "ask_volume5"),
		BidPrice5:  getFloat(raw, "bid_price5"),
		BidVolume5: getFloat(raw, "bid_volume5"),
	}

	if code := getString(raw, "ts_code"); code != "" {
		q.TsCode = code
	}
	if code := getString(raw, "code"); code != "" {
		q.Code = code
	}
	q = q.normalizeIdentity()
	if q.normalizedCode() == "" {
		return Quote{}, false
	}
	return q, true
}

func getString(raw map[string]interface{}, key string) string {
	v, ok := raw[key]
	if !ok || v == nil {
		return ""
	}
	return trimString(fmt.Sprintf("%v", v))
}

func getFloat(raw map[string]interface{}, key string) float64 {
	v, ok := raw[key]
	if !ok || v == nil {
		return 0
	}
	f, _ := toFloat64(v)
	return f
}

func toFloat64(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		s := trimString(x)
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil
	default:
		s := trimString(fmt.Sprintf("%v", v))
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil
	}
}

func trimString(s string) string {
	return strings.TrimSpace(s)
}
