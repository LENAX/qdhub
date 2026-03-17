package normalize

import (
	"encoding/json"
	"fmt"
	"strings"
)

// TushareTickRecordFields matches ws_collector tushareTickRecordFields for QDHub compatibility.
var TushareTickRecordFields = []string{
	"code", "name", "trade_time", "pre_price", "price", "open", "high", "low", "close",
	"open_int", "volume", "amount", "num",
	"ask_price1", "ask_volume1", "bid_price1", "bid_volume1",
	"ask_price2", "ask_volume2", "bid_price2", "bid_volume2",
	"ask_price3", "ask_volume3", "bid_price3", "bid_volume3",
	"ask_price4", "ask_volume4", "bid_price4", "bid_volume4",
	"ask_price5", "ask_volume5", "bid_price5", "bid_volume5",
}

// TickRow is a normalized tick row (map) compatible with QDHub LatestQuoteStore.
type TickRow map[string]interface{}

// NormalizeTushareRecord converts Tushare WS record (array or map) to a single row with ts_code/code and QDHub field names.
// Tushare WS quirk: 'price' is pre_close, 'pre_price' is current price; we swap to match QDHub.
func NormalizeTushareRecord(raw interface{}, code string) TickRow {
	var out map[string]interface{}
	switch v := raw.(type) {
	case map[string]interface{}:
		out = v
		if code == "" {
			if c, _ := v["code"].(string); strings.TrimSpace(c) != "" {
				code = strings.TrimSpace(c)
			}
		}
		if code != "" {
			out["code"] = code
			if _, ok := out["ts_code"]; !ok || strings.TrimSpace(fmt.Sprintf("%v", out["ts_code"])) == "" {
				out["ts_code"] = code
			}
		}
		if c, _ := out["ts_code"].(string); strings.TrimSpace(c) != "" {
			if _, ok := out["code"]; !ok || strings.TrimSpace(fmt.Sprintf("%v", out["code"])) == "" {
				out["code"] = c
			}
		}
	case []interface{}:
		out = make(map[string]interface{}, len(TushareTickRecordFields))
		for i, field := range TushareTickRecordFields {
			if i < len(v) {
				out[field] = v[i]
			}
		}
		if out["code"] == nil && code != "" {
			out["code"] = code
		}
		if out["ts_code"] == nil {
			if out["code"] != nil {
				out["ts_code"] = out["code"]
			} else if code != "" {
				out["ts_code"] = code
			}
		}
		if out["code"] == nil && out["ts_code"] != nil {
			out["code"] = out["ts_code"]
		}
		if out["ts_code"] == nil && code != "" {
			out["ts_code"] = code
		}
	default:
		return nil
	}
	// Tushare WS: 'price' is pre_close, 'pre_price' is current price — swap to match QDHub
	if out != nil {
		out["price"], out["pre_price"] = out["pre_price"], out["price"]
	}
	return out
}

// ParseTushareMessage parses a Tushare WS JSON message and returns (code, record, error).
// Returns nil record if no data or status false.
func ParseTushareMessage(msg []byte) (code string, record interface{}, err error) {
	var resp struct {
		Status  bool        `json:"status"`
		Message string      `json:"message"`
		Data    interface{} `json:"data"`
	}
	if err := json.Unmarshal(msg, &resp); err != nil {
		return "", nil, err
	}
	if !resp.Status && resp.Message != "" {
		return "", nil, fmt.Errorf("upstream status=false: %s", resp.Message)
	}
	if !resp.Status {
		return "", nil, fmt.Errorf("upstream status=false")
	}
	m, ok := resp.Data.(map[string]interface{})
	if !ok || m == nil {
		return "", nil, nil
	}
	if c, _ := m["code"].(string); c != "" {
		code = c
	}
	record = m["record"]
	return code, record, nil
}
