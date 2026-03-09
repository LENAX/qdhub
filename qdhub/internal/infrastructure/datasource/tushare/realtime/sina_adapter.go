package realtime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

const (
	sinaSource              = "sina"
	sinaQuoteURL            = "http://hq.sinajs.cn/list=%s"
	sinaQuoteMaxSymbols     = 50
	sinaTickURL             = "https://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradedetail.php?symbol=%s"
	sinaListURL             = "https://hq.sinajs.cn/list=nf_sh000001" // 可选：全市场列表接口，此处用指数代表
	sinaHTTPTimeout         = 15 * time.Second
	sinaQuoteResponsePrefix = "var hq_str_"
)

// sinaQuoteCols 新浪实时行情返回字段顺序（与 Tushare realtime_quote 对齐）
var sinaQuoteCols = []string{
	"name", "open", "pre_close", "price", "high", "low", "bid", "ask", "volume", "amount",
	"b1_v", "b1_p", "b2_v", "b2_p", "b3_v", "b3_p", "b4_v", "b4_p", "b5_v", "b5_p",
	"a1_v", "a1_p", "a2_v", "a2_p", "a3_v", "a3_p", "a4_v", "a4_p", "a5_v", "a5_p",
	"date", "time",
}

// SinaRealtimeAdapter 新浪财经实时行情 Adapter（仅 Pull）
type SinaRealtimeAdapter struct {
	client *http.Client
}

// NewSinaRealtimeAdapter 创建新浪实时 Adapter
func NewSinaRealtimeAdapter() *SinaRealtimeAdapter {
	return &SinaRealtimeAdapter{
		client: &http.Client{Timeout: sinaHTTPTimeout},
	}
}

// Source 实现 RealtimeAdapter
func (a *SinaRealtimeAdapter) Source() string { return sinaSource }

// SupportedAPIs 实现 RealtimeAdapter
func (a *SinaRealtimeAdapter) SupportedAPIs() []string {
	return []string{"realtime_quote", "realtime_tick", "realtime_list"}
}

// Supports 实现 RealtimeAdapter
func (a *SinaRealtimeAdapter) Supports(apiName string) bool {
	for _, name := range a.SupportedAPIs() {
		if name == apiName {
			return true
		}
	}
	return false
}

// SupportedModes 实现 RealtimeAdapter（仅 pull）
func (a *SinaRealtimeAdapter) SupportedModes(apiName string) []RealtimeMode {
	if a.Supports(apiName) {
		return []RealtimeMode{RealtimeModePull}
	}
	return nil
}

// SupportsPush 实现 RealtimeAdapter
func (a *SinaRealtimeAdapter) SupportsPush(apiName string) bool { return false }

// StartStream 实现 RealtimeAdapter（未实现 Push）
func (a *SinaRealtimeAdapter) StartStream(ctx context.Context, apiName string, params map[string]interface{}, onBatch func([]map[string]interface{}) error) error {
	return fmt.Errorf("sina adapter does not support push mode")
}

// Fetch 实现 RealtimeAdapter
func (a *SinaRealtimeAdapter) Fetch(ctx context.Context, apiName string, params map[string]interface{}) ([]map[string]interface{}, error) {
	switch apiName {
	case "realtime_quote":
		return a.fetchRealtimeQuote(ctx, params)
	case "realtime_tick":
		return a.fetchRealtimeTick(ctx, params)
	case "realtime_list":
		return a.fetchRealtimeList(ctx, params)
	default:
		return nil, fmt.Errorf("sina adapter: unsupported api %s", apiName)
	}
}

// tsCodeToSinaSymbol 将 Tushare ts_code 转为新浪代码：000001.SZ -> sz000001, 600000.SH -> sh600000
func tsCodeToSinaSymbol(tsCode string) string {
	tsCode = strings.TrimSpace(tsCode)
	if idx := strings.Index(tsCode, "."); idx > 0 {
		exchange := strings.ToLower(tsCode[idx+1:])
		code := tsCode[:idx]
		if exchange == "sh" {
			return "sh" + code
		}
		return "sz" + code
	}
	return tsCode
}

// sinaSymbolToTsCode 新浪代码转回 ts_code：sz000001 -> 000001.SZ
func sinaSymbolToTsCode(sinaSymbol string) string {
	sinaSymbol = strings.TrimSpace(sinaSymbol)
	if len(sinaSymbol) < 3 {
		return sinaSymbol
	}
	pre := strings.ToLower(sinaSymbol[:2])
	code := sinaSymbol[2:]
	if pre == "sh" {
		return code + ".SH"
	}
	if pre == "sz" {
		return code + ".SZ"
	}
	return sinaSymbol
}

func (a *SinaRealtimeAdapter) fetchRealtimeQuote(ctx context.Context, params map[string]interface{}) ([]map[string]interface{}, error) {
	tsCodeRaw, _ := params["ts_code"].(string)
	var symbols []string
	if tsCodeRaw != "" {
		for _, s := range strings.Split(tsCodeRaw, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				symbols = append(symbols, tsCodeToSinaSymbol(s))
			}
		}
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("sina realtime_quote: ts_code is required")
	}
	if len(symbols) > sinaQuoteMaxSymbols {
		symbols = symbols[:sinaQuoteMaxSymbols]
	}
	listParam := strings.Join(symbols, ",")
	url := fmt.Sprintf(sinaQuoteURL, listParam)
	body, err := a.getGBK(ctx, url)
	if err != nil {
		return nil, err
	}
	rows, err := a.parseSinaQuoteBody(body, symbols)
	if err != nil {
		return nil, err
	}
	if len(rows) > 0 {
		logrus.Infof("[Sina realtime_quote] fetched %d rows, symbols=%v, sample: ts_code=%v price=%v volume=%v",
			len(rows), symbols, rows[0]["ts_code"], rows[0]["price"], rows[0]["volume"])
	}
	return rows, nil
}

var reSinaQuoteLine = regexp.MustCompile(`var hq_str_(\w+)="(.*?)";`)

func (a *SinaRealtimeAdapter) parseSinaQuoteBody(body string, symbols []string) ([]map[string]interface{}, error) {
	var rows []map[string]interface{}
	matches := reSinaQuoteLine.FindAllStringSubmatch(body, -1)
	for _, m := range matches {
		if len(m) != 3 {
			continue
		}
		sinaSym := m[1]
		content := m[2]
		parts := strings.Split(content, ",")
		if len(parts) < len(sinaQuoteCols) {
			continue
		}
		row := make(map[string]interface{})
		row["ts_code"] = sinaSymbolToTsCode(sinaSym)
		for i, col := range sinaQuoteCols {
			if i >= len(parts) {
				break
			}
			s := strings.TrimSpace(parts[i])
			if col == "name" || col == "date" || col == "time" || strings.HasSuffix(col, "_v") {
				row[col] = s
				continue
			}
			if col == "volume" || col == "amount" {
				if v, err := strconv.ParseFloat(s, 64); err == nil {
					row[col] = v
				} else {
					row[col] = s
				}
				continue
			}
			if v, err := strconv.ParseFloat(s, 64); err == nil {
				row[col] = v
			} else {
				row[col] = s
			}
		}
		// Tushare 兼容字段：与 realtime_quote 表 (ts_code, trade_time, open, close, high, low, vol, amount) 对齐
		if p, ok := row["price"].(float64); ok {
			row["close"] = p
		}
		if v, ok := row["volume"].(float64); ok {
			row["vol"] = v
		}
		if date, _ := row["date"].(string); date != "" {
			if t, _ := row["time"].(string); t != "" {
				row["trade_time"] = date + " " + t
			} else {
				row["trade_time"] = date
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (a *SinaRealtimeAdapter) fetchRealtimeTick(ctx context.Context, params map[string]interface{}) ([]map[string]interface{}, error) {
	tsCodeRaw, _ := params["ts_code"].(string)
	tsCodeRaw = strings.TrimSpace(tsCodeRaw)
	if tsCodeRaw == "" {
		return nil, fmt.Errorf("sina realtime_tick: ts_code is required (single code)")
	}
	symbol := tsCodeToSinaSymbol(tsCodeRaw)
	url := fmt.Sprintf(sinaTickURL, symbol)
	body, err := a.getGBK(ctx, url)
	if err != nil {
		return nil, err
	}
	rows, err := a.parseSinaTickBody(body, sinaSymbolToTsCode(symbol))
	if err != nil {
		return nil, err
	}
	logrus.Infof("[Sina realtime_tick] fetched %d rows, ts_code=%s", len(rows), tsCodeRaw)
	return rows, nil
}

func (a *SinaRealtimeAdapter) parseSinaTickBody(body, tsCode string) ([]map[string]interface{}, error) {
	// 新浪分笔页为 HTML 表格，此处做简单按行解析；若格式变化需调整
	// 返回格式与 Tushare 一致：time, price, change, volume, amount, type
	lines := strings.Split(body, "\n")
	var rows []map[string]interface{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "<") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 4 {
			continue
		}
		row := map[string]interface{}{
			"ts_code": tsCode,
			"time":    strings.TrimSpace(parts[0]),
			"price":   parseFloat(parts[1]),
			"volume":  parseInt(parts[2]),
			"amount":  parseInt(parts[3]),
			"type":    "中性",
		}
		if len(parts) > 4 {
			row["change"] = parseFloat(parts[4])
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (a *SinaRealtimeAdapter) fetchRealtimeList(ctx context.Context, params map[string]interface{}) ([]map[string]interface{}, error) {
	// realtime_list：全市场列表或涨跌榜，此处请求一个通用列表接口返回一条记录占位
	// 实际业务可替换为具体新浪列表 URL
	body, err := a.getGBK(ctx, sinaListURL)
	if err != nil {
		return nil, err
	}
	// 若返回为单条行情格式，按 quote 解析；否则返回简单占位
	rows, _ := a.parseSinaQuoteBody(body, nil)
	if len(rows) > 0 {
		logrus.Infof("[Sina realtime_list] fetched %d rows", len(rows))
		return rows, nil
	}
	logrus.Infof("[Sina realtime_list] fallback placeholder")
	return []map[string]interface{}{{"source": sinaSource, "api": "realtime_list"}}, nil
}

func (a *SinaRealtimeAdapter) getGBK(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	// 模拟浏览器请求头，降低 403 概率
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Referer", "https://finance.sina.com.cn/")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := a.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("sina http status %d", resp.StatusCode)
	}
	utf8Reader := transform.NewReader(resp.Body, simplifiedchinese.GBK.NewDecoder())
	b, err := io.ReadAll(utf8Reader)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parseFloat(s string) float64 {
	s = strings.TrimSpace(s)
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func parseInt(s string) int64 {
	s = strings.TrimSpace(s)
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}
