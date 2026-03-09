package realtime

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	eastmoneySource      = "eastmoney"
	eastmoneyQuoteURL    = "https://push2.eastmoney.com/api/qt/stock/get?secid=%s&fields=f43,f57,f58,f60,f46,f47,f44,f45,f168,f169,f170,f71,f48,f49,f50,f51,f52,f161,f162,f163,f164,f165,f166,f167"
	eastmoneyListURL     = "http://82.push2.eastmoney.com/api/qt/clist/get"
	eastmoneyTickSSEURL  = "https://70.push2.eastmoney.com/api/qt/stock/details/sse"
	eastmoneyHTTPTimeout = 15 * time.Second
)

// EastmoneyRealtimeAdapter 东方财富实时行情 Adapter（仅 Pull，realtime_quote 单码）
type EastmoneyRealtimeAdapter struct {
	client *http.Client
}

// NewEastmoneyRealtimeAdapter 创建东财实时 Adapter
func NewEastmoneyRealtimeAdapter() *EastmoneyRealtimeAdapter {
	return &EastmoneyRealtimeAdapter{
		client: &http.Client{Timeout: eastmoneyHTTPTimeout},
	}
}

// Source 实现 RealtimeAdapter
func (a *EastmoneyRealtimeAdapter) Source() string { return eastmoneySource }

// SupportedAPIs 实现 RealtimeAdapter（东财 realtime_quote 仅单码，调用方需分片）
func (a *EastmoneyRealtimeAdapter) SupportedAPIs() []string {
	return []string{"realtime_quote", "realtime_tick", "realtime_list"}
}

// Supports 实现 RealtimeAdapter
func (a *EastmoneyRealtimeAdapter) Supports(apiName string) bool {
	for _, name := range a.SupportedAPIs() {
		if name == apiName {
			return true
		}
	}
	return false
}

// SupportedModes 实现 RealtimeAdapter
func (a *EastmoneyRealtimeAdapter) SupportedModes(apiName string) []RealtimeMode {
	if !a.Supports(apiName) {
		return nil
	}
	if apiName == "realtime_tick" {
		return []RealtimeMode{RealtimeModePull, RealtimeModePush}
	}
	return []RealtimeMode{RealtimeModePull}
}

// SupportsPush 实现 RealtimeAdapter
func (a *EastmoneyRealtimeAdapter) SupportsPush(apiName string) bool {
	return apiName == "realtime_tick"
}

// StartStream 实现 RealtimeAdapter：realtime_tick 使用东财 SSE 做 Push。
func (a *EastmoneyRealtimeAdapter) StartStream(ctx context.Context, apiName string, params map[string]interface{}, onBatch func([]map[string]interface{}) error) error {
	if apiName != "realtime_tick" {
		return fmt.Errorf("eastmoney adapter does not support push mode for api %s", apiName)
	}
	tsCodeRaw, _ := params["ts_code"].(string)
	tsCodeRaw = strings.TrimSpace(tsCodeRaw)
	if tsCodeRaw == "" {
		return fmt.Errorf("eastmoney realtime_tick: ts_code is required (single code)")
	}
	// 若传入多码，Push 模式只针对首个代码建立 SSE 连接；多码场景由上层启动多个 Collector 实例处理。
	if idx := strings.Index(tsCodeRaw, ","); idx > 0 {
		tsCodeRaw = strings.TrimSpace(tsCodeRaw[:idx])
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := a.startTickSSEStreamOnce(ctx, tsCodeRaw, onBatch); err != nil {
			// 出错后短暂退避再重连，具体重试策略交由上层控制整体生命周期。
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(3 * time.Second):
			}
			continue
		}

		// 正常结束（例如服务端主动关闭连接）也做一次短暂等待后重连，直到 ctx 取消。
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(1 * time.Second):
		}
	}
}

// Fetch 实现 RealtimeAdapter
func (a *EastmoneyRealtimeAdapter) Fetch(ctx context.Context, apiName string, params map[string]interface{}) ([]map[string]interface{}, error) {
	switch apiName {
	case "realtime_quote":
		return a.fetchRealtimeQuote(ctx, params)
	case "realtime_tick":
		return a.fetchRealtimeTick(ctx, params)
	case "realtime_list":
		return a.fetchRealtimeList(ctx, params)
	default:
		return nil, fmt.Errorf("eastmoney adapter: unsupported api %s", apiName)
	}
}

// tsCodeToEastmoneySecid 将 Tushare ts_code 转为东财 secid：000001.SZ -> 0.000001, 600000.SH -> 1.600000
func tsCodeToEastmoneySecid(tsCode string) string {
	tsCode = strings.TrimSpace(tsCode)
	if idx := strings.Index(tsCode, "."); idx > 0 {
		exchange := strings.ToUpper(tsCode[idx+1:])
		code := tsCode[:idx]
		if exchange == "SH" {
			return "1." + code
		}
		return "0." + code
	}
	return "0." + tsCode
}

type eastmoneyQuoteResp struct {
	Data *struct {
		F43  interface{} `json:"f43"`  // 最新价
		F57  interface{} `json:"f57"`  // 代码
		F58  interface{} `json:"f58"`  // 名称
		F60  interface{} `json:"f60"`  // 昨收
		F46  interface{} `json:"f46"`  // 开盘
		F47  interface{} `json:"f47"`  // 总手
		F44  interface{} `json:"f44"`  // 最高
		F45  interface{} `json:"f45"`  // 最低
		F168 interface{} `json:"f168"` // 委买
		F169 interface{} `json:"f169"` // 涨跌
		F170 interface{} `json:"f170"` // 涨跌幅
		F71  interface{} `json:"f71"`  // 均价
		F48  interface{} `json:"f48"`  // 委买量
		F49  interface{} `json:"f49"`  // 委买价
		F50  interface{} `json:"f50"`
		F51  interface{} `json:"f51"`
		F52  interface{} `json:"f52"`
		F161 interface{} `json:"f161"`
		F162 interface{} `json:"f162"`
		F163 interface{} `json:"f163"`
		F164 interface{} `json:"f164"`
		F165 interface{} `json:"f165"`
		F166 interface{} `json:"f166"`
		F167 interface{} `json:"f167"`
	} `json:"data"`
}

// eastmoneyListResp 东方财富全市场列表响应
type eastmoneyListResp struct {
	Data *struct {
		Total int                      `json:"total"`
		Diff  []map[string]interface{} `json:"diff"`
	} `json:"data"`
}

// eastmoneyTickResp 东方财富分时明细 SSE 响应
type eastmoneyTickResp struct {
	Data *struct {
		Details []string `json:"details"`
	} `json:"data"`
}

func toFloat(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}

func (a *EastmoneyRealtimeAdapter) fetchRealtimeQuote(ctx context.Context, params map[string]interface{}) ([]map[string]interface{}, error) {
	tsCodeRaw, _ := params["ts_code"].(string)
	tsCodeRaw = strings.TrimSpace(tsCodeRaw)
	if tsCodeRaw == "" {
		return nil, fmt.Errorf("eastmoney realtime_quote: ts_code is required (single code only)")
	}
	// 东财单码限制：只取第一个
	if idx := strings.Index(tsCodeRaw, ","); idx > 0 {
		tsCodeRaw = strings.TrimSpace(tsCodeRaw[:idx])
	}
	secid := tsCodeToEastmoneySecid(tsCodeRaw)
	url := fmt.Sprintf(eastmoneyQuoteURL, secid)
	body, err := a.get(ctx, url)
	if err != nil {
		return nil, err
	}
	var resp eastmoneyQuoteResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if resp.Data == nil {
		return []map[string]interface{}{}, nil
	}
	d := resp.Data
	row := map[string]interface{}{
		"ts_code":   tsCodeRaw,
		"name":      toString(d.F58),
		"open":      toFloat(d.F46),
		"pre_close": toFloat(d.F60),
		"price":     toFloat(d.F43),
		"high":      toFloat(d.F44),
		"low":       toFloat(d.F45),
		"bid":       toFloat(d.F49),
		"ask":       toFloat(d.F50),
		"volume":    toFloat(d.F47),
		"amount":    0,
		"date":      "",
		"time":      "",
	}
	row["b1_v"] = toFloat(d.F48)
	row["b1_p"] = toFloat(d.F49)
	row["b2_v"] = toFloat(d.F50)
	row["b2_p"] = toFloat(d.F51)
	row["a1_v"] = toFloat(d.F161)
	row["a1_p"] = toFloat(d.F162)
	row["a2_v"] = toFloat(d.F163)
	row["a2_p"] = toFloat(d.F164)
	row["a3_v"] = toFloat(d.F165)
	row["a3_p"] = toFloat(d.F166)
	row["a4_v"] = 0
	row["a4_p"] = 0
	row["a5_v"] = 0
	row["a5_p"] = 0
	row["b3_v"] = 0
	row["b3_p"] = 0
	row["b4_v"] = 0
	row["b4_p"] = 0
	row["b5_v"] = 0
	row["b5_p"] = 0
	return []map[string]interface{}{row}, nil
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func (a *EastmoneyRealtimeAdapter) fetchRealtimeTick(ctx context.Context, params map[string]interface{}) ([]map[string]interface{}, error) {
	tsCodeRaw, _ := params["ts_code"].(string)
	tsCodeRaw = strings.TrimSpace(tsCodeRaw)
	if tsCodeRaw == "" {
		return nil, fmt.Errorf("eastmoney realtime_tick: ts_code is required (single code)")
	}
	// 单次 Fetch：仅获取当前 SSE 流中的一批分笔明细快照。
	rows, err := a.readTickSSEOnce(ctx, tsCodeRaw)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (a *EastmoneyRealtimeAdapter) fetchRealtimeList(ctx context.Context, params map[string]interface{}) ([]map[string]interface{}, error) {
	// 参考 Python get_stock_all_a_dc：分页抓取所有 A 股实时行情，这里通过 goroutine 并发按页获取。
	pageSize := 200
	if v, ok := params["page_size"]; ok {
		switch x := v.(type) {
		case int:
			if x > 0 {
				pageSize = x
			}
		case float64:
			if int(x) > 0 {
				pageSize = int(x)
			}
		case string:
			if n, err := strconv.Atoi(x); err == nil && n > 0 {
				pageSize = n
			}
		}
	}

	firstResp, err := a.fetchListPage(ctx, 1, pageSize)
	if err != nil {
		return nil, err
	}
	if firstResp.Data == nil || len(firstResp.Data.Diff) == 0 {
		return []map[string]interface{}{}, nil
	}

	total := firstResp.Data.Total
	if total <= 0 {
		total = len(firstResp.Data.Diff)
	}
	pageCount := total / pageSize
	if total%pageSize != 0 {
		pageCount++
	}
	if pageCount <= 1 {
		return a.mapListRows(firstResp.Data.Diff), nil
	}

	// 并发抓取剩余页，限制最大并发以避免对东财造成过高压力。
	const maxWorkers = 8
	allDiffs := make([]map[string]interface{}, 0, total)
	allDiffs = append(allDiffs, firstResp.Data.Diff...)

	type pageResult struct {
		rows []map[string]interface{}
		err  error
	}
	resultsCh := make(chan pageResult, pageCount-1)

	sem := make(chan struct{}, maxWorkers)
	for p := 2; p <= pageCount; p++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		sem <- struct{}{}
		go func(page int) {
			defer func() { <-sem }()
			resp, e := a.fetchListPage(ctx, page, pageSize)
			if e != nil || resp.Data == nil || len(resp.Data.Diff) == 0 {
				resultsCh <- pageResult{rows: nil, err: e}
				return
			}
			resultsCh <- pageResult{rows: resp.Data.Diff, err: nil}
		}(p)
	}

	for i := 2; i <= pageCount; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r := <-resultsCh:
			if r.err != nil {
				// 某一页失败时跳过该页，其它页照常汇总，整体快照尽量完整。
				continue
			}
			allDiffs = append(allDiffs, r.rows...)
		}
	}

	rows := a.mapListRows(allDiffs)
	return rows, nil
}

func (a *EastmoneyRealtimeAdapter) get(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; qdhub/1.0)")
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("eastmoney http status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// fetchListPage 拉取东财全市场列表单页数据。
func (a *EastmoneyRealtimeAdapter) fetchListPage(ctx context.Context, page, pageSize int) (*eastmoneyListResp, error) {
	values := url.Values{}
	values.Set("pn", strconv.Itoa(page))
	values.Set("pz", strconv.Itoa(pageSize))
	values.Set("po", "1")
	values.Set("np", "1")
	values.Set("ut", "bd1d9ddb04089700cf9c27f6f7426281")
	values.Set("fltt", "2")
	values.Set("invt", "2")
	values.Set("fid", "f3")
	values.Set("fs", "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048")
	values.Set("fields", "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152")

	fullURL := eastmoneyListURL + "?" + values.Encode()
	body, err := a.get(ctx, fullURL)
	if err != nil {
		return nil, err
	}
	var resp eastmoneyListResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// mapListRows 将东财列表 diff 数组映射为 Tushare realtime_list 兼容字段。
func (a *EastmoneyRealtimeAdapter) mapListRows(diffs []map[string]interface{}) []map[string]interface{} {
	rows := make([]map[string]interface{}, 0, len(diffs))
	for _, d := range diffs {
		code := toString(d["f12"])
		market := toString(d["f13"])
		row := map[string]interface{}{
			"ts_code":       eastmoneyCodeToTs(code, market),
			"name":          toString(d["f14"]),
			"price":         toFloat(d["f2"]),
			"pct_change":    toFloat(d["f3"]),
			"change":        toFloat(d["f4"]),
			"volume":        toFloat(d["f5"]),
			"amount":        toFloat(d["f6"]),
			"swing":         toFloat(d["f7"]),
			"high":          toFloat(d["f15"]),
			"low":           toFloat(d["f16"]),
			"open":          toFloat(d["f17"]),
			"close":         toFloat(d["f18"]),
			"vol_ratio":     toFloat(d["f10"]),
			"turnover_rate": toFloat(d["f8"]),
			"pe":            toFloat(d["f9"]),
			"pb":            toFloat(d["f23"]),
			"total_mv":      toFloat(d["f20"]),
			"float_mv":      toFloat(d["f21"]),
			"rise":          toFloat(d["f11"]),
			"5min":          toFloat(d["f24"]),
			"60day":         toFloat(d["f25"]),
			"1tyear":        toFloat(d["f22"]),
		}
		rows = append(rows, row)
	}
	// 这里不强制排序，调用方如需按涨跌幅排序可自行处理；保持函数纯映射职责。
	return rows
}

// eastmoneyCodeToTs 将东财代码与市场标识转换为 Tushare ts_code。
func eastmoneyCodeToTs(code, market string) string {
	code = strings.TrimSpace(code)
	market = strings.TrimSpace(market)
	if code == "" {
		return ""
	}
	switch market {
	case "1":
		return code + ".SH"
	case "0":
		return code + ".SZ"
	}
	// 回退：按代码前缀简单推断
	if strings.HasPrefix(code, "6") {
		return code + ".SH"
	}
	return code + ".SZ"
}

// readTickSSEOnce 使用 SSE 接口拉取一次分笔明细快照。
func (a *EastmoneyRealtimeAdapter) readTickSSEOnce(ctx context.Context, tsCode string) ([]map[string]interface{}, error) {
	fullURL, err := buildTickSSEURL(tsCode)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; qdhub/1.0)")

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("eastmoney realtime_tick sse http status %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var eventLines []string
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			if len(eventLines) == 0 {
				continue
			}
			eventData := strings.Join(eventLines, "\n")
			rows, err := parseTickSSEEvent(eventData, tsCode)
			if err != nil {
				return nil, err
			}
			return rows, nil
		}
		if strings.HasPrefix(line, "data:") {
			eventLines = append(eventLines, line)
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		return nil, err
	}
	return []map[string]interface{}{}, nil
}

// startTickSSEStreamOnce 建立一次 SSE 连接并持续消费事件，直到连接关闭或 ctx 取消。
func (a *EastmoneyRealtimeAdapter) startTickSSEStreamOnce(ctx context.Context, tsCode string, onBatch func([]map[string]interface{}) error) error {
	fullURL, err := buildTickSSEURL(tsCode)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; qdhub/1.0)")

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("eastmoney realtime_tick sse http status %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var eventLines []string
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			if len(eventLines) == 0 {
				continue
			}
			eventData := strings.Join(eventLines, "\n")
			eventLines = eventLines[:0]
			rows, err := parseTickSSEEvent(eventData, tsCode)
			if err != nil {
				return err
			}
			if len(rows) > 0 {
				if err := onBatch(rows); err != nil {
					return err
				}
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			eventLines = append(eventLines, line)
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

// buildTickSSEURL 构造东财分时 SSE URL。
func buildTickSSEURL(tsCode string) (string, error) {
	tsCode = strings.TrimSpace(tsCode)
	if tsCode == "" {
		return "", fmt.Errorf("empty ts_code")
	}
	secid := tsCodeToEastmoneySecid(tsCode)
	values := url.Values{}
	values.Set("fields1", "f1,f2,f3,f4")
	values.Set("fields2", "f51,f52,f53,f54,f55")
	values.Set("mpi", "2000")
	values.Set("ut", "bd1d9ddb04089700cf9c27f6f7426281")
	values.Set("fltt", "2")
	values.Set("pos", "-0")
	values.Set("secid", secid)
	values.Set("wbp2u", "|0|0|0|web")
	return eastmoneyTickSSEURL + "?" + values.Encode(), nil
}

// parseTickSSEEvent 解析 SSE eventData 为 realtime_tick 兼容字段。
func parseTickSSEEvent(eventData, tsCode string) ([]map[string]interface{}, error) {
	if strings.TrimSpace(eventData) == "" {
		return []map[string]interface{}{}, nil
	}
	// 与 Python 实现保持一致：去掉 "data: " 前缀再解析 JSON。
	cleaned := strings.ReplaceAll(eventData, "data: ", "")
	cleaned = strings.TrimSpace(cleaned)
	if cleaned == "" {
		return []map[string]interface{}{}, nil
	}
	var resp eastmoneyTickResp
	if err := json.Unmarshal([]byte(cleaned), &resp); err != nil {
		return nil, err
	}
	if resp.Data == nil || len(resp.Data.Details) == 0 {
		return []map[string]interface{}{}, nil
	}
	rows := make([]map[string]interface{}, 0, len(resp.Data.Details))
	for _, detail := range resp.Data.Details {
		parts := strings.Split(detail, ",")
		if len(parts) < 5 {
			continue
		}
		timeStr := strings.TrimSpace(parts[0])
		price := toFloat(parts[1])
		vol := toFloat(parts[2])
		typeCode := strings.TrimSpace(parts[4])
		var typeStr string
		switch typeCode {
		case "2":
			typeStr = "买盘"
		case "1":
			typeStr = "卖盘"
		case "4":
			typeStr = "中性盘"
		default:
			typeStr = ""
		}
		row := map[string]interface{}{
			"ts_code": tsCode,
			"time":    timeStr,
			"price":   price,
			"change":  0.0,
			"volume":  vol,
			// SSE 明细未直接给出金额，这里按常规假设：价格 * 手数 * 100（股）。
			"amount": price * vol * 100,
			"type":   typeStr,
		}
		rows = append(rows, row)
	}
	return rows, nil
}
