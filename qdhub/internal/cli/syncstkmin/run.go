// Package syncstkmin 提供从 Tushare 拉取 stk_mins 并写入 DuckDB 的 CLI 逻辑。
package syncstkmin

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/infrastructure/datasource/tushare"
	duckdbfactory "qdhub/internal/infrastructure/quantdb/duckdb"
	"qdhub/internal/infrastructure/taskengine/jobs"
)

// Options 同步参数（由 cobra 填充）。
type Options struct {
	Token         string
	BaseURL       string
	DuckDBPath    string
	Table         string
	StartDate     string
	EndDate       string
	Freq          string
	WindowFreq    string
	ListStatus    string
	Concurrency   int
	InitTable     bool
	SyncBatchID   string
	RatePerMinute int
}

const (
	defaultTable       = "stk_mins"
	defaultFreq        = "1min"
	defaultWindowFreq  = "30D"
	defaultListStatus  = "L"
	defaultConcurrency = 16
)

// StkMinsDDL 与 Tushare stk_mins 文档一致的 8 列（占位表名为 stk_mins，可由调用方替换）。
const StkMinsDDL = `CREATE TABLE IF NOT EXISTS stk_mins (
	ts_code VARCHAR,
	trade_time VARCHAR,
	open DOUBLE,
	close DOUBLE,
	high DOUBLE,
	low DOUBLE,
	vol BIGINT,
	amount DOUBLE
);`

type halfOpenWindow struct {
	start, end string
}

// Run 执行：stock_basic → 时间窗 → 并发 stk_mins → 写入 DuckDB。
func Run(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.Token) == "" {
		return fmt.Errorf("tushare token 为空，请设置 --token 或环境变量 TUSHARE_TOKEN")
	}
	if strings.TrimSpace(opts.DuckDBPath) == "" {
		return fmt.Errorf("--duckdb-path 必填")
	}
	if strings.TrimSpace(opts.StartDate) == "" || strings.TrimSpace(opts.EndDate) == "" {
		return fmt.Errorf("--start-date 与 --end-date 必填")
	}
	if opts.Table == "" {
		opts.Table = defaultTable
	}
	if opts.Freq == "" {
		opts.Freq = defaultFreq
	}
	if opts.WindowFreq == "" {
		opts.WindowFreq = defaultWindowFreq
	}
	if opts.ListStatus == "" {
		opts.ListStatus = defaultListStatus
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = defaultConcurrency
	}

	rpm := opts.RatePerMinute
	if rpm <= 0 {
		rpm = tushare.DefaultRateLimitPerMinute
	}

	tsOpts := []tushare.ClientOption{
		tushare.WithToken(opts.Token),
		tushare.WithMaxConcurrent(opts.Concurrency),
		tushare.WithRateLimit(rpm),
	}
	if strings.TrimSpace(opts.BaseURL) != "" {
		tsOpts = append(tsOpts, tushare.WithBaseURL(strings.TrimSpace(opts.BaseURL)))
	}
	client := tushare.NewClient(tsOpts...)

	factory := duckdbfactory.NewFactory()
	defer factory.Close()

	qdb, err := factory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: opts.DuckDBPath,
	})
	if err != nil {
		return fmt.Errorf("打开 DuckDB: %w", err)
	}

	if opts.InitTable {
		ddl := strings.Replace(StkMinsDDL, "stk_mins", quoteIdent(opts.Table), 1)
		if _, err := qdb.Execute(ctx, ddl); err != nil {
			return fmt.Errorf("建表: %w", err)
		}
		logrus.Infof("已执行建表（若不存在）: %s", opts.Table)
	}

	codes, err := fetchAllTsCodes(ctx, client, opts.ListStatus)
	if err != nil {
		return err
	}
	logrus.Infof("stock_basic: 共 %d 个 ts_code (list_status=%s)", len(codes), opts.ListStatus)

	windows, err := buildStkMinsWindows(opts.StartDate, opts.EndDate, opts.WindowFreq)
	if err != nil {
		return err
	}
	logrus.Infof("时间窗: %d 个 (步长 %s)", len(windows), opts.WindowFreq)

	type syncJob struct {
		tsCode           string
		apiStart, apiEnd string
	}
	var jobList []syncJob
	for _, w := range windows {
		apiS, apiE := jobs.StkMinsAPIRangeFromHalfOpenWindow(w.start, w.end)
		for _, c := range codes {
			jobList = append(jobList, syncJob{tsCode: c, apiStart: apiS, apiEnd: apiE})
		}
	}
	logrus.Infof("待请求: %d 次 (代码数 × 时间窗)", len(jobList))

	sem := semaphore.NewWeighted(int64(opts.Concurrency))
	g, gctx := errgroup.WithContext(ctx)
	var done uint64
	var inserted int64
	var insMu sync.Mutex

	for _, j := range jobList {
		j := j
		if err := sem.Acquire(gctx, 1); err != nil {
			return err
		}
		g.Go(func() error {
			defer sem.Release(1)
			params := map[string]interface{}{
				"ts_code":    j.tsCode,
				"freq":       opts.Freq,
				"start_date": j.apiStart,
				"end_date":   j.apiEnd,
			}
			res, qerr := client.Query(gctx, "stk_mins", params)
			if qerr != nil {
				return fmt.Errorf("stk_mins %s [%s ~ %s]: %w", j.tsCode, j.apiStart, j.apiEnd, qerr)
			}
			rows, convErr := filterStkMinsRows(res.Data)
			if convErr != nil {
				return convErr
			}
			if len(rows) == 0 {
				atomic.AddUint64(&done, 1)
				return nil
			}
			var n int64
			var insErr error
			if strings.TrimSpace(opts.SyncBatchID) != "" {
				n, insErr = qdb.BulkInsertWithBatchID(gctx, opts.Table, rows, strings.TrimSpace(opts.SyncBatchID))
			} else {
				n, insErr = qdb.BulkInsert(gctx, opts.Table, rows)
			}
			if insErr != nil {
				return fmt.Errorf("写入 %s: %w", j.tsCode, insErr)
			}
			insMu.Lock()
			inserted += n
			insMu.Unlock()
			cur := atomic.AddUint64(&done, 1)
			if cur%500 == 0 {
				insMu.Lock()
				tot := inserted
				insMu.Unlock()
				logrus.Infof("进度: %d/%d 请求完成, 累计写入行数 %d", cur, len(jobList), tot)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	insMu.Lock()
	finalIns := inserted
	insMu.Unlock()
	logrus.Infof("完成: %d 次 API 请求, DuckDB 写入行数合计 %d", len(jobList), finalIns)
	return nil
}

func quoteIdent(name string) string {
	s := strings.ReplaceAll(name, `"`, `""`)
	return `"` + s + `"`
}

func fetchAllTsCodes(ctx context.Context, client *tushare.Client, listStatus string) ([]string, error) {
	const limit = 5000
	var out []string
	seen := make(map[string]struct{})
	for offset := 0; ; offset += limit {
		res, err := client.Query(ctx, "stock_basic", map[string]interface{}{
			"list_status": listStatus,
			"fields":      "ts_code",
			"limit":       limit,
			"offset":      offset,
		})
		if err != nil {
			return nil, fmt.Errorf("stock_basic offset=%d: %w", offset, err)
		}
		if len(res.Data) == 0 {
			break
		}
		for _, row := range res.Data {
			tc, _ := row["ts_code"].(string)
			tc = strings.TrimSpace(tc)
			if tc == "" {
				continue
			}
			if _, ok := seen[tc]; ok {
				continue
			}
			seen[tc] = struct{}{}
			out = append(out, tc)
		}
		if len(res.Data) < limit {
			break
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("stock_basic 未返回任何 ts_code")
	}
	return out, nil
}

func buildStkMinsWindows(startRaw, endRaw, windowFreq string) ([]halfOpenWindow, error) {
	stepStart, stepEnd, err := jobs.StkMinsGenerateDatetimeRangeStepSpan(startRaw, endRaw)
	if err != nil {
		return nil, err
	}
	tc := task.NewTaskContext(ctxForJobs(), "sync-stk-mins", "range", "", "", map[string]interface{}{
		"start":      stepStart,
		"end":        stepEnd,
		"freq":       windowFreq,
		"inclusive":  "both",
		"as_windows": true,
	})
	out, err := jobs.GenerateDatetimeRangeJob(tc)
	if err != nil {
		return nil, fmt.Errorf("生成时间窗: %w", err)
	}
	m, ok := out.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("GenerateDatetimeRangeJob 返回类型异常")
	}
	return parseHalfOpenWindows(m["windows"])
}

func ctxForJobs() context.Context {
	return context.Background()
}

func parseHalfOpenWindows(v interface{}) ([]halfOpenWindow, error) {
	if v == nil {
		return nil, fmt.Errorf("windows 为空")
	}
	switch x := v.(type) {
	case []map[string]string:
		var w []halfOpenWindow
		for _, it := range x {
			w = append(w, halfOpenWindow{start: it["start"], end: it["end"]})
		}
		return w, nil
	case []interface{}:
		var w []halfOpenWindow
		for _, it := range x {
			sm, ok := it.(map[string]string)
			if ok {
				w = append(w, halfOpenWindow{start: sm["start"], end: sm["end"]})
				continue
			}
			im, ok := it.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("无法解析 window 元素 %T", it)
			}
			s, _ := im["start"].(string)
			e, _ := im["end"].(string)
			w = append(w, halfOpenWindow{start: s, end: e})
		}
		return w, nil
	default:
		return nil, fmt.Errorf("windows 类型不支持: %T", v)
	}
}

func filterStkMinsRows(data []map[string]interface{}) ([]map[string]any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	out := make([]map[string]any, 0, len(data))
	for i, row := range data {
		m, err := RowToStkMins8(row)
		if err != nil {
			return nil, fmt.Errorf("第 %d 行: %w", i, err)
		}
		out = append(out, m)
	}
	return out, nil
}

// RowToStkMins8 将 Python/JSON 一行映射为 stk_mins 八字段；导出供 tests/unit 黑盒测试。
func RowToStkMins8(row map[string]interface{}) (map[string]any, error) {
	tsCode, err := asString(row["ts_code"])
	if err != nil {
		return nil, fmt.Errorf("ts_code: %w", err)
	}
	tradeTime, err := asString(row["trade_time"])
	if err != nil {
		return nil, fmt.Errorf("trade_time: %w", err)
	}
	open, err := asFloat64(row["open"])
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	close, err := asFloat64(row["close"])
	if err != nil {
		return nil, fmt.Errorf("close: %w", err)
	}
	high, err := asFloat64(row["high"])
	if err != nil {
		return nil, fmt.Errorf("high: %w", err)
	}
	low, err := asFloat64(row["low"])
	if err != nil {
		return nil, fmt.Errorf("low: %w", err)
	}
	vol, err := asInt64(row["vol"])
	if err != nil {
		return nil, fmt.Errorf("vol: %w", err)
	}
	amount, err := asFloat64(row["amount"])
	if err != nil {
		return nil, fmt.Errorf("amount: %w", err)
	}
	return map[string]any{
		"ts_code":    tsCode,
		"trade_time": tradeTime,
		"open":       open,
		"close":      close,
		"high":       high,
		"low":        low,
		"vol":        vol,
		"amount":     amount,
	}, nil
}

func asString(v interface{}) (string, error) {
	if v == nil {
		return "", fmt.Errorf("缺失")
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t), nil
	case float64:
		return strconv.FormatInt(int64(t), 10), nil
	default:
		return strings.TrimSpace(fmt.Sprint(t)), nil
	}
}

func asFloat64(v interface{}) (float64, error) {
	if v == nil {
		return 0, fmt.Errorf("缺失")
	}
	switch t := v.(type) {
	case float64:
		return t, nil
	case float32:
		return float64(t), nil
	case int:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case json.Number:
		return t.Float64()
	case string:
		return strconv.ParseFloat(strings.TrimSpace(t), 64)
	default:
		return 0, fmt.Errorf("类型 %T 无法转为 float64", v)
	}
}

func asInt64(v interface{}) (int64, error) {
	if v == nil {
		return 0, fmt.Errorf("缺失")
	}
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case float64:
		if math.Trunc(t) != t {
			return 0, fmt.Errorf("vol 非整数: %v", t)
		}
		return int64(t), nil
	case json.Number:
		return t.Int64()
	case string:
		return strconv.ParseInt(strings.TrimSpace(t), 10, 64)
	default:
		return 0, fmt.Errorf("类型 %T 无法转为 int64", v)
	}
}
