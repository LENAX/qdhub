// Package jobs provides Task Engine job functions for QDHub workflows.
package jobs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/infrastructure/datasource/tushare/realtime"
	"qdhub/internal/infrastructure/realtimebuffer"
	"qdhub/internal/infrastructure/realtimestore"
)

const defaultRealtimeSrc = "sina"

func toFloat(v interface{}) (float64, bool) {
	if f, ok := v.(float64); ok {
		return f, true
	}
	if s, ok := v.(string); ok {
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil
	}
	return 0, false
}

// sinaRowToTushareStoreRow 将 Sina realtime_quote 行映射为 Tushare Store 字段名，与 WS 推送结构一致。
func sinaRowToTushareStoreRow(row map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, 32)
	out["ts_code"], _ = row["ts_code"]
	out["code"], _ = row["ts_code"]
	if v, ok := row["name"].(string); ok {
		out["name"] = v
	}
	if v, ok := row["trade_time"].(string); ok {
		out["trade_time"] = v
	}
	if v, ok := toFloat(row["pre_close"]); ok {
		out["pre_price"] = v
	}
	for _, key := range []string{"price", "open", "high", "low", "close", "volume", "amount"} {
		if v, ok := toFloat(row[key]); ok {
			out[key] = v
		}
	}
	if _, ok := out["close"]; !ok {
		if v, ok := toFloat(row["price"]); ok {
			out["close"] = v
		}
	}
	out["open_int"] = float64(0)
	out["num"] = float64(0)
	for i := 1; i <= 5; i++ {
		bp, bv := fmt.Sprintf("b%d_p", i), fmt.Sprintf("b%d_v", i)
		ap, av := fmt.Sprintf("a%d_p", i), fmt.Sprintf("a%d_v", i)
		if v, ok := toFloat(row[bp]); ok {
			out[fmt.Sprintf("bid_price%d", i)] = v
		}
		if v, ok := toFloat(row[bv]); ok {
			out[fmt.Sprintf("bid_volume%d", i)] = v
		}
		if v, ok := toFloat(row[ap]); ok {
			out[fmt.Sprintf("ask_price%d", i)] = v
		}
		if v, ok := toFloat(row[av]); ok {
			out[fmt.Sprintf("ask_volume%d", i)] = v
		}
	}
	return out
}

// RealtimeDataCollectorJob 实时数据采集 Job：从 RealtimeAdapter 拉取一批数据并 Push 到 buffer。
// Pull 模式：单次 Fetch；多轮轮询由工作流或调度层多次触发实现。
//
// Params: api_name, ts_code, src(可选,默认sina), token, target_db_path, data_source_name, pull_interval_seconds, freq(可选)
func RealtimeDataCollectorJob(tc *task.TaskContext) (interface{}, error) {
	ctx := context.Background()
	apiName := tc.GetParamString("api_name")
	src := tc.GetParamString("src")
	if src == "" {
		src = defaultRealtimeSrc
	}

	if apiName == "" {
		return nil, fmt.Errorf("RealtimeDataCollector: api_name is required")
	}

	regInterface, ok := tc.GetDependency("RealtimeAdapterRegistry")
	if !ok || regInterface == nil {
		return nil, fmt.Errorf("RealtimeDataCollector: RealtimeAdapterRegistry dependency not found")
	}
	adapterRegistry, ok := regInterface.(realtime.RealtimeAdapterRegistry)
	if !ok {
		return nil, fmt.Errorf("RealtimeDataCollector: RealtimeAdapterRegistry invalid type")
	}
	adapter, ok := adapterRegistry.Get(src)
	if !ok {
		return nil, fmt.Errorf("RealtimeDataCollector: adapter not found for src=%s", src)
	}
	if !adapter.Supports(apiName) {
		return nil, fmt.Errorf("RealtimeDataCollector: adapter %s does not support api %s", src, apiName)
	}

	bufRegInterface, ok := tc.GetDependency("RealtimeBufferRegistry")
	if !ok || bufRegInterface == nil {
		return nil, fmt.Errorf("RealtimeDataCollector: RealtimeBufferRegistry dependency not found")
	}
	bufRegistry, ok := bufRegInterface.(realtimebuffer.Registry)
	if !ok {
		return nil, fmt.Errorf("RealtimeDataCollector: RealtimeBufferRegistry invalid type")
	}
	buffer := bufRegistry.GetOrCreate(tc.WorkflowInstanceID)

	params := buildRealtimeParams(tc)
	data, err := adapter.Fetch(ctx, apiName, params)
	if err != nil {
		logrus.Warnf("[RealtimeDataCollector] Fetch failed: api=%s src=%s err=%v", apiName, src, err)
		return nil, err
	}
	if len(data) == 0 {
		return map[string]interface{}{"count": 0, "api_name": apiName}, nil
	}

	buffer.Push(realtimebuffer.Batch{Data: data, Source: src, APIName: apiName})
	logrus.Printf("[RealtimeDataCollector] pushed %d rows for %s/%s", len(data), src, apiName)
	return map[string]interface{}{"count": len(data), "api_name": apiName}, nil
}

func buildRealtimeParams(tc *task.TaskContext) map[string]interface{} {
	params := make(map[string]interface{})
	if v := tc.GetParamString("ts_code"); v != "" {
		params["ts_code"] = v
	}
	if v := tc.GetParamString("src"); v != "" {
		params["src"] = v
	}
	if v := tc.GetParamString("freq"); v != "" {
		params["freq"] = v
	}
	if v := tc.GetParam("pull_interval_seconds"); v != nil {
		switch x := v.(type) {
		case float64:
			params["pull_interval_seconds"] = int(x)
		case int:
			params["pull_interval_seconds"] = x
		case string:
			if i, err := strconv.Atoi(x); err == nil {
				params["pull_interval_seconds"] = i
			}
		}
	}
	return params
}

// RealtimeSyncDataHandlerJob 从 buffer 消费数据并落库到 target_db_path 下以 api_name 为表名的表中。
// 表需已存在（由 create_tables 工作流预先创建）；若某批对应表不存在则跳过该批并打日志。
// Params: target_db_path, data_source_name
func RealtimeSyncDataHandlerJob(tc *task.TaskContext) (interface{}, error) {
	ctx := context.Background()
	targetDBPath := tc.GetParamString("target_db_path")
	if targetDBPath == "" {
		return nil, fmt.Errorf("RealtimeSyncDataHandler: target_db_path is required")
	}

	if _, err := GetQuantDBForPath(tc, targetDBPath); err != nil {
		return nil, fmt.Errorf("RealtimeSyncDataHandler: get QuantDB: %w", err)
	}

	bufRegInterface, ok := tc.GetDependency("RealtimeBufferRegistry")
	if !ok || bufRegInterface == nil {
		return nil, fmt.Errorf("RealtimeSyncDataHandler: RealtimeBufferRegistry dependency not found")
	}
	bufRegistry, ok := bufRegInterface.(realtimebuffer.Registry)
	if !ok {
		return nil, fmt.Errorf("RealtimeSyncDataHandler: RealtimeBufferRegistry invalid type")
	}
	buffer := bufRegistry.GetOrCreate(tc.WorkflowInstanceID)
	recv := buffer.Recv()

	var totalRows int64
	for batch := range recv {
		n, err := writeRealtimeBatchToDuckDB(ctx, tc, targetDBPath, batch.APIName, batch.Source, mapsToAny(batch.Data))
		if err != nil {
			// 已在内部打日志，这里继续下一批
			continue
		}
		totalRows += n
	}
	return map[string]interface{}{"total_rows": totalRows}, nil
}

// safeTableName 返回可作为表名的安全字符串（仅保留字母数字下划线）
func safeTableName(apiName string) string {
	var b []byte
	for _, c := range apiName {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			b = append(b, byte(c))
		}
	}
	return string(b)
}

func mapsToAny(data []map[string]interface{}) []map[string]any {
	out := make([]map[string]any, len(data))
	for i, m := range data {
		row := make(map[string]any)
		for k, v := range m {
			row[k] = v
		}
		out[i] = row
	}
	return out
}

// normalizeRealtimeRows 将 DataHandler 收到的 data 参数规范化为 []map[string]any，兼容多种可能的类型形态。
func normalizeRealtimeRows(raw interface{}) []map[string]any {
	if raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case []map[string]interface{}:
		return mapsToAny(v)
	case map[string]interface{}:
		// 兼容 DataArrivedPayload 形态：{data: [...], source: "...", ...}
		if inner, ok := v["data"]; ok {
			return normalizeRealtimeRows(inner)
		}
		return mapsToAny([]map[string]interface{}{v})
	case []interface{}:
		tmp := make([]map[string]any, 0, len(v))
		for _, item := range v {
			switch m := item.(type) {
			case map[string]interface{}:
				tmp = append(tmp, mapsToAny([]map[string]interface{}{m})[0])
			}
		}
		return tmp
	default:
		return nil
	}
}

// inferTargetDBPathFromData 尝试从 data 中的行元数据提取 target_db_path（由 QuotePullCollector 注入）。
func inferTargetDBPathFromData(raw interface{}) string {
	rows := normalizeRealtimeRows(raw)
	if len(rows) == 0 {
		logrus.Debugf("[RealtimeQuoteStreamHandler] no rows to infer target_db_path")
		return ""
	}
	if s, ok := rows[0]["target_db_path"].(string); ok && s != "" {
		return s
	}
	logrus.Debugf("[RealtimeQuoteStreamHandler] target_db_path not found in first row")
	return ""
}

// RealtimeCloseBufferJob 关闭当前实例的 buffer，使 RealtimeSyncDataHandler 的 for range 退出。
func RealtimeCloseBufferJob(tc *task.TaskContext) (interface{}, error) {
	bufRegInterface, ok := tc.GetDependency("RealtimeBufferRegistry")
	if !ok || bufRegInterface == nil {
		return nil, fmt.Errorf("RealtimeCloseBuffer: RealtimeBufferRegistry dependency not found")
	}
	bufRegistry, ok := bufRegInterface.(realtimebuffer.Registry)
	if !ok {
		return nil, fmt.Errorf("RealtimeCloseBuffer: RealtimeBufferRegistry invalid type")
	}
	bufRegistry.CloseAndRemove(tc.WorkflowInstanceID)
	logrus.Printf("[RealtimeCloseBuffer] closed buffer for instance %s", tc.WorkflowInstanceID)
	return nil, nil
}

// writeRealtimeBatchToDuckDB 将一批实时数据写入 DuckDB 中对应的表。
// 复用于旧的 buffer 模式与新的 Streaming DataHandler。
func writeRealtimeBatchToDuckDB(
	ctx context.Context,
	tc *task.TaskContext,
	targetDBPath string,
	apiName string,
	source string,
	rows []map[string]any,
) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tableName := safeTableName(apiName)
	if tableName == "" {
		logrus.Warnf("[writeRealtimeBatch] skip batch: empty api_name")
		return 0, fmt.Errorf("empty api_name")
	}

	// Fail fast check if table exists if QuantDBFactory is available
	quantDB, err := GetQuantDBForPath(tc, targetDBPath)
	if err == nil {
		exists, err := quantDB.TableExists(ctx, tableName)
		if err != nil {
			logrus.Warnf("[writeRealtimeBatch] TableExists %s: %v, skip batch", tableName, err)
			return 0, err
		}
		if !exists {
			logrus.Warnf("[writeRealtimeBatch] table %s does not exist, skip batch (run create_tables first)", tableName)
			return 0, fmt.Errorf("table %s does not exist", tableName)
		}
	}

	wqIntf, ok := tc.GetDependency("QuantDBWriteQueue")
	var n int64
	if !ok || wqIntf == nil {
		if quantDB == nil {
			return 0, fmt.Errorf("no QuantDB nor WriteQueue available")
		}
		n, err = quantDB.BulkInsert(ctx, tableName, rows)
	} else {
		wq := wqIntf.(datastore.QuantDBWriteQueue)
		n, err = wq.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{
			Path:      targetDBPath,
			TableName: tableName,
			Data:      rows,
		})
	}

	if err != nil {
		logrus.Warnf("[writeRealtimeBatch] BulkInsert %s: %v", tableName, err)
		return 0, err
	}
	logrus.Printf("[writeRealtimeBatch] wrote api=%s source=%s rows=%d", apiName, source, n)
	return n, nil
}

// RealtimeQuoteStreamHandlerJob 为 Streaming Workflow 提供 DataHandler：从 ctx.Params["data"] 读取实时行情数据，
// 并调用 writeRealtimeBatchToDuckDB 写入 DuckDB。该 Job 不依赖 RealtimeBufferRegistry。
func RealtimeQuoteStreamHandlerJob(tc *task.TaskContext) (interface{}, error) {
	ctx := context.Background()
	targetDBPath := tc.GetParamString("target_db_path")
	if targetDBPath == "" {
		// Streaming 模式下，DataHandler 可能只收到 data/source 参数，此时尝试从数据本身推断 target_db_path。
		targetDBPath = inferTargetDBPathFromData(tc.GetParam("data"))
	}
	if targetDBPath == "" {
		return nil, fmt.Errorf("RealtimeQuoteStreamHandler: target_db_path is required")
	}
	// 删除了手动的 quantDB creation，改为委托给 writeRealtimeBatchToDuckDB（内部优先使用 QuantDBWriteQueue）。

	apiName := tc.GetParamString("api_name")
	if apiName == "" {
		apiName = "realtime_quote"
	}
	source := tc.GetParamString("source")

	raw := tc.GetParam("data")
	rows := normalizeRealtimeRows(raw)
	// 兼容 task-engine 将 DataArrivedPayload 整体放入 params["data"] 时，从 payload 取 source
	if source == "" && raw != nil {
		if m, ok := raw.(map[string]interface{}); ok {
			if s, _ := m["source"].(string); s != "" {
				source = s
			}
		}
	}
	if source == "" {
		source = defaultRealtimeSrc
	}
	// 删除每行中的 target_db_path 元数据，避免作为表字段写入 DuckDB。
	for _, r := range rows {
		delete(r, "target_db_path")
	}
	if len(rows) == 0 {
		return map[string]interface{}{"total_rows": 0}, nil
	}

	n, err := writeRealtimeBatchToDuckDB(ctx, tc, targetDBPath, apiName, source, rows)
	if err != nil {
		return nil, err
	}
	// 多源时仅当前选中源写 Store；Sina realtime_quote 写前映射为 Tushare 字段名，与 WS 推送结构一致
	if source == defaultRealtimeSrc && apiName == "realtime_quote" {
		sel, _ := tc.GetDependency("RealtimeSourceSelector")
		if sel != nil {
			if selector, ok := sel.(*realtimestore.RealtimeSourceSelector); ok && selector.ShouldWriteToStore(realtimestore.SourceSina) {
				store := realtimestore.DefaultLatestQuoteStore()
				for _, row := range rows {
					tsCode, _ := row["ts_code"].(string)
					if tsCode == "" {
						continue
					}
					mapped := sinaRowToTushareStoreRow(row)
					store.Update(tsCode, mapped)
				}
			}
		}
	}
	return map[string]interface{}{"total_rows": n}, nil
}

type tushareBatchState struct {
	firstArrival time.Time
	rows         []map[string]any
}

var (
	tushareBatchMu     sync.Mutex
	tushareBatchByAPI  = make(map[string]*tushareBatchState)
	tushareBatchSize   = 1000
	tushareBatchMaxAge = 30 * time.Second
)

// TushareTickDBBatchWriteJob 作为 db_sink 的 DataHandler：
// 按 api_name 聚合批次，满足 1000 条或等待超时 30s 时批量写入 DuckDB。
func TushareTickDBBatchWriteJob(tc *task.TaskContext) (interface{}, error) {
	ctx := context.Background()
	apiName := tc.GetParamString("api_name")
	if strings.TrimSpace(apiName) == "" {
		apiName = "ts_realtime_mkt_tick"
	}
	targetDBPath := tc.GetParamString("target_db_path")
	if targetDBPath == "" {
		targetDBPath = inferTargetDBPathFromData(tc.GetParam("data"))
	}
	if targetDBPath == "" {
		return nil, fmt.Errorf("TushareTickDBBatchWrite: target_db_path is required")
	}

	rows := normalizeRealtimeRows(tc.GetParam("data"))
	if len(rows) == 0 {
		return map[string]interface{}{"flushed_rows": 0, "batched_rows": 0}, nil
	}

	now := time.Now()
	tushareBatchMu.Lock()
	state := tushareBatchByAPI[apiName]
	if state == nil {
		state = &tushareBatchState{firstArrival: now, rows: make([]map[string]any, 0, tushareBatchSize)}
		tushareBatchByAPI[apiName] = state
	}
	for _, r := range rows {
		delete(r, "target_db_path")
		state.rows = append(state.rows, r)
	}

	shouldFlush := len(state.rows) >= tushareBatchSize || now.Sub(state.firstArrival) >= tushareBatchMaxAge
	if !shouldFlush {
		batched := len(state.rows)
		tushareBatchMu.Unlock()
		return map[string]interface{}{"flushed_rows": 0, "batched_rows": batched}, nil
	}
	toFlush := make([]map[string]any, len(state.rows))
	copy(toFlush, state.rows)
	state.rows = state.rows[:0]
	state.firstArrival = now
	tushareBatchMu.Unlock()

	quantDB, err := GetQuantDBForPath(tc, targetDBPath)
	if err != nil {
		return nil, fmt.Errorf("TushareTickDBBatchWrite: get QuantDB: %w", err)
	}
	if err := ensureTushareTickTable(ctx, quantDB, apiName); err != nil {
		return nil, err
	}

	n, err := writeRealtimeBatchToDuckDB(ctx, tc, targetDBPath, apiName, "tushare_ws", toFlush)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"flushed_rows": n,
		"batched_rows": 0,
	}, nil
}

// TushareTickFrontendPushJob 作为 frontend_sink 的 DataHandler：
// 将最新行情按 ts_code 写入进程内 LatestQuoteStore，供 ws 接口推送。
func TushareTickFrontendPushJob(tc *task.TaskContext) (interface{}, error) {
	rows := normalizeRealtimeRows(tc.GetParam("data"))
	if len(rows) == 0 {
		return map[string]interface{}{"updated": 0}, nil
	}
	store := realtimestore.DefaultLatestQuoteStore()
	updated := 0
	for _, row := range rows {
		tsCode, _ := row["ts_code"].(string)
		tsCode = strings.TrimSpace(tsCode)
		if tsCode == "" {
			continue
		}
		delete(row, "target_db_path")
		cp := make(map[string]interface{}, len(row))
		for k, v := range row {
			cp[k] = v
		}
		store.Update(tsCode, cp)
		updated++
	}
	return map[string]interface{}{"updated": updated}, nil
}

func ensureTushareTickTable(
	ctx context.Context,
	quantDB interface {
		TableExists(ctx context.Context, tableName string) (bool, error)
		Execute(ctx context.Context, sql string, args ...any) (int64, error)
	},
	apiName string,
) error {
	tableName := safeTableName(apiName)
	if tableName == "" {
		return fmt.Errorf("invalid api_name: %s", apiName)
	}
	exists, err := quantDB.TableExists(ctx, tableName)
	if err != nil {
		return fmt.Errorf("check table exists %s: %w", tableName, err)
	}
	if exists {
		return nil
	}
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		code VARCHAR,
		ts_code VARCHAR,
		name VARCHAR,
		trade_time TIMESTAMP,
		pre_price DOUBLE,
		price DOUBLE,
		open DOUBLE,
		high DOUBLE,
		low DOUBLE,
		close DOUBLE,
		open_int DOUBLE,
		volume DOUBLE,
		amount DOUBLE,
		num DOUBLE,
		ask_price1 DOUBLE,
		ask_volume1 DOUBLE,
		bid_price1 DOUBLE,
		bid_volume1 DOUBLE,
		ask_price2 DOUBLE,
		ask_volume2 DOUBLE,
		bid_price2 DOUBLE,
		bid_volume2 DOUBLE,
		ask_price3 DOUBLE,
		ask_volume3 DOUBLE,
		bid_price3 DOUBLE,
		bid_volume3 DOUBLE,
		ask_price4 DOUBLE,
		ask_volume4 DOUBLE,
		bid_price4 DOUBLE,
		bid_volume4 DOUBLE,
		ask_price5 DOUBLE,
		ask_volume5 DOUBLE,
		bid_price5 DOUBLE,
		bid_volume5 DOUBLE,
		sync_batch_id VARCHAR,
		created_at TIMESTAMP
	)`, tableName)
	if _, err := quantDB.Execute(ctx, ddl); err != nil {
		return fmt.Errorf("create table %s: %w", tableName, err)
	}
	return nil
}
