// Package jobs provides Task Engine job functions for QDHub workflows.
package jobs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/sirupsen/logrus"

	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/infrastructure/datasource/tushare/realtime"
	"qdhub/internal/infrastructure/quantdb/duckdb"
	"qdhub/internal/infrastructure/realtimebuffer"
)

const defaultRealtimeSrc = "sina"

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

	quantDB, err := GetQuantDBForPath(tc, targetDBPath)
	if err != nil {
		return nil, fmt.Errorf("RealtimeSyncDataHandler: get QuantDB: %w", err)
	}
	defer quantDB.Close()

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
		n, err := writeRealtimeBatchToDuckDB(ctx, quantDB, batch.APIName, batch.Source, mapsToAny(batch.Data))
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
	logrus.Infof("[RealtimeQuoteStreamHandler] infer target_db_path from data, raw_type=%T", raw)
	rows := normalizeRealtimeRows(raw)
	if len(rows) == 0 {
		logrus.Infof("[RealtimeQuoteStreamHandler] no rows to infer target_db_path")
		return ""
	}
	logrus.Infof("[RealtimeQuoteStreamHandler] first row keys=%v", func() []string {
		keys := make([]string, 0, len(rows[0]))
		for k := range rows[0] {
			keys = append(keys, k)
		}
		return keys
	}())
	if s, ok := rows[0]["target_db_path"].(string); ok && s != "" {
		logrus.Infof("[RealtimeQuoteStreamHandler] inferred target_db_path=%s", s)
		return s
	}
	logrus.Infof("[RealtimeQuoteStreamHandler] target_db_path not found in first row")
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
	quantDB interface {
		TableExists(ctx context.Context, tableName string) (bool, error)
		BulkInsert(ctx context.Context, tableName string, data []map[string]any) (int64, error)
	},
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
	exists, err := quantDB.TableExists(ctx, tableName)
	if err != nil {
		logrus.Warnf("[writeRealtimeBatch] TableExists %s: %v, skip batch", tableName, err)
		return 0, err
	}
	if !exists {
		logrus.Warnf("[writeRealtimeBatch] table %s does not exist, skip batch (run create_tables first)", tableName)
		return 0, fmt.Errorf("table %s does not exist", tableName)
	}
	n, err := quantDB.BulkInsert(ctx, tableName, rows)
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
	// Streaming 模式下，目前 TaskContext 中可能尚未注入 QuantDBFactory 依赖，
	// 这里直接通过 DuckDB Factory 按路径创建连接，保证落库成功。
	factory := duckdb.NewFactory()
	quantDB, err := factory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: targetDBPath,
	})
	if err != nil {
		return nil, fmt.Errorf("RealtimeQuoteStreamHandler: create QuantDB: %w", err)
	}
	defer quantDB.Close()

	apiName := tc.GetParamString("api_name")
	if apiName == "" {
		apiName = "realtime_quote"
	}
	source := tc.GetParamString("source")

	raw := tc.GetParam("data")
	rows := normalizeRealtimeRows(raw)
	// 删除每行中的 target_db_path 元数据，避免作为表字段写入 DuckDB。
	for _, r := range rows {
		delete(r, "target_db_path")
	}
	if len(rows) == 0 {
		return map[string]interface{}{"total_rows": 0}, nil
	}

	n, err := writeRealtimeBatchToDuckDB(ctx, quantDB, apiName, source, rows)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"total_rows": n}, nil
}
