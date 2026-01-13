// Package jobs provides Task Engine job functions for QDHub workflows.
package jobs

import (
	"context"
	"fmt"
	"log"

	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/infrastructure/datasource"
)

// ==================== 数据源 Job Functions ====================

// QueryDataJob queries data from a data source API.
// Input params:
//   - data_source_name: string - The data source name
//   - api_name: string - The API name to query
//   - params: map[string]interface{} - Query parameters
//   - token: string - API token
//
// Output:
//   - data: []map[string]interface{} - The query result
//   - total: int64 - Total record count
//   - has_more: bool - Whether there are more records
func QueryDataJob(tc *task.TaskContext) (interface{}, error) {
	// Get parameters
	dataSourceName := tc.GetParamString("data_source_name")
	apiName := tc.GetParamString("api_name")
	token := tc.GetParamString("token")

	if dataSourceName == "" || apiName == "" {
		return nil, fmt.Errorf("data_source_name and api_name are required")
	}

	log.Printf("📡 [QueryData] 开始查询: %s/%s", dataSourceName, apiName)

	// Get query params
	var params map[string]interface{}
	paramsParam := tc.GetParam("params")
	if paramsParam != nil {
		if p, ok := paramsParam.(map[string]interface{}); ok {
			params = p
		}
	}

	// Get registry from dependencies
	registryInterface, ok := tc.GetDependency("DataSourceRegistry")
	if !ok {
		return nil, fmt.Errorf("DataSourceRegistry dependency not found")
	}
	registry := registryInterface.(*datasource.Registry)

	// Get client
	client, err := registry.GetClient(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get client: %w", err)
	}

	// Set token
	if token != "" {
		client.SetToken(token)
	}

	// Query data
	ctx := context.Background()
	result, err := client.Query(ctx, apiName, params)
	if err != nil {
		return nil, fmt.Errorf("failed to query data: %w", err)
	}

	log.Printf("✅ [QueryData] 查询成功: %s, 记录数=%d", apiName, len(result.Data))

	return map[string]interface{}{
		"data":     result.Data,
		"total":    result.Total,
		"has_more": result.HasMore,
		"api_name": apiName,
	}, nil
}

// ValidateTokenJob validates a data source token.
// Input params:
//   - data_source_name: string - The data source name
//   - token: string - The token to validate
//
// Output:
//   - valid: bool - Whether the token is valid
//   - data_source_name: string - The data source name
func ValidateTokenJob(tc *task.TaskContext) (interface{}, error) {
	// Get parameters
	dataSourceName := tc.GetParamString("data_source_name")
	token := tc.GetParamString("token")

	if dataSourceName == "" || token == "" {
		return nil, fmt.Errorf("data_source_name and token are required")
	}

	log.Printf("🔑 [ValidateToken] 开始验证: %s", dataSourceName)

	// Get registry from dependencies
	registryInterface, ok := tc.GetDependency("DataSourceRegistry")
	if !ok {
		return nil, fmt.Errorf("DataSourceRegistry dependency not found")
	}
	registry := registryInterface.(*datasource.Registry)

	// Get client
	client, err := registry.GetClient(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get client: %w", err)
	}

	// Set token and validate
	client.SetToken(token)
	ctx := context.Background()
	valid, err := client.ValidateToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to validate token: %w", err)
	}

	if valid {
		log.Printf("✅ [ValidateToken] Token 有效: %s", dataSourceName)
	} else {
		log.Printf("⚠️ [ValidateToken] Token 无效: %s", dataSourceName)
	}

	return map[string]interface{}{
		"valid":            valid,
		"data_source_name": dataSourceName,
	}, nil
}

// TestConnectionJob tests connection to a data source.
// Input params:
//   - data_source_name: string - The data source name
//   - token: string - API token (optional)
//
// Output:
//   - connected: bool - Whether the connection is successful
//   - data_source_name: string - The data source name
//   - latency_ms: int64 - Connection latency in milliseconds
func TestConnectionJob(tc *task.TaskContext) (interface{}, error) {
	// Get parameters
	dataSourceName := tc.GetParamString("data_source_name")
	token := tc.GetParamString("token")

	if dataSourceName == "" {
		return nil, fmt.Errorf("data_source_name is required")
	}

	log.Printf("🔌 [TestConnection] 测试连接: %s", dataSourceName)

	// Get registry from dependencies
	registryInterface, ok := tc.GetDependency("DataSourceRegistry")
	if !ok {
		return nil, fmt.Errorf("DataSourceRegistry dependency not found")
	}
	registry := registryInterface.(*datasource.Registry)

	// Get client
	client, err := registry.GetClient(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get client: %w", err)
	}

	// Set token if provided
	if token != "" {
		client.SetToken(token)
	}

	// Test connection by validating token or making a simple query
	ctx := context.Background()
	connected := true
	var latencyMs int64 = 0

	// Use ValidateToken as connection test
	_, err = client.ValidateToken(ctx)
	if err != nil {
		connected = false
		log.Printf("⚠️ [TestConnection] 连接失败: %s, error=%v", dataSourceName, err)
	} else {
		log.Printf("✅ [TestConnection] 连接成功: %s", dataSourceName)
	}

	return map[string]interface{}{
		"connected":        connected,
		"data_source_name": dataSourceName,
		"latency_ms":       latencyMs,
	}, nil
}
