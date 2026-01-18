// Package jobs provides Task Engine job functions for QDHub workflows.
package jobs

import (
	"context"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/datasource"
)

// ==================== 上游任务结果提取辅助函数 ====================

// extractFromUpstream 从上游任务结果中提取数据
// Task Engine 会将上游任务的返回值存储在 _cached_<TaskName> 参数中
func extractFromUpstream(tc *task.TaskContext, taskName string) map[string]interface{} {
	// 尝试从 _cached_ 参数中获取
	key := "_cached_" + taskName
	if upstream := tc.GetParam(key); upstream != nil {
		if upstreamMap, ok := upstream.(map[string]interface{}); ok {
			return upstreamMap
		}
	}
	return nil
}

// extractAPIURLsFromUpstream 从上游任务结果中提取 API URLs
func extractAPIURLsFromUpstream(tc *task.TaskContext) []string {
	var apiURLs []string

	// 遍历所有参数查找 _cached_ 前缀的参数
	for key, val := range tc.Params {
		if !strings.HasPrefix(key, "_cached_") {
			continue
		}

		// 尝试从结果中提取 api_urls
		if resultMap, ok := val.(map[string]interface{}); ok {
			if urlsRaw, ok := resultMap["api_urls"]; ok {
				switch v := urlsRaw.(type) {
				case []string:
					return v
				case []interface{}:
					for _, item := range v {
						if s, ok := item.(string); ok {
							apiURLs = append(apiURLs, s)
						}
					}
					return apiURLs
				}
			}
		}
	}

	return apiURLs
}

// extractCategoriesFromUpstream 从上游任务结果中提取 categories
func extractCategoriesFromUpstream(tc *task.TaskContext) []map[string]interface{} {
	// 遍历所有参数查找 _cached_ 前缀的参数
	for key, val := range tc.Params {
		if !strings.HasPrefix(key, "_cached_") {
			continue
		}

		// 尝试从结果中提取 categories
		if resultMap, ok := val.(map[string]interface{}); ok {
			if categoriesRaw, ok := resultMap["categories"]; ok {
				switch v := categoriesRaw.(type) {
				case []map[string]interface{}:
					return v
				case []interface{}:
					categories := make([]map[string]interface{}, 0, len(v))
					for _, item := range v {
						if m, ok := item.(map[string]interface{}); ok {
							categories = append(categories, m)
						}
					}
					return categories
				}
			}
		}
	}

	return nil
}

// NOTE: getParamKeys 已移至 sync_jobs.go

// MetadataJobDeps defines dependencies for metadata job functions.
type MetadataJobDeps struct {
	// DataSourceRegistry is the data source adapter registry.
	DataSourceRegistry *datasource.Registry
	// MetadataRepo is the metadata repository.
	MetadataRepo metadata.Repository
}

// ==================== 元数据刷新工作流 Job Functions ====================

// FetchCatalogJob fetches the API catalog from a data source.
// Input params:
//   - data_source_id: string - The data source ID
//   - data_source_name: string - The data source name (e.g., "tushare")
//
// Output:
//   - catalog_content: string - The raw HTML content
//   - doc_type: string - The document type
func FetchCatalogJob(tc *task.TaskContext) (interface{}, error) {
	// Get parameters
	dataSourceID := tc.GetParamString("data_source_id")
	dataSourceName := tc.GetParamString("data_source_name")

	if dataSourceID == "" || dataSourceName == "" {
		return nil, fmt.Errorf("data_source_id and data_source_name are required")
	}

	// Get registry from dependencies
	registryInterface, ok := tc.GetDependency("DataSourceRegistry")
	if !ok {
		return nil, fmt.Errorf("DataSourceRegistry dependency not found")
	}
	registry := registryInterface.(*datasource.Registry)

	// Get crawler
	crawler, err := registry.GetCrawler(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get crawler: %w", err)
	}

	// Fetch catalog page
	ctx := context.Background()
	content, docType, err := crawler.FetchCatalogPage(ctx, shared.ID(dataSourceID))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch catalog: %w", err)
	}

	return map[string]interface{}{
		"catalog_content": content,
		"doc_type":        string(docType),
		"data_source_id":  dataSourceID,
	}, nil
}

// ParseCatalogJob parses the catalog content to extract categories and API URLs.
// Input params:
//   - catalog_content: string - The raw HTML content (from FetchCatalogJob)
//   - doc_type: string - The document type
//   - data_source_id: string - The data source ID
//   - data_source_name: string - The data source name
//
// Output:
//   - categories: []APICategory - The parsed categories
//   - api_urls: []string - The API detail page URLs
func ParseCatalogJob(tc *task.TaskContext) (interface{}, error) {
	// Get parameters (may come from upstream task)
	catalogContent := tc.GetParamString("catalog_content")
	if catalogContent == "" {
		// Try to get from cached upstream result
		// Task Engine stores upstream results as _cached_<TaskID> or _cached_<TaskName>
		// First try by task name
		upstream := tc.GetParam("_cached_FetchCatalog")
		if upstream != nil {
			if upstreamMap, ok := upstream.(map[string]interface{}); ok {
				if content, ok := upstreamMap["catalog_content"].(string); ok {
					catalogContent = content
				}
			}
		}

		// If not found, try to find from any _cached_ parameter
		// Task Engine may use task ID instead of task name
		if catalogContent == "" {
			for key, val := range tc.Params {
				if strings.HasPrefix(key, "_cached_") {
					if upstreamMap, ok := val.(map[string]interface{}); ok {
						if content, ok := upstreamMap["catalog_content"].(string); ok {
							catalogContent = content
							logrus.Printf("Found catalog_content from %s", key)
							break
						}
					}
				}
			}
		}
	}

	dataSourceID := tc.GetParamString("data_source_id")
	dataSourceName := tc.GetParamString("data_source_name")

	if catalogContent == "" {
		return nil, fmt.Errorf("catalog_content is required")
	}

	// Get registry from dependencies
	registryInterface, ok := tc.GetDependency("DataSourceRegistry")
	if !ok {
		return nil, fmt.Errorf("DataSourceRegistry dependency not found")
	}
	registry := registryInterface.(*datasource.Registry)

	// Get parser
	parser, err := registry.GetParser(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get parser: %w", err)
	}

	// Parse catalog
	categories, apiURLs, err := parser.ParseCatalog(catalogContent)
	if err != nil {
		return nil, fmt.Errorf("failed to parse catalog: %w", err)
	}

	// Convert categories to serializable format
	categoryMaps := make([]map[string]interface{}, len(categories))
	for i, cat := range categories {
		categoryMaps[i] = map[string]interface{}{
			"id":             cat.ID.String(),
			"data_source_id": cat.DataSourceID.String(),
			"name":           cat.Name,
			"description":    cat.Description,
			"doc_path":       cat.DocPath,
			"sort_order":     cat.SortOrder,
		}
		if cat.ParentID != nil {
			categoryMaps[i]["parent_id"] = cat.ParentID.String()
		}
	}

	return map[string]interface{}{
		"categories":     categoryMaps,
		"api_urls":       apiURLs,
		"api_count":      len(apiURLs),
		"data_source_id": dataSourceID,
	}, nil
}

// SaveCategoriesJob saves the parsed categories to the repository.
// Input params:
//   - categories: []map[string]interface{} - The categories (from ParseCatalogJob)
//   - data_source_id: string - The data source ID
//
// Output:
//   - saved_count: int - Number of categories saved
func SaveCategoriesJob(tc *task.TaskContext) (interface{}, error) {
	// Get parameters
	dataSourceID := tc.GetParamString("data_source_id")
	if dataSourceID == "" {
		return nil, fmt.Errorf("data_source_id is required")
	}

	// Validate data source exists
	repoInterface, ok := tc.GetDependency("MetadataRepo")
	if !ok {
		return nil, fmt.Errorf("MetadataRepo dependency not found")
	}
	repo := repoInterface.(metadata.Repository)

	ctx := context.Background()
	ds, err := repo.GetDataSource(ctx, shared.ID(dataSourceID))
	if err != nil {
		return nil, fmt.Errorf("failed to check data source existence: %w", err)
	}
	if ds == nil {
		return nil, fmt.Errorf("data source not found: %s", dataSourceID)
	}

	categoriesParam := tc.GetParam("categories")
	if categoriesParam == nil {
		// Try to get from cached upstream result by task name first
		upstream := tc.GetParam("_cached_ParseCatalog")
		if upstream != nil {
			if upstreamMap, ok := upstream.(map[string]interface{}); ok {
				categoriesParam = upstreamMap["categories"]
			}
		}

		// If not found, try to find from any _cached_ parameter
		// Task Engine may use task ID instead of task name
		if categoriesParam == nil {
			categoryMaps := extractCategoriesFromUpstream(tc)
			if categoryMaps != nil {
				// Convert to interface{} for compatibility
				categoriesParam = categoryMaps
			}
		}
	}

	if categoriesParam == nil {
		return nil, fmt.Errorf("categories is required")
	}

	// repo already obtained above for validation

	// Convert categories from maps
	categoryMaps, ok := categoriesParam.([]map[string]interface{})
	if !ok {
		// Try interface slice
		if ifaces, ok := categoriesParam.([]interface{}); ok {
			categoryMaps = make([]map[string]interface{}, len(ifaces))
			for i, iface := range ifaces {
				if m, ok := iface.(map[string]interface{}); ok {
					categoryMaps[i] = m
				}
			}
		}
	}

	categories := make([]metadata.APICategory, 0, len(categoryMaps))
	for _, m := range categoryMaps {
		cat := metadata.APICategory{
			DataSourceID: shared.ID(dataSourceID),
		}
		if id, ok := m["id"].(string); ok {
			cat.ID = shared.ID(id)
		}
		if name, ok := m["name"].(string); ok {
			cat.Name = name
		}
		if desc, ok := m["description"].(string); ok {
			cat.Description = desc
		}
		if docPath, ok := m["doc_path"].(string); ok {
			cat.DocPath = docPath
		}
		if sortOrder, ok := m["sort_order"].(int); ok {
			cat.SortOrder = sortOrder
		}
		if parentID, ok := m["parent_id"].(string); ok && parentID != "" {
			pid := shared.ID(parentID)
			cat.ParentID = &pid
		}
		categories = append(categories, cat)
	}

	// Save categories (ctx already obtained above)
	logrus.Debugf("SaveCategoriesJob: Attempting to save %d categories for data_source_id=%s", len(categories), dataSourceID)
	if err := repo.SaveCategories(ctx, categories); err != nil {
		logrus.Errorf("SaveCategoriesJob: Failed to save categories: %v", err)
		return nil, fmt.Errorf("failed to save categories: %w", err)
	}
	logrus.Debugf("SaveCategoriesJob: Successfully saved %d categories", len(categories))

	return map[string]interface{}{
		"saved_count":    len(categories),
		"data_source_id": dataSourceID,
	}, nil
}

// FetchAPIDetailJob fetches a single API detail page.
// This is typically used as a sub-task in a template task.
// Input params:
//   - api_url: string - The API detail page URL
//   - data_source_name: string - The data source name
//
// Output:
//   - api_content: string - The raw HTML content
//   - doc_type: string - The document type
//   - api_url: string - The original URL
func FetchAPIDetailJob(tc *task.TaskContext) (interface{}, error) {
	// Get parameters
	apiURL := tc.GetParamString("api_url")
	dataSourceName := tc.GetParamString("data_source_name")

	if apiURL == "" || dataSourceName == "" {
		return nil, fmt.Errorf("api_url and data_source_name are required")
	}

	// Get registry from dependencies
	registryInterface, ok := tc.GetDependency("DataSourceRegistry")
	if !ok {
		return nil, fmt.Errorf("DataSourceRegistry dependency not found")
	}
	registry := registryInterface.(*datasource.Registry)

	// Get crawler
	crawler, err := registry.GetCrawler(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get crawler: %w", err)
	}

	// Fetch API detail page
	ctx := context.Background()
	content, docType, err := crawler.FetchAPIDetailPage(ctx, apiURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch API detail: %w", err)
	}

	return map[string]interface{}{
		"api_content": content,
		"doc_type":    string(docType),
		"api_url":     apiURL,
	}, nil
}

// ParseAPIDetailJob parses an API detail page to extract metadata.
// Input params:
//   - api_content: string - The raw HTML content (from FetchAPIDetailJob)
//   - data_source_id: string - The data source ID
//   - data_source_name: string - The data source name
//
// Output:
//   - api_metadata: map[string]interface{} - The parsed API metadata
func ParseAPIDetailJob(tc *task.TaskContext) (interface{}, error) {
	// Get parameters
	apiContent := tc.GetParamString("api_content")
	if apiContent == "" {
		// Try to get from cached upstream result
		upstream := tc.GetParam("_cached_FetchAPIDetail")
		if upstream != nil {
			if upstreamMap, ok := upstream.(map[string]interface{}); ok {
				if content, ok := upstreamMap["api_content"].(string); ok {
					apiContent = content
				}
			}
		}
	}

	dataSourceID := tc.GetParamString("data_source_id")
	dataSourceName := tc.GetParamString("data_source_name")

	if apiContent == "" {
		return nil, fmt.Errorf("api_content is required")
	}

	// Get registry from dependencies
	registryInterface, ok := tc.GetDependency("DataSourceRegistry")
	if !ok {
		return nil, fmt.Errorf("DataSourceRegistry dependency not found")
	}
	registry := registryInterface.(*datasource.Registry)

	// Get parser
	parser, err := registry.GetParser(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get parser: %w", err)
	}

	// Parse API detail
	apiMetadata, err := parser.ParseAPIDetail(apiContent)
	if err != nil {
		return nil, fmt.Errorf("failed to parse API detail: %w", err)
	}

	// Override data source ID
	apiMetadata.DataSourceID = shared.ID(dataSourceID)

	// Convert to serializable format
	result := map[string]interface{}{
		"id":             apiMetadata.ID.String(),
		"data_source_id": apiMetadata.DataSourceID.String(),
		"name":           apiMetadata.Name,
		"display_name":   apiMetadata.DisplayName,
		"description":    apiMetadata.Description,
		"endpoint":       apiMetadata.Endpoint,
		"permission":     apiMetadata.Permission,
		"status":         string(apiMetadata.Status),
	}

	// Add params and fields
	paramsJSON, _ := apiMetadata.MarshalRequestParamsJSON()
	fieldsJSON, _ := apiMetadata.MarshalResponseFieldsJSON()
	rateLimitJSON, _ := apiMetadata.MarshalRateLimitJSON()

	result["request_params"] = paramsJSON
	result["response_fields"] = fieldsJSON
	result["rate_limit"] = rateLimitJSON

	return map[string]interface{}{
		"api_metadata":   result,
		"data_source_id": dataSourceID,
	}, nil
}

// SaveAPIMetadataJob saves API metadata to the repository.
// Input params:
//   - api_metadata: map[string]interface{} - The API metadata (from ParseAPIDetailJob)
//
// Output:
//   - saved: bool - Whether the metadata was saved successfully
func SaveAPIMetadataJob(tc *task.TaskContext) (interface{}, error) {
	// Get parameters
	apiMetadataParam := tc.GetParam("api_metadata")
	if apiMetadataParam == nil {
		// Try to get from cached upstream result
		upstream := tc.GetParam("_cached_ParseAPIDetail")
		if upstream != nil {
			if upstreamMap, ok := upstream.(map[string]interface{}); ok {
				apiMetadataParam = upstreamMap["api_metadata"]
			}
		}
	}

	if apiMetadataParam == nil {
		return nil, fmt.Errorf("api_metadata is required")
	}

	// Get repository from dependencies
	repoInterface, ok := tc.GetDependency("MetadataRepo")
	if !ok {
		return nil, fmt.Errorf("MetadataRepo dependency not found")
	}
	repo := repoInterface.(metadata.Repository)

	// Convert from map
	metadataMap, ok := apiMetadataParam.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid api_metadata format")
	}

	apiMetadata := &metadata.APIMetadata{
		Status: shared.StatusActive,
	}

	if id, ok := metadataMap["id"].(string); ok {
		apiMetadata.ID = shared.ID(id)
	}
	if dsID, ok := metadataMap["data_source_id"].(string); ok {
		apiMetadata.DataSourceID = shared.ID(dsID)
	}
	if name, ok := metadataMap["name"].(string); ok {
		apiMetadata.Name = name
	}
	if displayName, ok := metadataMap["display_name"].(string); ok {
		apiMetadata.DisplayName = displayName
	}
	if desc, ok := metadataMap["description"].(string); ok {
		apiMetadata.Description = desc
	}
	if endpoint, ok := metadataMap["endpoint"].(string); ok {
		apiMetadata.Endpoint = endpoint
	}
	if permission, ok := metadataMap["permission"].(string); ok {
		apiMetadata.Permission = permission
	}

	// Parse JSON fields
	if paramsJSON, ok := metadataMap["request_params"].(string); ok && paramsJSON != "" {
		_ = apiMetadata.UnmarshalRequestParamsJSON(paramsJSON)
	}
	if fieldsJSON, ok := metadataMap["response_fields"].(string); ok && fieldsJSON != "" {
		_ = apiMetadata.UnmarshalResponseFieldsJSON(fieldsJSON)
	}
	if rateLimitJSON, ok := metadataMap["rate_limit"].(string); ok && rateLimitJSON != "" {
		_ = apiMetadata.UnmarshalRateLimitJSON(rateLimitJSON)
	}

	// Save to repository
	ctx := context.Background()
	if err := repo.SaveAPIMetadata(ctx, apiMetadata); err != nil {
		return nil, fmt.Errorf("failed to save API metadata: %w", err)
	}

	return map[string]interface{}{
		"saved":    true,
		"api_id":   apiMetadata.ID.String(),
		"api_name": apiMetadata.Name,
	}, nil
}

// SaveAPIMetadataBatchJob batch saves API metadata from all FetchAPIDetail sub-tasks.
// Note: extractAPIMetadataFromUpstream is defined in table_jobs.go
// Input params:
//   - data_source_id: string - The data source ID (required, will be validated)
//
// Output:
//   - saved_count: int - Number of API metadata saved
func SaveAPIMetadataBatchJob(tc *task.TaskContext) (interface{}, error) {
	// Get parameters
	dataSourceID := tc.GetParamString("data_source_id")
	if dataSourceID == "" {
		return nil, fmt.Errorf("data_source_id is required")
	}

	// Validate data source exists
	repoInterface, ok := tc.GetDependency("MetadataRepo")
	if !ok {
		return nil, fmt.Errorf("MetadataRepo dependency not found")
	}
	repo := repoInterface.(metadata.Repository)

	ctx := context.Background()
	ds, err := repo.GetDataSource(ctx, shared.ID(dataSourceID))
	if err != nil {
		return nil, fmt.Errorf("failed to check data source existence: %w", err)
	}
	if ds == nil {
		return nil, fmt.Errorf("data source not found: %s", dataSourceID)
	}

	// Extract api_metadata from all upstream sub-tasks
	// FetchAPIDetail sub-tasks call ParseAPIDetail, which returns api_metadata
	apiMetadataMaps := extractAPIMetadataFromUpstream(tc)
	if len(apiMetadataMaps) == 0 {
		logrus.Warnf("SaveAPIMetadataBatchJob: No api_metadata found from upstream tasks")
		return map[string]interface{}{
			"saved_count": 0,
			"message":     "No API metadata to save",
		}, nil
	}

	logrus.Debugf("SaveAPIMetadataBatchJob: Found %d API metadata from upstream tasks", len(apiMetadataMaps))

	// Convert maps to APIMetadata entities
	apiMetadataList := make([]metadata.APIMetadata, 0, len(apiMetadataMaps))
	for i, m := range apiMetadataMaps {
		apiMetadata := &metadata.APIMetadata{
			Status: shared.StatusActive,
		}

		if id, ok := m["id"].(string); ok {
			apiMetadata.ID = shared.ID(id)
		}
		if dsID, ok := m["data_source_id"].(string); ok {
			apiMetadata.DataSourceID = shared.ID(dsID)
		} else {
			// Fallback to the provided data_source_id
			apiMetadata.DataSourceID = shared.ID(dataSourceID)
		}
		if name, ok := m["name"].(string); ok {
			apiMetadata.Name = name
		}
		if displayName, ok := m["display_name"].(string); ok {
			apiMetadata.DisplayName = displayName
		}
		if desc, ok := m["description"].(string); ok {
			apiMetadata.Description = desc
		}
		if endpoint, ok := m["endpoint"].(string); ok {
			apiMetadata.Endpoint = endpoint
		}
		if permission, ok := m["permission"].(string); ok {
			apiMetadata.Permission = permission
		}

		// Parse JSON fields
		if paramsJSON, ok := m["request_params"].(string); ok && paramsJSON != "" {
			_ = apiMetadata.UnmarshalRequestParamsJSON(paramsJSON)
		}
		if fieldsJSON, ok := m["response_fields"].(string); ok && fieldsJSON != "" {
			_ = apiMetadata.UnmarshalResponseFieldsJSON(fieldsJSON)
		}
		if rateLimitJSON, ok := m["rate_limit"].(string); ok && rateLimitJSON != "" {
			_ = apiMetadata.UnmarshalRateLimitJSON(rateLimitJSON)
		}

		// Validate required fields
		if apiMetadata.ID == "" || apiMetadata.Name == "" {
			logrus.Warnf("SaveAPIMetadataBatchJob: Skipping invalid API metadata at index %d (missing ID or name)", i)
			continue
		}

		apiMetadataList = append(apiMetadataList, *apiMetadata)
	}

	if len(apiMetadataList) == 0 {
		return map[string]interface{}{
			"saved_count": 0,
			"message":     "No valid API metadata to save",
		}, nil
	}

	// Batch save to repository
	if err := repo.SaveAPIMetadataBatch(ctx, apiMetadataList); err != nil {
		return nil, fmt.Errorf("failed to batch save API metadata: %w", err)
	}

	logrus.Debugf("SaveAPIMetadataBatchJob: Successfully saved %d API metadata", len(apiMetadataList))

	return map[string]interface{}{
		"saved_count":    len(apiMetadataList),
		"data_source_id": dataSourceID,
	}, nil
}

// NOTE: QueryDataJob 和 ValidateTokenJob 已移至 datasource_jobs.go

// ==================== 模板任务 Job Functions ====================

// GenerateAPIDetailFetchSubTasksJob 是模板任务的 Job Function
// 从上游 ParseCatalogJob 的结果中提取 api_urls，为每个 URL 生成一个子任务
//
// Input params:
//   - data_source_id: string - The data source ID
//   - data_source_name: string - The data source name
//   - max_api_crawl: int - Maximum number of APIs to crawl (0 = no limit)
//
// Output:
//   - status: string - Operation status
//   - generated: int - Number of sub-tasks generated
//   - sub_tasks: []map[string]interface{} - Information about generated sub-tasks
func GenerateAPIDetailFetchSubTasksJob(tc *task.TaskContext) (interface{}, error) {
	logrus.Printf("📋 [GenerateAPIDetailFetchSubTasks] Job Function 执行, Params: %v", getParamKeys(tc.Params))

	// Get parameters
	dataSourceID := tc.GetParamString("data_source_id")
	dataSourceName := tc.GetParamString("data_source_name")
	maxAPICrawl, _ := tc.GetParamInt("max_api_crawl")

	// Get Engine from dependencies
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		return nil, fmt.Errorf("[GenerateAPIDetailFetchSubTasks] Engine dependency not found")
	}
	eng, ok := engineInterface.(*engine.Engine)
	if !ok {
		return nil, fmt.Errorf("[GenerateAPIDetailFetchSubTasks] Engine type conversion failed")
	}

	registry := eng.GetRegistry()
	if registry == nil {
		return nil, fmt.Errorf("[GenerateAPIDetailFetchSubTasks] cannot get Registry from Engine")
	}

	// Extract API URLs from upstream task result
	apiURLs := extractAPIURLsFromUpstream(tc)
	if len(apiURLs) == 0 {
		logrus.Printf("⚠️ [GenerateAPIDetailFetchSubTasks] 未找到 api_urls，Params keys: %v", getParamKeys(tc.Params))
		return map[string]interface{}{
			"status":    "no_data",
			"generated": 0,
			"message":   "未找到 api_urls，跳过子任务生成",
		}, nil
	}

	// Apply limit if specified
	if maxAPICrawl > 0 && len(apiURLs) > maxAPICrawl {
		logrus.Printf("📡 [GenerateAPIDetailFetchSubTasks] 限制爬取数量从 %d 到 %d", len(apiURLs), maxAPICrawl)
		apiURLs = apiURLs[:maxAPICrawl]
	}

	logrus.Printf("📡 [GenerateAPIDetailFetchSubTasks] 从上游任务获取到 %d 个 API URLs，开始生成子任务", len(apiURLs))

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID
	generatedCount := 0

	var subTaskInfos []map[string]interface{}
	for _, apiURL := range apiURLs {
		subTaskName := fmt.Sprintf("FetchAPIDetail_%d", generatedCount+1)
		subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("爬取 API 详情: %s", apiURL), registry).
			WithJobFunction("FetchAPIDetail", map[string]interface{}{
				"api_url":          apiURL,
				"data_source_id":   dataSourceID,
				"data_source_name": dataSourceName,
			}).
			WithTaskHandler(task.TaskStatusSuccess, "MetadataRefreshSuccess").
			WithTaskHandler(task.TaskStatusFailed, "MetadataRefreshFailure").
			Build()
		if err != nil {
			logrus.Printf("❌ [GenerateAPIDetailFetchSubTasks] 创建子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		bgCtx := context.Background()
		if err := eng.AddSubTaskToInstance(bgCtx, workflowInstanceID, subTask, parentTaskID); err != nil {
			logrus.Printf("❌ [GenerateAPIDetailFetchSubTasks] 添加子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		generatedCount++
		subTaskInfos = append(subTaskInfos, map[string]interface{}{
			"name":    subTaskName,
			"api_url": apiURL,
		})
		logrus.Printf("✅ [GenerateAPIDetailFetchSubTasks] 子任务已添加: %s (url=%s)", subTaskName, apiURL)
	}

	logrus.Printf("✅ [GenerateAPIDetailFetchSubTasks] 共生成 %d 个子任务", generatedCount)

	return map[string]interface{}{
		"status":    "success",
		"generated": generatedCount,
		"sub_tasks": subTaskInfos,
	}, nil
}

// GenerateAPIParseSubTasksJob 是模板任务的 Job Function
// 用于为已获取的 API 详情内容生成解析子任务
//
// Input params:
//   - data_source_id: string - The data source ID
//   - data_source_name: string - The data source name
//
// Output:
//   - status: string - Operation status
//   - generated: int - Number of sub-tasks generated
func GenerateAPIParseSubTasksJob(tc *task.TaskContext) (interface{}, error) {
	logrus.Printf("📋 [GenerateAPIParseSubTasks] Job Function 执行, Params: %v", getParamKeys(tc.Params))

	// Get parameters
	dataSourceID := tc.GetParamString("data_source_id")
	dataSourceName := tc.GetParamString("data_source_name")

	// Get Engine from dependencies
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		return nil, fmt.Errorf("[GenerateAPIParseSubTasks] Engine dependency not found")
	}
	eng, ok := engineInterface.(*engine.Engine)
	if !ok {
		return nil, fmt.Errorf("[GenerateAPIParseSubTasks] Engine type conversion failed")
	}

	registry := eng.GetRegistry()
	if registry == nil {
		return nil, fmt.Errorf("[GenerateAPIParseSubTasks] cannot get Registry from Engine")
	}

	// 从上游任务获取 API 内容
	// 这个任务依赖于 FetchAPIDetail 子任务的结果
	apiContent := tc.GetParamString("api_content")
	apiURL := tc.GetParamString("api_url")

	// 也尝试从 _cached_ 参数获取
	if apiContent == "" {
		if upstream := extractFromUpstream(tc, "FetchAPIDetail"); upstream != nil {
			if content, ok := upstream["api_content"].(string); ok {
				apiContent = content
			}
			if url, ok := upstream["api_url"].(string); ok && apiURL == "" {
				apiURL = url
			}
		}
	}

	if apiContent == "" {
		return map[string]interface{}{
			"status":    "no_data",
			"generated": 0,
			"message":   "未找到 api_content，跳过子任务生成",
		}, nil
	}

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID

	// 创建解析子任务
	subTaskName := fmt.Sprintf("ParseAPIDetail_%s", apiURL)
	subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("解析 API 详情: %s", apiURL), registry).
		WithJobFunction("ParseAPIDetail", map[string]interface{}{
			"api_content":      apiContent,
			"data_source_id":   dataSourceID,
			"data_source_name": dataSourceName,
		}).
		WithTaskHandler(task.TaskStatusSuccess, "MetadataRefreshSuccess").
		WithTaskHandler(task.TaskStatusFailed, "MetadataRefreshFailure").
		Build()
	if err != nil {
		return nil, fmt.Errorf("[GenerateAPIParseSubTasks] failed to create sub-task: %w", err)
	}

	bgCtx := context.Background()
	if err := eng.AddSubTaskToInstance(bgCtx, workflowInstanceID, subTask, parentTaskID); err != nil {
		return nil, fmt.Errorf("[GenerateAPIParseSubTasks] failed to add sub-task: %w", err)
	}

	logrus.Printf("✅ [GenerateAPIParseSubTasks] 子任务已添加: %s", subTaskName)

	return map[string]interface{}{
		"status":    "success",
		"generated": 1,
		"sub_tasks": []map[string]interface{}{
			{
				"name":    subTaskName,
				"api_url": apiURL,
			},
		},
	}, nil
}
