// Package workflows provides built-in workflow definitions for QDHub.
package workflows

import "fmt"

// BuiltInWorkflowID 内建workflow的固定ID
const (
	BuiltInWorkflowIDMetadataCrawl    = "builtin:metadata_crawl"
	BuiltInWorkflowIDCreateTables     = "builtin:create_tables"
	BuiltInWorkflowIDBatchDataSync    = "builtin:batch_data_sync"
	BuiltInWorkflowIDRealtimeDataSync = "builtin:realtime_data_sync"
	BuiltInWorkflowIDNewsRealtimeSync = "builtin:news_realtime_sync"
)

// BuiltInWorkflowName 内建workflow的英文名称（用于API调用）
const (
	BuiltInWorkflowNameMetadataCrawl    = "metadata_crawl"
	BuiltInWorkflowNameCreateTables     = "create_tables"
	BuiltInWorkflowNameBatchDataSync    = "batch_data_sync"
	BuiltInWorkflowNameRealtimeDataSync = "realtime_data_sync"
	BuiltInWorkflowNameNewsRealtimeSync = "news_realtime_sync"
)

// BuiltInWorkflowMeta 内建workflow元数据
type BuiltInWorkflowMeta struct {
	ID          string
	Name        string // 中文显示名称
	APIName     string // API调用使用的英文名称
	Description string
	Category    string // "metadata" or "sync"
}

// GetBuiltInWorkflows 返回所有内建workflow的元数据列表
func GetBuiltInWorkflows() []BuiltInWorkflowMeta {
	return []BuiltInWorkflowMeta{
		{
			ID:          BuiltInWorkflowIDMetadataCrawl,
			Name:        "元数据爬取",
			APIName:     BuiltInWorkflowNameMetadataCrawl,
			Description: "从数据源爬取API文档并保存元数据",
			Category:    "metadata",
		},
		{
			ID:          BuiltInWorkflowIDCreateTables,
			Name:        "创建数据表",
			APIName:     BuiltInWorkflowNameCreateTables,
			Description: "根据元数据创建数据表结构",
			Category:    "metadata",
		},
		{
			ID:          BuiltInWorkflowIDBatchDataSync,
			Name:        "批量数据同步",
			APIName:     BuiltInWorkflowNameBatchDataSync,
			Description: "批量同步历史数据到目标数据库",
			Category:    "sync",
		},
		{
			ID:          BuiltInWorkflowIDRealtimeDataSync,
			Name:        "实时数据同步",
			APIName:     BuiltInWorkflowNameRealtimeDataSync,
			Description: "实时增量同步数据到目标数据库",
			Category:    "sync",
		},
		{
			ID:          BuiltInWorkflowIDNewsRealtimeSync,
			Name:        "新闻实时同步",
			APIName:     BuiltInWorkflowNameNewsRealtimeSync,
			Description: "按 news_sync_checkpoint 每分钟/每小时增量拉取 Tushare 新闻快讯",
			Category:    "sync",
		},
	}
}

// GetBuiltInWorkflowIDByName 通过API名称获取固定ID
func GetBuiltInWorkflowIDByName(apiName string) (string, error) {
	meta, err := GetBuiltInWorkflowMetaByName(apiName)
	if err != nil {
		return "", err
	}
	return meta.ID, nil
}

// GetBuiltInWorkflowMetaByName 通过API名称获取元数据
func GetBuiltInWorkflowMetaByName(apiName string) (*BuiltInWorkflowMeta, error) {
	for _, meta := range GetBuiltInWorkflows() {
		if meta.APIName == apiName {
			return &meta, nil
		}
	}
	return nil, fmt.Errorf("built-in workflow not found: %s", apiName)
}

// IsBuiltInWorkflow 检查是否为内建workflow ID
func IsBuiltInWorkflow(id string) bool {
	for _, meta := range GetBuiltInWorkflows() {
		if meta.ID == id {
			return true
		}
	}
	return false
}
