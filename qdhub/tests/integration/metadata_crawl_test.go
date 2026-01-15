//go:build integration
// +build integration

package integration

import (
	"testing"

	"qdhub/internal/infrastructure/taskengine/workflows"
)

// ==================== Metadata Crawl Workflow 集成测试 ====================

// TestMetadataCrawlWorkflow_Build 测试工作流构建
func TestMetadataCrawlWorkflow_Build(t *testing.T) {
	t.Run("Valid params should create builder", func(t *testing.T) {
		builder := workflows.NewMetadataCrawlWorkflowBuilder(nil).
			WithDataSource("ds-001", "tushare").
			WithMaxAPICrawl(100)

		if builder == nil {
			t.Error("Builder should not be nil")
		}
	})

	t.Run("Builder chaining", func(t *testing.T) {
		builder := workflows.NewMetadataCrawlWorkflowBuilder(nil).
			WithParams(workflows.MetadataCrawlParams{
				DataSourceID:   "ds-001",
				DataSourceName: "tushare",
				MaxAPICrawl:    50,
			})

		if builder == nil {
			t.Error("Builder with params should not be nil")
		}
	})
}

// TestMetadataCrawlParams_Defaults 测试参数默认值
func TestMetadataCrawlParams_Defaults(t *testing.T) {
	params := workflows.MetadataCrawlParams{
		DataSourceID:   "ds-001",
		DataSourceName: "tushare",
	}

	if params.DataSourceID != "ds-001" {
		t.Errorf("Expected DataSourceID 'ds-001', got '%s'", params.DataSourceID)
	}
	if params.DataSourceName != "tushare" {
		t.Errorf("Expected DataSourceName 'tushare', got '%s'", params.DataSourceName)
	}
	if params.MaxAPICrawl != 0 {
		t.Errorf("Expected MaxAPICrawl 0 (no limit), got %d", params.MaxAPICrawl)
	}
}

// TestWorkflowFactory_MetadataCrawl 测试工作流工厂
func TestWorkflowFactory_MetadataCrawl(t *testing.T) {
	factory := workflows.NewWorkflowFactory(nil)

	t.Run("MetadataCrawl builder method", func(t *testing.T) {
		builder := factory.MetadataCrawl()
		if builder == nil {
			t.Error("MetadataCrawl() should return non-nil builder")
		}
	})

	t.Run("Builder with all options", func(t *testing.T) {
		builder := factory.MetadataCrawl().
			WithDataSource("ds-001", "tushare").
			WithMaxAPICrawl(200)

		if builder == nil {
			t.Error("Builder with all options should not be nil")
		}
	})
}
