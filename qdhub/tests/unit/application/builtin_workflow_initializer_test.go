package application_test

import (
	"testing"

	"qdhub/internal/infrastructure/taskengine/workflows"
)

func TestBuiltInWorkflowInitializer_GetBuiltInWorkflows(t *testing.T) {
	t.Run("Get all built-in workflows", func(t *testing.T) {
		builtInWorkflows := workflows.GetBuiltInWorkflows()
		if len(builtInWorkflows) == 0 {
			t.Fatal("Expected at least one built-in workflow")
		}

		// Verify all expected workflows are present
		expectedIDs := []string{
			workflows.BuiltInWorkflowIDMetadataCrawl,
			workflows.BuiltInWorkflowIDCreateTables,
			workflows.BuiltInWorkflowIDBatchDataSync,
			workflows.BuiltInWorkflowIDRealtimeDataSync,
		}

		foundIDs := make(map[string]bool)
		for _, meta := range builtInWorkflows {
			foundIDs[meta.ID] = true
			if meta.APIName == "" {
				t.Errorf("Workflow %s should have an APIName", meta.ID)
			}
			if meta.Name == "" {
				t.Errorf("Workflow %s should have a Name", meta.ID)
			}
		}

		for _, expectedID := range expectedIDs {
			if !foundIDs[expectedID] {
				t.Errorf("Expected workflow %s not found", expectedID)
			}
		}
	})
}

func TestBuiltInWorkflowInitializer_GetBuiltInWorkflowIDByName(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		workflowID, err := workflows.GetBuiltInWorkflowIDByName("metadata_crawl")
		if err != nil {
			t.Fatalf("GetBuiltInWorkflowIDByName failed: %v", err)
		}
		if workflowID != workflows.BuiltInWorkflowIDMetadataCrawl {
			t.Errorf("GetBuiltInWorkflowIDByName = %s, want %s", workflowID, workflows.BuiltInWorkflowIDMetadataCrawl)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		_, err := workflows.GetBuiltInWorkflowIDByName("non_existent_workflow")
		if err == nil {
			t.Fatal("Expected error for non-existent workflow")
		}
	})
}

func TestBuiltInWorkflowInitializer_IsBuiltInWorkflow(t *testing.T) {
	t.Run("Is built-in workflow", func(t *testing.T) {
		if !workflows.IsBuiltInWorkflow(workflows.BuiltInWorkflowIDMetadataCrawl) {
			t.Error("Expected metadata_crawl to be identified as built-in workflow")
		}
	})

	t.Run("Is not built-in workflow", func(t *testing.T) {
		if workflows.IsBuiltInWorkflow("custom:workflow") {
			t.Error("Expected custom workflow not to be identified as built-in")
		}
	})
}
