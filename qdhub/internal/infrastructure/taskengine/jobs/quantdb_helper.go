// Package jobs provides Task Engine job functions for QDHub workflows.
package jobs

import (
	"fmt"

	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/domain/datastore"
)

// GetQuantDBForPath returns a QuantDB adapter for the given target path using QuantDBFactory.
// targetDBPath must be the storage_path of a DuckDB Quant Data Store (from DB).
// Exported for use by compensation handlers.
func GetQuantDBForPath(tc *task.TaskContext, targetDBPath string) (datastore.QuantDB, error) {
	if targetDBPath == "" {
		return nil, datastore.ErrQuantDBPathRequired
	}
	factoryInterface, ok := tc.GetDependency("QuantDBFactory")
	if !ok || factoryInterface == nil {
		return nil, fmt.Errorf("QuantDBFactory dependency not found")
	}
	factory, ok := factoryInterface.(datastore.QuantDBFactory)
	if !ok {
		return nil, fmt.Errorf("QuantDBFactory has wrong type")
	}
	return factory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: targetDBPath,
	})
}
