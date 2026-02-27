// Package quantdb provides interfaces and adapters for quant data storage.
package quantdb

import (
	"context"
	"fmt"
	"sync"

	"qdhub/internal/application/impl"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

// QuantDBAdapterImpl implements impl.QuantDBAdapter interface.
// It manages multiple QuantDB connections, one per DataStore.
// For DuckDB, it delegates to a shared QuantDBFactory so that Task Engine jobs
// and application-level queries share the same underlying connection (avoiding
// the "invisible tables" problem caused by independent DuckDB engine instances).
type QuantDBAdapterImpl struct {
	factory     datastore.QuantDBFactory // shared factory (required for DuckDB)
	connections map[shared.ID]datastore.QuantDB
	mu          sync.RWMutex
}

// NewQuantDBAdapter creates a new QuantDBAdapter implementation.
// factory is the shared QuantDBFactory (typically duckdb.Factory) used by Task Engine;
// passing the same instance ensures both code paths share cached connections.
func NewQuantDBAdapter(factory datastore.QuantDBFactory) *QuantDBAdapterImpl {
	return &QuantDBAdapterImpl{
		factory:     factory,
		connections: make(map[shared.ID]datastore.QuantDB),
	}
}

// getOrCreateConnection gets an existing connection or creates a new one for the given DataStore.
func (a *QuantDBAdapterImpl) getOrCreateConnection(ctx context.Context, ds *datastore.QuantDataStore) (datastore.QuantDB, error) {
	// Try to get existing connection with read lock
	a.mu.RLock()
	if conn, exists := a.connections[ds.ID]; exists {
		a.mu.RUnlock()
		// Verify connection is still valid
		if err := conn.Ping(ctx); err == nil {
			return conn, nil
		}
		// Connection is stale, need to recreate
	} else {
		a.mu.RUnlock()
	}

	// Acquire write lock to create new connection
	a.mu.Lock()
	defer a.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, exists := a.connections[ds.ID]; exists {
		if err := conn.Ping(ctx); err == nil {
			return conn, nil
		}
		// Close stale connection
		_ = conn.Close()
		delete(a.connections, ds.ID)
	}

	// Create new connection based on DataStore type
	conn, err := a.createConnection(ctx, ds)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection for data store %s: %w", ds.ID, err)
	}

	a.connections[ds.ID] = conn
	return conn, nil
}

// createConnection creates a new QuantDB connection based on DataStore configuration.
// For DuckDB, it delegates to the shared Factory so that Task Engine and application
// queries use the same cached connection.
func (a *QuantDBAdapterImpl) createConnection(_ context.Context, ds *datastore.QuantDataStore) (datastore.QuantDB, error) {
	switch ds.Type {
	case datastore.DataStoreTypeDuckDB:
		storagePath := ds.StoragePath
		if storagePath == "" {
			storagePath = ds.DSN
		}
		if storagePath == "" {
			return nil, fmt.Errorf("DuckDB requires StoragePath or DSN")
		}

		return a.factory.Create(datastore.QuantDBConfig{
			Type:        datastore.DataStoreTypeDuckDB,
			StoragePath: storagePath,
		})

	case datastore.DataStoreTypeClickHouse:
		return nil, fmt.Errorf("ClickHouse adapter not implemented yet")

	case datastore.DataStoreTypePostgreSQL:
		return nil, fmt.Errorf("PostgreSQL adapter not implemented yet")

	default:
		return nil, fmt.Errorf("unsupported data store type: %s", ds.Type)
	}
}

// TestConnection tests the connection to a data store.
func (a *QuantDBAdapterImpl) TestConnection(ctx context.Context, ds *datastore.QuantDataStore) error {
	conn, err := a.getOrCreateConnection(ctx, ds)
	if err != nil {
		return err
	}
	return conn.Ping(ctx)
}

// ExecuteDDL executes DDL statement on a data store.
func (a *QuantDBAdapterImpl) ExecuteDDL(ctx context.Context, ds *datastore.QuantDataStore, ddl string) error {
	conn, err := a.getOrCreateConnection(ctx, ds)
	if err != nil {
		return err
	}

	_, err = conn.Execute(ctx, ddl)
	if err != nil {
		return fmt.Errorf("failed to execute DDL: %w", err)
	}
	return nil
}

// TableExists checks if a table exists in the data store.
func (a *QuantDBAdapterImpl) TableExists(ctx context.Context, ds *datastore.QuantDataStore, tableName string) (bool, error) {
	conn, err := a.getOrCreateConnection(ctx, ds)
	if err != nil {
		return false, err
	}
	return conn.TableExists(ctx, tableName)
}

// ListTables returns table names in the data store's database.
func (a *QuantDBAdapterImpl) ListTables(ctx context.Context, ds *datastore.QuantDataStore) ([]string, error) {
	conn, err := a.getOrCreateConnection(ctx, ds)
	if err != nil {
		return nil, err
	}
	return conn.ListTables(ctx)
}

// Query executes a SQL query on the data store and returns the results.
func (a *QuantDBAdapterImpl) Query(ctx context.Context, ds *datastore.QuantDataStore, sql string, args ...any) ([]map[string]any, error) {
	conn, err := a.getOrCreateConnection(ctx, ds)
	if err != nil {
		return nil, err
	}
	return conn.Query(ctx, sql, args...)
}

// InvalidateConnection drops the cached connection for the given data store ID.
func (a *QuantDBAdapterImpl) InvalidateConnection(id shared.ID) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if conn, exists := a.connections[id]; exists {
		_ = conn.Close()
		delete(a.connections, id)
	}
}

// Close closes all managed connections.
func (a *QuantDBAdapterImpl) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var lastErr error
	for id, conn := range a.connections {
		if err := conn.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close connection %s: %w", id, err)
		}
		delete(a.connections, id)
	}
	return lastErr
}

// Ensure QuantDBAdapterImpl implements impl.QuantDBAdapter interface
var _ impl.QuantDBAdapter = (*QuantDBAdapterImpl)(nil)
