package writequeue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"qdhub/internal/domain/datastore"
	"qdhub/pkg/config"
)

type writeResult struct {
	inserted int64
	err      error
}

type pendingWrite struct {
	req  datastore.QuantDBBatchWriteRequest
	resp chan writeResult
}

// Queue implements datastore.QuantDBWriteQueue
type Queue struct {
	config  config.WriteQueueConfig
	factory datastore.QuantDBFactory

	mu      sync.RWMutex
	writers map[string]*pathWriter
	closed  bool
}

// NewQueue creates a new QuantDBWriteQueue.
func NewQueue(cfg config.WriteQueueConfig, factory datastore.QuantDBFactory) *Queue {
	return &Queue{
		config:  cfg,
		factory: factory,
		writers: make(map[string]*pathWriter),
	}
}

// getOrStartWriter returns the pathWriter for the given path, creating it if it doesn't exist.
func (q *Queue) getOrStartWriter(path string) (*pathWriter, error) {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return nil, fmt.Errorf("write queue is closed")
	}
	pw, ok := q.writers[path]
	q.mu.RUnlock()

	if ok {
		return pw, nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, fmt.Errorf("write queue is closed")
	}

	pw, ok = q.writers[path]
	if !ok {
		pw = newPathWriter(path, q.config, q.factory)
		q.writers[path] = pw
		go pw.loop()
	}

	return pw, nil
}

// Enqueue adds a write request to the queue. Returns an error if the queue rejects it.
func (q *Queue) Enqueue(ctx context.Context, req datastore.QuantDBBatchWriteRequest) error {
	if !q.config.Enabled {
		return q.directWrite(ctx, req)
	}

	// Memory critical check
	memStatus := CheckMemoryStatus(q.config.MemoryCheckEnabled, q.config.MemoryHighMB, q.config.MemoryCriticalMB)
	if memStatus == MemStatusCritical {
		return fmt.Errorf("write queue rejected request: memory critical")
	}

	pw, err := q.getOrStartWriter(req.Path)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case pw.reqCh <- pendingWrite{req: req, resp: nil}:
		return nil
	}
}

// EnqueueAndWait adds a write request and blocks until the data is actually written to the DB.
func (q *Queue) EnqueueAndWait(ctx context.Context, req datastore.QuantDBBatchWriteRequest) (int64, error) {
	if !q.config.Enabled {
		return q.directWriteWithResult(ctx, req)
	}

	// Memory critical check
	memStatus := CheckMemoryStatus(q.config.MemoryCheckEnabled, q.config.MemoryHighMB, q.config.MemoryCriticalMB)
	if memStatus == MemStatusCritical {
		return 0, fmt.Errorf("write queue rejected request: memory critical")
	}

	pw, err := q.getOrStartWriter(req.Path)
	if err != nil {
		return 0, err
	}

	respCh := make(chan writeResult, 1)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case pw.reqCh <- pendingWrite{req: req, resp: respCh}:
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case res := <-respCh:
		return res.inserted, res.err
	}
}

// directWrite executes the insert immediately without queueing.
func (q *Queue) directWrite(ctx context.Context, req datastore.QuantDBBatchWriteRequest) error {
	_, err := q.directWriteWithResult(ctx, req)
	return err
}

func (q *Queue) directWriteWithResult(ctx context.Context, req datastore.QuantDBBatchWriteRequest) (int64, error) {
	db, err := q.factory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: req.Path,
	})
	if err != nil {
		return 0, err
	}
	if req.SyncBatchID != "" {
		return db.BulkInsertWithBatchID(ctx, req.TableName, req.Data, req.SyncBatchID)
	}
	return db.BulkInsert(ctx, req.TableName, req.Data)
}

// Close shuts down the queue and flushes all pending writes.
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	writers := make([]*pathWriter, 0, len(q.writers))
	for _, pw := range q.writers {
		writers = append(writers, pw)
	}
	q.mu.Unlock()

	var wg sync.WaitGroup
	for _, pw := range writers {
		wg.Add(1)
		go func(w *pathWriter) {
			defer wg.Done()
			w.close()
		}(pw)
	}
	wg.Wait()
	return nil
}

type tableKey struct {
	tableName   string
	syncBatchID string
}

type batchBuffer struct {
	rows    []map[string]any
	notifys []chan writeResult // Channels to notify when flushed
}

// pathWriter processes writes for a single path to serialize DuckDB writes.
type pathWriter struct {
	path    string
	config  config.WriteQueueConfig
	factory datastore.QuantDBFactory

	reqCh  chan pendingWrite
	doneCh chan struct{}
}

func newPathWriter(path string, cfg config.WriteQueueConfig, factory datastore.QuantDBFactory) *pathWriter {
	return &pathWriter{
		path:    path,
		config:  cfg,
		factory: factory,
		reqCh:   make(chan pendingWrite, 1000), // Buffer size for incoming requests
		doneCh:  make(chan struct{}),
	}
}

func (pw *pathWriter) loop() {
	ticker := time.NewTicker(time.Duration(pw.config.MaxWaitSec) * time.Second)
	defer ticker.Stop()

	buffers := make(map[tableKey]*batchBuffer)
	totalRows := 0

	flushAll := func(memStatus MemStatus) {
		if len(buffers) == 0 {
			return
		}
		reason := "timeout/size"
		if memStatus == MemStatusHigh {
			reason = "memory high"
		} else if memStatus == MemStatusCritical {
			reason = "memory critical"
		}
		logrus.Infof("[WriteQueue] Flushing path=%s (reason: %s), tables=%d, rows=%d", pw.path, reason, len(buffers), totalRows)
		pw.executeFlush(buffers)
		// Reset state
		buffers = make(map[tableKey]*batchBuffer)
		totalRows = 0
	}

	for {
		select {
		case <-pw.doneCh:
			// Flush remaining before exiting
			flushAll(MemStatusNormal)
			return
		case <-ticker.C:
			flushAll(MemStatusNormal)
		case req, ok := <-pw.reqCh:
			if !ok {
				// reqCh closed (if we choose to close it)
				flushAll(MemStatusNormal)
				return
			}

			// Pre-check memory before buffering
			memStatus := CheckMemoryStatus(pw.config.MemoryCheckEnabled, pw.config.MemoryHighMB, pw.config.MemoryCriticalMB)
			if memStatus >= MemStatusHigh {
				// Flush existing buffer first if memory is high
				flushAll(memStatus)
			}

			key := tableKey{
				tableName:   req.req.TableName,
				syncBatchID: req.req.SyncBatchID,
			}

			buf, ok := buffers[key]
			if !ok {
				buf = &batchBuffer{
					rows:    make([]map[string]any, 0, pw.config.BatchSize),
					notifys: make([]chan writeResult, 0),
				}
				buffers[key] = buf
			}

			buf.rows = append(buf.rows, req.req.Data...)
			if req.resp != nil {
				buf.notifys = append(buf.notifys, req.resp)
			}
			totalRows += len(req.req.Data)

			if len(buf.rows) >= pw.config.BatchSize {
				// We can selectively flush just this table, or all.
				// Flushing all ensures we keep memory usage low overall.
				flushAll(MemStatusNormal)
				// Restart the ticker since we just flushed
				ticker.Reset(time.Duration(pw.config.MaxWaitSec) * time.Second)
			}
		}
	}
}

func (pw *pathWriter) executeFlush(buffers map[tableKey]*batchBuffer) {
	ctx := context.Background()

	// Get database connection for this path
	db, err := pw.factory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: pw.path,
	})

	if err != nil {
		logrus.Errorf("[WriteQueue] Failed to open DB for path %s: %v", pw.path, err)
		// Notify all waiting channels about the error
		for _, buf := range buffers {
			for _, ch := range buf.notifys {
				ch <- writeResult{inserted: 0, err: err}
			}
		}
		return
	}

	for key, buf := range buffers {
		if len(buf.rows) == 0 {
			continue
		}

		var inserted int64
		var flushErr error

		if key.syncBatchID != "" {
			inserted, flushErr = db.BulkInsertWithBatchID(ctx, key.tableName, buf.rows, key.syncBatchID)
		} else {
			inserted, flushErr = db.BulkInsert(ctx, key.tableName, buf.rows)
		}

		if flushErr != nil {
			logrus.Errorf("[WriteQueue] BulkInsert failed path=%s, table=%s: %v", pw.path, key.tableName, flushErr)
		} else {
			logrus.Debugf("[WriteQueue] Flushed path=%s, table=%s, rows=%d", pw.path, key.tableName, inserted)
		}

		// Distribute the inserted count across waiting clients.
		// A simple approach is to return the total inserted, but ideally we should track per-request rows.
		// For now, if flushErr == nil, we consider it a success.
		for _, ch := range buf.notifys {
			ch <- writeResult{inserted: inserted, err: flushErr}
		}
	}
}

func (pw *pathWriter) close() {
	close(pw.doneCh)
}
