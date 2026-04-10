package realtimestore

import (
	"encoding/json"
	"sync"
)

// LatestQuoteStore 保存每个 ts_code 的最新一条行情快照。
type LatestQuoteStore struct {
	mu sync.RWMutex

	data map[string]*Quote

	version              uint64
	itemsSnapshotVersion uint64
	itemsSnapshotJSON    []byte
}

// NewLatestQuoteStore creates a new store.
func NewLatestQuoteStore() *LatestQuoteStore {
	return &LatestQuoteStore{
		data: make(map[string]*Quote),
	}
}

// Update updates latest quote by ts_code.
func (s *LatestQuoteStore) Update(tsCode string, quote map[string]interface{}) {
	q, ok := QuoteFromMap(tsCode, quote)
	if !ok {
		return
	}
	key := q.normalizedCode()
	if key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	cp := q.normalizeIdentity()
	s.data[key] = &cp
	s.version++
}

// Get returns latest quote by ts_code.
func (s *LatestQuoteStore) Get(tsCode string) (map[string]interface{}, bool) {
	s.mu.RLock()
	q, ok := s.data[tsCode]
	if !ok || q == nil {
		s.mu.RUnlock()
		return nil, false
	}
	cp := *q
	s.mu.RUnlock()
	return cp.ToMap(), true
}

// GetAll returns all latest quotes snapshot.
func (s *LatestQuoteStore) GetAll() map[string]map[string]interface{} {
	s.mu.RLock()
	out := make(map[string]map[string]interface{}, len(s.data))
	for tsCode, quote := range s.data {
		if quote == nil {
			continue
		}
		cp := *quote
		out[tsCode] = cp.ToMap()
	}
	s.mu.RUnlock()
	return out
}

// GetBatch returns latest quote snapshots for selected ts_codes.
func (s *LatestQuoteStore) GetBatch(tsCodes []string) map[string]map[string]interface{} {
	out := make(map[string]map[string]interface{}, len(tsCodes))
	for _, tsCode := range tsCodes {
		if row, ok := s.Get(tsCode); ok {
			out[tsCode] = row
		}
	}
	return out
}

// GetSubsetQuotes returns strong-typed snapshots for selected ts_codes.
func (s *LatestQuoteStore) GetSubsetQuotes(tsCodes []string) map[string]Quote {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]Quote, len(tsCodes))
	for _, tsCode := range tsCodes {
		if quote, ok := s.data[tsCode]; ok && quote != nil {
			out[tsCode] = quote.normalizeIdentity()
		}
	}
	return out
}

// BuildFullItemsJSON returns the cached JSON bytes of all items.
func (s *LatestQuoteStore) BuildFullItemsJSON() ([]byte, uint64, error) {
	s.mu.RLock()
	if s.itemsSnapshotJSON != nil && s.itemsSnapshotVersion == s.version {
		cached := append([]byte(nil), s.itemsSnapshotJSON...)
		version := s.version
		s.mu.RUnlock()
		return cached, version, nil
	}

	snapshot := make(map[string]Quote, len(s.data))
	version := s.version
	for tsCode, quote := range s.data {
		if quote == nil {
			continue
		}
		snapshot[tsCode] = quote.normalizeIdentity()
	}
	s.mu.RUnlock()

	itemsJSON, err := json.Marshal(snapshot)
	if err != nil {
		return nil, 0, err
	}

	s.mu.Lock()
	if s.version == version {
		s.itemsSnapshotJSON = append([]byte(nil), itemsJSON...)
		s.itemsSnapshotVersion = version
	}
	cached := s.itemsSnapshotJSON
	currentVersion := s.itemsSnapshotVersion
	if len(cached) == 0 {
		cached = itemsJSON
		currentVersion = version
	}
	cached = append([]byte(nil), cached...)
	s.mu.Unlock()
	return cached, currentVersion, nil
}

// Clear resets all cached quotes. Intended for tests and process-local cleanup.
func (s *LatestQuoteStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]*Quote)
	s.version++
	s.itemsSnapshotVersion = 0
	s.itemsSnapshotJSON = nil
}

var defaultLatestQuoteStore = NewLatestQuoteStore()

// DefaultLatestQuoteStore returns process-wide singleton store.
func DefaultLatestQuoteStore() *LatestQuoteStore {
	return defaultLatestQuoteStore
}
