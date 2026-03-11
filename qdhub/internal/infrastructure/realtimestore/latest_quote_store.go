package realtimestore

import "sync"

// LatestQuoteStore 保存每个 ts_code 的最新一条行情快照。
type LatestQuoteStore struct {
	data sync.Map // key: string(ts_code), value: map[string]interface{}
}

// NewLatestQuoteStore creates a new store.
func NewLatestQuoteStore() *LatestQuoteStore {
	return &LatestQuoteStore{}
}

// Update updates latest quote by ts_code.
func (s *LatestQuoteStore) Update(tsCode string, quote map[string]interface{}) {
	if tsCode == "" || quote == nil {
		return
	}
	cp := make(map[string]interface{}, len(quote))
	for k, v := range quote {
		cp[k] = v
	}
	s.data.Store(tsCode, cp)
}

// Get returns latest quote by ts_code.
func (s *LatestQuoteStore) Get(tsCode string) (map[string]interface{}, bool) {
	v, ok := s.data.Load(tsCode)
	if !ok {
		return nil, false
	}
	m, ok := v.(map[string]interface{})
	if !ok || m == nil {
		return nil, false
	}
	cp := make(map[string]interface{}, len(m))
	for k, x := range m {
		cp[k] = x
	}
	return cp, true
}

// GetAll returns all latest quotes snapshot.
func (s *LatestQuoteStore) GetAll() map[string]map[string]interface{} {
	out := make(map[string]map[string]interface{})
	s.data.Range(func(key, value interface{}) bool {
		tsCode, ok1 := key.(string)
		row, ok2 := value.(map[string]interface{})
		if !ok1 || !ok2 || row == nil {
			return true
		}
		cp := make(map[string]interface{}, len(row))
		for k, v := range row {
			cp[k] = v
		}
		out[tsCode] = cp
		return true
	})
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

var defaultLatestQuoteStore = NewLatestQuoteStore()

// DefaultLatestQuoteStore returns process-wide singleton store.
func DefaultLatestQuoteStore() *LatestQuoteStore {
	return defaultLatestQuoteStore
}
