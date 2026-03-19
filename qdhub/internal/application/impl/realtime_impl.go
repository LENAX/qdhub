// Package impl contains application service implementations.
package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/realtime"
	"qdhub/internal/domain/shared"
)

// RealtimeSourceHealthChecker runs a one-off health check for a RealtimeSource (optional; may be nil).
type RealtimeSourceHealthChecker interface {
	Check(ctx context.Context, src *realtime.RealtimeSource) (status string, errMsg string, err error)
}

// RealtimeSourceApplicationServiceImpl implements RealtimeSourceApplicationService.
type RealtimeSourceApplicationServiceImpl struct {
	repo    realtime.RealtimeSourceRepository
	checker RealtimeSourceHealthChecker // optional
}

// NewRealtimeSourceApplicationService creates a new RealtimeSourceApplicationService implementation.
func NewRealtimeSourceApplicationService(repo realtime.RealtimeSourceRepository, checker RealtimeSourceHealthChecker) contracts.RealtimeSourceApplicationService {
	return &RealtimeSourceApplicationServiceImpl{repo: repo, checker: checker}
}

// List returns all realtime sources.
func (s *RealtimeSourceApplicationServiceImpl) List(ctx context.Context) ([]*realtime.RealtimeSource, error) {
	list, err := s.repo.List()
	if err != nil {
		return nil, fmt.Errorf("list realtime sources: %w", err)
	}
	return list, nil
}

// Get returns a realtime source by ID.
func (s *RealtimeSourceApplicationServiceImpl) Get(ctx context.Context, id shared.ID) (*realtime.RealtimeSource, error) {
	src, err := s.repo.Get(id)
	if err != nil {
		return nil, fmt.Errorf("get realtime source: %w", err)
	}
	if src == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "realtime source not found", nil)
	}
	return src, nil
}

// Create creates a new realtime source.
func (s *RealtimeSourceApplicationServiceImpl) Create(ctx context.Context, req contracts.CreateRealtimeSourceRequest) (*realtime.RealtimeSource, error) {
	if err := validateRealtimeSourceType(req.Type); err != nil {
		return nil, err
	}
	if err := validateRealtimeSourceConfig(req.Type, req.Config); err != nil {
		return nil, err
	}
	src := realtime.NewRealtimeSource(req.Name, req.Type, req.Config, req.Priority, req.IsPrimary, req.HealthCheckOnStartup, req.Enabled)
	if err := s.repo.Create(src); err != nil {
		return nil, fmt.Errorf("create realtime source: %w", err)
	}
	return src, nil
}

// Update updates a realtime source.
func (s *RealtimeSourceApplicationServiceImpl) Update(ctx context.Context, id shared.ID, req contracts.UpdateRealtimeSourceRequest) (*realtime.RealtimeSource, error) {
	src, err := s.repo.Get(id)
	if err != nil {
		return nil, fmt.Errorf("get realtime source: %w", err)
	}
	if src == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "realtime source not found", nil)
	}
	if req.Name != nil {
		src.Name = *req.Name
	}
	if req.Config != nil {
		if err := validateRealtimeSourceConfig(src.Type, *req.Config); err != nil {
			return nil, err
		}
		src.Config = *req.Config
	}
	if req.Priority != nil {
		src.Priority = *req.Priority
	}
	if req.IsPrimary != nil {
		src.IsPrimary = *req.IsPrimary
	}
	if req.HealthCheckOnStartup != nil {
		src.HealthCheckOnStartup = *req.HealthCheckOnStartup
	}
	if req.Enabled != nil {
		src.Enabled = *req.Enabled
	}
	src.UpdatedAt = shared.Now()
	if err := s.repo.Update(src); err != nil {
		return nil, fmt.Errorf("update realtime source: %w", err)
	}
	return src, nil
}

// Delete deletes a realtime source.
func (s *RealtimeSourceApplicationServiceImpl) Delete(ctx context.Context, id shared.ID) error {
	src, err := s.repo.Get(id)
	if err != nil {
		return fmt.Errorf("get realtime source: %w", err)
	}
	if src == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "realtime source not found", nil)
	}
	if err := s.repo.Delete(id); err != nil {
		return fmt.Errorf("delete realtime source: %w", err)
	}
	return nil
}

// TriggerHealthCheck runs a one-off health check for the given source.
func (s *RealtimeSourceApplicationServiceImpl) TriggerHealthCheck(ctx context.Context, id shared.ID) (status string, errMsg string, err error) {
	if s.checker == nil {
		return "", "", fmt.Errorf("health check not configured")
	}
	src, err := s.repo.Get(id)
	if err != nil {
		return "", "", fmt.Errorf("get realtime source: %w", err)
	}
	if src == nil {
		return "", "", shared.NewDomainError(shared.ErrCodeNotFound, "realtime source not found", nil)
	}
	status, errMsg, err = s.checker.Check(ctx, src)
	if err != nil {
		return "", errMsg, err
	}
	src.UpdateHealth(status, errMsg)
	_ = s.repo.Update(src)
	return status, errMsg, nil
}

func validateRealtimeSourceType(typ string) error {
	switch typ {
	case realtime.TypeTushareProxy, realtime.TypeTushareWS, realtime.TypeSina, realtime.TypeEastmoney, realtime.TypeNews:
		return nil
	default:
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid type: %s", typ), nil)
	}
}

func validateRealtimeSourceConfig(typ, config string) error {
	var m map[string]interface{}
	if config != "" {
		if err := json.Unmarshal([]byte(config), &m); err != nil {
			return shared.NewDomainError(shared.ErrCodeValidation, "config must be valid JSON", err)
		}
	}
	if m == nil {
		m = make(map[string]interface{})
	}
	switch typ {
	case realtime.TypeTushareProxy:
		if v, _ := m["ws_url"].(string); strings.TrimSpace(v) == "" {
			return shared.NewDomainError(shared.ErrCodeValidation, "tushare_proxy requires config.ws_url", nil)
		}
		if v, _ := m["rsa_public_key_path"].(string); strings.TrimSpace(v) == "" {
			return shared.NewDomainError(shared.ErrCodeValidation, "tushare_proxy requires config.rsa_public_key_path", nil)
		}
	case realtime.TypeTushareWS:
		// endpoint optional, token can be from data_source
		return nil
	case realtime.TypeSina, realtime.TypeEastmoney:
		return nil
	case realtime.TypeNews:
		// 新闻源：config 可为空或 {"freq":"5MIN"} 等，由 SyncPlan 调度控制拉取频率
		return nil
	default:
		return nil
	}
	return nil
}
