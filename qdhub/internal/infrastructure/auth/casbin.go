// Package auth provides Casbin RBAC initialization.
package auth

import (
	"fmt"

	"qdhub/internal/infrastructure/persistence"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/jmoiron/sqlx"
	adapter "github.com/memwey/casbin-sqlx-adapter"
)

// RBACModelConf is the Casbin RBAC model configuration.
const RBACModelConf = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub) && keyMatch(r.obj, p.obj) && r.act == p.act
`

// NewCasbinEnforcer creates a new Casbin enforcer with database adapter.
func NewCasbinEnforcer(db *sqlx.DB, dbType persistence.DBType) (*casbin.Enforcer, error) {
	// Create adapter using sqlx.DB directly
	adapterInstance := adapter.NewAdapterByDB(db)

	// Load model from string
	modelInstance, err := model.NewModelFromString(RBACModelConf)
	if err != nil {
		return nil, fmt.Errorf("failed to load casbin model: %w", err)
	}

	// Create enforcer
	enforcer, err := casbin.NewEnforcer(modelInstance, adapterInstance)
	if err != nil {
		return nil, fmt.Errorf("failed to create casbin enforcer: %w", err)
	}

	// Load policies from database
	if err := enforcer.LoadPolicy(); err != nil {
		return nil, fmt.Errorf("failed to load policies: %w", err)
	}

	return enforcer, nil
}

// InitializeDefaultPolicies initializes default RBAC policies.
func InitializeDefaultPolicies(enforcer *casbin.Enforcer) error {
	// Admin role - full access to all resources
	adminPolicies := []struct {
		resource string
		action   string
	}{
		{"datasources", "read"},
		{"datasources", "write"},
		{"datasources", "delete"},
		{"sync-plans", "read"},
		{"sync-plans", "write"},
		{"sync-plans", "delete"},
		{"sync-plans", "execute"},
		{"datastores", "read"},
		{"datastores", "write"},
		{"datastores", "delete"},
		{"analysis", "read"},
		{"analysis", "write"},
		{"workflows", "read"},
		{"workflows", "write"},
		{"workflows", "delete"},
		{"workflows", "execute"},
		{"instances", "read"},
		{"users", "read"},
		{"users", "write"},
		{"users", "delete"},
	}

	// Operator role - read and execute, limited write
	operatorPolicies := []struct {
		resource string
		action   string
	}{
		{"datasources", "read"},
		{"sync-plans", "read"},
		{"sync-plans", "write"},
		{"sync-plans", "execute"},
		{"datastores", "read"},
		{"analysis", "read"},
		{"workflows", "read"},
		{"workflows", "execute"},
		{"instances", "read"},
	}

	// Viewer role - read only
	viewerPolicies := []struct {
		resource string
		action   string
	}{
		{"datasources", "read"},
		{"sync-plans", "read"},
		{"datastores", "read"},
		{"analysis", "read"},
		{"workflows", "read"},
		{"instances", "read"},
	}

	// 添加 g 规则：matcher 为 g(r.sub, p.sub)，需要 g(role, role) 为真，Enforce(role, resource, action) 才能通过
	for _, role := range []string{"admin", "operator", "viewer"} {
		if _, err := enforcer.AddGroupingPolicy(role, role); err != nil {
			return fmt.Errorf("failed to add grouping policy for role %s: %w", role, err)
		}
	}

	// Add admin policies
	for _, p := range adminPolicies {
		if _, err := enforcer.AddPolicy("admin", p.resource, p.action); err != nil {
			return fmt.Errorf("failed to add admin policy: %w", err)
		}
	}

	// Add operator policies
	for _, p := range operatorPolicies {
		if _, err := enforcer.AddPolicy("operator", p.resource, p.action); err != nil {
			return fmt.Errorf("failed to add operator policy: %w", err)
		}
	}

	// Add viewer policies
	for _, p := range viewerPolicies {
		if _, err := enforcer.AddPolicy("viewer", p.resource, p.action); err != nil {
			return fmt.Errorf("failed to add viewer policy: %w", err)
		}
	}

	// Save policies
	if err := enforcer.SavePolicy(); err != nil {
		return fmt.Errorf("failed to save policies: %w", err)
	}

	return nil
}

// EnsureAnalysisPolicies adds analysis resource policies if missing (for existing DBs that were created before analysis was added).
func EnsureAnalysisPolicies(enforcer *casbin.Enforcer) error {
	analysisPolicies := []struct {
		role     string
		resource string
		action   string
	}{
		{"admin", "analysis", "read"},
		{"admin", "analysis", "write"},
		{"operator", "analysis", "read"},
		{"viewer", "analysis", "read"},
	}
	for _, p := range analysisPolicies {
		ok, err := enforcer.HasPolicy(p.role, p.resource, p.action)
		if err != nil {
			return err
		}
		if !ok {
			if _, err := enforcer.AddPolicy(p.role, p.resource, p.action); err != nil {
				return fmt.Errorf("add analysis policy %s %s %s: %w", p.role, p.resource, p.action, err)
			}
		}
	}
	return enforcer.SavePolicy()
}
