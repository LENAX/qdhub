package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/authz"
	"github.com/casbin/casbin/v2"

	authinfra "qdhub/internal/infrastructure/auth"
)

const (
	// Context keys
	UserIDKey   = "user_id"
	UsernameKey = "username"
	RolesKey    = "roles"
)

// JWTAuthMiddleware returns a middleware that validates JWT tokens.
func JWTAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract token from Authorization header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			Error(c, http.StatusUnauthorized, 401, "authorization header required")
			c.Abort()
			return
		}

		// Check Bearer prefix
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || parts[0] != "Bearer" {
			Error(c, http.StatusUnauthorized, 401, "invalid authorization header format")
			c.Abort()
			return
		}

		tokenString := parts[1]

		// Get JWT manager from context (set by container)
		jwtManager, exists := c.Get("jwt_manager")
		if !exists {
			InternalError(c, "JWT manager not configured")
			c.Abort()
			return
		}

		// Validate token
		claims, err := jwtManager.(*authinfra.JWTManager).ValidateToken(tokenString)
		if err != nil {
			Error(c, http.StatusUnauthorized, 401, "invalid or expired token")
			c.Abort()
			return
		}

		// Set user information in context
		c.Set(UserIDKey, claims.UserID)
		c.Set(UsernameKey, claims.Username)
		c.Set(RolesKey, claims.Roles)

		c.Next()
	}
}

// CasbinRBACMiddleware returns a middleware that enforces RBAC using Casbin.
func CasbinRBACMiddleware(enforcer *casbin.Enforcer) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get user roles from context
		roles, exists := c.Get(RolesKey)
		if !exists {
			Error(c, http.StatusUnauthorized, 401, "user not authenticated")
			c.Abort()
			return
		}

		roleList := roles.([]string)
		if len(roleList) == 0 {
			Error(c, http.StatusForbidden, 403, "user has no roles")
			c.Abort()
			return
		}

		// Extract resource and action from route
		resource, action := extractResourceAndAction(c)
		if resource == "" || action == "" {
			Error(c, http.StatusBadRequest, 400, "unable to determine resource or action")
			c.Abort()
			return
		}

		// Check permissions for each role
		allowed := false
		for _, role := range roleList {
			if allowed, _ = enforcer.Enforce(role, resource, action); allowed {
				break
			}
		}

		if !allowed {
			Error(c, http.StatusForbidden, 403, "insufficient permissions")
			c.Abort()
			return
		}

		c.Next()
	}
}

// extractResourceAndAction extracts resource and action from the request.
func extractResourceAndAction(c *gin.Context) (string, string) {
	path := c.Request.URL.Path
	method := c.Request.Method

	// Remove /api/v1 prefix
	path = strings.TrimPrefix(path, "/api/v1")
	path = strings.TrimPrefix(path, "/")

	// Extract resource from path
	// Examples:
	// /sync-plans -> sync-plans
	// /executions/:id/cancel -> executions (then mapped to sync-plans)
	parts := strings.Split(path, "/")
	resource := parts[0]

	// executions 属于同步计划执行，复用 sync-plans 的权限策略
	if resource == "executions" {
		resource = "sync-plans"
	}

	// Map HTTP method to action
	var action string
	switch method {
	case http.MethodGet:
		action = "read"
	case http.MethodPost:
		// 执行类操作：触发、启用/禁用、取消/暂停/恢复
		if strings.Contains(path, "trigger") || strings.Contains(path, "enable") ||
			strings.Contains(path, "disable") || strings.Contains(path, "execute") ||
			strings.Contains(path, "cancel") || strings.Contains(path, "pause") || strings.Contains(path, "resume") {
			action = "execute"
		} else {
			action = "write"
		}
	case http.MethodPut, http.MethodPatch:
		action = "write"
	case http.MethodDelete:
		action = "delete"
	default:
		action = "read"
	}

	return resource, action
}

// RequireRole returns a middleware that requires a specific role.
func RequireRole(role string) gin.HandlerFunc {
	return func(c *gin.Context) {
		roles, exists := c.Get(RolesKey)
		if !exists {
			Error(c, http.StatusUnauthorized, 401, "user not authenticated")
			c.Abort()
			return
		}

		roleList := roles.([]string)
		hasRole := false
		for _, r := range roleList {
			if r == role {
				hasRole = true
				break
			}
		}

		if !hasRole {
			Error(c, http.StatusForbidden, 403, "insufficient permissions")
			c.Abort()
			return
		}

		c.Next()
	}
}

// AuthzMiddleware is a wrapper for gin-contrib/authz middleware.
// This provides an alternative way to use Casbin with gin-contrib/authz.
func AuthzMiddleware(enforcer *casbin.Enforcer) gin.HandlerFunc {
	return authz.NewAuthorizer(enforcer)
}
