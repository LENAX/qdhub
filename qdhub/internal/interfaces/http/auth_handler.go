package http

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
)

// AuthHandler handles authentication-related HTTP requests.
type AuthHandler struct {
	authSvc contracts.AuthApplicationService
}

// NewAuthHandler creates a new AuthHandler.
func NewAuthHandler(authSvc contracts.AuthApplicationService) *AuthHandler {
	return &AuthHandler{
		authSvc: authSvc,
	}
}

// RegisterRoutes registers authentication routes to the router group.
func (h *AuthHandler) RegisterRoutes(rg *gin.RouterGroup) {
	// Public routes (no authentication required)
	public := rg.Group("/auth")
	{
		public.POST("/register", h.Register)
		public.POST("/login", h.Login)
	}

	// Protected routes (authentication required)
	protected := rg.Group("/auth")
	protected.Use(JWTAuthMiddleware())
	{
		protected.POST("/refresh", h.RefreshToken)
		protected.GET("/me", h.GetCurrentUser)
		protected.PUT("/password", h.UpdatePassword)
	}
}

// Register handles POST /api/v1/auth/register
// @Summary      Register a new user
// @Description  Register a new user account
// @Tags         Auth
// @Accept       json
// @Produce      json
// @Param        request  body      contracts.RegisterRequest  true  "Registration details"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Router       /auth/register [post]
func (h *AuthHandler) Register(c *gin.Context) {
	var req contracts.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	resp, err := h.authSvc.Register(c.Request.Context(), req)
	if err != nil {
		HandleError(c, err)
		return
	}

	Created(c, resp)
}

// Login handles POST /api/v1/auth/login
// @Summary      Login user
// @Description  Authenticate user and return access tokens
// @Tags         Auth
// @Accept       json
// @Produce      json
// @Param        request  body      contracts.LoginRequest  true  "Login credentials"
// @Success      200      {object}  Response
// @Failure      401      {object}  Response
// @Failure      500      {object}  Response
// @Router       /auth/login [post]
func (h *AuthHandler) Login(c *gin.Context) {
	var req contracts.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	resp, err := h.authSvc.Login(c.Request.Context(), req)
	if err != nil {
		Error(c, http.StatusUnauthorized, 401, err.Error())
		return
	}

	Success(c, resp)
}

// RefreshToken handles POST /api/v1/auth/refresh
// @Summary      Refresh access token
// @Description  Refresh access token using refresh token
// @Tags         Auth
// @Accept       json
// @Produce      json
// @Param        refresh_token  body      object  true  "Refresh token"
// @Success      200      {object}  Response
// @Failure      401      {object}  Response
// @Failure      500      {object}  Response
// @Router       /auth/refresh [post]
// @Security     BearerAuth
func (h *AuthHandler) RefreshToken(c *gin.Context) {
	var req struct {
		RefreshToken string `json:"refresh_token" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	resp, err := h.authSvc.RefreshToken(c.Request.Context(), req.RefreshToken)
	if err != nil {
		Error(c, http.StatusUnauthorized, 401, err.Error())
		return
	}

	Success(c, resp)
}

// GetCurrentUser handles GET /api/v1/auth/me
// @Summary      Get current user
// @Description  Get current authenticated user information
// @Tags         Auth
// @Produce      json
// @Success      200      {object}  Response
// @Failure      401      {object}  Response
// @Failure      500      {object}  Response
// @Router       /auth/me [get]
// @Security     BearerAuth
func (h *AuthHandler) GetCurrentUser(c *gin.Context) {
	userID, exists := c.Get("user_id")
	if !exists {
		Error(c, http.StatusUnauthorized, 401, "user not authenticated")
		return
	}

	userInfo, err := h.authSvc.GetCurrentUser(c.Request.Context(), shared.ID(userID.(string)))
	if err != nil {
		HandleError(c, err)
		return
	}

	Success(c, userInfo)
}

// UpdatePassword handles PUT /api/v1/auth/password
// @Summary      Update password
// @Description  Update current user's password
// @Tags         Auth
// @Accept       json
// @Produce      json
// @Param        request  body      contracts.UpdatePasswordRequest  true  "Password update details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      401      {object}  Response
// @Failure      500      {object}  Response
// @Router       /auth/password [put]
// @Security     BearerAuth
func (h *AuthHandler) UpdatePassword(c *gin.Context) {
	userID, exists := c.Get("user_id")
	if !exists {
		Error(c, http.StatusUnauthorized, 401, "user not authenticated")
		return
	}

	var req contracts.UpdatePasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	if err := h.authSvc.UpdatePassword(c.Request.Context(), shared.ID(userID.(string)), req); err != nil {
		HandleError(c, err)
		return
	}

	Success(c, gin.H{"message": "password updated successfully"})
}
