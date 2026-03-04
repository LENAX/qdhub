package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/casbin/casbin/v2"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"qdhub/internal/application/contracts"
	authinfra "qdhub/internal/infrastructure/auth"

	_ "qdhub/docs" // Import generated swagger docs
)

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Host          string
	Port          int
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	Mode          string // debug, release, test
	EnableSwagger bool   // 生产环境建议设为 false，关闭 /swagger、/docs
}

// DefaultServerConfig returns the default server configuration.
// WriteTimeout 0 表示不限制，以支持 SSE 等长连接。
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Host:         "0.0.0.0",
		Port:         8080,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		Mode:         gin.ReleaseMode,
	}
}

// Server represents the HTTP server.
type Server struct {
	config     ServerConfig
	engine     *gin.Engine
	httpServer *http.Server

	// Handlers
	authHandler      *AuthHandler
	metadataHandler  *MetadataHandler
	dataStoreHandler *DataStoreHandler
	syncHandler      *SyncHandler
	workflowHandler  *WorkflowHandler
	analysisHandler  *AnalysisHandler

	// Auth components
	jwtManager *authinfra.JWTManager
	enforcer   *casbin.Enforcer

	// debugDBDSN 临时：当前连接的 DB DSN，用于 e2e 验证是否连错库，仅 debug 时设置
	debugDBDSN string
}

// NewServer creates a new HTTP server with the given configuration and services.
// debugDBDSN 可选，非空时注册 GET /api/v1/debug/database 返回当前连接的 DB DSN，用于 e2e 排查连错库。
// analysisSvc 可选，nil 时不注册 /analysis 路由。
func NewServer(
	config ServerConfig,
	authSvc contracts.AuthApplicationService,
	metadataSvc contracts.MetadataApplicationService,
	dataStoreSvc contracts.DataStoreApplicationService,
	dataQualitySvc contracts.DataQualityApplicationService,
	syncSvc contracts.SyncApplicationService,
	workflowSvc contracts.WorkflowApplicationService,
	analysisSvc contracts.AnalysisApplicationService,
	jwtManager *authinfra.JWTManager,
	enforcer *casbin.Enforcer,
	debugDBDSN string,
) *Server {
	// Set gin mode
	gin.SetMode(config.Mode)

	// Create gin engine
	engine := gin.New()
	// 允许去重连续斜杠，例如 /api/v1//sync-plans -> /api/v1/sync-plans
	engine.RemoveExtraSlash = true

	// Create handlers
	server := &Server{
		config:           config,
		engine:           engine,
		authHandler:      NewAuthHandler(authSvc),
		metadataHandler:  NewMetadataHandler(metadataSvc),
		dataStoreHandler: NewDataStoreHandler(dataStoreSvc, dataQualitySvc),
		syncHandler:      NewSyncHandler(syncSvc),
		workflowHandler:  NewWorkflowHandler(workflowSvc),
		jwtManager:       jwtManager,
		enforcer:         enforcer,
		debugDBDSN:       debugDBDSN,
	}
	if analysisSvc != nil {
		server.analysisHandler = NewAnalysisHandler(analysisSvc)
	}

	// Setup routes
	server.setupRoutes()

	return server
}

// setupRoutes configures all routes for the server.
func (s *Server) setupRoutes() {
	// Global middleware
	s.engine.Use(Recovery())
	s.engine.Use(Logger())
	s.engine.Use(CORS())

	// Inject JWT manager into context for middleware
	s.engine.Use(func(c *gin.Context) {
		c.Set("jwt_manager", s.jwtManager)
		c.Next()
	})

	// Health check
	s.engine.GET("/health", s.healthCheck)
	// Debug DB 接口仅在非 release 模式下启用，避免生产环境泄露 DSN
	if s.config.Mode != gin.ReleaseMode {
		// 临时 debug：根路径也注册，便于 e2e 访问（子进程可能未正确加载 /api/v1 路由）
		s.engine.GET("/debug/database", s.debugDatabase)
	}

	// Swagger documentation（生产环境可通过 server.enable_swagger=false 或 QDHUB_SERVER_ENABLE_SWAGGER=false 关闭）
	if s.config.EnableSwagger {
		s.engine.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
		s.engine.GET("/docs", func(c *gin.Context) {
			c.Redirect(302, "/swagger/index.html")
		})
		s.engine.GET("/docs/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	// API v1 routes
	v1 := s.engine.Group("/api/v1")
	{
		// 临时 debug：返回当前连接的 DB DSN，用于 e2e 验证是否连错库（仅在非 release 模式下注册）
		if s.config.Mode != gin.ReleaseMode {
			v1.GET("/debug/database", s.debugDatabase)
		}
		// Auth routes (public)
		s.authHandler.RegisterRoutes(v1)

		// Protected routes (require authentication and RBAC)
		protected := v1.Group("")
		protected.Use(JWTAuthMiddleware())
		if s.enforcer != nil {
			protected.Use(CasbinRBACMiddleware(s.enforcer))
		}
		{
			// Register all handler routes
			s.metadataHandler.RegisterRoutes(protected)
			s.dataStoreHandler.RegisterRoutes(protected)
			s.syncHandler.RegisterRoutes(protected)
			s.workflowHandler.RegisterRoutes(protected)
			if s.analysisHandler != nil {
				s.analysisHandler.RegisterRoutes(protected)
			}
		}
	}
}

// healthCheck handles GET /health. In debug mode, includes database_dsn for e2e to verify which DB the server uses.
func (s *Server) healthCheck(c *gin.Context) {
	out := gin.H{"status": "healthy", "version": "1.0.0"}
	c.JSON(http.StatusOK, out)
}

// debugDatabase 临时：GET /api/v1/debug/database 返回当前连接的 DB DSN，用于 e2e 验证是否连错库
func (s *Server) debugDatabase(c *gin.Context) {
	dsn := s.debugDBDSN
	if dsn == "" {
		dsn = "(not set)"
	}
	c.JSON(http.StatusOK, gin.H{
		"database_dsn": dsn,
	})
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)

	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      s.engine,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
	}

	logrus.Infof("[HTTP] Server starting on %s", addr)

	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	logrus.Info("[HTTP] Server shutting down...")

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown server: %w", err)
		}
	}

	logrus.Info("[HTTP] Server stopped")
	return nil
}

// Engine returns the underlying gin engine for testing purposes.
func (s *Server) Engine() *gin.Engine {
	return s.engine
}
