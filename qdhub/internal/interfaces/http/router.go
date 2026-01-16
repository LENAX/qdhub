package http

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"qdhub/internal/application/contracts"
)

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Host         string
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Mode         string // debug, release, test
}

// DefaultServerConfig returns the default server configuration.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Host:         "0.0.0.0",
		Port:         8080,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		Mode:         gin.ReleaseMode,
	}
}

// Server represents the HTTP server.
type Server struct {
	config     ServerConfig
	engine     *gin.Engine
	httpServer *http.Server

	// Handlers
	metadataHandler  *MetadataHandler
	dataStoreHandler *DataStoreHandler
	syncHandler      *SyncHandler
	workflowHandler  *WorkflowHandler
}

// NewServer creates a new HTTP server with the given configuration and services.
func NewServer(
	config ServerConfig,
	metadataSvc contracts.MetadataApplicationService,
	dataStoreSvc contracts.DataStoreApplicationService,
	syncSvc contracts.SyncApplicationService,
	workflowSvc contracts.WorkflowApplicationService,
) *Server {
	// Set gin mode
	gin.SetMode(config.Mode)

	// Create gin engine
	engine := gin.New()

	// Create handlers
	server := &Server{
		config:           config,
		engine:           engine,
		metadataHandler:  NewMetadataHandler(metadataSvc),
		dataStoreHandler: NewDataStoreHandler(dataStoreSvc),
		syncHandler:      NewSyncHandler(syncSvc),
		workflowHandler:  NewWorkflowHandler(workflowSvc),
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

	// Health check
	s.engine.GET("/health", s.healthCheck)

	// API v1 routes
	v1 := s.engine.Group("/api/v1")
	{
		// Register all handler routes
		s.metadataHandler.RegisterRoutes(v1)
		s.dataStoreHandler.RegisterRoutes(v1)
		s.syncHandler.RegisterRoutes(v1)
		s.workflowHandler.RegisterRoutes(v1)
	}
}

// healthCheck handles GET /health
func (s *Server) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "healthy",
		"version": "1.0.0",
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

	log.Printf("[HTTP] Server starting on %s", addr)

	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("[HTTP] Server shutting down...")

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown server: %w", err)
		}
	}

	log.Println("[HTTP] Server stopped")
	return nil
}

// Engine returns the underlying gin engine for testing purposes.
func (s *Server) Engine() *gin.Engine {
	return s.engine
}
