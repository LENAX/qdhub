// Package container provides dependency injection container for QDHub.
package container

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/scheduler"
	"qdhub/internal/infrastructure/taskengine"
	"qdhub/internal/infrastructure/taskengine/workflows"
	httpserver "qdhub/internal/interfaces/http"
)

// Container holds all application dependencies.
type Container struct {
	// Configuration
	config Config

	// Infrastructure
	DB *persistence.DB

	// Repositories
	DataSourceRepo  metadata.DataSourceRepository
	DataStoreRepo   datastore.QuantDataStoreRepository
	MappingRuleRepo datastore.DataTypeMappingRuleRepository
	SyncJobRepo     sync.SyncJobRepository
	WorkflowRepo    workflow.WorkflowDefinitionRepository
	MetadataRepo    metadata.Repository

	// Task Engine
	TaskEngine        *engine.Engine
	TaskEngineAdapter workflow.TaskEngineAdapter
	WorkflowFactory   *workflows.WorkflowFactory

	// Scheduler
	CronCalculator sync.CronScheduleCalculator
	JobScheduler   sync.JobScheduler

	// Application Services
	MetadataSvc  contracts.MetadataApplicationService
	DataStoreSvc contracts.DataStoreApplicationService
	SyncSvc      contracts.SyncApplicationService
	WorkflowSvc  contracts.WorkflowApplicationService

	// Built-in Workflow Initializer
	BuiltInInitializer *impl.BuiltInWorkflowInitializer

	// HTTP Server
	HTTPServer *httpserver.Server
}

// Config holds container configuration.
type Config struct {
	// Database
	DBDriver string
	DBDSN    string

	// Server
	ServerHost string
	ServerPort int
	ServerMode string

	// Task Engine
	TaskEngineMaxConcurrency int
	TaskEngineTimeout        int // seconds

	// Migration
	MigrationPath string
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		DBDriver:                 "sqlite",
		DBDSN:                    "./data/qdhub.db",
		ServerHost:               "0.0.0.0",
		ServerPort:               8080,
		ServerMode:               "release",
		TaskEngineMaxConcurrency: 10,
		TaskEngineTimeout:        60,
		MigrationPath:            "./migrations/001_init_schema.up.sql",
	}
}

// NewContainer creates a new container with the given configuration.
func NewContainer(config Config) *Container {
	return &Container{
		config: config,
	}
}

// Initialize initializes all dependencies in the correct order.
func (c *Container) Initialize(ctx context.Context) error {
	log.Println("Initializing dependency container...")

	// Step 1: Initialize database
	if err := c.initDatabase(); err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	// Step 2: Run migrations
	if err := c.runMigrations(); err != nil {
		log.Printf("Migration warning: %v", err)
	}

	// Step 3: Initialize repositories
	if err := c.initRepositories(); err != nil {
		return fmt.Errorf("failed to initialize repositories: %w", err)
	}

	// Step 4: Initialize Task Engine
	if err := c.initTaskEngine(ctx); err != nil {
		return fmt.Errorf("failed to initialize task engine: %w", err)
	}

	// Step 5: Initialize scheduler
	if err := c.initScheduler(); err != nil {
		return fmt.Errorf("failed to initialize scheduler: %w", err)
	}

	// Step 6: Initialize application services
	if err := c.initApplicationServices(); err != nil {
		return fmt.Errorf("failed to initialize application services: %w", err)
	}

	// Step 7: Initialize built-in workflows
	if err := c.initBuiltInWorkflows(ctx); err != nil {
		log.Printf("Warning: failed to initialize built-in workflows: %v", err)
		log.Printf("Built-in workflows can be initialized later")
	}

	// Step 8: Initialize HTTP server
	if err := c.initHTTPServer(); err != nil {
		return fmt.Errorf("failed to initialize HTTP server: %w", err)
	}

	log.Println("Dependency container initialized successfully")
	return nil
}

// initDatabase initializes the database connection.
func (c *Container) initDatabase() error {
	// Ensure data directory exists for SQLite
	if c.config.DBDriver == "sqlite" {
		if err := os.MkdirAll("./data", 0755); err != nil {
			return fmt.Errorf("failed to create data directory: %w", err)
		}
	}

	db, err := persistence.NewDB(c.config.DBDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	c.DB = db
	log.Printf("Database initialized: %s (%s)", c.config.DBDriver, c.config.DBDSN)
	return nil
}

// runMigrations runs database migrations.
func (c *Container) runMigrations() error {
	if c.config.MigrationPath == "" {
		return nil
	}

	migrationSQL, err := os.ReadFile(c.config.MigrationPath)
	if err != nil {
		return fmt.Errorf("failed to read migration file: %w", err)
	}

	if _, err := c.DB.Exec(string(migrationSQL)); err != nil {
		return fmt.Errorf("failed to run migration: %w", err)
	}

	log.Println("Database migrations applied successfully")
	return nil
}

// initRepositories initializes all repositories.
func (c *Container) initRepositories() error {
	c.DataSourceRepo = repository.NewDataSourceRepository(c.DB)
	c.DataStoreRepo = repository.NewQuantDataStoreRepository(c.DB)
	c.MappingRuleRepo = repository.NewDataTypeMappingRuleRepository(c.DB)
	c.SyncJobRepo = repository.NewSyncJobRepository(c.DB)

	workflowRepo, err := repository.NewWorkflowDefinitionRepository(c.DB)
	if err != nil {
		return fmt.Errorf("failed to create workflow repository: %w", err)
	}
	c.WorkflowRepo = workflowRepo

	// Metadata repository (for task engine dependencies)
	c.MetadataRepo = repository.NewMetadataRepository(c.DB)

	log.Println("Repositories initialized")
	return nil
}

// initTaskEngine initializes Task Engine and related components.
func (c *Container) initTaskEngine(ctx context.Context) error {
	// Create Task Engine aggregate repository
	taskEngineDSN := c.DB.DSN()
	aggregateRepo, err := sqlite.NewWorkflowAggregateRepoFromDSN(taskEngineDSN)
	if err != nil {
		return fmt.Errorf("failed to create task engine aggregate repository: %w", err)
	}

	// Create Task Engine instance
	eng, err := engine.NewEngineWithAggregateRepo(
		c.config.TaskEngineMaxConcurrency,
		c.config.TaskEngineTimeout,
		aggregateRepo,
	)
	if err != nil {
		return fmt.Errorf("failed to create task engine: %w", err)
	}

	// Start Task Engine
	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("failed to start task engine: %w", err)
	}

	c.TaskEngine = eng

	// Initialize Task Engine (register job functions and handlers)
	taskEngineDeps := &taskengine.Dependencies{
		DataSourceRegistry: nil, // TODO: Initialize datasource registry if needed
		MetadataRepo:       c.MetadataRepo,
	}
	if err := taskengine.Initialize(ctx, eng, taskEngineDeps); err != nil {
		return fmt.Errorf("failed to initialize task engine: %w", err)
	}

	// Create Task Engine adapter and workflow factory
	c.TaskEngineAdapter = taskengine.NewTaskEngineAdapter(eng)
	c.WorkflowFactory = taskengine.GetWorkflowFactory(eng)

	log.Println("Task Engine initialized")
	return nil
}

// initScheduler initializes the scheduler.
func (c *Container) initScheduler() error {
	c.CronCalculator = scheduler.NewCronSchedulerCalculatorAdapter()
	c.JobScheduler = scheduler.NewCronScheduler(nil) // TODO: Add job trigger callback
	c.JobScheduler.Start()

	log.Println("Scheduler initialized")
	return nil
}

// initApplicationServices initializes all application services.
func (c *Container) initApplicationServices() error {
	// Metadata service (using nil for parser factory - TODO: implement if needed)
	c.MetadataSvc = impl.NewMetadataApplicationService(c.DataSourceRepo, nil)

	// Workflow service (needed for dataStoreSvc)
	c.WorkflowSvc = impl.NewWorkflowApplicationService(c.WorkflowRepo, c.TaskEngineAdapter)

	// DataStore service
	c.DataStoreSvc = impl.NewDataStoreApplicationService(
		c.DataStoreRepo,
		c.MappingRuleRepo,
		c.DataSourceRepo,
		nil, // TODO: QuantDB adapter
		c.WorkflowSvc,
	)

	// Sync service
	c.SyncSvc = impl.NewSyncApplicationService(
		c.SyncJobRepo,
		c.WorkflowRepo,
		nil, // TODO: Sync execution callback
		c.CronCalculator,
		c.JobScheduler,
	)

	log.Println("Application services initialized")
	return nil
}

// initBuiltInWorkflows initializes built-in workflows.
func (c *Container) initBuiltInWorkflows(ctx context.Context) error {
	c.BuiltInInitializer = impl.NewBuiltInWorkflowInitializer(
		c.WorkflowRepo,
		c.WorkflowFactory,
		c.TaskEngineAdapter,
	)

	if err := c.BuiltInInitializer.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize built-in workflows: %w", err)
	}

	log.Println("Built-in workflows initialized")
	return nil
}

// initHTTPServer initializes the HTTP server.
func (c *Container) initHTTPServer() error {
	serverConfig := httpserver.ServerConfig{
		Host:         c.config.ServerHost,
		Port:         c.config.ServerPort,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		Mode:         c.config.ServerMode,
	}

	c.HTTPServer = httpserver.NewServer(
		serverConfig,
		c.MetadataSvc,
		c.DataStoreSvc,
		c.SyncSvc,
		c.WorkflowSvc,
	)

	log.Println("HTTP server initialized")
	return nil
}

// Shutdown gracefully shuts down all resources.
func (c *Container) Shutdown(ctx context.Context) error {
	log.Println("Shutting down dependency container...")

	// Shutdown HTTP server
	if c.HTTPServer != nil {
		if err := c.HTTPServer.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down HTTP server: %v", err)
		}
	}

	// Shutdown scheduler
	if c.JobScheduler != nil {
		stopCtx := c.JobScheduler.Stop()
		// Wait for scheduler to stop or context timeout
		select {
		case <-stopCtx.Done():
			// Scheduler stopped successfully
		case <-ctx.Done():
			log.Printf("Context cancelled while waiting for scheduler to stop")
		}
	}

	// Shutdown Task Engine
	if c.TaskEngine != nil {
		c.TaskEngine.Stop()
	}

	// Close database
	if c.DB != nil {
		if err := c.DB.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}

	log.Println("Dependency container shut down")
	return nil
}
