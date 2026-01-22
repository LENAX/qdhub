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
	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/datasource"
	"qdhub/internal/infrastructure/datasource/tushare"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/scheduler"
	"qdhub/internal/infrastructure/taskengine"
	"qdhub/internal/infrastructure/taskengine/workflows"
	httpserver "qdhub/internal/interfaces/http"
)

// DependencyContainer defines the interface for dependency injection container.
type DependencyContainer interface {
	// Infrastructure
	GetDB() *persistence.DB
	GetHTTPServer() *httpserver.Server

	// Repositories
	GetDataSourceRepo() metadata.DataSourceRepository
	GetDataStoreRepo() datastore.QuantDataStoreRepository
	GetMappingRuleRepo() datastore.DataTypeMappingRuleRepository
	GetSyncPlanRepo() sync.SyncPlanRepository
	GetWorkflowRepo() workflow.WorkflowDefinitionRepository
	GetMetadataRepo() metadata.Repository

	// Task Engine
	GetTaskEngine() *engine.Engine
	GetTaskEngineAdapter() workflow.TaskEngineAdapter
	GetWorkflowFactory() *workflows.WorkflowFactory
	GetWorkflowExecutor() workflow.WorkflowExecutor
	GetDataSourceRegistry() *datasource.Registry

	// Scheduler
	GetCronCalculator() sync.CronScheduleCalculator
	GetPlanScheduler() sync.PlanScheduler

	// Domain Services
	GetDependencyResolver() sync.DependencyResolver

	// Application Services
	GetMetadataSvc() contracts.MetadataApplicationService
	GetDataStoreSvc() contracts.DataStoreApplicationService
	GetSyncSvc() contracts.SyncApplicationService
	GetWorkflowSvc() contracts.WorkflowApplicationService

	// Built-in Workflow Initializer
	GetBuiltInInitializer() *impl.BuiltInWorkflowInitializer

	// Lifecycle
	Initialize(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// infrastructureModule holds infrastructure-related dependencies.
type infrastructureModule struct {
	DB         *persistence.DB
	HTTPServer *httpserver.Server
}

// repositoryModule holds all repository dependencies.
type repositoryModule struct {
	DataSourceRepo  metadata.DataSourceRepository
	DataStoreRepo   datastore.QuantDataStoreRepository
	MappingRuleRepo datastore.DataTypeMappingRuleRepository
	SyncPlanRepo    sync.SyncPlanRepository
	WorkflowRepo    workflow.WorkflowDefinitionRepository
	MetadataRepo    metadata.Repository
}

// taskEngineModule holds task engine related dependencies.
type taskEngineModule struct {
	TaskEngine         *engine.Engine
	TaskEngineAdapter  workflow.TaskEngineAdapter
	WorkflowFactory    *workflows.WorkflowFactory
	WorkflowExecutor   workflow.WorkflowExecutor
	DataSourceRegistry *datasource.Registry
}

// schedulerModule holds scheduler related dependencies.
type schedulerModule struct {
	CronCalculator     sync.CronScheduleCalculator
	PlanScheduler      sync.PlanScheduler
	DependencyResolver sync.DependencyResolver
}

// applicationServiceModule holds application service dependencies.
type applicationServiceModule struct {
	MetadataSvc  contracts.MetadataApplicationService
	DataStoreSvc contracts.DataStoreApplicationService
	SyncSvc      contracts.SyncApplicationService
	WorkflowSvc  contracts.WorkflowApplicationService
}

// Container holds all application dependencies and implements DependencyContainer interface.
// For backward compatibility, all fields are directly accessible.
type Container struct {
	// Configuration
	config Config

	// Infrastructure (backward compatibility: direct field access)
	DB         *persistence.DB
	HTTPServer *httpserver.Server

	// Repositories (backward compatibility: direct field access)
	DataSourceRepo  metadata.DataSourceRepository
	DataStoreRepo   datastore.QuantDataStoreRepository
	MappingRuleRepo datastore.DataTypeMappingRuleRepository
	SyncPlanRepo    sync.SyncPlanRepository
	WorkflowRepo    workflow.WorkflowDefinitionRepository
	MetadataRepo    metadata.Repository

	// Task Engine (backward compatibility: direct field access)
	TaskEngine         *engine.Engine
	TaskEngineAdapter  workflow.TaskEngineAdapter
	WorkflowFactory    *workflows.WorkflowFactory
	WorkflowExecutor   workflow.WorkflowExecutor
	DataSourceRegistry *datasource.Registry

	// Scheduler (backward compatibility: direct field access)
	CronCalculator     sync.CronScheduleCalculator
	PlanScheduler      sync.PlanScheduler
	DependencyResolver sync.DependencyResolver

	// Application Services (backward compatibility: direct field access)
	MetadataSvc  contracts.MetadataApplicationService
	DataStoreSvc contracts.DataStoreApplicationService
	SyncSvc      contracts.SyncApplicationService
	WorkflowSvc  contracts.WorkflowApplicationService

	// Built-in Workflow Initializer
	BuiltInInitializer *impl.BuiltInWorkflowInitializer
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
	logrus.Info("Initializing dependency container...")

	// Step 1: Initialize infrastructure (database)
	if err := c.initDatabase(); err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	// Step 2: Run migrations
	if err := c.runMigrations(); err != nil {
		logrus.Warnf("Migration warning: %v", err)
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
		logrus.Warnf("Warning: failed to initialize built-in workflows: %v", err)
		logrus.Warn("Built-in workflows can be initialized later")
	}

	// Step 8: Initialize HTTP server
	if err := c.initHTTPServer(); err != nil {
		return fmt.Errorf("failed to initialize HTTP server: %w", err)
	}

	logrus.Info("Dependency container initialized successfully")
	return nil
}

// DependencyContainer interface implementation

// GetDB returns the database instance.
func (c *Container) GetDB() *persistence.DB {
	return c.DB
}

// GetHTTPServer returns the HTTP server instance.
func (c *Container) GetHTTPServer() *httpserver.Server {
	return c.HTTPServer
}

// GetDataSourceRepo returns the data source repository.
func (c *Container) GetDataSourceRepo() metadata.DataSourceRepository {
	return c.DataSourceRepo
}

// GetDataStoreRepo returns the data store repository.
func (c *Container) GetDataStoreRepo() datastore.QuantDataStoreRepository {
	return c.DataStoreRepo
}

// GetMappingRuleRepo returns the mapping rule repository.
func (c *Container) GetMappingRuleRepo() datastore.DataTypeMappingRuleRepository {
	return c.MappingRuleRepo
}

// GetSyncPlanRepo returns the sync plan repository.
func (c *Container) GetSyncPlanRepo() sync.SyncPlanRepository {
	return c.SyncPlanRepo
}

// GetWorkflowRepo returns the workflow repository.
func (c *Container) GetWorkflowRepo() workflow.WorkflowDefinitionRepository {
	return c.WorkflowRepo
}

// GetMetadataRepo returns the metadata repository.
func (c *Container) GetMetadataRepo() metadata.Repository {
	return c.MetadataRepo
}

// GetTaskEngine returns the task engine instance.
func (c *Container) GetTaskEngine() *engine.Engine {
	return c.TaskEngine
}

// GetTaskEngineAdapter returns the task engine adapter.
func (c *Container) GetTaskEngineAdapter() workflow.TaskEngineAdapter {
	return c.TaskEngineAdapter
}

// GetWorkflowFactory returns the workflow factory.
func (c *Container) GetWorkflowFactory() *workflows.WorkflowFactory {
	return c.WorkflowFactory
}

// GetWorkflowExecutor returns the workflow executor.
func (c *Container) GetWorkflowExecutor() workflow.WorkflowExecutor {
	return c.WorkflowExecutor
}

// GetDataSourceRegistry returns the data source registry.
func (c *Container) GetDataSourceRegistry() *datasource.Registry {
	return c.DataSourceRegistry
}

// GetCronCalculator returns the cron calculator.
func (c *Container) GetCronCalculator() sync.CronScheduleCalculator {
	return c.CronCalculator
}

// GetPlanScheduler returns the plan scheduler.
func (c *Container) GetPlanScheduler() sync.PlanScheduler {
	return c.PlanScheduler
}

// GetDependencyResolver returns the dependency resolver.
func (c *Container) GetDependencyResolver() sync.DependencyResolver {
	return c.DependencyResolver
}

// GetMetadataSvc returns the metadata application service.
func (c *Container) GetMetadataSvc() contracts.MetadataApplicationService {
	return c.MetadataSvc
}

// GetDataStoreSvc returns the data store application service.
func (c *Container) GetDataStoreSvc() contracts.DataStoreApplicationService {
	return c.DataStoreSvc
}

// GetSyncSvc returns the sync application service.
func (c *Container) GetSyncSvc() contracts.SyncApplicationService {
	return c.SyncSvc
}

// GetWorkflowSvc returns the workflow application service.
func (c *Container) GetWorkflowSvc() contracts.WorkflowApplicationService {
	return c.WorkflowSvc
}

// GetBuiltInInitializer returns the built-in workflow initializer.
func (c *Container) GetBuiltInInitializer() *impl.BuiltInWorkflowInitializer {
	return c.BuiltInInitializer
}

// initDatabase initializes the database connection (infrastructure module).
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
	logrus.Infof("Database initialized: %s (%s)", c.config.DBDriver, c.config.DBDSN)
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

	logrus.Info("Database migrations applied successfully")
	return nil
}

// initRepositories initializes all repositories (repository module).
func (c *Container) initRepositories() error {
	c.DataSourceRepo = repository.NewDataSourceRepository(c.DB)
	c.DataStoreRepo = repository.NewQuantDataStoreRepository(c.DB)
	c.MappingRuleRepo = repository.NewDataTypeMappingRuleRepository(c.DB)
	c.SyncPlanRepo = repository.NewSyncPlanRepository(c.DB)

	workflowRepo, err := repository.NewWorkflowDefinitionRepository(c.DB)
	if err != nil {
		return fmt.Errorf("failed to create workflow repository: %w", err)
	}
	c.WorkflowRepo = workflowRepo

	// Metadata repository (for task engine dependencies)
	c.MetadataRepo = repository.NewMetadataRepository(c.DB)

	logrus.Info("Repositories initialized")
	return nil
}

// initTaskEngine initializes Task Engine and related components (task engine module).
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

	// Initialize DataSourceRegistry and register adapters
	c.DataSourceRegistry = datasource.NewRegistry()

	// Register Tushare adapter
	tushareAdapter := tushare.NewAdapter()
	if err := c.DataSourceRegistry.RegisterAdapter(tushareAdapter); err != nil {
		return fmt.Errorf("failed to register tushare adapter: %w", err)
	}
	log.Println("Registered Tushare data source adapter")

	// Initialize Task Engine (register job functions and handlers)
	taskEngineDeps := &taskengine.Dependencies{
		DataSourceRegistry: c.DataSourceRegistry,
		MetadataRepo:       c.MetadataRepo,
	}
	if err := taskengine.Initialize(ctx, eng, taskEngineDeps); err != nil {
		return fmt.Errorf("failed to initialize task engine: %w", err)
	}

	// Create Task Engine adapter and workflow factory
	c.TaskEngineAdapter = taskengine.NewTaskEngineAdapter(eng)
	c.WorkflowFactory = taskengine.GetWorkflowFactory(eng)

	// Create WorkflowExecutor (domain service for executing built-in workflows)
	// This avoids direct dependency between application services
	c.WorkflowExecutor = taskengine.NewWorkflowExecutor(c.WorkflowRepo, c.TaskEngineAdapter, c.MetadataRepo)

	logrus.Info("Task Engine initialized")
	return nil
}

// initScheduler initializes the scheduler (scheduler module).
func (c *Container) initScheduler() error {
	c.CronCalculator = scheduler.NewCronSchedulerCalculatorAdapter()
	c.PlanScheduler = scheduler.NewCronScheduler(nil) // TODO: Add plan trigger callback
	c.PlanScheduler.Start()

	// Initialize dependency resolver
	c.DependencyResolver = sync.NewDependencyResolver()

	logrus.Info("Scheduler initialized")
	return nil
}

// initApplicationServices initializes all application services (application service module).
func (c *Container) initApplicationServices() error {
	// Workflow service (for workflow management API)
	c.WorkflowSvc = impl.NewWorkflowApplicationService(c.WorkflowRepo, c.TaskEngineAdapter)

	// Metadata service
	// 注意：MetadataSvc 使用 WorkflowExecutor（领域服务接口）而不是 WorkflowSvc（应用服务）
	// 这符合依赖倒置原则，避免了应用服务之间的直接依赖
	c.MetadataSvc = impl.NewMetadataApplicationService(c.DataSourceRepo, c.MetadataRepo, nil, c.WorkflowExecutor)

	// DataStore service
	// 注意：DataStoreSvc 现在使用 WorkflowExecutor（领域服务接口）而不是 WorkflowSvc
	c.DataStoreSvc = impl.NewDataStoreApplicationService(
		c.DataStoreRepo,
		c.DataSourceRepo,
		c.WorkflowExecutor,
	)

	// Sync service
	// 使用 SyncPlan 模型
	c.SyncSvc = impl.NewSyncApplicationService(
		c.SyncPlanRepo,
		c.CronCalculator,
		c.PlanScheduler,
		c.DataSourceRepo,
		c.WorkflowExecutor,
		c.DependencyResolver,
	)

	logrus.Info("Application services initialized")
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

	logrus.Info("Built-in workflows initialized")
	return nil
}

// initHTTPServer initializes the HTTP server (infrastructure module).
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

	logrus.Info("HTTP server initialized")
	return nil
}

// Shutdown gracefully shuts down all resources.
func (c *Container) Shutdown(ctx context.Context) error {
	logrus.Info("Shutting down dependency container...")

	// Shutdown HTTP server
	if c.HTTPServer != nil {
		if err := c.HTTPServer.Shutdown(ctx); err != nil {
			logrus.Errorf("Error shutting down HTTP server: %v", err)
		}
	}

	// Shutdown scheduler
	if c.PlanScheduler != nil {
		stopCtx := c.PlanScheduler.Stop()
		// Wait for scheduler to stop or context timeout
		select {
		case <-stopCtx.Done():
			// Scheduler stopped successfully
		case <-ctx.Done():
			logrus.Warn("Context cancelled while waiting for scheduler to stop")
		}
	}

	// Shutdown Task Engine
	if c.TaskEngine != nil {
		c.TaskEngine.Stop()
	}

	// Close database
	if c.DB != nil {
		if err := c.DB.Close(); err != nil {
			logrus.Errorf("Error closing database: %v", err)
		}
	}

	logrus.Info("Dependency container shut down")
	return nil
}
