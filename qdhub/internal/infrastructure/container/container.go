// Package container provides dependency injection container for QDHub.
package container

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/casbin/casbin/v2"
	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
	authinfra "qdhub/internal/infrastructure/auth"
	"qdhub/internal/infrastructure/datasource"
	"qdhub/internal/infrastructure/datasourcevalidator"
	"qdhub/internal/infrastructure/datasource/tushare"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/persistence/uow"
	analysisinfra "qdhub/internal/infrastructure/analysis"
	"qdhub/internal/infrastructure/quantdb"
	"qdhub/internal/infrastructure/quantdb/duckdb"
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
	planExecutor       *scheduler.ScheduledPlanExecutor
	DependencyResolver sync.DependencyResolver

	// Application Services (backward compatibility: direct field access)
	MetadataSvc  contracts.MetadataApplicationService
	DataStoreSvc contracts.DataStoreApplicationService
	SyncSvc      contracts.SyncApplicationService
	WorkflowSvc  contracts.WorkflowApplicationService
	AuthSvc      contracts.AuthApplicationService
	AnalysisSvc contracts.AnalysisApplicationService

	// Built-in Workflow Initializer
	BuiltInInitializer *impl.BuiltInWorkflowInitializer

	// QuantDB adapter (支持 DuckDB, ClickHouse 等)
	QuantDBAdapter datastore.QuantDB

	// Unit of Work for transaction management
	UoW contracts.UnitOfWork

	// Auth components
	UserRepo     auth.UserRepository
	UserRoleRepo auth.UserRoleRepository
	JWTManager   *authinfra.JWTManager
	Enforcer     *casbin.Enforcer
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

	// QuantDB (DuckDB) - 默认数据存储路径
	// 如果设置，将在 Task Engine 初始化时创建 QuantDB adapter
	DefaultDuckDBPath string

	// Auth configuration
	JWTSecret         string        // JWT 签名密钥
	JWTExpiration     time.Duration // Access token 过期时间
	RefreshExpiration time.Duration // Refresh token 过期时间
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		DBDriver:                 "sqlite",
		DBDSN:                    "./data/qdhub.db",
		ServerHost:               "0.0.0.0",
		ServerPort:               8080,
		ServerMode:               "release",
		TaskEngineMaxConcurrency: 100,
		TaskEngineTimeout:        120, // 单任务执行超时（秒），元数据爬取等可能较慢
		MigrationPath:            "./migrations/001_init_schema.up.sql",
		JWTSecret:                "change-me-in-production",
		JWTExpiration:            24 * time.Hour,
		RefreshExpiration:        7 * 24 * time.Hour,
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

	// Step 5.5: Initialize Unit of Work
	if err := c.initUnitOfWork(); err != nil {
		return fmt.Errorf("failed to initialize unit of work: %w", err)
	}

	// Step 5.6: Initialize auth components
	if err := c.initAuth(); err != nil {
		return fmt.Errorf("failed to initialize auth: %w", err)
	}

	// Step 6: Initialize application services
	if err := c.initApplicationServices(); err != nil {
		return fmt.Errorf("failed to initialize application services: %w", err)
	}

	// Step 6.5: Restore enabled plans to scheduler (after restart)
	if err := c.restoreScheduledPlans(ctx); err != nil {
		logrus.Warnf("Warning: failed to restore scheduled plans: %v", err)
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

// runMigrations runs database migrations in order.
// Order: 001 -> 002_auth -> 002_seed_mapping_rules (SQLite only) -> 003 -> 004 (SQLite only) -> 005 -> 006_seed -> 007 -> 008_seed_guest.
func (c *Container) runMigrations() error {
	if c.config.MigrationPath == "" {
		return nil
	}

	// 001_init_schema
	migrationSQL, err := os.ReadFile(c.config.MigrationPath)
	if err != nil {
		return fmt.Errorf("failed to read migration file: %w", err)
	}
	if _, err := c.DB.Exec(string(migrationSQL)); err != nil {
		return fmt.Errorf("failed to run migration: %w", err)
	}

	// 002_auth_schema (driver-specific)
	if err := c.runAuthMigration(); err != nil {
		return fmt.Errorf("failed to run auth migration: %w", err)
	}

	// 002_seed_mapping_rules (SQLite: INSERT OR IGNORE)
	if c.config.DBDriver == "sqlite" {
		if err := c.runMigrationFile("./migrations/002_seed_mapping_rules.up.sql"); err != nil {
			return fmt.Errorf("failed to run 002_seed_mapping_rules: %w", err)
		}
	} else {
		logrus.Info("Skipping 002_seed_mapping_rules (SQLite only)")
	}

	// 003_sync_plan_migration
	if err := c.runMigrationFile("./migrations/003_sync_plan_migration.up.sql"); err != nil {
		return fmt.Errorf("failed to run 003_sync_plan_migration: %w", err)
	}

	// 004_api_sync_strategy (SQLite: INSERT OR REPLACE, randomblob)
	if c.config.DBDriver == "sqlite" {
		if err := c.runMigrationFile("./migrations/004_api_sync_strategy.up.sql"); err != nil {
			return fmt.Errorf("failed to run 004_api_sync_strategy: %w", err)
		}
	} else {
		logrus.Info("Skipping 004_api_sync_strategy (SQLite only)")
	}

	// 005_sync_plan_default_params (idempotent: ignore "duplicate column" on re-run)
	if err := c.runMigrationFileOrIgnoreDuplicateColumn("./migrations/005_sync_plan_default_params.up.sql"); err != nil {
		return fmt.Errorf("failed to run 005_sync_plan_default_params: %w", err)
	}

	// 006_seed_default_admin (driver-specific)
	if err := c.runDefaultAdminSeed(); err != nil {
		return fmt.Errorf("failed to seed default admin: %w", err)
	}

	// 007_daily_adj_factor_trade_date_expand
	if err := c.runMigrationFile("./migrations/007_daily_adj_factor_trade_date_expand.up.sql"); err != nil {
		return fmt.Errorf("failed to run 007_daily_adj_factor_trade_date_expand: %w", err)
	}

	// 009_add_api_metadata_param_dependencies (idempotent: 001 may already have the column)
	if err := c.runMigrationFileOrIgnoreDuplicateColumn("./migrations/009_add_api_metadata_param_dependencies.up.sql"); err != nil {
		return fmt.Errorf("failed to run 009_add_api_metadata_param_dependencies: %w", err)
	}

	// 008_seed_guest_user (driver-specific)
	if err := c.runGuestSeed(); err != nil {
		return fmt.Errorf("failed to seed guest user: %w", err)
	}

	logrus.Info("Database migrations applied successfully")
	return nil
}

// runMigrationFile reads and executes a single migration file.
func (c *Container) runMigrationFile(path string) error {
	migrationSQL, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if _, err := c.DB.Exec(string(migrationSQL)); err != nil {
		return err
	}
	logrus.Infof("Migration applied: %s", path)
	return nil
}

// runMigrationFileOrIgnoreDuplicateColumn runs a migration file; on "duplicate column" error (e.g. 005 re-run) it logs and returns nil.
func (c *Container) runMigrationFileOrIgnoreDuplicateColumn(path string) error {
	migrationSQL, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	_, err = c.DB.Exec(string(migrationSQL))
	if err != nil && (strings.Contains(err.Error(), "duplicate column") || strings.Contains(err.Error(), "Duplicate column")) {
		logrus.Infof("Migration %s already applied (column exists), skipping", path)
		return nil
	}
	if err != nil {
		return err
	}
	logrus.Infof("Migration applied: %s", path)
	return nil
}

// runAuthMigration runs the auth schema migration based on database type.
func (c *Container) runAuthMigration() error {
	var migrationFile string
	switch c.config.DBDriver {
	case "postgres":
		migrationFile = "./migrations/002_auth_schema.postgres.up.sql"
	case "mysql":
		migrationFile = "./migrations/002_auth_schema.mysql.up.sql"
	default:
		migrationFile = "./migrations/002_auth_schema.sqlite.up.sql"
	}

	migrationSQL, err := os.ReadFile(migrationFile)
	if err != nil {
		// Migration file might not exist, skip silently
		logrus.Warnf("Auth migration file not found: %s", migrationFile)
		return nil
	}

	if _, err := c.DB.Exec(string(migrationSQL)); err != nil {
		return fmt.Errorf("failed to run auth migration: %w", err)
	}

	logrus.Info("Auth migration applied successfully")
	return nil
}

// runDefaultAdminSeed runs the default admin user seed migration based on database type.
func (c *Container) runDefaultAdminSeed() error {
	var migrationFile string
	switch c.config.DBDriver {
	case "postgres":
		migrationFile = "./migrations/006_seed_default_admin.postgres.up.sql"
	case "mysql":
		migrationFile = "./migrations/006_seed_default_admin.mysql.up.sql"
	default:
		migrationFile = "./migrations/006_seed_default_admin.sqlite.up.sql"
	}

	migrationSQL, err := os.ReadFile(migrationFile)
	if err != nil {
		logrus.Warnf("Default admin seed file not found: %s", migrationFile)
		return nil
	}

	if _, err := c.DB.Exec(string(migrationSQL)); err != nil {
		return fmt.Errorf("failed to run default admin seed: %w", err)
	}

	logrus.Info("Default admin seed applied successfully")
	return nil
}

// runGuestSeed runs the guest user (viewer role) seed migration based on database type.
func (c *Container) runGuestSeed() error {
	var migrationFile string
	switch c.config.DBDriver {
	case "postgres":
		migrationFile = "./migrations/008_seed_guest_user.postgres.up.sql"
	case "mysql":
		migrationFile = "./migrations/008_seed_guest_user.mysql.up.sql"
	default:
		migrationFile = "./migrations/008_seed_guest_user.sqlite.up.sql"
	}

	migrationSQL, err := os.ReadFile(migrationFile)
	if err != nil {
		logrus.Warnf("Guest seed file not found: %s", migrationFile)
		return nil
	}

	if _, err := c.DB.Exec(string(migrationSQL)); err != nil {
		return fmt.Errorf("failed to run guest seed: %w", err)
	}

	logrus.Info("Guest user seed applied successfully")
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

	// Auth repositories
	userRepoImpl := repository.NewUserRepository(c.DB)
	c.UserRepo = userRepoImpl
	c.UserRoleRepo = userRepoImpl // UserRepositoryImpl implements both interfaces

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
	eng.SetInstanceManagerVersion(engine.InstanceManagerV3)

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

	// QuantDBFactory：按 target_db_path（来自数据库 Quant Data Store）按需创建 DuckDB 连接，与 config 默认路径解耦
	taskEngineDeps.QuantDBFactory = duckdb.NewFactory()
	log.Printf("[Container] ✅ QuantDBFactory (DuckDB) registered; sync/table jobs use target_db_path from data store")

	// 可选：若配置了默认 DuckDB 路径，仍注入单例 QuantDB 以兼容旧逻辑（优先使用 QuantDBFactory）
	if c.config.DefaultDuckDBPath != "" {
		log.Printf("[Container] DefaultDuckDBPath from config: '%s' (optional)", c.config.DefaultDuckDBPath)
		quantDBAdapter := duckdb.NewAdapter(c.config.DefaultDuckDBPath)
		if err := quantDBAdapter.Connect(ctx); err != nil {
			log.Printf("[Container] WARNING: Failed to connect to default DuckDB at %s: %v", c.config.DefaultDuckDBPath, err)
		} else {
			taskEngineDeps.QuantDB = quantDBAdapter
			c.QuantDBAdapter = quantDBAdapter
			log.Printf("[Container] ✅ QuantDB (DuckDB) default adapter also initialized: %s", c.config.DefaultDuckDBPath)
		}
	}

	if err := taskengine.Initialize(ctx, eng, taskEngineDeps); err != nil {
		return fmt.Errorf("failed to initialize task engine: %w", err)
	}

	// Create Task Engine adapter and workflow factory
	c.TaskEngineAdapter = taskengine.NewTaskEngineAdapter(eng, c.config.TaskEngineMaxConcurrency)
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

	c.planExecutor = scheduler.NewScheduledPlanExecutor()
	c.PlanScheduler = scheduler.NewCronScheduler(c.planExecutor)
	c.PlanScheduler.Start()

	// Initialize dependency resolver
	c.DependencyResolver = sync.NewDependencyResolver()

	logrus.Info("Scheduler initialized")
	return nil
}

// initUnitOfWork initializes the Unit of Work for transaction management.
func (c *Container) initUnitOfWork() error {
	c.UoW = uow.NewUnitOfWork(c.DB)
	logrus.Info("Unit of Work initialized")
	return nil
}

// initAuth initializes authentication and authorization components.
func (c *Container) initAuth() error {
	// Initialize JWT manager
	c.JWTManager = authinfra.NewJWTManager(
		c.config.JWTSecret,
		c.config.JWTExpiration,
		c.config.RefreshExpiration,
	)

	// Determine database type
	var dbType persistence.DBType
	switch c.config.DBDriver {
	case "postgres":
		dbType = persistence.DBTypePostgres
	case "mysql":
		dbType = persistence.DBTypeMySQL
	default:
		dbType = persistence.DBTypeSQLite
	}

	// Initialize Casbin enforcer
	enforcer, err := authinfra.NewCasbinEnforcer(c.DB.DB, dbType)
	if err != nil {
		return fmt.Errorf("failed to create casbin enforcer: %w", err)
	}
	c.Enforcer = enforcer

	// Initialize default policies (only if casbin_rule table is empty)
	// Check if policies exist by trying to get all policies
	allPolicies, err := enforcer.GetPolicy()
	if err != nil {
		return fmt.Errorf("failed to get policies: %w", err)
	}
	if len(allPolicies) == 0 {
		if err := authinfra.InitializeDefaultPolicies(enforcer); err != nil {
			return fmt.Errorf("failed to initialize default policies: %w", err)
		}
		logrus.Info("Default RBAC policies initialized")
	}

	logrus.Info("Auth components initialized")
	return nil
}

// initApplicationServices initializes all application services (application service module).
func (c *Container) initApplicationServices() error {
	// Workflow service (for workflow management API)
	c.WorkflowSvc = impl.NewWorkflowApplicationService(c.WorkflowRepo, c.TaskEngineAdapter)

	// Metadata service
	// 注意：MetadataSvc 使用 WorkflowExecutor（领域服务接口）而不是 WorkflowSvc（应用服务）
	// 这符合依赖倒置原则，避免了应用服务之间的直接依赖
	c.MetadataSvc = impl.NewMetadataApplicationService(c.DataSourceRepo, c.MetadataRepo, nil, c.WorkflowExecutor, datasourcevalidator.NewTokenValidator())

	// DataStore service（注入 QuantDBAdapter：新建 DuckDB 时生成文件，非 DuckDB 时尝试连接；校验/更新/删除依赖此 adapter）
	c.DataStoreSvc = impl.NewDataStoreApplicationService(
		c.DataStoreRepo,
		c.DataSourceRepo,
		c.SyncPlanRepo,
		c.WorkflowExecutor,
		quantdb.NewQuantDBAdapter(),
	)

	// Sync service
	// 使用 SyncPlan 模型
	c.SyncSvc = impl.NewSyncApplicationService(
		c.SyncPlanRepo,
		c.CronCalculator,
		c.PlanScheduler,
		c.DataSourceRepo,
		c.DataStoreRepo,
		c.WorkflowExecutor,
		c.DependencyResolver,
		c.TaskEngineAdapter,
		c.UoW,
	)

	// Auth service
	passwordHasher := auth.NewBcryptPasswordHasher(0) // Use default cost
	c.AuthSvc = impl.NewAuthApplicationService(
		c.UserRepo,
		c.UserRoleRepo,
		passwordHasher,
		c.JWTManager,
	)

	// Deferred injection: executor needs SyncSvc (breaks init cycle)
	c.planExecutor.SetSyncService(c.SyncSvc)

	// 注册 SyncCallbackInvoker，供 DataSyncCompleteHandler 触发 execution 回调（Plan.MarkCompleted）
	taskengine.RegisterSyncCallbackInvoker(c.TaskEngine, c.SyncSvc)

	logrus.Info("Application services initialized")
	return nil
}

// restoreScheduledPlans re-registers enabled plans with cron expression to the scheduler.
func (c *Container) restoreScheduledPlans(ctx context.Context) error {
	plans, err := c.SyncPlanRepo.GetEnabledPlans()
	if err != nil {
		return fmt.Errorf("get enabled plans: %w", err)
	}

	var restored int
	for _, plan := range plans {
		if plan.CronExpression != nil && *plan.CronExpression != "" {
			if err := c.PlanScheduler.SchedulePlan(plan.ID.String(), *plan.CronExpression); err != nil {
				logrus.Warnf("Failed to restore plan %s: %v", plan.ID, err)
				continue
			}
			restored++
		}
	}

	logrus.Infof("Restored %d scheduled plan(s)", restored)
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

	// 当已初始化 QuantDB 时，创建分析应用服务并注册 /analysis 路由
	if c.QuantDBAdapter != nil {
		var readers *analysisinfra.Readers
		if c.DataSourceRegistry != nil && c.MetadataRepo != nil {
			tokenResolver := &analysisinfra.TokenResolverImpl{Repo: c.MetadataRepo}
			fallback := analysisinfra.NewFallbackProvider("tushare", c.DataSourceRegistry, tokenResolver)
			readers = analysisinfra.NewReadersWithFallback(c.QuantDBAdapter, fallback)
		} else {
			readers = analysisinfra.NewReaders(c.QuantDBAdapter)
		}
		c.AnalysisSvc = impl.NewAnalysisApplicationService(analysisinfra.NewAnalysisServiceFromReaders(readers))
	}

	c.HTTPServer = httpserver.NewServer(
		serverConfig,
		c.AuthSvc,
		c.MetadataSvc,
		c.DataStoreSvc,
		c.SyncSvc,
		c.WorkflowSvc,
		c.AnalysisSvc,
		c.JWTManager,
		c.Enforcer,
		c.config.DBDSN, // 临时：供 GET /api/v1/debug/database 返回，便于 e2e 验证是否连错库
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

	// Close QuantDB adapter
	if c.QuantDBAdapter != nil {
		if err := c.QuantDBAdapter.Close(); err != nil {
			logrus.Errorf("Error closing QuantDB adapter: %v", err)
		}
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
