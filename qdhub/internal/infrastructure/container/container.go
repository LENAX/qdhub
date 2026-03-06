// Package container provides dependency injection container for QDHub.
package container

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"golang.org/x/crypto/bcrypt"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/casbin/casbin/v2"
	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
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
	"qdhub/internal/infrastructure/data_sync/cache"
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
	GetDataQualitySvc() contracts.DataQualityApplicationService
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
	MetadataSvc    contracts.MetadataApplicationService
	DataStoreSvc   contracts.DataStoreApplicationService
	DataQualitySvc contracts.DataQualityApplicationService
	SyncSvc        contracts.SyncApplicationService
	WorkflowSvc    contracts.WorkflowApplicationService
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
	MetadataSvc    contracts.MetadataApplicationService
	DataStoreSvc   contracts.DataStoreApplicationService
	DataQualitySvc contracts.DataQualityApplicationService
	SyncSvc        contracts.SyncApplicationService
	WorkflowSvc    contracts.WorkflowApplicationService
	AuthSvc        contracts.AuthApplicationService
	AnalysisSvc    contracts.AnalysisApplicationService

	// Built-in Workflow Initializer
	BuiltInInitializer *impl.BuiltInWorkflowInitializer

	// QuantDB adapter (支持 DuckDB, ClickHouse 等)
	QuantDBAdapter  datastore.QuantDB
	QuantDBFactory  datastore.QuantDBFactory

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
	ServerHost     string
	ServerPort     int
	ServerMode     string
	EnableSwagger  bool   // 生产环境建议 false，关闭 /swagger、/docs
	AdminPassword  string // 可选：覆盖默认 admin 密码（生产环境设强密码，如通过 QDHUB_AUTH_ADMIN_PASSWORD）
	GuestPassword  string // 可选：覆盖默认 guest 密码（生产环境设强密码，如通过 QDHUB_AUTH_GUEST_PASSWORD）

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
		EnableSwagger:            false, // 默认关闭；开发环境可通过 server.enable_swagger=true 或 QDHUB_SERVER_ENABLE_SWAGGER=true 开启
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

	// Step 6.2: Reconcile running sync executions from workflow engine state (startup self-healing)
	if svcImpl, ok := c.SyncSvc.(*impl.SyncApplicationServiceImpl); ok {
		if err := svcImpl.ReconcileRunningExecutions(ctx); err != nil {
			logrus.Warnf("Warning: failed to reconcile running sync executions: %v", err)
		}
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

// GetDataQualitySvc returns the data quality application service.
func (c *Container) GetDataQualitySvc() contracts.DataQualityApplicationService {
	return c.DataQualitySvc
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

// runMigrations 自动扫描并按顺序执行所有 *.up.sql 迁移文件。
// 行为与 CLI `qdhub migrate up` 保持一致：
//   - 按文件名排序应用（升序）
//   - 按数据库驱动过滤 driver-specific 迁移（*.sqlite.* / *.postgres.* / *.mysql.*）
//   - 对 "already exists"/"duplicate column" 等错误视为已应用，安全跳过
// 同时在所有迁移执行完后，应用 admin/guest 密码覆盖（若配置了环境变量）。
func (c *Container) runMigrations() error {
	if c.config.MigrationPath == "" {
		return nil
	}

	// 迁移文件所在目录：默认使用 MigrationPath 所在目录，未设置则退回 ./migrations
	migrationsDir := filepath.Dir(c.config.MigrationPath)
	if migrationsDir == "" || migrationsDir == "." {
		migrationsDir = "./migrations"
	}

	driver := c.config.DBDriver
	if driver == "" {
		driver = "sqlite"
	}

	// 确保 schema_migrations 表存在（用于记录迁移执行记录）
	if err := c.ensureSchemaMigrationsTable(driver); err != nil {
		return fmt.Errorf("failed to ensure schema_migrations table: %w", err)
	}

	// 扫描所有 *.up.sql
	upFiles, err := filepath.Glob(filepath.Join(migrationsDir, "*.up.sql"))
	if err != nil {
		return fmt.Errorf("failed to find migration files: %w", err)
	}
	upFiles = filterMigrationsByDriver(upFiles, driver)

	if len(upFiles) == 0 {
		logrus.Info("No migration files found")
		return nil
	}

	// 按文件名排序（升序），保证执行顺序确定
	sort.Strings(upFiles)

	appliedCount := 0
	for _, file := range upFiles {
		version := migrationVersionFromFile(file)

		// 若 schema_migrations 已记录，则直接跳过
		applied, err := c.isMigrationApplied(version)
		if err != nil {
			return fmt.Errorf("failed to check migration status for %s: %w", version, err)
		}
		if applied {
			logrus.Infof("Skip (recorded in schema_migrations): %s", version)
			continue
		}

		migrationSQL, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", file, err)
		}

		logrus.Infof("Applying migration (container): %s", filepath.Base(file))

		if _, err := c.DB.Exec(string(migrationSQL)); err != nil {
			msg := err.Error()
			// 与 CLI 行为保持一致：表已存在 / 列已存在时视为已应用；
			// 同时为兼容老库，这里会在 schema_migrations 中补上一条记录。
			if strings.Contains(msg, "already exists") || strings.Contains(msg, "Already exists") ||
				strings.Contains(msg, "duplicate column") || strings.Contains(msg, "Duplicate column") {
				logrus.Infof("  Skip (already applied in DB, backfilling schema_migrations): %s", version)
				if err := c.recordMigration(version); err != nil {
					return fmt.Errorf("failed to record existing migration %s: %w", version, err)
				}
				continue
			}
			return fmt.Errorf("failed to apply migration %s: %w", file, err)
		}

		if err := c.recordMigration(version); err != nil {
			return fmt.Errorf("failed to record migration %s: %w", version, err)
		}

		appliedCount++
		logrus.Infof("  Applied successfully: %s", version)
	}

	// 迁移之后应用 admin/guest 密码覆盖（若配置了强密码）
	if err := c.applyAdminPasswordOverride(); err != nil {
		return fmt.Errorf("failed to apply admin password override: %w", err)
	}
	if err := c.applyGuestPasswordOverride(); err != nil {
		return fmt.Errorf("failed to apply guest password override: %w", err)
	}

	if appliedCount == 0 {
		logrus.Info("All migrations already applied")
	} else {
		logrus.Infof("Applied %d migration(s) (container startup)", appliedCount)
	}

	logrus.Info("Database migrations applied successfully")
	return nil
}

// ensureSchemaMigrationsTable 创建 schema_migrations 表（若不存在）。
func (c *Container) ensureSchemaMigrationsTable(driver string) error {
	var sql string
	switch driver {
	case "postgres", "postgresql":
		sql = `CREATE TABLE IF NOT EXISTS schema_migrations (
	version VARCHAR(255) PRIMARY KEY,
	applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`
	default: // sqlite, mysql 等
		sql = `CREATE TABLE IF NOT EXISTS schema_migrations (
	version TEXT PRIMARY KEY,
	applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`
	}
	_, err := c.DB.Exec(sql)
	return err
}

// migrationVersionFromFile 从迁移文件名推导 version（与 scripts/migrate.sh 一致）。
// 例如：001_init_schema.up.sql -> 001_init_schema
//       004_auth_schema.sqlite.up.sql -> 004_auth_schema.sqlite
func migrationVersionFromFile(path string) string {
	base := filepath.Base(path)
	return strings.TrimSuffix(base, ".up.sql")
}

// isMigrationApplied 检查 schema_migrations 中是否已有记录。
func (c *Container) isMigrationApplied(version string) (bool, error) {
	var count int
	// 这里版本号来源于文件名，输入可控，直接拼接避免占位符差异问题。
	query := fmt.Sprintf("SELECT COUNT(*) FROM schema_migrations WHERE version = '%s';", version)
	if err := c.DB.Get(&count, query); err != nil {
		return false, err
	}
	return count > 0, nil
}

// recordMigration 在 schema_migrations 中记录一条迁移执行记录。
// 若已存在（UNIQUE 冲突），则安全忽略。
func (c *Container) recordMigration(version string) error {
	sql := fmt.Sprintf("INSERT INTO schema_migrations (version) VALUES ('%s');", version)
	if _, err := c.DB.Exec(sql); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "UNIQUE constraint failed") ||
			strings.Contains(msg, "duplicate key") ||
			strings.Contains(msg, "Duplicate entry") {
			// 已经有记录，忽略
			return nil
		}
		return err
	}
	return nil
}

// filterMigrationsByDriver 根据数据库驱动过滤迁移文件。
// 行为与 cmd/migrate.go 中的逻辑保持一致：
//   - *.sqlite.* 仅在 driver=sqlite 时执行
//   - *.postgres.* 仅在 driver=postgres 时执行
//   - *.mysql.* 仅在 driver=mysql 时执行
//   - 其他不带 driver 后缀的迁移对所有 driver 生效
func filterMigrationsByDriver(files []string, driver string) []string {
	if driver == "" {
		driver = "sqlite"
	}
	var out []string
	for _, f := range files {
		base := filepath.Base(f)
		if strings.Contains(base, ".sqlite.") {
			if driver == "sqlite" {
				out = append(out, f)
			}
			continue
		}
		if strings.Contains(base, ".postgres.") {
			if driver == "postgres" {
				out = append(out, f)
			}
			continue
		}
		if strings.Contains(base, ".mysql.") {
			if driver == "mysql" {
				out = append(out, f)
			}
			continue
		}
		out = append(out, f)
	}
	return out
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

// applyAdminPasswordOverride 若配置了 AdminPassword，则更新默认 admin 用户的密码哈希（生产环境强密码）。
func (c *Container) applyAdminPasswordOverride() error {
	if c.config.AdminPassword == "" {
		return nil
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(c.config.AdminPassword), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("bcrypt admin password: %w", err)
	}
	var query string
	switch c.config.DBDriver {
	case "postgres":
		query = "UPDATE users SET password_hash = $1, updated_at = CURRENT_TIMESTAMP WHERE username = $2"
	default:
		query = "UPDATE users SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?"
	}
	res, err := c.DB.Exec(query, string(hash), "admin")
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected > 0 {
		logrus.Info("Admin password override applied (strong password from config/env)")
	}
	return nil
}

// applyGuestPasswordOverride 若配置了 GuestPassword，则更新默认 guest 用户的密码哈希（生产环境强密码）。
func (c *Container) applyGuestPasswordOverride() error {
	if c.config.GuestPassword == "" {
		return nil
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(c.config.GuestPassword), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("bcrypt guest password: %w", err)
	}
	var query string
	switch c.config.DBDriver {
	case "postgres":
		query = "UPDATE users SET password_hash = $1, updated_at = CURRENT_TIMESTAMP WHERE username = $2"
	default:
		query = "UPDATE users SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?"
	}
	res, err := c.DB.Exec(query, string(hash), "guest")
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected > 0 {
		logrus.Info("Guest password override applied (viewer-only account from config/env)")
	}
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
	// 同一个 Factory 实例同时注入 Task Engine 和 Application Service，确保共享同一连接
	c.QuantDBFactory = duckdb.NewFactory()
	taskEngineDeps.QuantDBFactory = c.QuantDBFactory
	log.Printf("[Container] ✅ QuantDBFactory (DuckDB) registered; sync/table jobs use target_db_path from data store")

	// CommonDataCache：公共数据内存缓存（TTL 24h），SyncAPIDataJob 按 common_data_apis 走 Cache→DuckDB→API
	taskEngineDeps.CommonDataCache = cache.NewMemoryCommonDataCache(0)
	log.Printf("[Container] ✅ CommonDataCache (memory, TTL 24h) registered")

	// 可选：若配置了默认 DuckDB 路径，通过共享 Factory 获取连接（避免独立 DuckDB 引擎实例）
	if c.config.DefaultDuckDBPath != "" {
		log.Printf("[Container] DefaultDuckDBPath from config: '%s' (optional)", c.config.DefaultDuckDBPath)
		quantDBAdapter, err := c.QuantDBFactory.Create(datastore.QuantDBConfig{
			Type:        datastore.DataStoreTypeDuckDB,
			StoragePath: c.config.DefaultDuckDBPath,
		})
		if err != nil {
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
	} else {
		logrus.Info("Existing RBAC policies loaded from database")
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

	// QuantDB adapter（共享实例供 DataStore 与 DataQuality 使用）
	quantDBAdapter := quantdb.NewQuantDBAdapter(c.QuantDBFactory)

	// DataStore service（注入 QuantDBAdapter：共享 QuantDBFactory 确保与 Task Engine 使用同一连接）
	c.DataStoreSvc = impl.NewDataStoreApplicationService(
		c.DataStoreRepo,
		c.DataSourceRepo,
		c.SyncPlanRepo,
		c.WorkflowExecutor,
		quantDBAdapter,
	)

	// Sync service（需在 DataQuality 之前创建，因 DataQuality 依赖 SyncSvc 用于一键修复）
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
		c.MetadataRepo,
		c.QuantDBFactory,
	)

	// DataQuality service（独立应用服务，归属 datastore 领域，依赖 SyncSvc 用于一键修复）
	c.DataQualitySvc = impl.NewDataQualityApplicationService(c.DataStoreRepo, c.SyncPlanRepo, quantDBAdapter, c.SyncSvc)

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

// restoreScheduledPlans re-registers schedulable plans (non-disabled with cron) to the scheduler.
func (c *Container) restoreScheduledPlans(ctx context.Context) error {
	plans, err := c.SyncPlanRepo.GetSchedulablePlans()
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
	// WriteTimeout 设为 0 以支持长连接（如 progress-stream SSE），否则约 30s 后连接会被断开
	serverConfig := httpserver.ServerConfig{
		Host:          c.config.ServerHost,
		Port:          c.config.ServerPort,
		ReadTimeout:   30 * time.Second,
		WriteTimeout:  0,
		Mode:          c.config.ServerMode,
		EnableSwagger: c.config.EnableSwagger,
	}

	// 分析服务数据源：优先用默认 QuantDB；否则从 DataStore 中取第一个有效的 DuckDB 并连接
	// 注意：使用共享 QuantDBFactory 获取连接，避免创建独立的 DuckDB 引擎实例
	analysisQuantDB := c.QuantDBAdapter
	if analysisQuantDB == nil && c.DataStoreRepo != nil && c.QuantDBFactory != nil {
		stores, err := c.DataStoreRepo.List()
		if err != nil {
			logrus.Warnf("[Container] Analysis: cannot list data stores: %v", err)
		} else if len(stores) == 0 {
			logrus.Info("[Container] Analysis: no data stores in DB, /analysis routes will not be registered. Add a DuckDB data store and sync stock_basic/daily for stock charts.")
		} else {
			for _, ds := range stores {
				if ds.Type != datastore.DataStoreTypeDuckDB || strings.TrimSpace(ds.StoragePath) == "" || ds.Status != shared.StatusActive {
					continue
				}
				adapter, err := c.QuantDBFactory.Create(datastore.QuantDBConfig{
					Type:        datastore.DataStoreTypeDuckDB,
					StoragePath: ds.StoragePath,
				})
				if err != nil {
					logrus.Warnf("[Container] Analysis: skip data store %s (%s): %v", ds.Name, ds.StoragePath, err)
					continue
				}
				c.QuantDBAdapter = adapter
				analysisQuantDB = adapter
				logrus.Infof("[Container] Analysis using data store: %s (%s). Ensure tables stock_basic and daily exist (sync via Data Sync).", ds.Name, ds.StoragePath)
				break
			}
			if analysisQuantDB == nil {
				logrus.Info("[Container] Analysis: no connectable DuckDB data store found (wrong type, empty path, inactive, or connect failed). /analysis routes will not be registered.")
			}
		}
	}
	if analysisQuantDB != nil {
		var readers *analysisinfra.Readers
		if c.DataSourceRegistry != nil && c.MetadataRepo != nil {
			tokenResolver := &analysisinfra.TokenResolverImpl{Repo: c.MetadataRepo}
			fallback := analysisinfra.NewFallbackProvider("tushare", c.DataSourceRegistry, tokenResolver)
			readers = analysisinfra.NewReadersWithFallback(analysisQuantDB, fallback)
		} else {
			readers = analysisinfra.NewReaders(analysisQuantDB)
		}
		c.AnalysisSvc = impl.NewAnalysisApplicationService(analysisinfra.NewAnalysisServiceFromReaders(readers))
	}

	c.HTTPServer = httpserver.NewServer(
		serverConfig,
		c.AuthSvc,
		c.MetadataSvc,
		c.DataStoreSvc,
		c.DataQualitySvc,
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
