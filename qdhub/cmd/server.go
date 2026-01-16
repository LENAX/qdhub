package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"qdhub/internal/application/impl"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/scheduler"
	httpserver "qdhub/internal/interfaces/http"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the QDHub HTTP server",
	Long: `Start the QDHub HTTP server with all API endpoints.

The server provides RESTful APIs for:
  - Data source and metadata management
  - Quantitative data store configuration
  - Sync job management and execution
  - Workflow definition and execution

Swagger documentation is available at /swagger/index.html`,
	RunE: runServer,
}

var (
	serverHost string
	serverPort int
	serverMode string
)

func init() {
	rootCmd.AddCommand(serverCmd)

	// Server-specific flags
	serverCmd.Flags().StringVar(&serverHost, "host", "0.0.0.0", "server host address")
	serverCmd.Flags().IntVar(&serverPort, "port", 8080, "server port")
	serverCmd.Flags().StringVar(&serverMode, "mode", "release", "server mode (debug, release, test)")

	// Bind to viper
	viper.BindPFlag("server.host", serverCmd.Flags().Lookup("host"))
	viper.BindPFlag("server.port", serverCmd.Flags().Lookup("port"))
	viper.BindPFlag("server.mode", serverCmd.Flags().Lookup("mode"))
}

func runServer(cmd *cobra.Command, args []string) error {
	// Get configuration from viper
	host := viper.GetString("server.host")
	port := viper.GetInt("server.port")
	mode := viper.GetString("server.mode")
	dbDriver := viper.GetString("database.driver")
	dbDSN := viper.GetString("database.dsn")

	// Apply defaults if not set
	if host == "" {
		host = serverHost
	}
	if port == 0 {
		port = serverPort
	}
	if mode == "" {
		mode = serverMode
	}
	if dbDriver == "" {
		dbDriver = "sqlite"
	}
	if dbDSN == "" {
		dbDSN = "./data/qdhub.db"
	}

	log.Printf("Starting QDHub server...")
	log.Printf("  Host: %s", host)
	log.Printf("  Port: %d", port)
	log.Printf("  Mode: %s", mode)
	log.Printf("  Database: %s (%s)", dbDriver, dbDSN)

	// Ensure data directory exists for SQLite
	if dbDriver == "sqlite" {
		if err := os.MkdirAll("./data", 0755); err != nil {
			return fmt.Errorf("failed to create data directory: %w", err)
		}
	}

	// Initialize database
	db, err := persistence.NewDB(dbDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Run migrations (if migration file exists)
	if migrationSQL, err := os.ReadFile("./migrations/001_init_schema.up.sql"); err == nil {
		if _, err := db.Exec(string(migrationSQL)); err != nil {
			log.Printf("Migration warning (may already exist): %v", err)
		} else {
			log.Println("Database migrations applied successfully")
		}
	}

	// Initialize repositories
	dataSourceRepo := repository.NewDataSourceRepository(db)
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	mappingRuleRepo := repository.NewDataTypeMappingRuleRepository(db)
	syncJobRepo := repository.NewSyncJobRepository(db)
	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	if err != nil {
		return fmt.Errorf("failed to create workflow repository: %w", err)
	}

	// Initialize infrastructure services
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	jobScheduler := scheduler.NewCronScheduler(nil) // TODO: Add job trigger callback
	jobScheduler.Start()
	defer jobScheduler.Stop()

	// Initialize application services
	// Note: Using nil for adapters that aren't fully implemented yet
	metadataSvc := impl.NewMetadataApplicationService(dataSourceRepo, nil)
	dataStoreSvc := impl.NewDataStoreApplicationService(dataStoreRepo, mappingRuleRepo, dataSourceRepo, nil)
	syncSvc := impl.NewSyncApplicationService(syncJobRepo, workflowRepo, nil, cronCalculator, jobScheduler)
	workflowSvc := impl.NewWorkflowApplicationService(workflowRepo, nil)

	// Configure server
	serverConfig := httpserver.ServerConfig{
		Host:         host,
		Port:         port,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		Mode:         mode,
	}

	// Create and configure HTTP server
	server := httpserver.NewServer(serverConfig, metadataSvc, dataStoreSvc, syncSvc, workflowSvc)

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := server.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		return fmt.Errorf("server error: %w", err)
	case sig := <-quit:
		log.Printf("Received signal %v, shutting down...", sig)
	}

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown error: %w", err)
	}

	log.Println("Server stopped gracefully")
	return nil
}

func ginModeFromString(mode string) string {
	switch mode {
	case "debug":
		return gin.DebugMode
	case "test":
		return gin.TestMode
	default:
		return gin.ReleaseMode
	}
}
