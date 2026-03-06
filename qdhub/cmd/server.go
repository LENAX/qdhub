package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"

	"qdhub/internal/infrastructure/container"
)

var serverCmd = &cobra.Command{
	Use:   "server [config-file]",
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
	serverHost        string
	serverPort        int
	serverMode        string
	defaultDuckDBPath string
)

func init() {
	rootCmd.AddCommand(serverCmd)

	// Server-specific flags
	serverCmd.Flags().StringVar(&serverHost, "host", "0.0.0.0", "server host address")
	serverCmd.Flags().IntVar(&serverPort, "port", 8080, "server port")
	serverCmd.Flags().StringVar(&serverMode, "mode", "release", "server mode (debug, release, test)")
	serverCmd.Flags().StringVar(&defaultDuckDBPath, "duckdb-path", "", "default DuckDB path for data sync (optional)")

	// Bind to viper
	viper.BindPFlag("server.host", serverCmd.Flags().Lookup("host"))
	viper.BindPFlag("server.port", serverCmd.Flags().Lookup("port"))
	viper.BindPFlag("server.mode", serverCmd.Flags().Lookup("mode"))
	viper.BindPFlag("quantdb.duckdb_path", serverCmd.Flags().Lookup("duckdb-path"))
}

func runServer(cmd *cobra.Command, args []string) error {
	// Config file: 1) positional arg (e2e 使用), 2) root --config, 3) viper
	dbDSN := ""
	duckDBPath := ""
	configPath := ""
	taskEngineWorkerCount := 0 // 0 表示使用 container 默认值
	if len(args) > 0 && args[0] != "" {
		configPath = args[0]
	}
	if configPath == "" {
		configPath, _ = cmd.Root().PersistentFlags().GetString("config")
	}
	if configPath == "" {
		configPath = viper.ConfigFileUsed()
	}
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err == nil {
			var cfg struct {
				Database struct {
					DSN string `yaml:"dsn"`
				} `yaml:"database"`
				QuantDB struct {
					DuckDBPath string `yaml:"duckdb_path"`
				} `yaml:"quantdb"`
				TaskEngine struct {
					WorkerCount int `yaml:"worker_count"`
				} `yaml:"task_engine"`
			}
			if err := yaml.Unmarshal(data, &cfg); err == nil {
				if cfg.Database.DSN != "" {
					dbDSN = cfg.Database.DSN
				}
				if cfg.QuantDB.DuckDBPath != "" {
					duckDBPath = cfg.QuantDB.DuckDBPath
				}
				if cfg.TaskEngine.WorkerCount > 0 {
					taskEngineWorkerCount = cfg.TaskEngine.WorkerCount
				}
			}
		}
	}
	// Fall back to viper (flags + env) when not from config file
	if dbDSN == "" {
		dbDSN = viper.GetString("database.dsn")
	}
	if duckDBPath == "" {
		duckDBPath = viper.GetString("quantdb.duckdb_path")
	}

	host := viper.GetString("server.host")
	port := viper.GetInt("server.port")
	mode := viper.GetString("server.mode")
	dbDriver := viper.GetString("database.driver")

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
	if duckDBPath == "" {
		duckDBPath = defaultDuckDBPath
	}

	log.Printf("Starting QDHub server...")
	log.Printf("  Host: %s", host)
	log.Printf("  Port: %d", port)
	log.Printf("  Mode: %s", mode)
	log.Printf("  Database: %s (%s)", dbDriver, dbDSN)
	// E2E: 写入 .dsn_resolved 到 config 所在目录，便于测试对比 server 实际使用的 DB（不依赖 dbDSN 是否含 /）
	if configPath != "" && dbDSN != "" {
		dsnDebugPath := filepath.Join(filepath.Dir(configPath), ".dsn_resolved")
		if writeErr := os.WriteFile(dsnDebugPath, []byte(dbDSN), 0600); writeErr != nil {
			log.Printf("  [e2e] 写入 .dsn_resolved 失败: %v", writeErr)
		} else {
			log.Printf("  [e2e] 已写入 .dsn_resolved -> %s", dsnDebugPath)
		}
	}
	log.Printf("  DuckDB Path (from config): '%s'", viper.GetString("quantdb.duckdb_path"))
	log.Printf("  DuckDB Path (from flag): '%s'", defaultDuckDBPath)
	log.Printf("  DuckDB Path (final): '%s'", duckDBPath)
	if duckDBPath != "" {
		log.Printf("  ✅ DuckDB Path: %s", duckDBPath)
	}
	if taskEngineWorkerCount > 0 {
		log.Printf("  Task Engine worker_count (from config): %d", taskEngineWorkerCount)
	}

	// Create container configuration
	config := container.DefaultConfig()
	config.DBDriver = dbDriver
	config.DBDSN = dbDSN
	config.ServerHost = host
	config.ServerPort = port
	config.ServerMode = mode
	config.DefaultDuckDBPath = duckDBPath
	if taskEngineWorkerCount > 0 {
		config.TaskEngineMaxConcurrency = taskEngineWorkerCount
	}
	if viper.IsSet("server.enable_swagger") {
		config.EnableSwagger = viper.GetBool("server.enable_swagger")
	}
	config.AdminPassword = viper.GetString("auth.admin_password")
	config.GuestPassword = viper.GetString("auth.guest_password")

	// Create and initialize container
	ctr := container.NewContainer(config)
	ctx := context.Background()

	if err := ctr.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize container: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := ctr.Shutdown(shutdownCtx); err != nil {
			logrus.Errorf("Error during container shutdown: %v", err)
		}
	}()

	// Start HTTP server in goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := ctr.HTTPServer.Start(); err != nil {
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
		logrus.Infof("Received signal %v, shutting down...", sig)
	}

	logrus.Info("Server stopped gracefully")
	return nil
}
