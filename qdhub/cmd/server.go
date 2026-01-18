package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"qdhub/internal/infrastructure/container"
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

	logrus.Infof("Starting QDHub server...")
	logrus.Infof("  Host: %s", host)
	logrus.Infof("  Port: %d", port)
	logrus.Infof("  Mode: %s", mode)
	logrus.Infof("  Database: %s (%s)", dbDriver, dbDSN)

	// Create container configuration
	config := container.DefaultConfig()
	config.DBDriver = dbDriver
	config.DBDSN = dbDSN
	config.ServerHost = host
	config.ServerPort = port
	config.ServerMode = mode

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
