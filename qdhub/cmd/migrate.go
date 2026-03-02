package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"qdhub/internal/infrastructure/persistence"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run database migrations",
	Long: `Run database migrations to set up or update the database schema.

Migrations are applied in order based on their filenames.
Use 'migrate up' to apply pending migrations or 'migrate down' to rollback.`,
}

var migrateUpCmd = &cobra.Command{
	Use:   "up",
	Short: "Apply all pending migrations",
	Long:  `Apply all pending database migrations in order.`,
	RunE:  runMigrateUp,
}

var migrateDownCmd = &cobra.Command{
	Use:   "down",
	Short: "Rollback the last migration",
	Long:  `Rollback the last applied database migration.`,
	RunE:  runMigrateDown,
}

var migrateStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show migration status",
	Long:  `Show the current status of database migrations.`,
	RunE:  runMigrateStatus,
}

var migrationsDir string

func init() {
	rootCmd.AddCommand(migrateCmd)
	migrateCmd.AddCommand(migrateUpCmd)
	migrateCmd.AddCommand(migrateDownCmd)
	migrateCmd.AddCommand(migrateStatusCmd)

	// Migration-specific flags
	migrateCmd.PersistentFlags().StringVar(&migrationsDir, "migrations-dir", "./migrations", "directory containing migration files")
}

// filterMigrationsByDriver keeps only migration files that match the current driver.
// Files without .sqlite. / .postgres. / .mysql. in the name are shared and always kept.
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

func runMigrateUp(cmd *cobra.Command, args []string) error {
	db, err := openDatabase()
	if err != nil {
		return err
	}
	defer db.Close()

	driver := viper.GetString("database.driver")
	if driver == "" {
		driver = "sqlite"
	}
	logrus.Infof("Database driver: %s", driver)

	// Get all migration files and filter by driver
	upFiles, err := filepath.Glob(filepath.Join(migrationsDir, "*.up.sql"))
	if err != nil {
		return fmt.Errorf("failed to find migration files: %w", err)
	}
	upFiles = filterMigrationsByDriver(upFiles, driver)

	if len(upFiles) == 0 {
		logrus.Info("No migration files found")
		return nil
	}

	// Sort migrations by name
	sort.Strings(upFiles)

	// Apply each migration
	appliedCount := 0
	for _, file := range upFiles {
		migrationSQL, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", file, err)
		}

		logrus.Infof("Applying migration: %s", filepath.Base(file))

		if _, err := db.Exec(string(migrationSQL)); err != nil {
			// Already applied
			if strings.Contains(err.Error(), "already exists") {
				logrus.Infof("  Skip (already applied)")
				continue
			}
			// Idempotent column add (e.g. 009)
			if strings.Contains(err.Error(), "duplicate column") || strings.Contains(err.Error(), "Duplicate column") {
				logrus.Infof("  Skip (column already exists)")
				continue
			}
			return fmt.Errorf("failed to apply migration %s: %w", file, err)
		}

		appliedCount++
		logrus.Infof("  Applied successfully")
	}

	if appliedCount == 0 {
		logrus.Info("All migrations already applied")
	} else {
		logrus.Infof("Applied %d migration(s)", appliedCount)
	}

	return nil
}

func runMigrateDown(cmd *cobra.Command, args []string) error {
	db, err := openDatabase()
	if err != nil {
		return err
	}
	defer db.Close()

	driver := viper.GetString("database.driver")
	if driver == "" {
		driver = "sqlite"
	}

	downFiles, err := filepath.Glob(filepath.Join(migrationsDir, "*.down.sql"))
	if err != nil {
		return fmt.Errorf("failed to find migration files: %w", err)
	}
	downFiles = filterMigrationsByDriver(downFiles, driver)

	if len(downFiles) == 0 {
		logrus.Info("No down migration files found")
		return nil
	}

	// Sort in reverse order (rollback latest first)
	sort.Sort(sort.Reverse(sort.StringSlice(downFiles)))

	// Apply the first (latest) down migration
	file := downFiles[0]
	migrationSQL, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("failed to read migration %s: %w", file, err)
	}

	logrus.Infof("Rolling back migration: %s", filepath.Base(file))

	if _, err := db.Exec(string(migrationSQL)); err != nil {
		return fmt.Errorf("failed to rollback migration %s: %w", file, err)
	}

	logrus.Info("Rollback completed successfully")
	return nil
}

func runMigrateStatus(cmd *cobra.Command, args []string) error {
	db, err := openDatabase()
	if err != nil {
		return err
	}
	defer db.Close()

	driver := viper.GetString("database.driver")
	if driver == "" {
		driver = "sqlite"
	}

	upFiles, err := filepath.Glob(filepath.Join(migrationsDir, "*.up.sql"))
	if err != nil {
		return fmt.Errorf("failed to find migration files: %w", err)
	}
	upFiles = filterMigrationsByDriver(upFiles, driver)

	if len(upFiles) == 0 {
		fmt.Println("No migration files found")
		return nil
	}

	sort.Strings(upFiles)

	fmt.Println("Migration Status:")
	fmt.Println("=================")

	for _, file := range upFiles {
		name := filepath.Base(file)
		// Check if table exists (simple heuristic)
		status := "unknown"
		fmt.Printf("  %s: %s\n", name, status)
	}

	return nil
}

func openDatabase() (*persistence.DB, error) {
	dbDriver := viper.GetString("database.driver")
	dbDSN := viper.GetString("database.dsn")

	if dbDriver == "" {
		dbDriver = "sqlite"
	}
	if dbDSN == "" {
		dbDSN = "./data/qdhub.db"
	}

	// Ensure data directory exists for SQLite
	if dbDriver == "sqlite" {
		if err := os.MkdirAll("./data", 0755); err != nil {
			return nil, fmt.Errorf("failed to create data directory: %w", err)
		}
	}

	db, err := persistence.NewDB(dbDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	return db, nil
}
