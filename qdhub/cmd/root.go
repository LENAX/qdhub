// Package cmd provides CLI commands for QDHub.
package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	verbose bool

	// Version information (set at build time)
	Version   = "0.1.0-beta.1"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

// rootCmd represents the base command.
var rootCmd = &cobra.Command{
	Use:   "qdhub",
	Short: "QDHub - Quantitative Data Hub Management System",
	Long: `QDHub is a comprehensive quantitative data management system 
that provides metadata management, automatic table creation, 
data synchronization, and workflow orchestration.

Features:
  - Metadata management for data source APIs
  - Automatic table schema generation
  - Data synchronization with workflow engine
  - RESTful API with Swagger documentation`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	// Global flags
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is ./configs/config.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "enable verbose output")

	// Bind flags to viper
	viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		// Search for config in default locations
		viper.AddConfigPath("./configs")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.qdhub")
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
	}

	// Read in environment variables that match
	viper.SetEnvPrefix("QDHUB")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// Read config file
	if err := viper.ReadInConfig(); err == nil {
		if verbose {
			fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
		}
	}
}
