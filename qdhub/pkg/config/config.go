// Package config provides configuration management for QDHub.
package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration.
type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Database   DatabaseConfig   `yaml:"database"`
	TaskEngine TaskEngineConfig `yaml:"task_engine"`
	DataSource DataSourceConfig `yaml:"datasources"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// DatabaseConfig holds database connection settings.
type DatabaseConfig struct {
	Driver string `yaml:"driver"` // sqlite or postgres
	DSN    string `yaml:"dsn"`
}

// TaskEngineConfig holds task engine settings.
type TaskEngineConfig struct {
	WorkerCount int `yaml:"worker_count"`
	TaskTimeout int `yaml:"task_timeout"` // seconds
}

// DataSourceConfig holds data source settings.
type DataSourceConfig struct {
	Tushare TushareConfig `yaml:"tushare"`
}

// TushareConfig holds Tushare-specific settings.
type TushareConfig struct {
	Enabled bool   `yaml:"enabled"`
	BaseURL string `yaml:"base_url"`
	DocURL  string `yaml:"doc_url"`
}

// Load reads configuration from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Default returns a default configuration.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			Host: "0.0.0.0",
			Port: 8080,
		},
		Database: DatabaseConfig{
			Driver: "sqlite",
			DSN:    "./data/qdhub.db",
		},
		TaskEngine: TaskEngineConfig{
			WorkerCount: 100,
			TaskTimeout: 60,
		},
		DataSource: DataSourceConfig{
			Tushare: TushareConfig{
				Enabled: true,
				BaseURL: "http://api.tushare.pro",
				DocURL:  "https://tushare.pro/document/2",
			},
		},
	}
}
