// Package main is the entry point for QDHub server.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"qdhub/pkg/config"
)

var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	showVersion := flag.Bool("version", false, "show version info")
	flag.Parse()

	if *showVersion {
		fmt.Printf("QDHub %s (built at %s)\n", version, buildTime)
		os.Exit(0)
	}

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Printf("Failed to load config from %s, using defaults: %v", *configPath, err)
		cfg = config.Default()
	}

	log.Printf("Starting QDHub server on %s:%d", cfg.Server.Host, cfg.Server.Port)

	// TODO: Initialize dependencies
	// TODO: Start HTTP server

	log.Printf("QDHub server started successfully")

	// Block forever (placeholder for actual server)
	select {}
}
