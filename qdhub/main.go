// Package main is the entry point for QDHub CLI.
package main

import (
	"os"

	"qdhub/cmd"
)

func main() {
	// Initialize logger
	InitLogger()

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
