// Package main is the entry point for QDHub CLI.
package main

import (
	"os"

	"qdhub/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
