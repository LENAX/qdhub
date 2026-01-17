// Package main is the entry point for QDHub CLI.
package main

import (
	"io"
	"log"
	"os"
	"strings"

	"qdhub/cmd"
)

// levelWriter wraps io.Writer to automatically add log level prefix
type levelWriter struct {
	writer io.Writer
}

func (w *levelWriter) Write(p []byte) (n int, err error) {
	// Check if log message already contains a level tag
	msg := string(p)
	if strings.Contains(msg, "[") && strings.Contains(msg, "]") {
		// Message already has a level tag, write as-is
		return w.writer.Write(p)
	}

	// Determine level based on message content
	level := "[INFO]"
	msgLower := strings.ToLower(msg)
	if strings.Contains(msgLower, "error") || strings.Contains(msgLower, "failed") || strings.Contains(msgLower, "fail") {
		level = "[ERROR]"
	} else if strings.Contains(msgLower, "warning") || strings.Contains(msgLower, "warn") {
		level = "[WARN]"
	} else if strings.Contains(msgLower, "panic") || strings.Contains(msgLower, "fatal") {
		level = "[FATAL]"
	} else if strings.Contains(msgLower, "debug") {
		level = "[DEBUG]"
	}

	// Insert level after timestamp and file:line, before message
	// Format: 2024/01/01 12:00:00.000000 file.go:10: [INFO] message
	parts := strings.SplitN(msg, ": ", 2)
	if len(parts) == 2 {
		// Insert level after file:line
		formatted := parts[0] + ": " + level + " " + parts[1]
		return w.writer.Write([]byte(formatted))
	}

	// Fallback: prepend level
	formatted := level + " " + msg
	return w.writer.Write([]byte(formatted))
}

func main() {
	// Configure global log format: level, timestamp, file:line
	// Format: 2024/01/01 12:00:00.000000 file.go:10: [INFO] message
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	
	// Set custom writer to automatically add log level
	log.SetOutput(&levelWriter{writer: os.Stderr})

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
