// Package main provides logging configuration for QDHub.
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

// customFormatter formats logs as: timestamp [LEVEL] file:line message
type customFormatter struct {
	forceColors   bool
	disableColors bool
}

func (f *customFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	timestamp := entry.Time.Format("2006-01-02 15:04:05.000")
	level := strings.ToUpper(entry.Level.String())

	// Get caller information for line number
	caller := ""
	if entry.HasCaller() {
		caller = fmt.Sprintf("%s:%d", filepath.Base(entry.Caller.File), entry.Caller.Line)
	} else {
		caller = "unknown:0"
	}

	// Apply color to level only if colors are enabled
	levelStr := fmt.Sprintf("[%s]", level)
	if !f.disableColors && (f.forceColors || isTerminal(os.Stderr)) {
		levelStr = f.colorizeLevel(levelStr, entry.Level)
	}

	// Format: timestamp [LEVEL] file:line message
	// Example: 2026-01-17 18:10:00.123 [INFO] container.go:100 example info
	msg := fmt.Sprintf("%s %s %s %s",
		timestamp,
		levelStr,
		caller,
		entry.Message,
	)

	// Add fields if any
	if len(entry.Data) > 0 {
		msg += fmt.Sprintf(" %v", entry.Data)
	}

	msg += "\n"

	return []byte(msg), nil
}

// colorizeLevel applies ANSI color codes to the level string based on log level
func (f *customFormatter) colorizeLevel(levelStr string, level logrus.Level) string {
	var colorCode string
	switch level {
	case logrus.DebugLevel:
		colorCode = "\033[36m" // Cyan
	case logrus.InfoLevel:
		colorCode = "\033[32m" // Green
	case logrus.WarnLevel:
		colorCode = "\033[33m" // Yellow
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		colorCode = "\033[31m" // Red
	default:
		colorCode = "\033[0m" // Reset
	}
	resetCode := "\033[0m"
	return colorCode + levelStr + resetCode
}

// isTerminal checks if the writer is a terminal
func isTerminal(w io.Writer) bool {
	file, ok := w.(*os.File)
	if !ok {
		return false
	}
	return isatty(file)
}

// isatty checks if a file is a terminal
func isatty(f *os.File) bool {
	// Use file.Stat() to check if it's a character device (terminal)
	stat, err := f.Stat()
	if err != nil {
		return false
	}
	// Check if it's a character device (terminal)
	return (stat.Mode() & os.ModeCharDevice) != 0
}

// terminalHook writes logs to terminal with colors
type terminalHook struct {
	writer    io.Writer
	formatter logrus.Formatter
}

func (h *terminalHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *terminalHook) Fire(entry *logrus.Entry) error {
	formatted, err := h.formatter.Format(entry)
	if err != nil {
		return err
	}
	_, err = h.writer.Write(formatted)
	return err
}

// fileHook writes logs to file without colors
type fileHook struct {
	writer    io.Writer
	formatter logrus.Formatter
}

func (h *fileHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *fileHook) Fire(entry *logrus.Entry) error {
	formatted, err := h.formatter.Format(entry)
	if err != nil {
		return err
	}
	_, err = h.writer.Write(formatted)
	return err
}

// InitLogger initializes the logger with rotating file handler and custom formatter.
func InitLogger() {
	// Create rotating file handler
	logFile := &lumberjack.Logger{
		Filename:   "./logs/qdhub.log",
		MaxSize:    100, // megabytes
		MaxBackups: 5,   // keep 5 old log files
		MaxAge:     30,  // days
		Compress:   true,
		LocalTime:  true,
	}

	// Ensure log directory exists
	isTerminalOutput := isTerminal(os.Stderr)
	if err := os.MkdirAll("./logs", 0755); err != nil {
		// If we can't create log directory, fall back to stderr only
		logrus.SetOutput(os.Stderr)
		// Set formatter with colors for terminal
		logrus.SetFormatter(&customFormatter{
			forceColors:   isTerminalOutput,
			disableColors: !isTerminalOutput,
		})
	} else {
		// Use hooks to write to both terminal (with colors) and file (without colors)
		// Terminal output with colors
		if isTerminalOutput {
			logrus.AddHook(&terminalHook{
				writer:    os.Stderr,
				formatter: &customFormatter{forceColors: true, disableColors: false},
			})
		}
		// File output without colors
		logrus.AddHook(&fileHook{
			writer:    logFile,
			formatter: &customFormatter{forceColors: false, disableColors: true},
		})
		// Disable default output to avoid duplicate logs
		logrus.SetOutput(io.Discard)
		// Set formatter without colors as default (for hooks)
		logrus.SetFormatter(&customFormatter{
			forceColors:   false,
			disableColors: true,
		})
	}

	// Enable caller information for line numbers
	logrus.SetReportCaller(true)

	// Set log level (can be configured via environment variable LOG_LEVEL)
	logrus.SetLevel(logrus.InfoLevel)
}
