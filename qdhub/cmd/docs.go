package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/cobra"
)

var docsCmd = &cobra.Command{
	Use:   "docs",
	Short: "Generate Swagger API documentation",
	Long: `Generate Swagger API documentation using swag.

This command uses the swag tool to scan the codebase and generate
Swagger documentation files in the docs/ directory.

The generated documentation will be available at /swagger/index.html
when the HTTP server is running.`,
	RunE: runDocs,
}

var (
	docsOutputDir       string
	docsParseDependency bool
	docsParseInternal   bool
)

func init() {
	rootCmd.AddCommand(docsCmd)

	// Docs-specific flags
	docsCmd.Flags().StringVar(&docsOutputDir, "output", "./docs", "Output directory for generated docs")
	docsCmd.Flags().BoolVar(&docsParseDependency, "parse-dependency", false, "Parse dependency")
	docsCmd.Flags().BoolVar(&docsParseInternal, "parse-internal", false, "Parse internal packages")
}

func runDocs(cmd *cobra.Command, args []string) error {
	// Check if swag is installed
	swagPath, err := exec.LookPath("swag")
	if err != nil {
		// Try common Go bin paths
		homeDir, _ := os.UserHomeDir()
		gopath := os.Getenv("GOPATH")

		possiblePaths := []string{
			filepath.Join(homeDir, "go", "bin", "swag"),
		}

		if gopath != "" {
			possiblePaths = append(possiblePaths, filepath.Join(gopath, "bin", "swag"))
		}

		// Check if swag exists in any of the common paths
		found := false
		for _, path := range possiblePaths {
			if _, statErr := os.Stat(path); statErr == nil {
				swagPath = path
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("swag tool not found. Please install it with: go install github.com/swaggo/swag/cmd/swag@latest")
		}
	}

	fmt.Println("Generating Swagger documentation...")
	fmt.Printf("  Using swag: %s\n", swagPath)
	fmt.Printf("  Output directory: %s\n", docsOutputDir)

	// Ensure output directory exists
	if err := os.MkdirAll(docsOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Get the project root (where go.mod is located)
	projectRoot, err := findProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to find project root: %w", err)
	}

	// Build swag command
	swagCmd := exec.Command(swagPath, "init")
	swagCmd.Dir = projectRoot
	swagCmd.Env = os.Environ()

	// Set output directory
	swagCmd.Args = append(swagCmd.Args, "-o", docsOutputDir)

	// Set parse flags
	if docsParseDependency {
		swagCmd.Args = append(swagCmd.Args, "--parseDependency")
	}
	if docsParseInternal {
		swagCmd.Args = append(swagCmd.Args, "--parseInternal")
	}

	// Set the general API info file
	swagCmd.Args = append(swagCmd.Args, "-g", "docs/swagger.go")

	// Run swag
	output, err := swagCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to generate docs: %w\nOutput: %s", err, string(output))
	}

	fmt.Println("Swagger documentation generated successfully!")
	fmt.Printf("  Files generated in: %s\n", docsOutputDir)
	fmt.Println("  Access documentation at: http://localhost:8080/swagger/index.html")

	return nil
}

// findProjectRoot finds the project root by looking for go.mod
func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		goModPath := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("go.mod not found")
}
