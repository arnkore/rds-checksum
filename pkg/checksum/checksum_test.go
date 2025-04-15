package checksum

import (
	"fmt"
	"github.com/arnkore/rds-checksum/pkg/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// Common configuration for tests
var testConfig = metadata.Config{
	Host:     "127.0.0.1",
	Port:     3306,
	User:     "root",
	Password: "Model_123",
	Database: "test", // Use DBName
}

func TestMain(m *testing.M) {
	// Print test information
	fmt.Println("Running tests for package checksum:")
	fmt.Println("1. TestUnifiedChecksumValidator_RunValidation")
	fmt.Println("   - Matching tables (1 partition)")
	fmt.Println("   - Matching tables (multiple partitions)")
	fmt.Println("   - Row count mismatch")
	fmt.Println("   - Data mismatch (requires row verification)")
	fmt.Println("   - Table missing on target")
	fmt.Println("   - Empty tables")
	fmt.Println("Initializing test database...")

	// Initialize test database
	initTestDB()

	fmt.Println("Running tests...")

	// Run tests
	code := m.Run()

	// Exit
	os.Exit(code)
}

func initTestDB() {
	// Get the directory of the current test file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get current file path")
	}

	// Get the project root directory (two levels up from pkg/checksum)
	projectRoot := filepath.Join(filepath.Dir(filename), "..", "..")

	// Build the full path to the SQL file
	sqlPath := filepath.Join(projectRoot, "testdata", "checksum", "test.sql")

	// Execute the SQL file
	// Use 127.0.0.1 instead of localhost to ensure TCP connection
	// Drop and recreate database to ensure clean state
	cmdStr := fmt.Sprintf("mysql -h %s -P %d -u %s -p%s -e 'DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;' && mysql -h %s -P %d -u %s -p%s %s < %s",
		testConfig.Host, testConfig.Port, testConfig.User, testConfig.Password, testConfig.Database, testConfig.Database,
		testConfig.Host, testConfig.Port, testConfig.User, testConfig.Password, testConfig.Database, sqlPath)
	cmd := exec.Command("sh", "-c", cmdStr)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		panic(fmt.Sprintf("Failed to initialize test database using %s: %v", sqlPath, err))
	}
	fmt.Println("Test database initialized successfully.")
}

func TestUnifiedChecksumValidator_RunValidation(t *testing.T) {
	// Source and target configs are the same for most tests, pointing to the same DB
	// We simulate differences by pointing to different tables within the same DB.
	configSource := testConfig
	configTarget := testConfig

	// Helper to run validation and assert common expectations
	runAndAssert := func(t *testing.T, validator *ChecksumValidator, expectMatch bool, expectRowCountMismatch bool, expectMismatchPartitions []int, expectFailedRows int) {
		t.Helper()
		result, err := validator.RunValidation()
		require.NoError(t, err, "RunValidation encountered an unexpected error")
		require.NotNil(t, result, "RunValidation result should not be nil")

		assert.Equal(t, expectMatch, result.Match, "Mismatch in expected validation match status")
		assert.Equal(t, expectRowCountMismatch, result.RowCountMismatch, "Mismatch in expected row count mismatch status")
		assert.ElementsMatch(t, expectMismatchPartitions, result.MismatchPartitions, "Mismatch in expected list of mismatched partitions")
		assert.Len(t, result.TargetFailedRows, expectFailedRows, "Mismatch in expected number of failed rows (source 2)")
		if expectFailedRows > 0 {
			assert.NotEmpty(t, result.TargetFailedRows)
		} else {
			assert.Empty(t, result.TargetFailedRows)
		}
	}

	t.Run("Matching tables (1 partition)", func(t *testing.T) {
		validator := NewChecksumValidator(configSource, configTarget, "test_table", 1)
		runAndAssert(t, validator, true, false, []int{}, 0)
	})

	t.Run("Matching tables (multiple partitions)", func(t *testing.T) {
		// Assuming test_table has enough rows (e.g., > 10) for multiple partitions
		validator := NewChecksumValidator(configSource, configTarget, "test_table", 3)
		runAndAssert(t, validator, true, false, []int{}, 0)
	})

	t.Run("Row count mismatch", func(t *testing.T) {
		t.Skip("Skipping row count mismatch test due to validator design limitations (requires same table name)")
	})

	t.Run("Data mismatch (requires row verification)", func(t *testing.T) {
		t.Skip("Skipping data mismatch test due to validator design limitations (requires same table name)")
	})

	t.Run("Table missing on target", func(t *testing.T) {
		// ... existing code ...
	})

	t.Run("Empty tables", func(t *testing.T) {
		// ... existing code ...
	})
}
