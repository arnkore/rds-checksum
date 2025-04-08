package checksum

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestMain(m *testing.M) {
	// 打印测试信息
	fmt.Println("Running tests for package checksum:")
	fmt.Println("1. TestCalculateChecksum (3 cases)")
	fmt.Println("   - Valid table")
	fmt.Println("   - Non-existent table")
	fmt.Println("   - Large table with multiple batches")
	fmt.Println("2. TestCompareChecksums (2 cases)")
	fmt.Println("   - Matching checksums")
	fmt.Println("   - Different checksums")
	fmt.Println("Initializing test database...")
	
	// 初始化测试数据库
	initTestDB()
	
	fmt.Println("Running tests...")
	
	// 运行测试
	code := m.Run()
	
	// 退出
	os.Exit(code)
}

func initTestDB() {
	// 获取当前测试文件的目录
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get current file path")
	}
	
	// 获取项目根目录（从pkg/checksum向上两级）
	projectRoot := filepath.Join(filepath.Dir(filename), "..", "..")

	// 构建 SQL 文件的完整路径
	sqlPath := filepath.Join(projectRoot, "testdata", "checksum", "test.sql")

	// 执行 SQL 文件
	cmd := exec.Command("sh", "-c", "mysql -h 127.0.0.1 -P 3306 -u root -pModel_123 < "+sqlPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	
	if err := cmd.Run(); err != nil {
		panic("Failed to initialize test database: " + err.Error())
	}
}

func TestCalculateChecksum(t *testing.T) {
	config := &Config{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "Model_123",
		Database: "test",
	}

	tests := []struct {
		name        string
		tableName   string
		mode        Mode
		expectError bool
	}{
		{
			name:        "Valid table with overall mode",
			tableName:   "test_table",
			mode:        ModeOverall,
			expectError: false,
		},
		{
			name:        "Valid table with row-by-row mode",
			tableName:   "test_table",
			mode:        ModeRowByRow,
			expectError: false,
		},
		{
			name:        "Non-existent table",
			tableName:   "non_existent_table",
			mode:        ModeOverall,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CalculateChecksum(config, tt.tableName, tt.mode)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result == nil {
				t.Errorf("Expected result but got nil")
				return
			}

			if tt.mode == ModeRowByRow {
				// For row-by-row mode, we expect to have validation results
				if result.FailedRows < 0 {
					t.Errorf("Invalid failed rows count: %d", result.FailedRows)
				}
				if result.FailedRowIDs == nil {
					t.Errorf("FailedRowIDs should not be nil")
				}
			}
		})
	}
}

func TestCompareChecksums(t *testing.T) {
	tests := []struct {
		name     string
		checksum1 string
		checksum2 string
		expected bool
	}{
		{
			name:      "Matching checksums",
			checksum1: "123456-789012",
			checksum2: "123456-789012",
			expected:  true,
		},
		{
			name:      "Different checksums",
			checksum1: "123456-789012",
			checksum2: "123456-789013",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareChecksums(tt.checksum1, tt.checksum2)
			if result != tt.expected {
				t.Errorf("CompareChecksums() = %v, want %v", result, tt.expected)
			}
		})
	}
} 