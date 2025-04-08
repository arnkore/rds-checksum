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
	fmt.Println("1. TestCalculateChecksum (2 cases)")
	fmt.Println("   - Valid table")
	fmt.Println("   - Non-existent table")
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
	tests := []struct {
		name     string
		table    string
		expected string
		wantErr  bool
	}{
		{
			name:     "Valid table",
			table:    "test_table",
			expected: "",
			wantErr:  false,
		},
		{
			name:     "Non-existent table",
			table:    "non_existent_table",
			expected: "",
			wantErr:  true,
		},
	}

	config := &Config{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "Model_123",
		Database: "test",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CalculateChecksum(config, tt.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("CalculateChecksum() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == "" {
				t.Error("CalculateChecksum() returned empty checksum for valid table")
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
			name:     "Matching checksums",
			checksum1: "123456",
			checksum2: "123456",
			expected: true,
		},
		{
			name:     "Different checksums",
			checksum1: "123456",
			checksum2: "654321",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompareChecksums(tt.checksum1, tt.checksum2); got != tt.expected {
				t.Errorf("CompareChecksums() = %v, want %v", got, tt.expected)
			}
		})
	}
} 