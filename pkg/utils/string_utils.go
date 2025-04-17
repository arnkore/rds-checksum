package utils

import (
	"hash/crc32"
	"strings"
)

func ConcateQuotedColumns(columns []string) string {
	if len(columns) == 0 {
		return ""
	}

	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		// 使用反引号将每个列名括起来
		quotedCols[i] = "`" + col + "`"
	}

	// 使用逗号将处理后的列名连接起来
	return strings.Join(quotedCols, ",")
}

func CalculateCRC32(input string) uint32 {
	// 使用 IEEE 多项式创建 CRC32 校验
	data := []byte(input)
	return crc32.ChecksumIEEE(data)
}
