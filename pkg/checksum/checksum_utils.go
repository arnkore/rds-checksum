package checksum

import (
	"hash/crc32"
	"strconv"
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

func ConvertIntArrToStringArr(arr []int64) []string {
	li := make([]string, len(arr))
	for i := 0; i < len(arr); i++ {
		li[i] = strconv.FormatInt(arr[i], 10)
	}
	return li
}

// CompareChecksumMap compares source and target checksum maps and returns a list of keys
// present in the source map but either missing or having a different checksum in the target map.
func CompareChecksumMap(srcMap, targetMap map[int64]uint32) []int64 {
	var inconsistentPKeys []int64

	for key, sourceChecksum := range srcMap {
		targetChecksum, ok := targetMap[key]
		if !ok || sourceChecksum != targetChecksum {
			inconsistentPKeys = append(inconsistentPKeys, key)
		}
	}

	return inconsistentPKeys
}
