package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func toInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func toUint64(val interface{}) (uint64, bool) {
	switch v := val.(type) {
	case int:
		return uint64(v), true
	case int64:
		return uint64(v), true
	case float32:
		return uint64(v), true
	case float64:
		return uint64(v), true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	case string:
		if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	case string:
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func convertToInt8(val interface{}, tableName, colName string) int8 {
	if v, ok := toInt64(val); ok {
		return int8(v)
	}
	fmt.Println("Invalid int8", tableName, colName, val)
	return 0
}

func convertToInt16(val interface{}, tableName, colName string) int16 {
	if v, ok := toInt64(val); ok {
		return int16(v)
	}
	fmt.Println("Invalid int16", tableName, colName, val)
	return 0
}

func convertToInt32(val interface{}, tableName, colName string) int32 {
	if v, ok := toInt64(val); ok {
		return int32(v)
	}
	fmt.Println("Invalid int32", tableName, colName, val)
	return 0
}

func convertToInt64(val interface{}, tableName, colName string) int64 {
	if v, ok := toInt64(val); ok {
		return v
	}
	fmt.Println("Invalid int64", tableName, colName, val)
	return 0
}

func convertToUInt8(val interface{}, tableName, colName string) uint8 {
	if v, ok := toUint64(val); ok {
		return uint8(v)
	}
	fmt.Println("Invalid uint8", tableName, colName, val)
	return 0
}

func convertToUInt16(val interface{}, tableName, colName string) uint16 {
	if v, ok := toUint64(val); ok {
		return uint16(v)
	}
	fmt.Println("Invalid uint16", tableName, colName, val)
	return 0
}

func convertToUInt32(val interface{}, tableName, colName string) uint32 {
	if v, ok := toUint64(val); ok {
		return uint32(v)
	}
	fmt.Println("Invalid uint32", tableName, colName, val)
	return 0
}

func convertToUInt64(val interface{}, tableName, colName string) uint64 {
	if v, ok := toUint64(val); ok {
		return v
	}
	fmt.Println("Invalid uint64", tableName, colName, val)
	return 0
}

func convertToFloat32(val interface{}, tableName, colName string) float32 {
	if v, ok := toFloat64(val); ok {
		return float32(v)
	}
	fmt.Println("Invalid float32", tableName, colName, val)
	return 0
}

func convertToFloat64(val interface{}, tableName, colName string) float64 {
	if v, ok := toFloat64(val); ok {
		return v
	}
	fmt.Println("Invalid float64", tableName, colName, val)
	return 0
}

func convertToString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', 3, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', 3, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func convertToTime(val interface{}, tableName, colName string) time.Time {
	switch v := val.(type) {
	case int:
		return time.UnixMilli(int64(v))
	case int64:
		return time.UnixMilli(v)
	case float32:
		return time.UnixMilli(int64(v))
	case float64:
		return time.UnixMilli(int64(v))
	case string:
		if date, err := time.Parse(time.RFC3339, strings.Replace(v, " ", "T", 1)+"Z"); err == nil {
			return date
		}
		fmt.Println("Invalid date", tableName, colName, v)
	default:
		fmt.Println("Invalid time", tableName, colName, val)
	}
	return time.Time{}
}

func convertToArray(val interface{}, tableName, colName string) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		return v
	case string:
		var arr []interface{}
		if err := json.Unmarshal([]byte(v), &arr); err == nil {
			return arr
		}
		fmt.Println("Invalid array", tableName, colName, v)
	default:
		fmt.Println("Invalid array", tableName, colName, val)
	}
	return []interface{}{}
}

func convertToStringMap(val interface{}, tableName, colName string) map[string]string {
	switch v := val.(type) {
	case map[string]string:
		return v
	case map[string]interface{}:
		result := make(map[string]string, len(v))
		for k, val := range v {
			result[k] = convertToString(val)
		}
		return result
	case string:
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(v), &m); err == nil {
			result := make(map[string]string, len(m))
			for k, val := range m {
				result[k] = convertToString(val)
			}
			return result
		}
		fmt.Println("Invalid string map", tableName, colName, v)
	default:
		fmt.Println("Invalid string map", tableName, colName, val)
	}
	return make(map[string]string)
}

func convertToFloat32Map(val interface{}, tableName, colName string) map[string]float32 {
	switch v := val.(type) {
	case map[string]float32:
		return v
	case map[string]interface{}:
		result := make(map[string]float32, len(v))
		for k, val := range v {
			result[k] = convertToFloat32(val, tableName, colName)
		}
		return result
	case string:
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(v), &m); err == nil {
			result := make(map[string]float32, len(m))
			for k, val := range m {
				result[k] = convertToFloat32(val, tableName, colName)
			}
			return result
		}
		fmt.Println("Invalid float32 map", tableName, colName, v)
	default:
		fmt.Println("Invalid float32 map", tableName, colName, val)
	}
	return make(map[string]float32)
}

func convertToFloat64Map(val interface{}, tableName, colName string) map[string]float64 {
	switch v := val.(type) {
	case map[string]float64:
		return v
	case map[string]interface{}:
		result := make(map[string]float64, len(v))
		for k, val := range v {
			result[k] = convertToFloat64(val, tableName, colName)
		}
		return result
	case string:
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(v), &m); err == nil {
			result := make(map[string]float64, len(m))
			for k, val := range m {
				result[k] = convertToFloat64(val, tableName, colName)
			}
			return result
		}
		fmt.Println("Invalid float64 map", tableName, colName, v)
	default:
		fmt.Println("Invalid float64 map", tableName, colName, val)
	}
	return make(map[string]float64)
}
