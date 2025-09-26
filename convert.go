package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func convertToInt8(val interface{}, tableName string, colName string) int8 {
	switch v := val.(type) {
	case int:
		return int8(v)
	case int64:
		return int8(v)
	case float32:
		return int8(v)
	case float64:
		return int8(v)
	case bool:
		if v {
			return 1
		} else {
			return 0
		}
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 8); err == nil {
			return int8(parsed)
		} else {
			fmt.Println("Invalid int8 number", tableName, colName, v)
			return 0
		}
	default:
		fmt.Println("Invalid col conversion to int8", tableName, colName, v)
		return 0
	}
}

func convertToInt16(val interface{}, tableName string, colName string) int16 {
	switch v := val.(type) {
	case int:
		return int16(v)
	case int64:
		return int16(v)
	case float32:
		return int16(v)
	case float64:
		return int16(v)
	case bool:
		if v {
			return 1
		} else {
			return 0
		}
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 16); err == nil {
			return int16(parsed)
		} else {
			fmt.Println("Invalid int16 number", tableName, colName, v)
			return 0
		}
	default:
		fmt.Println("Invalid col conversion to int16", tableName, colName, v)
		return 0
	}
}

func convertToInt32(val interface{}, tableName string, colName string) int32 {
	switch v := val.(type) {
	case int:
		return int32(v)
	case int64:
		return int32(v)
	case float32:
		return int32(v)
	case float64:
		return int32(v)
	case bool:
		if v {
			return 1
		} else {
			return 0
		}
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 32); err == nil {
			return int32(parsed)
		} else {
			fmt.Println("Invalid int32 number", tableName, colName, v)
			return 0
		}
	default:
		fmt.Println("Invalid col conversion to int32", tableName, colName, v)
		return 0
	}
}

func convertToInt64(val interface{}, tableName string, colName string) int64 {
	switch v := val.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	case bool:
		if v {
			return 1
		} else {
			return 0
		}
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed
		} else {
			fmt.Println("Invalid int64 number", tableName, colName, v)
			return 0
		}
	default:
		fmt.Println("Invalid col conversion to int64", tableName, colName, v)
		return 0
	}
}

func convertToUInt8(val interface{}, tableName, colName string) uint8 {
	switch v := val.(type) {
	case int:
		return uint8(v)
	case int64:
		return uint8(v)
	case float32:
		return uint8(v)
	case float64:
		return uint8(v)
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		if parsed, err := strconv.ParseUint(v, 10, 8); err == nil {
			return uint8(parsed)
		}
		fmt.Println("Invalid uint8 number", tableName, colName, v)
		return 0
	default:
		fmt.Println("Invalid col conversion to uint8", tableName, colName, v)
		return 0
	}
}

func convertToUInt16(val interface{}, tableName, colName string) uint16 {
	switch v := val.(type) {
	case int:
		return uint16(v)
	case int64:
		return uint16(v)
	case float32:
		return uint16(v)
	case float64:
		return uint16(v)
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		if parsed, err := strconv.ParseUint(v, 10, 16); err == nil {
			return uint16(parsed)
		}
		fmt.Println("Invalid uint16 number", tableName, colName, v)
		return 0
	default:
		fmt.Println("Invalid col conversion to uint16", tableName, colName, v)
		return 0
	}
}

func convertToUInt32(val interface{}, tableName, colName string) uint32 {
	switch v := val.(type) {
	case int:
		return uint32(v)
	case int64:
		return uint32(v)
	case float32:
		return uint32(v)
	case float64:
		return uint32(v)
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		if parsed, err := strconv.ParseUint(v, 10, 32); err == nil {
			return uint32(parsed)
		}
		fmt.Println("Invalid uint32 number", tableName, colName, v)
		return 0
	default:
		fmt.Println("Invalid col conversion to uint32", tableName, colName, v)
		return 0
	}
}

func convertToUInt64(val interface{}, tableName, colName string) uint64 {
	switch v := val.(type) {
	case int:
		return uint64(v)
	case int64:
		return uint64(v)
	case float32:
		return uint64(v)
	case float64:
		return uint64(v)
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
			return parsed
		}
		fmt.Println("Invalid uint64 number", tableName, colName, v)
		return 0
	default:
		fmt.Println("Invalid col conversion to uint64", tableName, colName, v)
		return 0
	}
}

func convertToArray(val interface{}, tableName, colName string) []interface{} {
	switch v := val.(type) {
	case []interface{}:
		return v
	case string:
		// Try to parse JSON array string
		var arr []interface{}
		if err := json.Unmarshal([]byte(v), &arr); err == nil {
			return arr
		}
		fmt.Println("Invalid array conversion from string", tableName, colName, v)
		return []interface{}{}
	default:
		fmt.Println("Invalid col conversion to array", tableName, colName, v)
		return []interface{}{}
	}
}

// Helper functions for type conversion
func convertToFloat32(val interface{}, tableName, colName string) float32 {
	switch v := val.(type) {
	case int:
		return float32(v)
	case int64:
		return float32(v)
	case float32:
		return v
	case float64:
		return float32(v)
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return float32(parsed)
		}
		fmt.Println("Invalid float32 number", tableName, colName, "'"+v+"'")
		return 0
	default:
		fmt.Println("Invalid col conversion to float32", tableName, colName, v)
		return 0
	}
}

func convertToFloat64(val interface{}, tableName, colName string) float64 {
	switch v := val.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return parsed
		}
		fmt.Println("Invalid float64 number", tableName, colName, v)
		return 0
	default:
		fmt.Println("Invalid col conversion to float64", tableName, colName, v)
		return 0
	}
}

func convertToString(val interface{}) string {
	switch v := val.(type) {
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', 3, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', 3, 64)
	case string:
		return v
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
		return time.Time{}
	default:
		fmt.Println("Invalid col conversion to time", tableName, colName, v)
		return time.Time{}
	}
}

func convertToStringMap(val interface{}, tableName, colName string) map[string]string {
	switch v := val.(type) {
	case map[string]interface{}:
		result := make(map[string]string)
		for k, val := range v {
			result[k] = convertToString(val)
		}
		return result
	case map[string]string:
		return v
	case string:
		// Try to parse JSON string
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(v), &result); err == nil {
			stringMap := make(map[string]string)
			for k, val := range result {
				stringMap[k] = convertToString(val)
			}
			return stringMap
		}
		fmt.Println("Invalid map conversion from string", tableName, colName, v)
		return make(map[string]string)
	default:
		fmt.Println("Invalid col conversion to string map", tableName, colName, v)
		return make(map[string]string)
	}
}

func convertToFloat32Map(val interface{}, tableName, colName string) map[string]float32 {
	switch v := val.(type) {
	case map[string]interface{}:
		result := make(map[string]float32)
		for k, val := range v {
			result[k] = convertToFloat32(val, tableName, colName)
		}
		return result
	case map[string]float32:
		return v
	case string:
		// Try to parse JSON string
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(v), &result); err == nil {
			float32Map := make(map[string]float32)
			for k, val := range result {
				float32Map[k] = convertToFloat32(val, tableName, colName)
			}
			return float32Map
		}
		fmt.Println("Invalid map conversion from string", tableName, colName, v)
		return make(map[string]float32)
	default:
		fmt.Println("Invalid col conversion to float32 map", tableName, colName, v)
		return make(map[string]float32)
	}
}

func convertToFloat64Map(val interface{}, tableName, colName string) map[string]float64 {
	switch v := val.(type) {
	case map[string]interface{}:
		result := make(map[string]float64)
		for k, val := range v {
			result[k] = convertToFloat64(val, tableName, colName)
		}
		return result
	case map[string]float64:
		return v
	case string:
		// Try to parse JSON string
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(v), &result); err == nil {
			float64Map := make(map[string]float64)
			for k, val := range result {
				float64Map[k] = convertToFloat64(val, tableName, colName)
			}
			return float64Map
		}
		fmt.Println("Invalid map conversion from string", tableName, colName, v)
		return make(map[string]float64)
	default:
		fmt.Println("Invalid col conversion to float64 map", tableName, colName, v)
		return make(map[string]float64)
	}
}
