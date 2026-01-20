package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var reColumnName = regexp.MustCompile("[^a-zA-Z0-9_]+")

func parsePacket(data []byte) *ParsedPacket {
	if len(data) == 0 {
		fmt.Println("Empty packet")
		return nil
	}

	if data[0] == '{' {
		return parseJSONPacket(data)
	}
	return parseOldPacket(string(data))
}

func parseJSONPacket(data []byte) *ParsedPacket {
	var obj map[string]interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		fmt.Println("JSON parse error:", err)
		return nil
	}
	if obj == nil {
		fmt.Println("JSON returned nil")
		return nil
	}

	tableName, ok := obj["_t"].(string)
	delete(obj, "_t")
	if !ok || tableName == "" {
		fmt.Println("Missing table name")
		return nil
	}

	return buildPacket(tableName, obj)
}

func parseOldPacket(data string) *ParsedPacket {
	if !strings.HasSuffix(data, "\n") {
		fmt.Println("Missing newline")
		return nil
	}
	data = strings.TrimSuffix(data, "\n")

	parts := SplitWithEscaping(data, ",", "\\")
	if len(parts) == 0 {
		fmt.Println("Empty parts")
		return nil
	}
	tableName := reColumnName.ReplaceAllString(parts[0], "")
	if tableName == "" {
		fmt.Println("Empty table name")
		return nil
	}

	obj := make(map[string]interface{})
	for i := 1; i+1 < len(parts); i += 2 {
		colName := reColumnName.ReplaceAllString(parts[i], "")
		rawVal := parts[i+1]

		rawVal = strings.ReplaceAll(rawVal, "\\\"", "\x00")
		isString := strings.HasPrefix(rawVal, "\"")
		rawVal = strings.ReplaceAll(rawVal, "\"", "")
		rawVal = strings.ReplaceAll(rawVal, "\x00", "\"")

		var val interface{}
		if isString {
			val = rawVal
		} else if rawVal == "t" {
			val = true
		} else if rawVal == "f" {
			val = false
		} else if rawVal == "n" {
			val = nil
		} else if parsed, err := strconv.ParseFloat(rawVal, 64); err == nil {
			val = parsed
		} else {
			fmt.Println("Parse error:", tableName, colName)
			continue
		}
		obj[colName] = val
	}

	return buildPacket(tableName, obj)
}

func buildPacket(tableName string, obj map[string]interface{}) *ParsedPacket {
	items := make([]ColValue, 0, len(obj))
	for col, val := range obj {
		items = append(items, ColValue{column: col, value: val})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].column < items[j].column
	})

	columns := make([]string, len(items))
	values := make([]interface{}, len(items))
	for i, item := range items {
		columns[i] = item.column
		values[i] = item.value
	}

	return &ParsedPacket{
		tableName: tableName,
		columns:   columns,
		values:    values,
	}
}
