package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func parsePacket(x []byte) *ParsedPacket {
	if x[0] != '{' {
		//fmt.Println("Old log format:", string(x[:150]))
		return parseOldPacket(string(x))
	}

	var arbitrary_json map[string]interface{}
	err := json.Unmarshal([]byte(x), &arbitrary_json)

	if err != nil {
		fmt.Println("Parse packet error", err)
		return nil
	}

	if arbitrary_json == nil {
		fmt.Println("Parse json returned nil")
		return nil
	}

	tableName, tableNameOk := arbitrary_json["_t"].(string)
	delete(arbitrary_json, "_t")

	if !tableNameOk || tableName == "" {
		fmt.Println("Table name is empty")
		return nil
	}

	items := make([]ColValue, 0, len(arbitrary_json))

	for columnName, v := range arbitrary_json {
		items = append(items, ColValue{column: columnName, value: v})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].column < items[j].column
	})

	columns := make([]string, 0, len(items))
	for _, v := range items {
		columns = append(columns, v.column)
	}

	values := make([]interface{}, 0, len(items))
	for _, v := range items {
		values = append(values, v.value)
	}

	return &ParsedPacket{
		tableName: tableName,
		columns:   columns,
		values:    values,
	}
}

var regColumnName = regexp.MustCompile("[^a-zA-Z_]+")

func parseOldPacket(x string) *ParsedPacket {
	if !strings.HasSuffix(x, "\n") {
		fmt.Println("No newline at end")
		return nil
	}

	x = strings.TrimSuffix(x, "\n")

	parts := SplitWithEscaping(x, ",", "\\")

	tableName := regColumnName.ReplaceAllString(parts[0], "")

	if tableName == "" {
		fmt.Println("Table name is empty")
		return nil
	}

	isColumn := true
	columnName := ""
	items := make([]ColValue, 0, (len(parts)-1)/2)

	for _, v := range parts[1:] {
		isString := strings.HasPrefix(v, "\"")

		v = strings.ReplaceAll(v, "\\\"", "\x00")
		v = strings.ReplaceAll(v, "\"", "")
		v = strings.ReplaceAll(v, "\x00", "\"")

		if isColumn {
			columnName = regColumnName.ReplaceAllString(v, "")
		} else {
			if isString {
				items = append(items, ColValue{column: columnName, value: v})
			} else if v == "t" {
				items = append(items, ColValue{column: columnName, value: true})
			} else if v == "f" {
				items = append(items, ColValue{column: columnName, value: false})
			} else if v == "n" {
				items = append(items, ColValue{column: columnName, value: nil})
			} else {
				parsed, err := strconv.ParseFloat(v, 64)
				if err == nil {
					items = append(items, ColValue{column: columnName, value: parsed})
				} else {
					fmt.Println("Old format col error", tableName, columnName)
				}
			}
		}
		isColumn = !isColumn
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].column < items[j].column
	})

	columns := make([]string, 0, len(items))
	for _, v := range items {
		columns = append(columns, v.column)
	}

	values := make([]interface{}, 0, len(items))
	for _, v := range items {
		values = append(values, v.value)
	}

	return &ParsedPacket{
		tableName: tableName,
		columns:   columns,
		values:    values,
	}
}
