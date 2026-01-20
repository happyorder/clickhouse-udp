package main

import (
	"testing"
)

func TestNormalizeClickHouseType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"String", "String"},
		{"UInt8", "UInt8"},
		{"UInt64", "UInt64"},
		{"DateTime64(3)", "DateTime64"},
		{"DateTime64(9)", "DateTime64"},
		{"Nullable(Float32)", "Float32"},
		{"Nullable(Float64)", "Float64"},
		{"LowCardinality(String)", "String"},
		{"Array(String)", "Array(String)"},
		{"Array(UInt8)", "Array(UInt8)"},
		{"Array(LowCardinality(String))", "Array(String)"},
		{"Map(String, String)", "Map(String,String)"},
		{"Map(String, Float32)", "Map(String,Float32)"},
		{"Map(LowCardinality(String), String)", "Map(String,String)"},
		{"Map(LowCardinality(String), Float64)", "Map(String,Float64)"},
		{"Nullable(LowCardinality(String))", "String"},
	}

	for _, tt := range tests {
		got := normalizeClickHouseType(tt.input)
		if got != tt.expected {
			t.Errorf("normalizeClickHouseType(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestFindColumnIndex(t *testing.T) {
	columns := []string{"id", "name", "value", "timestamp"}

	tests := []struct {
		name     string
		expected int
	}{
		{"id", 0},
		{"name", 1},
		{"value", 2},
		{"timestamp", 3},
		{"missing", -1},
		{"", -1},
	}

	for _, tt := range tests {
		got := findColumnIndex(columns, tt.name)
		if got != tt.expected {
			t.Errorf("findColumnIndex(%q) = %d, want %d", tt.name, got, tt.expected)
		}
	}
}

func TestFindColumnIndexEmpty(t *testing.T) {
	got := findColumnIndex([]string{}, "any")
	if got != -1 {
		t.Errorf("findColumnIndex on empty slice = %d, want -1", got)
	}
}

func TestSplitWithEscaping(t *testing.T) {
	tests := []struct {
		input     string
		separator string
		escape    string
		expected  []string
	}{
		{"a,b,c", ",", "\\", []string{"a", "b", "c"}},
		{"a\\,b,c", ",", "\\", []string{"a,b", "c"}},
		{"hello\\,world", ",", "\\", []string{"hello,world"}},
		{"no-sep", ",", "\\", []string{"no-sep"}},
		{"a,,b", ",", "\\", []string{"a", "", "b"}},
		{"\\,start,middle\\,end", ",", "\\", []string{",start", "middle,end"}},
	}

	for _, tt := range tests {
		got := SplitWithEscaping(tt.input, tt.separator, tt.escape)
		if len(got) != len(tt.expected) {
			t.Errorf("SplitWithEscaping(%q) length = %d, want %d", tt.input, len(got), len(tt.expected))
			continue
		}
		for i := range got {
			if got[i] != tt.expected[i] {
				t.Errorf("SplitWithEscaping(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.expected[i])
			}
		}
	}
}

func TestGetValues(t *testing.T) {
	rows := []*ParsedPacket{
		{values: []interface{}{"a", 1, true}},
		{values: []interface{}{"b", 2, false}},
		{values: []interface{}{"c", 3, true}},
	}

	vals := getValues(rows, 0)
	if len(vals) != 3 {
		t.Errorf("getValues length = %d, want 3", len(vals))
	}
	if vals[0] != "a" || vals[1] != "b" || vals[2] != "c" {
		t.Errorf("getValues returned wrong values: %v", vals)
	}

	vals = getValues(rows, 1)
	if vals[0] != 1 || vals[1] != 2 || vals[2] != 3 {
		t.Errorf("getValues for index 1 returned wrong values: %v", vals)
	}
}

func TestGetValuesNegativeIndex(t *testing.T) {
	rows := []*ParsedPacket{{values: []interface{}{"a"}}}
	vals := getValues(rows, -1)
	if vals != nil {
		t.Error("getValues with negative index should return nil")
	}
}

func TestGetValuesEmpty(t *testing.T) {
	vals := getValues([]*ParsedPacket{}, 0)
	if len(vals) != 0 {
		t.Error("getValues with empty rows should return empty slice")
	}
}

func TestQueuedPacketListPacketKey(t *testing.T) {
	pkt1 := &ParsedPacket{tableName: "logs", columns: []string{"a", "b"}}
	pkt2 := &ParsedPacket{tableName: "logs", columns: []string{"a", "b"}}
	pkt3 := &ParsedPacket{tableName: "logs", columns: []string{"a", "c"}}

	if pkt1.PacketKey() != pkt2.PacketKey() {
		t.Error("identical packets should have same key")
	}
	if pkt1.PacketKey() == pkt3.PacketKey() {
		t.Error("different column packets should have different keys")
	}
}

func TestValidTableName(t *testing.T) {
	valid := []string{"logs", "user_events", "_private", "Table1", "log_2024"}
	for _, name := range valid {
		if !validTableName.MatchString(name) {
			t.Errorf("expected %q to be valid", name)
		}
	}

	invalid := []string{"", "123start", "table-name", "table.name", "table;drop", "table name"}
	for _, name := range invalid {
		if validTableName.MatchString(name) {
			t.Errorf("expected %q to be invalid", name)
		}
	}
}

func TestCurrentDbSchemaThreadSafe(t *testing.T) {
	schema := &CurrentDbSchema{}

	testMap := DbTableColumnMap{"test": []ColumnInfo{{Name: "col1"}}}
	schema.Set(&testMap)

	got := schema.Get()
	if got == nil {
		t.Error("Get returned nil after Set")
	}
	if len(*got) != 1 {
		t.Error("Get returned wrong schema")
	}
}
