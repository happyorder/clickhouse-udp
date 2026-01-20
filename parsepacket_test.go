package main

import (
	"testing"
)

func TestParsePacketEmpty(t *testing.T) {
	result := parsePacket([]byte{})
	if result != nil {
		t.Error("parsePacket(empty) should return nil")
	}
}

func TestParseJSONPacket(t *testing.T) {
	input := []byte(`{"_t":"logs","level":"info","message":"hello"}`)
	result := parsePacket(input)

	if result == nil {
		t.Fatal("parsePacket returned nil")
	}
	if result.tableName != "logs" {
		t.Errorf("tableName = %q, want %q", result.tableName, "logs")
	}
	if len(result.columns) != 2 {
		t.Errorf("columns count = %d, want 2", len(result.columns))
	}
	// Columns should be sorted
	if result.columns[0] != "level" || result.columns[1] != "message" {
		t.Errorf("columns = %v, want [level, message]", result.columns)
	}
}

func TestParseJSONPacketMissingTable(t *testing.T) {
	input := []byte(`{"level":"info"}`)
	result := parsePacket(input)
	if result != nil {
		t.Error("parsePacket without _t should return nil")
	}
}

func TestParseJSONPacketInvalid(t *testing.T) {
	input := []byte(`{invalid json}`)
	result := parsePacket(input)
	if result != nil {
		t.Error("parsePacket with invalid JSON should return nil")
	}
}

func TestParseJSONPacketWithTypes(t *testing.T) {
	input := []byte(`{"_t":"metrics","count":42,"rate":3.14,"enabled":true}`)
	result := parsePacket(input)

	if result == nil {
		t.Fatal("parsePacket returned nil")
	}
	if result.tableName != "metrics" {
		t.Errorf("tableName = %q, want %q", result.tableName, "metrics")
	}
	if len(result.columns) != 3 {
		t.Errorf("columns count = %d, want 3", len(result.columns))
	}
}

func TestParseOldPacket(t *testing.T) {
	input := []byte("events,name,\"click\",count,5\n")
	result := parsePacket(input)

	if result == nil {
		t.Fatal("parsePacket returned nil")
	}
	if result.tableName != "events" {
		t.Errorf("tableName = %q, want %q", result.tableName, "events")
	}
	if len(result.columns) != 2 {
		t.Errorf("columns count = %d, want 2", len(result.columns))
	}
}

func TestParseOldPacketNoNewline(t *testing.T) {
	input := []byte("events,name,\"click\"")
	result := parsePacket(input)
	if result != nil {
		t.Error("parsePacket without newline should return nil")
	}
}

func TestParseOldPacketBooleans(t *testing.T) {
	input := []byte("flags,active,t,deleted,f\n")
	result := parsePacket(input)

	if result == nil {
		t.Fatal("parsePacket returned nil")
	}

	activeIdx := -1
	deletedIdx := -1
	for i, col := range result.columns {
		if col == "active" {
			activeIdx = i
		}
		if col == "deleted" {
			deletedIdx = i
		}
	}

	if activeIdx < 0 || result.values[activeIdx] != true {
		t.Errorf("active value should be true")
	}
	if deletedIdx < 0 || result.values[deletedIdx] != false {
		t.Errorf("deleted value should be false")
	}
}

func TestParseOldPacketNull(t *testing.T) {
	input := []byte("data,value,n\n")
	result := parsePacket(input)

	if result == nil {
		t.Fatal("parsePacket returned nil")
	}
	if len(result.values) != 1 || result.values[0] != nil {
		t.Error("null value should be nil")
	}
}

func TestParseOldPacketEscapedComma(t *testing.T) {
	input := []byte("logs,msg,\"hello\\,world\"\n")
	result := parsePacket(input)

	if result == nil {
		t.Fatal("parsePacket returned nil")
	}
	if result.values[0] != "hello,world" {
		t.Errorf("escaped comma not handled: got %q", result.values[0])
	}
}

func TestParseOldPacketEmptyTable(t *testing.T) {
	input := []byte(",col,val\n")
	result := parsePacket(input)
	if result != nil {
		t.Error("parsePacket with empty table should return nil")
	}
}

func TestBuildPacket(t *testing.T) {
	obj := map[string]interface{}{
		"zebra": 1,
		"apple": 2,
		"mango": 3,
	}
	result := buildPacket("test", obj)

	if result.tableName != "test" {
		t.Errorf("tableName = %q, want %q", result.tableName, "test")
	}
	// Should be sorted alphabetically
	expected := []string{"apple", "mango", "zebra"}
	for i, col := range result.columns {
		if col != expected[i] {
			t.Errorf("columns[%d] = %q, want %q", i, col, expected[i])
		}
	}
}

func TestPacketKey(t *testing.T) {
	pkt := &ParsedPacket{
		tableName: "logs",
		columns:   []string{"a", "b", "c"},
	}
	key := pkt.PacketKey()
	expected := "logs:a,b,c"
	if key != expected {
		t.Errorf("PacketKey() = %q, want %q", key, expected)
	}
}

func TestParseOldPacketColumnWithNumbers(t *testing.T) {
	input := []byte("events,field1,\"value\",count2,42\n")
	result := parsePacket(input)

	if result == nil {
		t.Fatal("parsePacket returned nil")
	}

	hasField1 := false
	hasCount2 := false
	for _, col := range result.columns {
		if col == "field1" {
			hasField1 = true
		}
		if col == "count2" {
			hasCount2 = true
		}
	}
	if !hasField1 || !hasCount2 {
		t.Errorf("columns with numbers not preserved: %v", result.columns)
	}
}

func TestParseOldPacketEmptyAfterTrim(t *testing.T) {
	input := []byte("\n")
	result := parsePacket(input)
	if result != nil {
		t.Error("parsePacket with only newline should return nil")
	}
}
