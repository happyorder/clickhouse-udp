package main

import (
	"testing"
	"time"
)

func TestToInt64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected int64
		ok       bool
	}{
		{int(42), 42, true},
		{int64(100), 100, true},
		{float32(3.7), 3, true},
		{float64(9.9), 9, true},
		{true, 1, true},
		{false, 0, true},
		{"123", 123, true},
		{"invalid", 0, false},
		{nil, 0, false},
	}

	for _, tt := range tests {
		got, ok := toInt64(tt.input)
		if ok != tt.ok || got != tt.expected {
			t.Errorf("toInt64(%v) = (%d, %v), want (%d, %v)", tt.input, got, ok, tt.expected, tt.ok)
		}
	}
}

func TestToUint64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected uint64
		ok       bool
	}{
		{int(42), 42, true},
		{int64(100), 100, true},
		{float64(9.9), 9, true},
		{true, 1, true},
		{false, 0, true},
		{"456", 456, true},
		{"invalid", 0, false},
	}

	for _, tt := range tests {
		got, ok := toUint64(tt.input)
		if ok != tt.ok || got != tt.expected {
			t.Errorf("toUint64(%v) = (%d, %v), want (%d, %v)", tt.input, got, ok, tt.expected, tt.ok)
		}
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
		ok       bool
	}{
		{int(42), 42.0, true},
		{int64(100), 100.0, true},
		{float32(3.5), 3.5, true},
		{float64(9.9), 9.9, true},
		{true, 1.0, true},
		{false, 0.0, true},
		{"3.14", 3.14, true},
		{" 2.5 ", 2.5, true},
		{"invalid", 0, false},
	}

	for _, tt := range tests {
		got, ok := toFloat64(tt.input)
		if ok != tt.ok {
			t.Errorf("toFloat64(%v) ok = %v, want %v", tt.input, ok, tt.ok)
		}
		if ok && got != tt.expected {
			t.Errorf("toFloat64(%v) = %f, want %f", tt.input, got, tt.expected)
		}
	}
}

func TestConvertToString(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{"hello", "hello"},
		{int(42), "42"},
		{int64(100), "100"},
		{true, "true"},
		{false, "false"},
		{nil, ""},
	}

	for _, tt := range tests {
		got := convertToString(tt.input)
		if got != tt.expected {
			t.Errorf("convertToString(%v) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestConvertToTime(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected time.Time
	}{
		{int64(1609459200000), time.UnixMilli(1609459200000)},
		{float64(1609459200000), time.UnixMilli(1609459200000)},
		{"2021-01-01 00:00:00", time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		got := convertToTime(tt.input, "test", "col")
		if !got.Equal(tt.expected) {
			t.Errorf("convertToTime(%v) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestConvertToArray(t *testing.T) {
	arr := []interface{}{"a", "b", "c"}
	got := convertToArray(arr, "test", "col")
	if len(got) != 3 {
		t.Errorf("convertToArray returned %d elements, want 3", len(got))
	}

	jsonArr := `["x", "y"]`
	got = convertToArray(jsonArr, "test", "col")
	if len(got) != 2 {
		t.Errorf("convertToArray(json) returned %d elements, want 2", len(got))
	}
}

func TestConvertToStringMap(t *testing.T) {
	input := map[string]interface{}{"key": "value", "num": 42}
	got := convertToStringMap(input, "test", "col")
	if got["key"] != "value" {
		t.Errorf("convertToStringMap[key] = %q, want %q", got["key"], "value")
	}
	if got["num"] != "42" {
		t.Errorf("convertToStringMap[num] = %q, want %q", got["num"], "42")
	}

	jsonMap := `{"a": "b"}`
	got = convertToStringMap(jsonMap, "test", "col")
	if got["a"] != "b" {
		t.Errorf("convertToStringMap(json)[a] = %q, want %q", got["a"], "b")
	}
}

func TestConvertToFloat64Map(t *testing.T) {
	input := map[string]interface{}{"x": 1.5, "y": int(2)}
	got := convertToFloat64Map(input, "test", "col")
	if got["x"] != 1.5 {
		t.Errorf("convertToFloat64Map[x] = %f, want 1.5", got["x"])
	}
	if got["y"] != 2.0 {
		t.Errorf("convertToFloat64Map[y] = %f, want 2.0", got["y"])
	}
}

func TestConvertIntTypes(t *testing.T) {
	if got := convertToInt8(int64(127), "t", "c"); got != 127 {
		t.Errorf("convertToInt8 = %d, want 127", got)
	}
	if got := convertToInt16(int64(1000), "t", "c"); got != 1000 {
		t.Errorf("convertToInt16 = %d, want 1000", got)
	}
	if got := convertToInt32(int64(100000), "t", "c"); got != 100000 {
		t.Errorf("convertToInt32 = %d, want 100000", got)
	}
	if got := convertToInt64(int64(999999), "t", "c"); got != 999999 {
		t.Errorf("convertToInt64 = %d, want 999999", got)
	}
}

func TestConvertUintTypes(t *testing.T) {
	if got := convertToUInt8(int64(255), "t", "c"); got != 255 {
		t.Errorf("convertToUInt8 = %d, want 255", got)
	}
	if got := convertToUInt16(int64(65535), "t", "c"); got != 65535 {
		t.Errorf("convertToUInt16 = %d, want 65535", got)
	}
	if got := convertToUInt32(int64(100000), "t", "c"); got != 100000 {
		t.Errorf("convertToUInt32 = %d, want 100000", got)
	}
	if got := convertToUInt64(int64(999999), "t", "c"); got != 999999 {
		t.Errorf("convertToUInt64 = %d, want 999999", got)
	}
}

func TestConvertFloatTypes(t *testing.T) {
	if got := convertToFloat32(3.14, "t", "c"); got != float32(3.14) {
		t.Errorf("convertToFloat32 = %f, want 3.14", got)
	}
	if got := convertToFloat64(3.14159, "t", "c"); got != 3.14159 {
		t.Errorf("convertToFloat64 = %f, want 3.14159", got)
	}
}
