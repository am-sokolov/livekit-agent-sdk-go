package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetH264UnencryptedBytes(t *testing.T) {
	tests := []struct {
		name     string
		payload  []byte
		expected int
	}{
		{
			name:     "empty payload",
			payload:  []byte{},
			expected: 0,
		},
		{
			name: "single NAL - IDR (type 5)",
			payload: []byte{
				0x65, // IDR NAL unit type (0x65 & 0x1F = 5)
				0x01, 0x02, 0x03,
			},
			expected: 1,
		},
		{
			name: "single NAL - non-IDR (type 1)",
			payload: []byte{
				0x41, // non-IDR NAL unit type (0x41 & 0x1F = 1)
				0x01, 0x02, 0x03,
			},
			expected: 1,
		},
		{
			name: "single NAL - SPS (type 7)",
			payload: []byte{
				0x67, // SPS NAL unit type (0x67 & 0x1F = 7)
				0x01, 0x02, 0x03,
			},
			expected: 1,
		},
		{
			name: "single NAL - PPS (type 8)",
			payload: []byte{
				0x68, // PPS NAL unit type (0x68 & 0x1F = 8)
				0x01, 0x02, 0x03,
			},
			expected: 1,
		},
		{
			name: "FU-A (type 28)",
			payload: []byte{
				0x7C, // FU indicator (0x7C & 0x1F = 28)
				0x85, // FU header (S=1, E=0, R=0, Type=5)
				0x01, 0x02, 0x03,
			},
			expected: 2,
		},
		{
			name: "FU-A middle fragment",
			payload: []byte{
				0x7C, // FU indicator (type 28)
				0x05, // FU header (S=0, E=0, R=0, Type=5)
				0x01, 0x02, 0x03,
			},
			expected: 2,
		},
		{
			name: "FU-A end fragment",
			payload: []byte{
				0x7C, // FU indicator (type 28)
				0x45, // FU header (S=0, E=1, R=0, Type=5)
				0x01, 0x02, 0x03,
			},
			expected: 2,
		},
		{
			name: "STAP-A (type 24)",
			payload: []byte{
				0x78,       // STAP-A type (0x78 & 0x1F = 24)
				0x00, 0x04, // Size
				0x67, 0x01, 0x02, 0x03,
			},
			expected: 1,
		},
		{
			name: "unknown NAL type (>23, not special)",
			payload: []byte{
				0x79, // Type 25 - unknown
				0x01, 0x02, 0x03,
			},
			expected: 1, // fallback
		},
		{
			name: "single NAL - type 23 (max single)",
			payload: []byte{
				0x77, // Type 23 (0x77 & 0x1F = 23)
				0x01, 0x02, 0x03,
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getH264UnencryptedBytes(tt.payload)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestNeedsRBSPUnescaping(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "empty data",
			data:     []byte{},
			expected: false,
		},
		{
			name:     "short data (1 byte)",
			data:     []byte{0x00},
			expected: false,
		},
		{
			name:     "short data (2 bytes)",
			data:     []byte{0x00, 0x00},
			expected: false,
		},
		{
			name:     "no emulation prevention needed",
			data:     []byte{0x01, 0x02, 0x03, 0x04, 0x05},
			expected: false,
		},
		{
			name:     "has emulation prevention at start",
			data:     []byte{0x00, 0x00, 0x03, 0x00, 0x05},
			expected: true,
		},
		{
			name:     "has emulation prevention in middle",
			data:     []byte{0x01, 0x02, 0x00, 0x00, 0x03, 0x01, 0x05},
			expected: true,
		},
		{
			name:     "has emulation prevention at end",
			data:     []byte{0x01, 0x02, 0x00, 0x00, 0x03},
			expected: true,
		},
		{
			name:     "multiple emulation prevention bytes",
			data:     []byte{0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x03, 0x01},
			expected: true,
		},
		{
			name:     "0x00 0x00 without 0x03",
			data:     []byte{0x00, 0x00, 0x01, 0x02, 0x03},
			expected: false,
		},
		{
			name:     "0x00 0x00 0x04 (not prevention byte)",
			data:     []byte{0x00, 0x00, 0x04, 0x05},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := needsRBSPUnescaping(tt.data)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestFindNALUIndices(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected []int
	}{
		{
			name:     "empty data",
			data:     []byte{},
			expected: nil,
		},
		{
			name:     "no start codes",
			data:     []byte{0x01, 0x02, 0x03, 0x04},
			expected: nil,
		},
		{
			name: "single 3-byte start code",
			data: []byte{
				0x00, 0x00, 0x01, // 3-byte start code
				0x67, 0x01, 0x02, // SPS NAL
			},
			expected: []int{3}, // Index of 0x67
		},
		{
			name: "single 4-byte start code",
			data: []byte{
				0x00, 0x00, 0x00, 0x01, // 4-byte start code
				0x67, 0x01, 0x02, // SPS NAL
			},
			expected: []int{4}, // Index of 0x67
		},
		{
			name: "multiple NALUs with 4-byte start codes",
			data: []byte{
				0x00, 0x00, 0x00, 0x01, // Start code
				0x67, 0x01, 0x02, // SPS
				0x00, 0x00, 0x00, 0x01, // Start code
				0x68, 0x03, // PPS
				0x00, 0x00, 0x00, 0x01, // Start code
				0x65, 0x04, 0x05, // IDR
			},
			expected: []int{4, 11, 17}, // Indices of NAL headers
		},
		{
			name: "mixed 3-byte and 4-byte start codes",
			data: []byte{
				0x00, 0x00, 0x00, 0x01, // 4-byte start code
				0x67, 0x01, // SPS
				0x00, 0x00, 0x01, // 3-byte start code
				0x68, 0x02, // PPS
			},
			expected: []int{4, 9}, // Indices of NAL headers
		},
		{
			name: "start code at end (incomplete)",
			data: []byte{
				0x00, 0x00, 0x00, 0x01,
				0x67, 0x01,
				0x00, 0x00, 0x01, // Start code at end, but no NAL data
			},
			expected: []int{4, 9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findNALUIndices(tt.data)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestIsH264SliceNALU(t *testing.T) {
	tests := []struct {
		name     string
		nalType  byte
		expected bool
	}{
		{"non-IDR slice type 1", 1, true},
		{"IDR slice type 5", 5, true},
		{"SPS type 7", 7, false},
		{"PPS type 8", 8, false},
		{"SEI type 6", 6, false},
		{"AUD type 9", 9, false},
		{"type 0", 0, false},
		{"type 2", 2, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isH264SliceNALU(tt.nalType)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestFindSliceNALUUnencryptedBytes(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		naluIndices []int
		expected    int
	}{
		{
			name:        "empty indices",
			data:        []byte{0x67, 0x01, 0x02},
			naluIndices: []int{},
			expected:    0,
		},
		{
			name: "first NALU is IDR slice",
			data: []byte{
				0x00, 0x00, 0x00, 0x01,
				0x65, 0x01, 0x02, 0x03, // IDR at index 4
			},
			naluIndices: []int{4},
			expected:    6, // 4 + 2
		},
		{
			name: "SPS, PPS, then IDR slice",
			data: []byte{
				0x00, 0x00, 0x00, 0x01,
				0x67, 0x01, 0x02, // SPS at index 4
				0x00, 0x00, 0x00, 0x01,
				0x68, 0x03, // PPS at index 11
				0x00, 0x00, 0x00, 0x01,
				0x65, 0x04, 0x05, 0x06, // IDR at index 17
			},
			naluIndices: []int{4, 11, 17},
			expected:    19, // 17 + 2
		},
		{
			name: "non-IDR slice type 1",
			data: []byte{
				0x00, 0x00, 0x00, 0x01,
				0x41, 0x01, 0x02, // Non-IDR (type 1) at index 4
			},
			naluIndices: []int{4},
			expected:    6, // 4 + 2
		},
		{
			name: "no slice NALU (only SPS/PPS)",
			data: []byte{
				0x00, 0x00, 0x00, 0x01,
				0x67, 0x01, // SPS
				0x00, 0x00, 0x00, 0x01,
				0x68, 0x02, // PPS
			},
			naluIndices: []int{4, 10},
			expected:    0, // No slice found
		},
		{
			name:        "index out of bounds",
			data:        []byte{0x00, 0x00, 0x00, 0x01, 0x67},
			naluIndices: []int{4, 100}, // 100 is out of bounds
			expected:    0,             // No valid slice
		},
		{
			name: "slice at end with limited data",
			data: []byte{
				0x00, 0x00, 0x00, 0x01,
				0x65, 0x01, // IDR at index 4, only 2 bytes after
			},
			naluIndices: []int{4},
			expected:    6, // 4 + 2, exactly at end
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findSliceNALUUnencryptedBytes(tt.data, tt.naluIndices)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestParseRBSP(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{
			name:     "empty data",
			input:    []byte{},
			expected: []byte{},
		},
		{
			name:     "no emulation prevention",
			input:    []byte{0x01, 0x02, 0x03, 0x04, 0x05},
			expected: []byte{0x01, 0x02, 0x03, 0x04, 0x05},
		},
		{
			name:     "single emulation prevention 0x00 0x00 0x03 0x00",
			input:    []byte{0x00, 0x00, 0x03, 0x00},
			expected: []byte{0x00, 0x00, 0x00},
		},
		{
			name:     "single emulation prevention 0x00 0x00 0x03 0x01",
			input:    []byte{0x00, 0x00, 0x03, 0x01},
			expected: []byte{0x00, 0x00, 0x01},
		},
		{
			name:     "single emulation prevention 0x00 0x00 0x03 0x02",
			input:    []byte{0x00, 0x00, 0x03, 0x02},
			expected: []byte{0x00, 0x00, 0x02},
		},
		{
			name:     "single emulation prevention 0x00 0x00 0x03 0x03",
			input:    []byte{0x00, 0x00, 0x03, 0x03},
			expected: []byte{0x00, 0x00, 0x03},
		},
		{
			name:     "emulation prevention in middle",
			input:    []byte{0x01, 0x02, 0x00, 0x00, 0x03, 0x01, 0x04, 0x05},
			expected: []byte{0x01, 0x02, 0x00, 0x00, 0x01, 0x04, 0x05},
		},
		{
			name:     "multiple emulation preventions",
			input:    []byte{0x00, 0x00, 0x03, 0x00, 0x01, 0x00, 0x00, 0x03, 0x01},
			expected: []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01},
		},
		{
			name:     "consecutive emulation preventions",
			input:    []byte{0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x01},
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x01},
		},
		{
			name:     "short data (2 bytes)",
			input:    []byte{0x00, 0x00},
			expected: []byte{0x00, 0x00},
		},
		{
			name:     "0x00 0x00 at end without 0x03",
			input:    []byte{0x01, 0x02, 0x00, 0x00},
			expected: []byte{0x01, 0x02, 0x00, 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseRBSP(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}
