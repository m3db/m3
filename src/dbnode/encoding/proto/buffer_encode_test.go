// Copyright (c) 2024 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferEncodeTagAndWireType(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		wireType int8
		expected []byte
	}{
		{
			name:     "tag 1 wire type 0",
			tag:      1,
			wireType: 0,
			expected: []byte{0x08},
		},
		{
			name:     "tag 17 wire type 0",
			tag:      17,
			wireType: 0,
			expected: []byte{0x88, 0x01},
		},
		{
			name:     "tag 1 wire type 1",
			tag:      1,
			wireType: 1,
			expected: []byte{0x09},
		},
		{
			name:     "tag 1 wire type 2",
			tag:      1,
			wireType: 2,
			expected: []byte{0x0A},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(nil)
			cb.encodeTagAndWireType(tt.tag, tt.wireType)
			assert.Equal(t, tt.expected, cb.buf)
		})
	}
}

func TestBufferEncodeVarint(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected []byte
	}{
		{
			name:     "single byte",
			input:    1,
			expected: []byte{0x01},
		},
		{
			name:     "two bytes",
			input:    129,
			expected: []byte{0x81, 0x01},
		},
		{
			name:     "three bytes",
			input:    16513,
			expected: []byte{0x81, 0x81, 0x01},
		},
		{
			name:     "max uint64",
			input:    0xFFFFFFFFFFFFFFFF,
			expected: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(nil)
			cb.encodeVarint(tt.input)
			assert.Equal(t, tt.expected, cb.buf)
		})
	}
}

func TestBufferEncodeFixed64(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected []byte
	}{
		{
			name:     "zero",
			input:    0,
			expected: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:     "one",
			input:    1,
			expected: []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:     "max uint64",
			input:    0xFFFFFFFFFFFFFFFF,
			expected: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(nil)
			cb.encodeFixed64(tt.input)
			assert.Equal(t, tt.expected, cb.buf)
		})
	}
}

func TestBufferEncodeFixed32(t *testing.T) {
	tests := []struct {
		name     string
		input    uint32
		expected []byte
	}{
		{
			name:     "zero",
			input:    0,
			expected: []byte{0x00, 0x00, 0x00, 0x00},
		},
		{
			name:     "one",
			input:    1,
			expected: []byte{0x01, 0x00, 0x00, 0x00},
		},
		{
			name:     "max uint32",
			input:    0xFFFFFFFF,
			expected: []byte{0xFF, 0xFF, 0xFF, 0xFF},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(nil)
			cb.encodeFixed32(tt.input)
			assert.Equal(t, tt.expected, cb.buf)
		})
	}
}

func TestBufferEncodeRawBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{
			name:     "empty bytes",
			input:    []byte{},
			expected: []byte{0x00},
		},
		{
			name:     "single byte",
			input:    []byte{0x01},
			expected: []byte{0x01, 0x01},
		},
		{
			name:     "multiple bytes",
			input:    []byte{0x01, 0x02, 0x03},
			expected: []byte{0x03, 0x01, 0x02, 0x03},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(nil)
			cb.encodeRawBytes(tt.input)
			assert.Equal(t, tt.expected, cb.buf)
		})
	}
}

func TestBufferAppend(t *testing.T) {
	tests := []struct {
		name     string
		initial  []byte
		append   []byte
		expected []byte
	}{
		{
			name:     "append to empty buffer",
			initial:  []byte{},
			append:   []byte{1, 2, 3},
			expected: []byte{1, 2, 3},
		},
		{
			name:     "append to non-empty buffer",
			initial:  []byte{1, 2, 3},
			append:   []byte{4, 5, 6},
			expected: []byte{1, 2, 3, 4, 5, 6},
		},
		{
			name:     "append empty bytes",
			initial:  []byte{1, 2, 3},
			append:   []byte{},
			expected: []byte{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(tt.initial)
			cb.append(tt.append)
			assert.Equal(t, tt.expected, cb.buf)
		})
	}
}

// nolint:dupl
func TestEncodeZigZag32(t *testing.T) {
	tests := []struct {
		name     string
		input    int32
		expected uint64
	}{
		{
			name:     "zero",
			input:    0,
			expected: 0,
		},
		{
			name:     "positive number",
			input:    1,
			expected: 2,
		},
		{
			name:     "negative number",
			input:    -1,
			expected: 1,
		},
		{
			name:     "large positive number",
			input:    0x7FFFFFFF,
			expected: 0xFFFFFFFE,
		},
		{
			name:     "large negative number",
			input:    -0x80000000,
			expected: 0xFFFFFFFF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeZigZag32(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// nolint:dupl
func TestEncodeZigZag64(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected uint64
	}{
		{
			name:     "zero",
			input:    0,
			expected: 0,
		},
		{
			name:     "positive number",
			input:    1,
			expected: 2,
		},
		{
			name:     "negative number",
			input:    -1,
			expected: 1,
		},
		{
			name:     "large positive number",
			input:    0x7FFFFFFFFFFFFFFF,
			expected: 0xFFFFFFFFFFFFFFFE,
		},
		{
			name:     "large negative number",
			input:    -0x8000000000000000,
			expected: 0xFFFFFFFFFFFFFFFF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeZigZag64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
