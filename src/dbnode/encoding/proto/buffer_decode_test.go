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

func TestBuffer(t *testing.T) {
	tests := []struct {
		name     string
		buf      []byte
		index    int
		expected *buffer
	}{
		{
			name:     "empty buffer",
			buf:      []byte{},
			index:    0,
			expected: &buffer{buf: []byte{}, index: 0},
		},
		{
			name:     "non-empty buffer",
			buf:      []byte{1, 2, 3},
			index:    0,
			expected: &buffer{buf: []byte{1, 2, 3}, index: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(tt.buf)
			assert.Equal(t, tt.expected, cb)
		})
	}
}

func TestBufferReset(t *testing.T) {
	tests := []struct {
		name     string
		initial  []byte
		new      []byte
		expected *buffer
	}{
		{
			name:     "reset empty buffer",
			initial:  []byte{1, 2, 3},
			new:      []byte{},
			expected: &buffer{buf: []byte{}, index: 0},
		},
		{
			name:     "reset non-empty buffer",
			initial:  []byte{1, 2, 3},
			new:      []byte{4, 5, 6},
			expected: &buffer{buf: []byte{4, 5, 6}, index: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(tt.initial)
			cb.reset(tt.new)
			assert.Equal(t, tt.expected, cb)
		})
	}
}

func TestBufferEOF(t *testing.T) {
	tests := []struct {
		name     string
		buf      []byte
		index    int
		expected bool
	}{
		{
			name:     "empty buffer",
			buf:      []byte{},
			index:    0,
			expected: true,
		},
		{
			name:     "non-empty buffer at start",
			buf:      []byte{1, 2, 3},
			index:    0,
			expected: false,
		},
		{
			name:     "non-empty buffer at end",
			buf:      []byte{1, 2, 3},
			index:    3,
			expected: true,
		},
		{
			name:     "non-empty buffer past end",
			buf:      []byte{1, 2, 3},
			index:    4,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := &buffer{buf: tt.buf, index: tt.index}
			assert.Equal(t, tt.expected, cb.eof())
		})
	}
}

func TestBufferSkip(t *testing.T) {
	tests := []struct {
		name     string
		buf      []byte
		index    int
		count    int
		expected int
		success  bool
	}{
		{
			name:     "skip within bounds",
			buf:      []byte{1, 2, 3},
			index:    0,
			count:    2,
			expected: 0,
			success:  true,
		},
		{
			name:     "skip to end",
			buf:      []byte{1, 2, 3},
			index:    0,
			count:    3,
			expected: 0,
			success:  true,
		},
		{
			name:     "skip past end",
			buf:      []byte{1, 2, 3},
			index:    0,
			count:    4,
			expected: 0,
			success:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := &buffer{buf: tt.buf, index: tt.index}
			result, success := cb.skip(tt.count)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.success, success)
			if success {
				assert.Equal(t, tt.index+tt.count, cb.index)
			}
		})
	}
}

func TestBufferDecodeVarint(t *testing.T) {
	tests := []struct {
		name     string
		buf      []byte
		expected uint64
		err      bool
	}{
		{
			name:     "single byte",
			buf:      []byte{0x01},
			expected: 1,
			err:      false,
		},
		{
			name:     "two bytes",
			buf:      []byte{0x81, 0x01},
			expected: 129,
			err:      false,
		},
		{
			name:     "three bytes",
			buf:      []byte{0x81, 0x81, 0x01},
			expected: 16513,
			err:      false,
		},
		{
			name:     "empty buffer",
			buf:      []byte{},
			expected: 0,
			err:      true,
		},
		{
			name:     "overflow",
			buf:      []byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81},
			expected: 0,
			err:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(tt.buf)
			result, err := cb.decodeVarint()
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestBufferDecodeTagAndWireType(t *testing.T) {
	tests := []struct {
		name     string
		buf      []byte
		expected int32
		wireType int8
		err      bool
	}{
		{
			name:     "valid tag and wire type",
			buf:      []byte{0x08}, // tag 1, wire type 0
			expected: 1,
			wireType: 0,
			err:      false,
		},
		{
			name:     "valid tag and wire type with multiple bytes",
			buf:      []byte{0x88, 0x01}, // tag 17, wire type 0
			expected: 17,
			wireType: 0,
			err:      false,
		},
		{
			name:     "empty buffer",
			buf:      []byte{},
			expected: 0,
			wireType: 0,
			err:      true,
		},
		{
			name:     "tag too large",
			buf:      []byte{0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88},
			expected: 0,
			wireType: 0,
			err:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(tt.buf)
			tag, wireType, err := cb.decodeTagAndWireType()
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, tag)
				assert.Equal(t, tt.wireType, wireType)
			}
		})
	}
}

func TestBufferDecodeFixed64(t *testing.T) {
	tests := []struct {
		name     string
		buf      []byte
		expected uint64
		err      bool
	}{
		{
			name:     "valid fixed64",
			buf:      []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expected: 1,
			err:      false,
		},
		{
			name:     "valid fixed64 with high bits set",
			buf:      []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			expected: 0xFFFFFFFFFFFFFFFF,
			err:      false,
		},
		{
			name:     "buffer too short",
			buf:      []byte{0x01, 0x00, 0x00, 0x00},
			expected: 0,
			err:      true,
		},
		{
			name:     "empty buffer",
			buf:      []byte{},
			expected: 0,
			err:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(tt.buf)
			result, err := cb.decodeFixed64()
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestBufferDecodeFixed32(t *testing.T) {
	tests := []struct {
		name     string
		buf      []byte
		expected uint64
		err      bool
	}{
		{
			name:     "valid fixed32",
			buf:      []byte{0x01, 0x00, 0x00, 0x00},
			expected: 1,
			err:      false,
		},
		{
			name:     "valid fixed32 with high bits set",
			buf:      []byte{0xFF, 0xFF, 0xFF, 0xFF},
			expected: 0xFFFFFFFF,
			err:      false,
		},
		{
			name:     "buffer too short",
			buf:      []byte{0x01, 0x00},
			expected: 0,
			err:      true,
		},
		{
			name:     "empty buffer",
			buf:      []byte{},
			expected: 0,
			err:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(tt.buf)
			result, err := cb.decodeFixed32()
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// nolint:dupl
func TestDecodeZigZag32(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected int32
	}{
		{
			name:     "zero",
			input:    0,
			expected: 0,
		},
		{
			name:     "positive number",
			input:    2,
			expected: 1,
		},
		{
			name:     "negative number",
			input:    1,
			expected: -1,
		},
		{
			name:     "large positive number",
			input:    0xFFFFFFFE,
			expected: 0x7FFFFFFF,
		},
		{
			name:     "large negative number",
			input:    0xFFFFFFFF,
			expected: -0x80000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodeZigZag32(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// nolint:dupl
func TestDecodeZigZag64(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected int64
	}{
		{
			name:     "zero",
			input:    0,
			expected: 0,
		},
		{
			name:     "positive number",
			input:    2,
			expected: 1,
		},
		{
			name:     "negative number",
			input:    1,
			expected: -1,
		},
		{
			name:     "large positive number",
			input:    0xFFFFFFFFFFFFFFFE,
			expected: 0x7FFFFFFFFFFFFFFF,
		},
		{
			name:     "large negative number",
			input:    0xFFFFFFFFFFFFFFFF,
			expected: -0x8000000000000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodeZigZag64(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBufferDecodeRawBytes(t *testing.T) {
	tests := []struct {
		buf      []byte
		expected []byte
		name     string
		alloc    bool
		err      bool
	}{
		{
			name:     "valid bytes with allocation",
			buf:      []byte{0x03, 0x01, 0x02, 0x03},
			alloc:    true,
			expected: []byte{0x01, 0x02, 0x03},
			err:      false,
		},
		{
			name:     "valid bytes without allocation",
			buf:      []byte{0x03, 0x01, 0x02, 0x03},
			alloc:    false,
			expected: []byte{0x01, 0x02, 0x03},
			err:      false,
		},
		{
			name:     "empty bytes",
			buf:      []byte{0x00},
			alloc:    true,
			expected: []byte{},
			err:      false,
		},
		{
			name:     "buffer too short",
			buf:      []byte{0x03, 0x01},
			alloc:    true,
			expected: nil,
			err:      true,
		},
		{
			name:     "empty buffer",
			buf:      []byte{},
			alloc:    true,
			expected: nil,
			err:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := newCodedBuffer(tt.buf)
			result, err := cb.decodeRawBytes(tt.alloc)
			if tt.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
