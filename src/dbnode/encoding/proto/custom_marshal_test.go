// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"
	"math"
	"testing"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/stretchr/testify/require"
)

func TestCustomMarshal(t *testing.T) {
	testCases := []struct {
		message *dynamic.Message
	}{
		{
			message: newVL(0, 0, 0, nil, nil),
		},
		{
			message: newVL(1, 1, 1, []byte("some-delivery-id"), nil),
		},
		{
			message: newVL(2, 2, 2, []byte("some-delivery-id-2"), map[string]string{"key": "val"}),
		},
	}

	marshaller := newCustomMarshaller()
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test_case_%d", i), func(t *testing.T) {
			marshaller.reset()
			customMarshalVL(t, marshaller, tc.message)

			unmarshalM := dynamic.NewMessage(testVLSchema)
			require.NoError(t, unmarshalM.Unmarshal(marshaller.bytes()))

			require.True(t, dynamic.Equal(tc.message, unmarshalM))
		})
	}
}

func customMarshalVL(t *testing.T, marshaller customFieldMarshaller, m *dynamic.Message) {
	marshaller.encFloat64(1, m.GetFieldByNumber(1).(float64))
	marshaller.encFloat64(2, m.GetFieldByNumber(2).(float64))
	marshaller.encInt64(3, m.GetFieldByNumber(3).(int64))
	marshaller.encBytes(4, m.GetFieldByNumber(4).([]byte))

	// Fields set to their default value are not marshalled in the Protobuf3 format so to generate
	// the bytes that represent the attributes map we create a new VL message where every field is
	// set to its default value except for the attributes map and then Marshal() it into a byte stream
	// which will create a stream that only includes the attributes field.
	var (
		attributeMapIface      = m.GetFieldByNumber(5).(map[interface{}]interface{})
		attributeMap           = mapInterfaceToMapString(attributeMapIface)
		attributesM            = newVL(0, 0, 0, nil, attributeMap)
		attributeMapBytes, err = attributesM.Marshal()
	)
	require.NoError(t, err)
	marshaller.encPartialProto(attributeMapBytes)
}

func mapInterfaceToMapString(ifaceMap map[interface{}]interface{}) map[string]string {
	stringMap := make(map[string]string, len(ifaceMap))
	for key, val := range ifaceMap {
		stringMap[key.(string)] = val.(string)
	}
	return stringMap
}

func TestCustomMarshallerFloat64(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    float64
		expected []byte
	}{
		{
			name:     "zero value",
			tag:      1,
			value:    0.0,
			expected: nil,
		},
		{
			name:     "positive value",
			tag:      1,
			value:    1.5,
			expected: []byte{0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x3F},
		},
		{
			name:     "negative value",
			tag:      1,
			value:    -1.5,
			expected: []byte{0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0xBF},
		},
		{
			name:     "max float64",
			tag:      1,
			value:    math.MaxFloat64,
			expected: []byte{0x09, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0x7F},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encFloat64(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerFloat32(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    float32
		expected []byte
	}{
		{
			name:     "zero value",
			tag:      1,
			value:    0.0,
			expected: nil,
		},
		{
			name:     "positive value",
			tag:      1,
			value:    1.5,
			expected: []byte{0x0D, 0x00, 0x00, 0xC0, 0x3F},
		},
		{
			name:     "negative value",
			tag:      1,
			value:    -1.5,
			expected: []byte{0x0D, 0x00, 0x00, 0xC0, 0xBF},
		},
		{
			name:     "max float32",
			tag:      1,
			value:    math.MaxFloat32,
			expected: []byte{0x0D, 0xFF, 0xFF, 0x7F, 0x7F},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encFloat32(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerBool(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    bool
		expected []byte
	}{
		{
			name:     "false value",
			tag:      1,
			value:    false,
			expected: nil,
		},
		{
			name:     "true value",
			tag:      1,
			value:    true,
			expected: []byte{0x08, 0x01},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encBool(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerInt32(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    int32
		expected []byte
	}{
		{
			name:     "zero value",
			tag:      1,
			value:    0,
			expected: nil,
		},
		{
			name:     "positive value",
			tag:      1,
			value:    42,
			expected: []byte{0x08, 0x2A},
		},
		{
			name:     "negative value",
			tag:      1,
			value:    -42,
			expected: []byte{0x08, 0xD6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		},
		{
			name:     "max int32",
			tag:      1,
			value:    math.MaxInt32,
			expected: []byte{0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0x07},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encInt32(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerSInt32(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    int32
		expected []byte
	}{
		{
			name:     "zero value",
			tag:      1,
			value:    0,
			expected: nil,
		},
		{
			name:     "positive value",
			tag:      1,
			value:    42,
			expected: []byte{0x08, 0x54},
		},
		{
			name:     "negative value",
			tag:      1,
			value:    -42,
			expected: []byte{0x08, 0x53},
		},
		{
			name:     "max int32",
			tag:      1,
			value:    math.MaxInt32,
			expected: []byte{0x08, 0xFE, 0xFF, 0xFF, 0xFF, 0x0F},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encSInt32(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerSFixedInt32(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    int32
		expected []byte
	}{
		{
			name:     "zero value",
			tag:      1,
			value:    0,
			expected: []byte{0x0D, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:     "positive value",
			tag:      1,
			value:    42,
			expected: []byte{0x0D, 0x2A, 0x00, 0x00, 0x00},
		},
		{
			name:     "negative value",
			tag:      1,
			value:    -42,
			expected: []byte{0x0D, 0xD6, 0xFF, 0xFF, 0xFF},
		},
		{
			name:     "max int32",
			tag:      1,
			value:    math.MaxInt32,
			expected: []byte{0x0D, 0xFF, 0xFF, 0xFF, 0x7F},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encSFixedInt32(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerUInt32(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    uint32
		expected []byte
	}{
		{
			name:     "zero value",
			tag:      1,
			value:    0,
			expected: nil,
		},
		{
			name:     "positive value",
			tag:      1,
			value:    42,
			expected: []byte{0x08, 0x2A},
		},
		{
			name:     "max uint32",
			tag:      1,
			value:    math.MaxUint32,
			expected: []byte{0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encUInt32(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerInt64(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    int64
		expected []byte
	}{
		{
			name:     "zero value",
			tag:      1,
			value:    0,
			expected: nil,
		},
		{
			name:     "positive value",
			tag:      1,
			value:    42,
			expected: []byte{0x08, 0x2A},
		},
		{
			name:     "negative value",
			tag:      1,
			value:    -42,
			expected: []byte{0x08, 0xD6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		},
		{
			name:     "max int64",
			tag:      1,
			value:    math.MaxInt64,
			expected: []byte{0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encInt64(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerSInt64(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    int64
		expected []byte
	}{
		{
			name:     "zero value",
			tag:      1,
			value:    0,
			expected: nil,
		},
		{
			name:     "positive value",
			tag:      1,
			value:    42,
			expected: []byte{0x08, 0x54},
		},
		{
			name:     "negative value",
			tag:      1,
			value:    -42,
			expected: []byte{0x08, 0x53},
		},
		{
			name:     "max int64",
			tag:      1,
			value:    math.MaxInt64,
			expected: []byte{0x08, 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encSInt64(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerSFixedInt64(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    int64
		expected []byte
	}{
		{
			name:     "zero value",
			tag:      1,
			value:    0,
			expected: []byte{0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:     "positive value",
			tag:      1,
			value:    42,
			expected: []byte{0x09, 0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			name:     "negative value",
			tag:      1,
			value:    -42,
			expected: []byte{0x09, 0xD6, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		},
		{
			name:     "max int64",
			tag:      1,
			value:    math.MaxInt64,
			expected: []byte{0x09, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encSFixedInt64(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerUInt64(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    uint64
		expected []byte
	}{
		{
			name:     "zero value",
			tag:      1,
			value:    0,
			expected: nil,
		},
		{
			name:     "positive value",
			tag:      1,
			value:    42,
			expected: []byte{0x08, 0x2A},
		},
		{
			name:     "max uint64",
			tag:      1,
			value:    math.MaxUint64,
			expected: []byte{0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encUInt64(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerBytes(t *testing.T) {
	tests := []struct {
		name     string
		tag      int32
		value    []byte
		expected []byte
	}{
		{
			name:     "empty bytes",
			tag:      1,
			value:    []byte{},
			expected: nil,
		},
		{
			name:     "single byte",
			tag:      1,
			value:    []byte{0x01},
			expected: []byte{0x0A, 0x01, 0x01},
		},
		{
			name:     "multiple bytes",
			tag:      1,
			value:    []byte{0x01, 0x02, 0x03},
			expected: []byte{0x0A, 0x03, 0x01, 0x02, 0x03},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newCustomMarshaller()
			m.encBytes(tt.tag, tt.value)
			require.Equal(t, tt.expected, m.bytes())
		})
	}
}

func TestCustomMarshallerReset(t *testing.T) {
	m := newCustomMarshaller()

	// Add some data
	m.encInt32(1, 42)
	require.NotEmpty(t, m.bytes())

	// Reset
	m.reset()
	require.Empty(t, m.bytes())

	// Verify we can still use it after reset
	m.encInt32(1, 42)
	require.Equal(t, []byte{0x08, 0x2A}, m.bytes())
}
