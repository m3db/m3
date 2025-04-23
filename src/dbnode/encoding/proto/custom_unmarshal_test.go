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
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/stretchr/testify/require"
)

func TestCustomFieldUnmarshaller(t *testing.T) {
	// Store in a var to prevent the compiler from complaining about overflow errors.
	neg1 := -1

	testCases := []struct {
		timestamp  time.Time
		latitude   float64
		longitude  float64
		epoch      int64
		deliveryID []byte
		attributes map[string]string

		expectedSortedCustomFields []unmarshalValue
	}{
		{
			latitude:  0.1,
			longitude: 1.1,
			epoch:     -1,

			expectedSortedCustomFields: []unmarshalValue{
				{
					fieldNumber: 1,
					v:           math.Float64bits(0.1),
				},
				{
					fieldNumber: 2,
					v:           math.Float64bits(1.1),
				},
				{
					fieldNumber: 3,
					v:           uint64(neg1),
				},
			},
		},
		{
			latitude:   0.1,
			longitude:  1.1,
			epoch:      0,
			deliveryID: []byte("123123123123"),

			expectedSortedCustomFields: []unmarshalValue{
				{
					fieldNumber: 1,
					v:           math.Float64bits(0.1),
				},
				{
					fieldNumber: 2,
					v:           math.Float64bits(1.1),
				},
				// Note that epoch (field number 3) is not included here because default
				// value are not included in a marshalled protobuf stream (their absence
				// implies a default vlaue for a field) which means they're also not
				// returned by the `sortedCustomFieldValues` method.
				{
					fieldNumber: 4,
					bytes:       []byte("123123123123"),
				},
			},
		},
		{
			latitude:   0.2,
			longitude:  2.2,
			epoch:      1,
			deliveryID: []byte("789789789789"),
			attributes: map[string]string{"key1": "val1"},

			expectedSortedCustomFields: []unmarshalValue{
				{
					fieldNumber: 1,
					v:           math.Float64bits(0.2),
				},
				{
					fieldNumber: 2,
					v:           math.Float64bits(2.2),
				},
				{
					fieldNumber: 3,
					v:           (1),
				},
				{
					fieldNumber: 4,
					bytes:       []byte("789789789789"),
				},
			},
		},
	}

	unmarshaller := newCustomFieldUnmarshaller(customUnmarshallerOptions{})
	for _, tc := range testCases {
		vl := newVL(
			tc.latitude, tc.longitude, tc.epoch, tc.deliveryID, tc.attributes)
		marshalledVL, err := vl.Marshal()
		require.NoError(t, err)

		unmarshaller.resetAndUnmarshal(testVLSchema, marshalledVL)
		sortedCustomFieldValues := unmarshaller.sortedCustomFieldValues()
		require.Equal(t, len(tc.expectedSortedCustomFields), len(sortedCustomFieldValues))

		lastFieldNum := -1
		for i, curr := range sortedCustomFieldValues {
			var (
				fieldNum = curr.fieldNumber
				expected = tc.expectedSortedCustomFields[i]
			)
			// Make sure iteration is sorted and values match.
			require.True(t, int(fieldNum) > lastFieldNum)
			require.Equal(t, expected, curr)
			switch fieldNum {
			case 1:
				require.Equal(t, tc.latitude, curr.asFloat64())
			case 2:
				require.Equal(t, tc.longitude, curr.asFloat64())
			case 3:
				require.Equal(t, tc.epoch, curr.asInt64())
			case 4:
				require.True(t, bytes.Equal(tc.deliveryID, curr.asBytes()))
			}
			i++
		}

		if len(tc.attributes) > 0 {
			require.Equal(t, len(tc.attributes), unmarshaller.numNonCustomValues())
			nonCustomFieldValues := unmarshaller.sortedNonCustomFieldValues()
			require.Equal(t, 1, len(nonCustomFieldValues))
			require.Equal(t, int32(5), nonCustomFieldValues[0].fieldNum)

			assertAttributesEqualMarshalledBytes(
				t,
				nonCustomFieldValues[0].marshalled,
				tc.attributes)
		} else {
			require.Equal(t, 0, unmarshaller.numNonCustomValues())
		}
	}
}

func assertAttributesEqualMarshalledBytes(
	t *testing.T,
	actualMarshalled []byte,
	attrs map[string]string,
) {
	m := dynamic.NewMessage(testVLSchema)
	m.SetFieldByName("attributes", attrs)
	expectedMarshalled, err := m.Marshal()

	require.NoError(t, err)
	require.Equal(t, expectedMarshalled, actualMarshalled)
}

func TestCustomUnmarshaller(t *testing.T) {
	tests := []struct {
		name              string
		fields            []*dpb.FieldDescriptorProto
		input             []byte
		expectedCustom    []unmarshalValue
		expectedNonCustom []marshalledField
		skipUnknown       bool
		expectError       bool
	}{
		{
			name: "empty message",
			fields: []*dpb.FieldDescriptorProto{
				{Name: proto.String("field1"), Number: proto.Int32(1), Type: dpb.FieldDescriptorProto_TYPE_INT32.Enum()},
			},
			input:             []byte{},
			expectedCustom:    []unmarshalValue{},
			expectedNonCustom: []marshalledField{},
		},
		{
			name: "single int32 field",
			fields: []*dpb.FieldDescriptorProto{
				{Name: proto.String("field1"), Number: proto.Int32(1), Type: dpb.FieldDescriptorProto_TYPE_INT32.Enum()},
			},
			input: []byte{0x08, 0x2A}, // field 1, value 42
			expectedCustom: []unmarshalValue{
				{fieldNumber: 1, v: 42},
			},
			expectedNonCustom: []marshalledField{},
		},
		{
			name: "unknown field with skip",
			fields: []*dpb.FieldDescriptorProto{
				{Name: proto.String("field1"), Number: proto.Int32(1), Type: dpb.FieldDescriptorProto_TYPE_INT32.Enum()},
			},
			input: []byte{0x08, 0x2A, 0x10, 0x01}, // field 1=42, unknown field 2=1
			expectedCustom: []unmarshalValue{
				{fieldNumber: 1, v: 42},
			},
			expectedNonCustom: []marshalledField{},
			skipUnknown:       true,
		},
		{
			name: "unknown field without skip",
			fields: []*dpb.FieldDescriptorProto{
				{Name: proto.String("field1"), Number: proto.Int32(1), Type: dpb.FieldDescriptorProto_TYPE_INT32.Enum()},
			},
			input:       []byte{0x08, 0x2A, 0x10, 0x01}, // field 1=42, unknown field 2=1
			expectError: true,
		},
		{
			name: "repeated field",
			fields: []*dpb.FieldDescriptorProto{
				{Name: proto.String("field1"), Number: proto.Int32(1),
					Type:  dpb.FieldDescriptorProto_TYPE_INT32.Enum(),
					Label: dpb.FieldDescriptorProto_LABEL_REPEATED.Enum()},
			},
			input:          []byte{0x08, 0x2A, 0x08, 0x2B}, // field 1=[42, 43]
			expectedCustom: []unmarshalValue{},
			expectedNonCustom: []marshalledField{
				{fieldNum: 1, marshalled: []byte{0x08, 0x2A, 0x08, 0x2B}},
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert field descriptors to proto format
			schema := createTestSchema(tt.fields, i)
			//fields := make([]*dpb.FieldDescriptorProto, 0, len(schema.GetFields()))
			//for _, f := range schema.GetFields() {
			//	fields = append(fields, f.AsFieldDescriptorProto())
			//}

			unmarshaller := newCustomFieldUnmarshaller(customUnmarshallerOptions{
				skipUnknownFields: tt.skipUnknown,
			})

			err := unmarshaller.resetAndUnmarshal(schema, tt.input)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Verify custom values
			customValues := unmarshaller.sortedCustomFieldValues()
			require.Equal(t, len(tt.expectedCustom), len(customValues))
			for i, expected := range tt.expectedCustom {
				require.Equal(t, expected.fieldNumber, customValues[i].fieldNumber)
				require.Equal(t, expected.v, customValues[i].v)
			}

			// Verify non-custom values
			nonCustomValues := unmarshaller.sortedNonCustomFieldValues()
			require.Equal(t, len(tt.expectedNonCustom), len(nonCustomValues))
			for i, expected := range tt.expectedNonCustom {
				require.Equal(t, expected.fieldNum, nonCustomValues[i].fieldNum)
				require.Equal(t, expected.marshalled, nonCustomValues[i].marshalled)
			}
		})
	}
}

func TestUnmarshalValue(t *testing.T) {
	tests := []struct {
		name      string
		value     unmarshalValue
		asBool    bool
		asUint64  uint64
		asInt64   int64
		asFloat64 float64
		asBytes   []byte
	}{
		{
			name:      "bool false",
			value:     unmarshalValue{v: 0},
			asBool:    false,
			asUint64:  0,
			asInt64:   0,
			asFloat64: 0.0,
		},
		{
			name:    "bytes",
			value:   unmarshalValue{bytes: []byte{0x01, 0x02, 0x03}},
			asBytes: []byte{0x01, 0x02, 0x03},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.asBool, tt.value.asBool())
			require.Equal(t, tt.asUint64, tt.value.asUint64())
			require.Equal(t, tt.asInt64, tt.value.asInt64())
			require.Equal(t, tt.asFloat64, tt.value.asFloat64())
			require.Equal(t, tt.asBytes, tt.value.asBytes())
		})
	}
}

func createTestSchema(fields []*dpb.FieldDescriptorProto, i int) *desc.MessageDescriptor {
	// Generate a unique name for each schema
	uniqueName := fmt.Sprintf("test_%d.proto", i)

	msg := &dpb.DescriptorProto{
		Name:  proto.String("TestMessage"),
		Field: fields,
	}
	file := &dpb.FileDescriptorProto{
		Name:        proto.String(uniqueName),
		Package:     proto.String("test"),
		MessageType: []*dpb.DescriptorProto{msg},
		Syntax:      proto.String("proto3"),
	}
	fd, err := desc.CreateFileDescriptor(file)
	if err != nil {
		panic(err)
	}
	return fd.GetMessageTypes()[0]
}
