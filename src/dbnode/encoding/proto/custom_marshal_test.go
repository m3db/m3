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
