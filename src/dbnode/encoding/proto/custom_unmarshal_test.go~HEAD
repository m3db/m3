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
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCustomFieldUnmarshaler(t *testing.T) {
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
				// value are not included in a marshaled protobuf stream (their absence
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
			attributes: map[string]string{"key1": "val1", "key2": "val2", "key3": "val3"},

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

	unmarshaler := newCustomFieldUnmarshaler()
	for _, tc := range testCases {
		vl := newVL(
			tc.latitude, tc.longitude, tc.epoch, tc.deliveryID, tc.attributes)
		marshaledVL, err := vl.Marshal()
		require.NoError(t, err)

		unmarshaler.resetAndUnmarshal(testVLSchema, marshaledVL)
		sortedCustomFieldValues := unmarshaler.sortedCustomFieldValues()
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
			require.Equal(t, len(tc.attributes), unmarshaler.numNonCustomValues())
			m := unmarshaler.nonCustomFieldValues()
			assertAttributesEqual(
				t,
				tc.attributes,
				m.GetFieldByName("attributes").(map[interface{}]interface{}))
		} else {
			require.Equal(t, 0, unmarshaler.numNonCustomValues())
		}
	}
}
