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
	"testing"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalIter(t *testing.T) {
	testCases := []struct {
		timestamp  time.Time
		latitude   float64
		longitude  float64
		epoch      int64
		deliveryID []byte
		attributes map[string]string

		expectedIter    map[int32]unmarshalValue
		expectedSkipped *dynamic.Message
	}{
		{
			latitude:  0.1,
			longitude: 1.1,
			epoch:     -1,

			expectedIter: map[int32]unmarshalValue{
				1: {
					float64Val: 0.1,
				},
				2: {
					float64Val: 1.1,
				},
				3: {
					int64Val: -1,
				},
			},
		},
		{
			latitude:   0.1,
			longitude:  1.1,
			epoch:      0,
			deliveryID: []byte("123123123123"),

			expectedIter: map[int32]unmarshalValue{
				1: {
					float64Val: 0.1,
				},
				2: {
					float64Val: 1.1,
				},
				// TODO: Leave a comment about this
				// 3: {
				// 	int64Val: 0,
				// },
				4: {
					bytesVal: []byte("123123123123"),
				},
			},
		},
		{
			latitude:   0.2,
			longitude:  2.2,
			epoch:      1,
			deliveryID: []byte("789789789789"),
			attributes: map[string]string{"key1": "val1", "key2": "val2", "key3": "val3"},

			expectedIter: map[int32]unmarshalValue{
				1: {
					float64Val: 0.2,
				},
				2: {
					float64Val: 2.2,
				},
				3: {
					int64Val: 1,
				},
				4: {
					bytesVal: []byte("789789789789"),
				},
				5: {
					ifaceVal: map[string]string{"key1": "val1"},
				},
			},

			expectedSkipped: newVL(
				0, 0, 0, nil, map[string]string{"key1": "val1", "key2": "val2", "key3": "val3"}),
		},
	}

	unmarshalIter := newUnmarshalIter()
	for _, tc := range testCases {
		vl := newVL(
			tc.latitude, tc.longitude, tc.epoch, tc.deliveryID, tc.attributes)
		marshaledVL, err := vl.Marshal()
		require.NoError(t, err)

		unmarshalIter.reset(testVLSchema, marshaledVL)

		i := 0
		for unmarshalIter.next() {
			var (
				curr     = unmarshalIter.current()
				fieldNum = curr.fd.GetNumber()
				expected = tc.expectedIter[fieldNum]
			)

			curr.fd = nil
			require.Equal(t, expected, curr)
			i++
		}

		// TODO: Make this a method?
		require.NoError(t, unmarshalIter.err)
		require.Equal(t, len(tc.expectedIter), i)

		if len(tc.attributes) > 0 {
			require.Equal(t, len(tc.attributes), unmarshalIter.numSkipped())
			m, err := unmarshalIter.skipped()
			require.NoError(t, err)
			assertAttributesEqual(
				t,
				tc.attributes,
				m.GetFieldByName("attributes").(map[interface{}]interface{}))
		} else {
			require.Equal(t, 0, unmarshalIter.numSkipped())
		}
	}
}
