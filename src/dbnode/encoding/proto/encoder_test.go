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

	"github.com/jhump/protoreflect/desc"
	"github.com/stretchr/testify/require"
)

func TestTSZFields(t *testing.T) {
	testCases := []struct {
		schema   *desc.MessageDescriptor
		expected []customFieldState
	}{
		{
			schema: newVLMessageDescriptor(),
			expected: []customFieldState{
				{fieldNum: 1, fieldType: cFloat64},     // latitude
				{fieldNum: 2, fieldType: cFloat64},     // longitude
				{fieldNum: 3, fieldType: cSignedInt64}, // numTrips
				{fieldNum: 4, fieldType: cBytes},       // deliveryID
			},
		},
	}

	for _, tc := range testCases {
		tszFields := customFields(nil, tc.schema)
		require.Equal(t, tc.expected, tszFields)
	}
}
