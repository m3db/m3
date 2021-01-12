// Copyright (c) 2021 Uber Technologies, Inc.
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

package test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByteSliceDataContained(t *testing.T) {
	outter := []byte("abcde")
	for i := 0; i < len(outter); i++ {
		for j := i; j <= len(outter); j++ {
			inner := outter[i:j]
			require.True(t, ByteSliceDataContained(outter, inner))
		}
	}
}

func TestByteSliceDataContainedNot(t *testing.T) {
	data := []byte("__abcde__")
	outterStart := 2
	outterEnd := 7
	outter := data[outterStart:outterEnd]

	tests := [][]byte{
		data,
		data[outterStart-1 : outterEnd+1],

		data[outterStart : outterEnd+1],
		data[outterStart : outterEnd+2],

		data[outterStart-1 : outterEnd],
		data[outterStart-2 : outterEnd],
	}

	for _, tt := range tests {
		t.Run(string(tt), func(t *testing.T) {
			require.False(t, ByteSliceDataContained(outter, tt))
		})
	}
}
