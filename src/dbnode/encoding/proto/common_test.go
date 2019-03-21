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

	"github.com/stretchr/testify/require"
)

func TestNumBitsRequiredForNumUpToN(t *testing.T) {
	testCases := []struct {
		input          int
		expectedOutput int
	}{
		{
			input:          0,
			expectedOutput: 0,
		},
		{
			input:          1,
			expectedOutput: 1,
		},
		{
			input:          2,
			expectedOutput: 1,
		},
		{
			input:          4,
			expectedOutput: 2,
		},
		{
			input:          8,
			expectedOutput: 3,
		},
		{
			input:          16,
			expectedOutput: 4,
		},
		{
			input:          31,
			expectedOutput: 4,
		},
		{
			input:          32,
			expectedOutput: 5,
		},
		{
			input:          64,
			expectedOutput: 6,
		},
		{
			input:          128,
			expectedOutput: 7,
		},
	}

	for _, tc := range testCases {
		output := numBitsRequiredForNumUpToN(tc.input)
		require.Equal(t, tc.expectedOutput, output)
	}
}
