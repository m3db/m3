// Copyright (c) 2016 Uber Technologies, Inc.
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

package msgpack

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func testBufferedEncoder() BufferedEncoder {
	return newBufferedEncoder()
}

func TestBufferedEncoderReset(t *testing.T) {
	encoder := testBufferedEncoder()
	inputs := []interface{}{1, 2.0, "foo", byte(8)}

	// Encode for the first time
	for _, input := range inputs {
		encoder.Encode(input)
	}
	encoded := encoder.Bytes()
	results := make([]byte, len(encoded))
	copy(results, encoded)

	// Reset the encoder
	encoder.Reset()

	// Encode for the second time
	for _, input := range inputs {
		encoder.Encode(input)
	}
	encoded = encoder.Bytes()
	results2 := make([]byte, len(encoded))
	copy(results2, encoded)

	require.Equal(t, results, results2)
}

func TestBufferedEncoderClose(t *testing.T) {
	encoder := testBufferedEncoder()
	require.False(t, encoder.closed)

	// Close the encoder should set the flag
	encoder.Close()
	require.True(t, encoder.closed)

	// Close the encoder again should be a no-op
	encoder.Close()
	require.True(t, encoder.closed)
}
