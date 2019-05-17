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
// all copies or substantial portions of the Softwarw.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package docs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompressorDecompressor(t *testing.T) {
	rawBytes := []byte("some long string that goes on")
	for _, codec := range ValidCompressionTypes {
		t.Run(codec.String(), func(t *testing.T) {
			copts := CompressionOptions{
				Type:     codec,
				PageSize: 1 << 14,
			}
			c, err := copts.newCompressor()
			require.NoError(t, err)
			compressed, err := c.Compress(rawBytes)
			require.NoError(t, err)

			d, err := copts.newDecompressor()
			require.NoError(t, err)
			decompressed, err := d.Decompress(compressed)
			require.NoError(t, err)

			require.Equal(t, string(rawBytes), string(decompressed))
		})
	}
}
