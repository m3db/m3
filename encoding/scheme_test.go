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

package encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemeTrimEndOfStreamSuccess(t *testing.T) {
	s := defaultMarkerEncodingScheme.(*markerEncodingScheme)

	head := []byte{0x1, 0x2}
	for i := 0; i < 256; i++ {
		for j := 0; j < 8; j++ {
			encoded := append(head, s.tails[i][j]...)
			trimmed, pos, err := s.TrimEndOfStream(encoded)
			require.NoError(t, err)
			require.Equal(t, len(head)+1, len(trimmed))
			require.Equal(t, head, trimmed[:len(head)])
			require.Equal(t, j+1, pos)
			require.Equal(t, byte(i)>>uint(7-j), trimmed[len(trimmed)-1]>>uint(8-pos))
		}
	}
}
