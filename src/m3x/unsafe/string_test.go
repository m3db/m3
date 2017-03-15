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

package xunsafe

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToBytesSmallString(t *testing.T) {
	str := "foobarbaz"
	b := ToBytes(str)
	require.Equal(t, []byte(str), []byte(b))
	require.Equal(t, len(str), len(b))
	require.Equal(t, len(str), cap(b))
}

func TestToBytesLargeString(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 65536; i++ {
		buf.WriteByte(byte(i % 256))
	}
	str := buf.String()
	b := ToBytes(str)
	require.Equal(t, []byte(str), []byte(b))
	require.Equal(t, len(str), len(b))
	require.Equal(t, len(str), cap(b))
}

func TestToBytesStillValidAfterStringIsGCed(t *testing.T) {
	b := testStringToBytes()

	// The source string is now out of scope, forcing a GC
	runtime.GC()

	// Assert the underlying byte slice is still valid
	require.Equal(t, []byte("foobarbaz"), []byte(b))
	require.Equal(t, 9, len(b))
	require.Equal(t, 9, cap(b))
}

func testStringToBytes() []byte {
	str := "foobarbaz"
	b := ToBytes(str)
	return b
}
