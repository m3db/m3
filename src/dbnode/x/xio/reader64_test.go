// Copyright (c) 2020 Uber Technologies, Inc.
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

package xio

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBytesReader64(t *testing.T) {
	var (
		data = []byte{4, 5, 6, 7, 8, 9, 1, 2, 3, 0, 10, 11, 12, 13, 14, 15, 16, 17}
		r    = NewBytesReader64(nil)
	)

	for l := 0; l < len(data); l++ {
		testBytesReader64(t, r, data[:l])
	}
}

func testBytesReader64(t *testing.T, r *BytesReader64, data []byte) {
	t.Helper()
	r.Reset(data)

	var (
		peeked = []byte{}
		read   = []byte{}
		buf    [8]byte
		word   uint64
		n      byte
		err    error
	)

	for {
		word, n, err = r.Peek64()
		if err != nil {
			break
		}
		binary.BigEndian.PutUint64(buf[:], word)
		peeked = append(peeked, buf[:n]...)

		word, n, err = r.Read64()
		if err != nil {
			break
		}
		binary.BigEndian.PutUint64(buf[:], word)
		read = append(read, buf[:n]...)
	}

	require.Equal(t, io.EOF, err)
	assert.Equal(t, data, peeked)
	assert.Equal(t, data, read)
}
