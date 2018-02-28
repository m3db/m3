// Copyright (c) 2018 Uber Technologies, Inc.
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

package serialize

import (
	"bytes"
	"testing"

	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/require"
)

var (
	initialBufferLength = 1 << 12
)

func newTestEncoder() Encoder {
	return newEncoder(initialBufferLength, nil)
}

func TestEmptyEncode(t *testing.T) {
	e := newTestEncoder()
	require.NoError(t, e.Encode(ident.EmptyTagIterator))

	b := e.Data().Get()
	require.Len(t, b, 4)
	require.Equal(t, headerMagicBytes, b[:2])
	require.Equal(t, []byte{0x0, 0x0}, b[2:4])
}

func TestEncodeResetTwice(t *testing.T) {
	e := newTestEncoder()
	require.NoError(t, e.Encode(ident.EmptyTagIterator))
	e.Reset()
	e.Reset()
	require.NoError(t, e.Encode(ident.EmptyTagIterator))
}

func TestInuseEncode(t *testing.T) {
	e := newTestEncoder()
	require.NoError(t, e.Encode(ident.EmptyTagIterator))
	require.Error(t, e.Encode(ident.EmptyTagIterator))
}

func TestEncoderLeavesOriginalIterator(t *testing.T) {
	e := newTestEncoder()
	tags := ident.NewTagIterator(
		ident.StringTag("abc", "defg"),
		ident.StringTag("x", "bar"),
	)

	require.Equal(t, 2, tags.Remaining())
	require.NoError(t, e.Encode(tags))
	require.Equal(t, 2, tags.Remaining())
}

func TestSimpleEncode(t *testing.T) {
	e := newTestEncoder()

	tags := ident.NewTagIterator(
		ident.StringTag("abc", "defg"),
		ident.StringTag("x", "bar"),
	)
	require.NoError(t, e.Encode(tags))

	b := e.Data().Get()
	numExpectedBytes := 2 /* header */ + 2 /* num tags */ +
		2 /* abc length */ + len("abc") +
		2 /* defg length */ + len("defg") +
		2 /* x length */ + len("x") +
		2 /* bar length */ + len("bar")
	require.Len(t, b, numExpectedBytes)
	require.Equal(t, headerMagicBytes, b[:2])
	require.Equal(t, encodeUInt16(2), b[2:4])
	require.Equal(t, uint16(3), decodeUInt16(b[4:6])) /* len abc */
	require.Equal(t, "abc", string(b[6:9]))
	require.Equal(t, uint16(4), decodeUInt16(b[9:11])) /* len defg */
	require.Equal(t, "defg", string(b[11:15]))
	require.Equal(t, uint16(1), decodeUInt16(b[15:17])) /* len x */
	require.Equal(t, "x", string(b[17:18]))
	require.Equal(t, uint16(3), decodeUInt16(b[18:20])) /* len bar */
	require.Equal(t, "bar", string(b[20:23]))
}

func TestEncoderErrorEncoding(t *testing.T) {
	e := newTestEncoder()
	tags := ident.NewTagIterator(
		ident.StringTag("abc", "defg"),
		ident.StringTag("x", nstring(int(MaxTagLiteralLength)+1)),
	)

	require.Error(t, e.Encode(tags))
	require.Panics(t, func() {
		// no valid data to return
		e.Data().Get()
	})

	e.Reset()
	tags = ident.NewTagIterator(ident.StringTag("abc", "defg"))
	require.NoError(t, e.Encode(tags))
}

func nstring(n int) string {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.WriteByte(byte('a'))
	}
	return buf.String()
}
