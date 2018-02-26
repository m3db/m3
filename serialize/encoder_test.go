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
	require.NoError(t, e.Encode(ident.StringID(""), ident.EmptyTagIterator))

	b := e.Data()
	require.Len(t, b, 6)
	require.Equal(t, headerMagicBytes, b[:2])
	require.Equal(t, []byte{0x0, 0x0, 0x0, 0x0}, b[2:])
}

func TestInuseEncode(t *testing.T) {
	e := newTestEncoder()
	require.NoError(t, e.Encode(ident.StringID(""), ident.EmptyTagIterator))
	require.Error(t, e.Encode(nil, ident.EmptyTagIterator))
}

func TestEncoderLeavesOriginalIterator(t *testing.T) {
	e := newTestEncoder()
	id := ident.StringID("str")
	tags := ident.NewTagIterator(
		ident.StringTag("abc", "defg"),
		ident.StringTag("x", "bar"),
	)

	require.Equal(t, 2, tags.Remaining())
	require.NoError(t, e.Encode(id, tags))
	require.Equal(t, 2, tags.Remaining())
}

func TestSimpleEncode(t *testing.T) {
	e := newTestEncoder()

	id := ident.StringID("strng")
	tags := ident.NewTagIterator(
		ident.StringTag("abc", "defg"),
		ident.StringTag("x", "bar"),
	)
	require.NoError(t, e.Encode(id, tags))

	b := e.Data()
	numExpectedBytes := 2 /* header */ + 2 /* num tags */ +
		2 /* str length */ + len("strng") +
		2 /* abc length */ + len("abc") +
		2 /* defg length */ + len("defg") +
		2 /* x length */ + len("x") +
		2 /* bar length */ + len("bar")
	require.Len(t, b, numExpectedBytes)
	require.Equal(t, headerMagicBytes, b[:2])
	require.Equal(t, encodeUInt16(2), b[2:4])
	require.Equal(t, uint16(5), decodeUInt16(b[4:6])) /* len abc */
	require.Equal(t, "strng", string(b[6:11]))
	require.Equal(t, uint16(3), decodeUInt16(b[11:13])) /* len abc */
	require.Equal(t, "abc", string(b[13:16]))
	require.Equal(t, uint16(4), decodeUInt16(b[16:18])) /* len defg */
	require.Equal(t, "defg", string(b[18:22]))
	require.Equal(t, uint16(1), decodeUInt16(b[22:24])) /* len x */
	require.Equal(t, "x", string(b[24:25]))
	require.Equal(t, uint16(3), decodeUInt16(b[25:27])) /* len bar */
	require.Equal(t, "bar", string(b[27:30]))
}
