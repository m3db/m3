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

package serialize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUncheckedEmptyDecode(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, []byte{0x0, 0x0}...)

	d := newTestUncheckedTagDecoder()
	d.reset(b)
	require.False(t, d.next())
	require.NoError(t, d.err)
}

func TestUncheckedNilDecode(t *testing.T) {
	d := newTestUncheckedTagDecoder()
	d.reset(nil)
	require.False(t, d.next())
	require.NoError(t, d.err)
}

func TestUncheckedEmptyTagNameDecode(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(1, make([]byte, 2))...) /* num tags */
	b = append(b, encodeUInt16(0, make([]byte, 2))...) /* len empty string */
	b = append(b, encodeUInt16(4, make([]byte, 2))...) /* len defg */
	b = append(b, []byte("defg")...)

	d := newTestUncheckedTagDecoder()
	d.reset(b)
	require.False(t, d.next())
	require.Error(t, d.err)
}

func TestUncheckedEmptyTagValueDecode(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(1, make([]byte, 2))...) /* num tags */
	b = append(b, encodeUInt16(1, make([]byte, 2))...) /* len "1" */
	b = append(b, []byte("a")...)                      /* tag name */
	b = append(b, encodeUInt16(0, make([]byte, 2))...) /* len tag value */

	d := newTestUncheckedTagDecoder()
	d.reset(b)
	assertUncheckedNextTag(t, d, "a", "")
	require.False(t, d.next())
	require.NoError(t, d.err)
}

func TestUncheckedDecodeHeaderMissing(t *testing.T) {
	var b []byte
	b = append(b, []byte{0x0, 0x0}...)
	b = append(b, []byte{0x0, 0x0}...)

	d := newTestUncheckedTagDecoder()
	d.reset(b)
	require.False(t, d.next())
	require.Error(t, d.err)
}

func TestUncheckedDecodeResetsErrState(t *testing.T) {
	var b []byte
	b = append(b, []byte{0x0, 0x0}...)
	b = append(b, []byte{0x0, 0x0}...)

	d := newTestUncheckedTagDecoder()
	d.reset(b)
	require.False(t, d.next())
	require.Error(t, d.err)

	d.reset(nil)
	require.NoError(t, d.err)
}

func TestUncheckedDecodeSimple(t *testing.T) {
	b := testTagDecoderBytesRaw()
	d := newTestUncheckedTagDecoder()
	d.reset(b)
	require.NoError(t, d.err)

	assertUncheckedNextTag(t, d, "abc", "defg")
	assertUncheckedNextTag(t, d, "x", "bar")

	require.False(t, d.next())
	require.NoError(t, d.err)
}

func TestUncheckedDecodeTooManyTags(t *testing.T) {
	b := testTagDecoderBytesRaw()
	d := newUncheckedTagDecoder(NewTagSerializationLimits().SetMaxNumberTags(1))
	d.reset(b)
	require.Error(t, d.err)
}

func TestUncheckedDecodeLiteralTooLong(t *testing.T) {
	b := testTagDecoderBytesRaw()
	d := newUncheckedTagDecoder(NewTagSerializationLimits().
		SetMaxNumberTags(2).
		SetMaxTagLiteralLength(3))
	d.reset(b)
	require.NoError(t, d.err)
	require.False(t, d.next())
	require.Error(t, d.err)
}

func TestUncheckedDecodeMissingTags(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(2, make([]byte, 2))...) /* num tags */

	d := newTestUncheckedTagDecoder()
	d.reset(b)
	require.NoError(t, d.err)

	require.False(t, d.next())
	require.Error(t, d.err)
}

func TestUncheckedDecodeMissingValue(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(2, make([]byte, 2))...) /* num tags */
	b = append(b, encodeUInt16(3, make([]byte, 2))...) /* len abc */
	b = append(b, []byte("abc")...)

	b = append(b, encodeUInt16(4, make([]byte, 2))...) /* len defg */
	b = append(b, []byte("defg")...)

	b = append(b, encodeUInt16(1, make([]byte, 2))...) /* len x */
	b = append(b, []byte("x")...)

	d := newTestUncheckedTagDecoder()
	d.reset(b)
	require.NoError(t, d.err)

	assertUncheckedNextTag(t, d, "abc", "defg")
	require.False(t, d.next())
	require.Error(t, d.err)
}

// assert the next tag in the iteration. ensures the decoder and faster decoder match.
func assertUncheckedNextTag(t *testing.T, d *uncheckedDecoder, name, value string) {
	require.True(t, d.next())
	curName, curValue := d.current()
	require.Equal(t, name, string(curName))
	require.Equal(t, value, string(curValue))
}

func newTestUncheckedTagDecoder() *uncheckedDecoder {
	return newUncheckedTagDecoder(NewTagSerializationLimits())
}
