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

	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

var testBytesPool pool.CheckedBytesPool

func init() {
	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{pool.Bucket{
		Capacity: 1024,
		Count:    10,
	}}, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	testBytesPool = bytesPool
}

func testDecoderBytes() []byte {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(uint16(2))...) /* num tags */

	b = append(b, encodeUInt16(3)...) /* len abc */
	b = append(b, []byte("abc")...)

	b = append(b, encodeUInt16(4)...) /* len defg */
	b = append(b, []byte("defg")...)

	b = append(b, encodeUInt16(1)...) /* len x */
	b = append(b, []byte("x")...)

	b = append(b, encodeUInt16(3)...) /* len bar */
	b = append(b, []byte("bar")...)

	return b
}

func TestEmptyDecode(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, []byte{0x0, 0x0}...)

	d := newDecoder(nil, testBytesPool)
	d.Reset(b)
	require.False(t, d.Next())
	require.NoError(t, d.Err())
	d.Close()
	d.Finalize()
}

func TestDecodeHeaderMissing(t *testing.T) {
	var b []byte
	b = append(b, []byte{0x0, 0x0}...)
	b = append(b, []byte{0x0, 0x0}...)

	d := newDecoder(nil, testBytesPool)
	d.Reset(b)
	require.False(t, d.Next())
	require.Error(t, d.Err())
	d.Close()
	d.Finalize()
}

func TestDecodeSimple(t *testing.T) {
	b := testDecoderBytes()
	d := newDecoder(nil, testBytesPool)
	d.Reset(b)
	require.NoError(t, d.Err())

	require.True(t, d.Next())
	tag := d.Current()
	require.Equal(t, "abc", tag.Name.String())
	require.Equal(t, "defg", tag.Value.String())

	require.True(t, d.Next())
	tag = d.Current()
	require.Equal(t, "x", tag.Name.String())
	require.Equal(t, "bar", tag.Value.String())

	require.False(t, d.Next())
	require.NoError(t, d.Err())

	d.Close()
	d.Finalize()
}

func TestDecodeMissingTags(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(uint16(2))...) /* num tags */

	d := newDecoder(nil, testBytesPool)
	d.Reset(b)
	require.NoError(t, d.Err())

	require.False(t, d.Next())
	require.Error(t, d.Err())
}

func TestDecodeMissingValue(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(uint16(2))...) /* num tags */
	b = append(b, encodeUInt16(3)...)         /* len abc */
	b = append(b, []byte("abc")...)

	b = append(b, encodeUInt16(4)...) /* len defg */
	b = append(b, []byte("defg")...)

	b = append(b, encodeUInt16(1)...) /* len x */
	b = append(b, []byte("x")...)

	d := newDecoder(nil, testBytesPool)
	d.Reset(b)
	require.NoError(t, d.Err())

	require.True(t, d.Next())
	require.False(t, d.Next())
	require.Error(t, d.Err())
}
