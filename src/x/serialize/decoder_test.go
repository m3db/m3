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
	"fmt"
	"testing"

	"github.com/m3db/m3x/checked"
	xtest "github.com/m3db/m3x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testDecodeOpts = NewTagDecoderOptions()
)

func TestEmptyDecode(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, []byte{0x0, 0x0}...)

	d := newTagDecoder(testDecodeOpts, nil)
	d.Reset(wrapAsCheckedBytes(b))
	require.False(t, d.Next())
	require.NoError(t, d.Err())
	d.Close()
}

func TestEmptyTagNameDecode(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(uint16(1))...) /* num tags */
	b = append(b, encodeUInt16(0)...)         /* len empty string */
	b = append(b, encodeUInt16(4)...)         /* len defg */
	b = append(b, []byte("defg")...)

	d := newTagDecoder(testDecodeOpts, nil)
	d.Reset(wrapAsCheckedBytes(b))
	require.False(t, d.Next())
	require.Error(t, d.Err())
}

func TestEmptyTagValueDecode(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(uint16(1))...) /* num tags */
	b = append(b, encodeUInt16(1)...)         /* len "1" */
	b = append(b, []byte("a")...)             /* tag name */
	b = append(b, encodeUInt16(0)...)         /* len tag value */

	d := newTagDecoder(testDecodeOpts, nil)
	d.Reset(wrapAsCheckedBytes(b))
	require.True(t, d.Next())
	tag := d.Current()
	require.Equal(t, "a", tag.Name.String())
	require.Equal(t, "", tag.Value.String())
	require.False(t, d.Next())
	require.NoError(t, d.Err())
}

func TestDecodeHeaderMissing(t *testing.T) {
	var b []byte
	b = append(b, []byte{0x0, 0x0}...)
	b = append(b, []byte{0x0, 0x0}...)

	d := newTestTagDecoder()
	d.Reset(wrapAsCheckedBytes(b))
	require.False(t, d.Next())
	require.Error(t, d.Err())
	d.Close()
}

func TestDecodeResetsErrState(t *testing.T) {
	var b []byte
	b = append(b, []byte{0x0, 0x0}...)
	b = append(b, []byte{0x0, 0x0}...)

	d := newTestTagDecoder()
	d.Reset(wrapAsCheckedBytes(b))
	require.False(t, d.Next())
	require.Error(t, d.Err())

	d.Reset(testTagDecoderBytes())
	require.NoError(t, d.Err())
}

func TestDecodeSimple(t *testing.T) {
	b := testTagDecoderBytes()
	d := newTestTagDecoder()
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
}

func TestDecodeTooManyTags(t *testing.T) {
	b := testTagDecoderBytes()
	opts := testDecodeOpts.SetTagSerializationLimits(
		NewTagSerializationLimits().SetMaxNumberTags(1))
	d := newTagDecoder(opts, nil)
	d.Reset(b)
	require.Error(t, d.Err())
}

func TestDecodeLiteralTooLong(t *testing.T) {
	b := testTagDecoderBytes()
	opts := testDecodeOpts.SetTagSerializationLimits(
		NewTagSerializationLimits().
			SetMaxNumberTags(2).
			SetMaxTagLiteralLength(3))
	d := newTagDecoder(opts, nil)
	d.Reset(b)
	require.NoError(t, d.Err())
	require.False(t, d.Next())
	require.Error(t, d.Err())
}

func TestDecodeMissingTags(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(uint16(2))...) /* num tags */

	d := newTestTagDecoder()
	d.Reset(wrapAsCheckedBytes(b))
	require.NoError(t, d.Err())

	require.False(t, d.Next())
	require.Error(t, d.Err())
}

func TestDecodeOwnershipFinalize(t *testing.T) {
	var b []byte
	b = append(b, headerMagicBytes...)
	b = append(b, encodeUInt16(uint16(2))...) /* num tags */

	wrappedBytes := wrapAsCheckedBytes(b)
	require.Equal(t, 0, wrappedBytes.NumRef())

	d := newTestTagDecoder()
	d.Reset(wrappedBytes)
	require.NoError(t, d.Err())
	require.NotEqual(t, 0, wrappedBytes.NumRef())

	require.False(t, d.Next())
	require.Error(t, d.Err())

	d.Close()
	require.Equal(t, 0, wrappedBytes.NumRef())
	wrappedBytes.IncRef()
	require.Nil(t, wrappedBytes.Bytes())
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

	d := newTestTagDecoder()
	d.Reset(wrapAsCheckedBytes(b))
	require.NoError(t, d.Err())

	require.True(t, d.Next())
	require.False(t, d.Next())
	require.Error(t, d.Err())
}

func TestDecodeDuplicateLifecycle(t *testing.T) {
	b := testTagDecoderBytes()
	d := newTestTagDecoder()
	d.Reset(b)
	require.NoError(t, d.Err())

	oldLen := d.Remaining()
	copy := d.Duplicate()
	require.Equal(t, oldLen, copy.Remaining())

	for copy.Next() {
		tag := copy.Current() // keep looping
		tag.Name.Bytes()      // ensure we can get values too
		tag.Value.Bytes()     // and don't panic
	}
	require.NoError(t, copy.Err())
	copy.Close()
	d.Close()
}

func TestDecodeDuplicateIteration(t *testing.T) {
	b := testTagDecoderBytes()
	d := newTestTagDecoder()
	d.Reset(b)
	require.NoError(t, d.Err())
	require.True(t, d.Next())

	oldLen := d.Remaining()
	copy := d.Duplicate()
	require.Equal(t, oldLen, copy.Remaining())

	for copy.Next() {
		tag := copy.Current() // keep looping
		tag.Name.Bytes()      // ensure we can get values too
		tag.Value.Bytes()     // and don't panic
	}
	require.NoError(t, copy.Err())
	copy.Close()

	dec := d.(*decoder)
	require.True(t, dec.checkedData.NumRef() >= 3, fmt.Sprintf("%d", dec.checkedData.NumRef()))
	require.NotPanics(t, func() {
		d.Close()
	})
}

func TestDecodeDuplicateLifecycleMocks(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	rawData := testTagDecoderBytesRaw()
	mockBytes := checked.NewMockBytes(ctrl)
	mockBytes.EXPECT().Bytes().Return(rawData).AnyTimes()

	mockBytes.EXPECT().IncRef()
	d := newTestTagDecoder()
	d.Reset(mockBytes)
	require.NoError(t, d.Err())

	mockBytes.EXPECT().IncRef().Times(2)
	require.True(t, d.Next())
	tag := d.Current()
	require.Equal(t, "abc", tag.Name.String())
	require.Equal(t, "defg", tag.Value.String())

	mockBytes.EXPECT().IncRef().Times(3)
	dupe := d.Duplicate()
	require.NoError(t, dupe.Err())

	mockBytes.EXPECT().DecRef().Times(2)
	mockBytes.EXPECT().IncRef().Times(2)
	require.True(t, d.Next())
	tag = d.Current()
	require.Equal(t, "x", tag.Name.String())
	require.Equal(t, "bar", tag.Value.String())

	mockBytes.EXPECT().DecRef().Times(2)
	require.False(t, d.Next())
	require.NoError(t, d.Err())

	mockBytes.EXPECT().DecRef()
	mockBytes.EXPECT().NumRef().Return(3)
	d.Close()

	mockBytes.EXPECT().DecRef().Times(2)
	mockBytes.EXPECT().IncRef().Times(2)
	require.True(t, dupe.Next())
	tag = dupe.Current()
	require.Equal(t, "x", tag.Name.String())
	require.Equal(t, "bar", tag.Value.String())

	mockBytes.EXPECT().DecRef().Times(2)
	require.False(t, dupe.Next())
	require.NoError(t, dupe.Err())

	mockBytes.EXPECT().DecRef()
	mockBytes.EXPECT().NumRef().Return(0)
	mockBytes.EXPECT().Finalize()
	dupe.Close()
}

func newTestTagDecoder() TagDecoder {
	return newTagDecoder(testDecodeOpts, nil)
}

func wrapAsCheckedBytes(b []byte) checked.Bytes {
	opts := checked.NewBytesOptions().SetFinalizer(
		checked.BytesFinalizerFn(func(b checked.Bytes) {
			b.IncRef()
			b.Reset(nil)
			b.DecRef()
		}))
	cb := checked.NewBytes(nil, opts)
	cb.IncRef()
	cb.Reset(b)
	cb.DecRef()
	return cb
}

func testTagDecoderBytesRaw() []byte {
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

func testTagDecoderBytes() checked.Bytes {
	return wrapAsCheckedBytes(testTagDecoderBytesRaw())
}
