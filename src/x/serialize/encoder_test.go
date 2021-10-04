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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
)

func newTestEncoderOpts() TagEncoderOptions {
	return NewTagEncoderOptions()
}

func newTestTagEncoder() TagEncoder {
	return newTagEncoder(defaultNewCheckedBytesFn, newTestEncoderOpts(), nil)
}

func TestEmptyEncode(t *testing.T) {
	e := newTestTagEncoder()
	require.NoError(t, e.Encode(ident.EmptyTagIterator))

	bc, ok := e.Data()
	require.True(t, ok)
	require.NotNil(t, bc)
	b := bc.Bytes()
	require.Len(t, b, 4)
	require.Equal(t, headerMagicBytes, b[:2])
	require.Equal(t, []byte{0x0, 0x0}, b[2:4])
}

func TestEncodeResetTwice(t *testing.T) {
	e := newTestTagEncoder()
	require.NoError(t, e.Encode(ident.EmptyTagIterator))
	e.Reset()
	e.Reset()
	require.NoError(t, e.Encode(ident.EmptyTagIterator))
}

func TestInuseEncode(t *testing.T) {
	e := newTestTagEncoder()
	require.NoError(t, e.Encode(ident.EmptyTagIterator))
	require.Error(t, e.Encode(ident.EmptyTagIterator))
}

func TestTagEncoderLeavesOriginalIterator(t *testing.T) {
	e := newTestTagEncoder()
	tags := ident.NewTagsIterator(ident.NewTags(
		ident.StringTag("abc", "defg"),
		ident.StringTag("x", "bar"),
	))

	require.Equal(t, 2, tags.Remaining())
	require.NoError(t, e.Encode(tags))
	require.Equal(t, 2, tags.Remaining())
}

func TestTagEncoderEmpty(t *testing.T) {
	e := newTestTagEncoder()
	tags := ident.MustNewTagStringsIterator("abc", "")
	require.NoError(t, e.Encode(tags))

	e = newTestTagEncoder()
	tags = ident.MustNewTagStringsIterator("", "abc")
	require.Error(t, e.Encode(tags))
}

func TestSimpleEncode(t *testing.T) {
	e := newTestTagEncoder()

	tags := ident.NewTagsIterator(ident.NewTags(
		ident.StringTag("abc", "defg"),
		ident.StringTag("x", "bar"),
	))
	require.NoError(t, e.Encode(tags))

	bc, ok := e.Data()
	require.True(t, ok)
	require.NotNil(t, bc)
	b := bc.Bytes()
	numExpectedBytes := 2 /* header */ + 2 /* num tags */ +
		2 /* abc length */ + len("abc") +
		2 /* defg length */ + len("defg") +
		2 /* x length */ + len("x") +
		2 /* bar length */ + len("bar")
	require.Len(t, b, numExpectedBytes)
	require.Equal(t, headerMagicBytes, b[:2])
	require.Equal(t, encodeUInt16(2, make([]byte, 2)), b[2:4])
	require.Equal(t, uint16(3), decodeUInt16(b[4:6])) /* len abc */
	require.Equal(t, "abc", string(b[6:9]))
	require.Equal(t, uint16(4), decodeUInt16(b[9:11])) /* len defg */
	require.Equal(t, "defg", string(b[11:15]))
	require.Equal(t, uint16(1), decodeUInt16(b[15:17])) /* len x */
	require.Equal(t, "x", string(b[17:18]))
	require.Equal(t, uint16(3), decodeUInt16(b[18:20])) /* len bar */
	require.Equal(t, "bar", string(b[20:23]))
}

func TestTagEncoderErrorEncoding(t *testing.T) {
	opts := NewTagEncoderOptions()
	e := newTagEncoder(defaultNewCheckedBytesFn, opts, nil)
	maxLiteralLen := opts.TagSerializationLimits().MaxTagLiteralLength()
	tags := ident.NewTagsIterator(ident.NewTags(
		ident.StringTag("abc", "defg"),
		ident.StringTag("x", nstring(1+int(maxLiteralLen))),
	))

	require.Error(t, e.Encode(tags))
	d, ok := e.Data()
	require.Nil(t, d)
	require.False(t, ok)

	e.Reset()
	tags = ident.NewTagsIterator(ident.NewTags(ident.StringTag("abc", "defg")))
	require.NoError(t, e.Encode(tags))
}

func TestLiteralIsTooLongLogging(t *testing.T) {
	observerCore, observedLogs := observer.New(zapcore.InfoLevel)
	instrumentOpts := instrument.NewOptions().SetLogger(zap.New(observerCore))
	opts := NewTagEncoderOptions().SetInstrumentOptions(instrumentOpts)
	e := newTagEncoder(defaultNewCheckedBytesFn, opts, nil)
	maxLiteralLen := opts.TagSerializationLimits().MaxTagLiteralLength()
	tags := ident.NewTagsIterator(ident.NewTags(
		ident.StringTag("abc", "defg"),
		ident.StringTag("x", nstring(1+int(maxLiteralLen))),
	))

	for i := 0; i < 2*maxLiteralIsTooLongLogCount; i++ {
		require.Error(t, e.Encode(tags))
		e.Reset()
	}

	filteredLogs := observedLogs.Filter(func(e observer.LoggedEntry) bool {
		return e.Message == errTagLiteralTooLong.Error()
	})

	require.Equal(t, maxLiteralIsTooLongLogCount, filteredLogs.Len())
	for _, e := range filteredLogs.All() {
		for _, c := range e.Context {
			if c.Key == "literal" {
				assert.Len(t, c.String, literalIsTooLongLogPrefixLength)
			}
		}
	}
}

func TestEmptyTagIterEncode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBytes := checked.NewMockBytes(ctrl)
	newBytesFn := func([]byte, checked.BytesOptions) checked.Bytes {
		return mockBytes
	}

	iter := ident.NewMockTagIterator(ctrl)

	mockBytes.EXPECT().IncRef()
	mockBytes.EXPECT().Reset(gomock.Any())
	gomock.InOrder(
		mockBytes.EXPECT().NumRef().Return(0),
		iter.EXPECT().Rewind(),
		iter.EXPECT().Remaining().Return(0),
		iter.EXPECT().Next().Return(false),
		iter.EXPECT().Err().Return(nil),
		iter.EXPECT().Rewind(),
	)

	enc := newTagEncoder(newBytesFn, newTestEncoderOpts(), nil)
	require.NoError(t, enc.Encode(iter))
}

func TestTooManyTags(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	iter := ident.NewMockTagIterator(ctrl)
	testOpts := newTestEncoderOpts()

	maxNumTags := testOpts.TagSerializationLimits().MaxNumberTags()
	gomock.InOrder(
		iter.EXPECT().Rewind(),
		iter.EXPECT().Remaining().Return(1+int(maxNumTags)),
		iter.EXPECT().Rewind(),
	)

	enc := newTagEncoder(defaultNewCheckedBytesFn, testOpts, nil)
	require.Error(t, enc.Encode(iter))
}

func TestSingleValueTagIterEncode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBytes := checked.NewMockBytes(ctrl)
	newBytesFn := func([]byte, checked.BytesOptions) checked.Bytes {
		return mockBytes
	}

	iter := ident.NewMockTagIterator(ctrl)

	mockBytes.EXPECT().IncRef()
	mockBytes.EXPECT().Reset(gomock.Any())
	gomock.InOrder(
		mockBytes.EXPECT().NumRef().Return(0),
		iter.EXPECT().Rewind(),
		iter.EXPECT().Remaining().Return(1),
		iter.EXPECT().Next().Return(true),
		iter.EXPECT().Current().Return(
			ident.StringTag("some", "tag"),
		),
		iter.EXPECT().Next().Return(false),
		iter.EXPECT().Err().Return(nil),
		iter.EXPECT().Rewind(),
	)

	enc := newTagEncoder(newBytesFn, newTestEncoderOpts(), nil)
	require.NoError(t, enc.Encode(iter))

	mockBytes.EXPECT().NumRef().Return(1)
	require.Error(t, enc.Encode(iter))

	mockBytes.EXPECT().NumRef().Return(1)
	mockBytes.EXPECT().Reset(nil)
	mockBytes.EXPECT().DecRef()
	enc.Reset()
}

func nstring(n int) string {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.WriteByte(byte('a'))
	}
	return buf.String()
}
