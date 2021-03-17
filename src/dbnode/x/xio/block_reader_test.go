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

package xio

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	start     = time.Now().Truncate(time.Minute)
	blockSize = time.Minute
	errTest   = fmt.Errorf("err")
)

func buildBlock(t *testing.T) (BlockReader, *MockSegmentReader) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	reader := NewMockSegmentReader(ctrl)
	return BlockReader{reader, start, blockSize}, reader
}

func TestCloneBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	seg := ts.Segment{}

	reader := NewMockSegmentReader(ctrl)
	reader.EXPECT().Read64().Return(uint64(0), byte(0), errTest).Times(1)
	reader.EXPECT().Read64().Return(uint64(123456), byte(10), nil).Times(1)
	reader.EXPECT().Reset(seg).Return().Times(1)

	clonedReader := NewMockSegmentReader(ctrl)
	clonedReader.EXPECT().Read64().Return(uint64(1337), byte(2), nil).Times(1)

	reader.EXPECT().Clone(nil).Return(clonedReader, nil).Times(1)

	b := BlockReader{
		SegmentReader: reader,
		Start:         start,
		BlockSize:     blockSize,
	}

	word, n, err := b.Read64()
	require.Equal(t, uint64(0), word)
	require.Equal(t, byte(0), n)
	require.Equal(t, errTest, err)

	word, n, err = b.Read64()
	require.Equal(t, uint64(123456), word)
	require.Equal(t, byte(10), n)
	require.NoError(t, err)

	b2, err := b.CloneBlock(nil)
	require.NoError(t, err)

	startReset := start.Add(time.Hour)
	blockSizeReset := time.Hour * 5

	b.ResetWindowed(seg, startReset, blockSizeReset)
	require.Equal(t, b.Start, startReset)
	require.Equal(t, b.BlockSize, blockSizeReset)

	require.Equal(t, b2.Start, start)
	require.Equal(t, b2.BlockSize, blockSize)

	word, n, err = b2.Read64()

	require.Equal(t, uint64(1337), word)
	require.Equal(t, byte(2), n)
	require.NoError(t, err)
}

func TestBlockReaderStartEnd(t *testing.T) {
	br, _ := buildBlock(t)
	assert.Equal(t, br.Start, start)
	assert.Equal(t, br.BlockSize, blockSize)
}

func TestBlockReaderClone(t *testing.T) {
	br, sr := buildBlock(t)
	sr.EXPECT().Clone(nil).Return(nil, errTest).Times(1)
	r, err := br.Clone(nil)
	require.Nil(t, r)
	require.Equal(t, err, errTest)

	sr.EXPECT().Clone(nil).Return(sr, nil).Times(1)
	r, err = br.Clone(nil)
	require.NoError(t, err)

	require.Equal(t, r, sr)
	require.Equal(t, br.Start, start)
	require.Equal(t, br.BlockSize, blockSize)
}

func TestBlockReaderRead(t *testing.T) {
	br, sr := buildBlock(t)

	sr.EXPECT().Read64().Return(uint64(0), byte(0), errTest).Times(1)
	word, n, err := br.Read64()
	require.Equal(t, uint64(0), word)
	require.Equal(t, byte(0), n)
	require.Equal(t, err, errTest)

	sr.EXPECT().Read64().Return(uint64(100), byte(1), nil).Times(1)
	word, n, err = br.Read64()
	require.Equal(t, uint64(100), word)
	require.Equal(t, byte(1), n)
	require.NoError(t, err)
}

func TestBlockReaderFinalize(t *testing.T) {
	br, sr := buildBlock(t)
	sr.EXPECT().Finalize().Times(1)
	br.Finalize()
}

func TestBlockReaderSegment(t *testing.T) {
	br, sr := buildBlock(t)
	segment := ts.Segment{}
	sr.EXPECT().Segment().Return(segment, errTest).Times(1)
	_, err := br.Segment()
	require.Equal(t, err, errTest)

	sr.EXPECT().Segment().Return(segment, nil).Times(1)
	seg, err := br.Segment()
	require.Equal(t, seg, segment)
	require.NoError(t, err)
}

func TestBlockReaderReset(t *testing.T) {
	br, sr := buildBlock(t)
	segment := ts.Segment{}
	sr.EXPECT().Reset(segment).Times(1)
	br.Reset(segment)
}

func TestBlockReaderResetWindowed(t *testing.T) {
	br, sr := buildBlock(t)
	segment := ts.Segment{}
	sr.EXPECT().Reset(segment).Times(1)
	startReset := start.Add(time.Hour)
	blockSizeReset := time.Hour * 5
	br.ResetWindowed(segment, startReset, blockSizeReset)
	require.Equal(t, br.Start, startReset)
	require.Equal(t, br.BlockSize, blockSizeReset)
}

func TestBlockIsEmpty(t *testing.T) {
	assert.True(t, EmptyBlockReader.IsEmpty())
	assert.True(t, BlockReader{}.IsEmpty())

	assert.False(t, BlockReader{
		Start: start,
	}.IsEmpty())
	assert.False(t, BlockReader{
		BlockSize: blockSize,
	}.IsEmpty())

	block, reader := buildBlock(t)
	assert.False(t, BlockReader{
		SegmentReader: reader,
	}.IsEmpty())
	assert.False(t, block.IsEmpty())
}

func TestBlockIsNotEmpty(t *testing.T) {
	assert.False(t, EmptyBlockReader.IsNotEmpty())
	assert.False(t, BlockReader{}.IsNotEmpty())

	assert.True(t, BlockReader{
		Start: start,
	}.IsNotEmpty())
	assert.True(t, BlockReader{
		BlockSize: blockSize,
	}.IsNotEmpty())

	block, reader := buildBlock(t)
	assert.True(t, BlockReader{
		SegmentReader: reader,
	}.IsNotEmpty())
	assert.True(t, block.IsNotEmpty())
}

func TestFilterEmptyBlockReadersSliceOfSlicesInPlace(t *testing.T) {
	var (
		head          = checked.NewBytes([]byte("some-data"), checked.NewBytesOptions())
		segment       = ts.NewSegment(head, nil, 0, 0)
		segmentReader = NewSegmentReader(segment)
	)
	notEmpty := BlockReader{
		SegmentReader: segmentReader,
	}

	noneEmpty := []BlockReader{notEmpty}
	someEmpty := []BlockReader{notEmpty, EmptyBlockReader}
	allEmpty := []BlockReader{EmptyBlockReader}
	unfiltered := [][]BlockReader{noneEmpty, someEmpty, allEmpty}
	filtered, err := FilterEmptyBlockReadersSliceOfSlicesInPlace(unfiltered)
	require.NoError(t, err)
	require.Equal(t, 2, len(filtered))
	require.Equal(t, 1, len(filtered[0]))
	require.Equal(t, 1, len(filtered[1]))
}

func TestFilterEmptyBlockReadersInPlace(t *testing.T) {
	var (
		head          = checked.NewBytes([]byte("some-data"), checked.NewBytesOptions())
		segment       = ts.NewSegment(head, nil, 0, 0)
		segmentReader = NewSegmentReader(segment)
	)
	notEmpty := BlockReader{
		SegmentReader: segmentReader,
	}

	unfiltered := []BlockReader{notEmpty, EmptyBlockReader}
	filtered, err := FilterEmptyBlockReadersInPlace(unfiltered)
	require.NoError(t, err)
	require.Equal(t, 1, len(filtered))
}
