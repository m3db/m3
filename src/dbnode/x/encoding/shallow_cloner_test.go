//nolint:lll
// Copyright (c) 2024 Uber Technologies, Inc.
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
	"fmt"
	"testing"
	"time"

	enc "github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/serialize"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestCloneMultiReaderIterator(t *testing.T) {
	f := newFixture(t)
	defer f.ctrl.Finish()

	f.shallowCloner.cloneBlockReaderFn = func(b xio.BlockReader, _ xtime.UnixNano, _ time.Duration) (xio.BlockReader, error) {
		return b, nil
	}

	testBlockSize := time.Hour
	now := time.Now()
	testTimeFn := func(i int) xtime.UnixNano { return xtime.ToUnixNano(now.Add(time.Duration(i) * testBlockSize)) }
	testBlock := func(start int, data int) xio.BlockReader {
		rawHead := []byte(fmt.Sprintf("head-%d", data))
		rawTail := []byte(fmt.Sprintf("tail-%d", data))
		head := checked.NewBytes(rawHead, nil)
		tail := checked.NewBytes(rawTail, nil)
		seg := ts.NewSegment(head, tail, 0, ts.FinalizeNone)
		return xio.BlockReader{
			SegmentReader: xio.NewSegmentReader(seg),
			BlockSize:     testBlockSize,
			Start:         testTimeFn(start),
		}
	}

	testBlocks := [][]xio.BlockReader{
		{testBlock(0, 0)},
		{testBlock(1, 0), testBlock(1, 1)},
		{testBlock(2, 0), testBlock(2, 1), testBlock(2, 2)},
	}

	mockIter := enc.NewMockMultiReaderIterator(f.ctrl)
	mockReaders := xio.NewMockReaderSliceOfSlicesIterator(f.ctrl)
	gomock.InOrder(
		mockIter.EXPECT().Readers().Return(mockReaders),
		mockReaders.EXPECT().Index().Return(0),
		mockReaders.EXPECT().CurrentReaders().Return(1, testTimeFn(0), testBlockSize),
		mockReaders.EXPECT().CurrentReaderAt(0).Return(testBlocks[0][0]),
		mockReaders.EXPECT().Next().Return(true),
		mockReaders.EXPECT().CurrentReaders().Return(2, testTimeFn(1), testBlockSize),
		mockReaders.EXPECT().CurrentReaderAt(0).Return(testBlocks[1][0]),
		mockReaders.EXPECT().CurrentReaderAt(1).Return(testBlocks[1][1]),
		mockReaders.EXPECT().Next().Return(true),
		mockReaders.EXPECT().CurrentReaders().Return(3, testTimeFn(2), testBlockSize),
		mockReaders.EXPECT().CurrentReaderAt(0).Return(testBlocks[2][0]),
		mockReaders.EXPECT().CurrentReaderAt(1).Return(testBlocks[2][1]),
		mockReaders.EXPECT().CurrentReaderAt(2).Return(testBlocks[2][2]),
		mockReaders.EXPECT().Next().Return(false),
		mockReaders.EXPECT().RewindToIndex(0),
		f.pools.multiReaderIterator.EXPECT().Get().Return(mockIter),
		mockIter.EXPECT().Schema().Return(nil),
		mockIter.EXPECT().ResetSliceOfSlices(gomock.Any(), gomock.Any()).DoAndReturn(
			func(readers xio.ReaderSliceOfSlicesIterator, _ namespace.SchemaDescr) {
				require.True(t, readers.Next())
				l, s, b := readers.CurrentReaders()
				require.Equal(t, 1, l)
				require.Equal(t, testTimeFn(0), s)
				require.Equal(t, testBlockSize, b)
				assertBlocksAreEqualAndPointToSameData(t, testBlocks[0][0], readers.CurrentReaderAt(0))

				require.True(t, readers.Next())
				l, s, b = readers.CurrentReaders()
				require.Equal(t, 2, l)
				require.Equal(t, testTimeFn(1), s)
				require.Equal(t, testBlockSize, b)
				assertBlocksAreEqualAndPointToSameData(t, testBlocks[1][0], readers.CurrentReaderAt(0))
				assertBlocksAreEqualAndPointToSameData(t, testBlocks[1][1], readers.CurrentReaderAt(1))

				require.True(t, readers.Next())
				l, s, b = readers.CurrentReaders()
				require.Equal(t, 3, l)
				require.Equal(t, testTimeFn(2), s)
				require.Equal(t, testBlockSize, b)
				assertBlocksAreEqualAndPointToSameData(t, testBlocks[2][0], readers.CurrentReaderAt(0))
				assertBlocksAreEqualAndPointToSameData(t, testBlocks[2][1], readers.CurrentReaderAt(1))
				assertBlocksAreEqualAndPointToSameData(t, testBlocks[2][2], readers.CurrentReaderAt(2))
				require.False(t, readers.Next())
			},
		),
	)

	_, err := f.shallowCloner.cloneMultiReaderIterator(mockIter)
	require.NoError(t, err)
}

func TestCloneBlockReader(t *testing.T) {
	f := newFixture(t)
	defer f.ctrl.Finish()

	testStart := xtime.ToUnixNano(time.Now())
	testBlockSize := time.Hour

	rawHead := []byte("stub-head")
	rawTail := []byte("stub-tail")
	head := checked.NewBytes(rawHead, nil)
	head.IncRef()
	tail := checked.NewBytes(rawTail, nil)
	tail.IncRef()

	mockSegment := ts.Segment{
		Head:  head,
		Tail:  tail,
		Flags: ts.FinalizeNone,
	}
	mockSegmentReader := xio.NewSegmentReader(mockSegment)

	testReader := xio.BlockReader{
		SegmentReader: mockSegmentReader,
		Start:         testStart,
		BlockSize:     time.Hour,
	}

	gomock.InOrder(
		f.pools.checkedBytesWrapper.EXPECT().Get(rawHead).DoAndReturn(func(b []byte) checked.Bytes {
			require.True(t, xtest.ByteSlicesBackedBySameData(rawHead, b))
			return checked.NewBytes(b, nil)
		}),
		f.pools.checkedBytesWrapper.EXPECT().Get(rawTail).DoAndReturn(func(b []byte) checked.Bytes {
			require.True(t, xtest.ByteSlicesBackedBySameData(rawTail, b))
			return checked.NewBytes(b, nil)
		}),
	)
	clonedReader, err := f.shallowCloner.cloneBlockReader(testReader, testStart, testBlockSize)
	require.NoError(t, err)

	assertBlocksAreEqualAndPointToSameData(t, testReader, clonedReader)
}

func assertBlocksAreEqualAndPointToSameData(t *testing.T, exp xio.BlockReader, obs xio.BlockReader) {
	require.Equal(t, exp.BlockSize, obs.BlockSize)
	require.Equal(t, exp.Start, obs.Start)

	expSeg, err := exp.Segment()
	require.NoError(t, err)

	obsSeg, err := obs.Segment()
	require.NoError(t, err)
	require.Equal(t, ts.FinalizeNone, obsSeg.Flags)

	require.True(t, expSeg.Equal(&obsSeg))
	require.True(t, xtest.ByteSlicesBackedBySameData(expSeg.Head.Bytes(), obsSeg.Head.Bytes()))
	require.True(t, xtest.ByteSlicesBackedBySameData(expSeg.Tail.Bytes(), obsSeg.Tail.Bytes()))
}

func newFixture(t *testing.T) fixture {
	ctrl := gomock.NewController(t)
	pools := &testPools{
		multiReaderIteratorArray: enc.NewMockMultiReaderIteratorArrayPool(ctrl),
		multiReaderIterator:      enc.NewMockMultiReaderIteratorPool(ctrl),
		seriesIterator:           enc.NewMockSeriesIteratorPool(ctrl),
		checkedBytesWrapper:      xpool.NewMockCheckedBytesWrapperPool(ctrl),
		id:                       ident.NewMockPool(ctrl),
	}
	cloner := shallowCloner{pools: pools}
	return fixture{
		t:             t,
		ctrl:          ctrl,
		pools:         pools,
		shallowCloner: cloner,
	}
}

type fixture struct {
	t             *testing.T
	ctrl          *gomock.Controller
	pools         *testPools
	shallowCloner shallowCloner
}

type testPools struct {
	multiReaderIteratorArray *enc.MockMultiReaderIteratorArrayPool
	multiReaderIterator      *enc.MockMultiReaderIteratorPool
	seriesIterator           *enc.MockSeriesIteratorPool
	checkedBytesWrapper      *xpool.MockCheckedBytesWrapperPool
	id                       *ident.MockPool
}

var _ enc.IteratorPools = (*testPools)(nil)

func (t *testPools) TagDecoder() serialize.TagDecoderPool               { panic("unimplemented") }
func (t *testPools) TagEncoder() serialize.TagEncoderPool               { panic("unimplemented") }
func (t *testPools) CheckedBytesWrapper() xpool.CheckedBytesWrapperPool { return t.checkedBytesWrapper }
func (t *testPools) ID() ident.Pool                                     { return t.id }
func (t *testPools) MultiReaderIterator() enc.MultiReaderIteratorPool   { return t.multiReaderIterator }
func (t *testPools) SeriesIterator() enc.SeriesIteratorPool             { return t.seriesIterator }
func (t *testPools) MultiReaderIteratorArray() enc.MultiReaderIteratorArrayPool {
	return t.multiReaderIteratorArray
}
