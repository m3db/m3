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

package block

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDatabaseBlock(ctrl *gomock.Controller) *dbBlock {
	opts := NewOptions()
	b := NewDatabaseBlock(xtime.Now(), 0, ts.Segment{}, opts, namespace.Context{}).(*dbBlock)
	return b
}

func testDatabaseSeriesBlocks() *databaseSeriesBlocks {
	return NewDatabaseSeriesBlocks(0).(*databaseSeriesBlocks)
}

func testDatabaseSeriesBlocksWithTimes(times []xtime.UnixNano, sizes []time.Duration) *databaseSeriesBlocks {
	opts := NewOptions()
	blocks := testDatabaseSeriesBlocks()
	for i := range times {
		block := opts.DatabaseBlockPool().Get()
		block.Reset(times[i], sizes[i], ts.Segment{}, namespace.Context{})
		blocks.AddBlock(block)
	}
	return blocks
}

func validateBlocks(t *testing.T, blocks *databaseSeriesBlocks, minTime,
	maxTime xtime.UnixNano, expectedTimes []xtime.UnixNano, expectedSizes []time.Duration) {
	require.Equal(t, minTime, blocks.MinTime())
	require.Equal(t, maxTime, blocks.MaxTime())
	allBlocks := blocks.elems
	require.Equal(t, len(expectedTimes), len(allBlocks))
	for i, timestamp := range expectedTimes {
		block, exists := allBlocks[timestamp]
		require.True(t, exists)
		assert.Equal(t, block.BlockSize(), expectedSizes[i])
	}
}

func TestDatabaseBlockReadFromClosedBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	block := testDatabaseBlock(ctrl)
	block.Close()
	_, err := block.Stream(ctx)
	require.Equal(t, errReadFromClosedBlock, err)
}

func TestDatabaseBlockChecksum(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block := testDatabaseBlock(ctrl)
	block.checksum = uint32(10)

	checksum, err := block.Checksum()
	require.NoError(t, err)
	require.Equal(t, block.checksum, checksum)
}

type segmentReaderFinalizeCounter struct {
	xio.SegmentReader
	// Use a pointer so we can update it from the Finalize method
	// which must not be a pointer receiver (in order to satisfy
	// the interface)
	finalizeCount *int
}

func (r segmentReaderFinalizeCounter) Finalize() {
	*r.finalizeCount++
}

// TestDatabaseBlockMerge lazily merges two blocks and verifies that the correct
// data is returned, as well as that the underlying streams are not double-finalized
// (regression test).
func TestDatabaseBlockMerge(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Test data
	curr := xtime.Now()
	data := []ts.Datapoint{
		ts.Datapoint{
			TimestampNanos: curr,
			Value:          0,
		},
		ts.Datapoint{
			TimestampNanos: curr.Add(time.Second),
			Value:          1,
		},
	}
	durations := []time.Duration{
		time.Minute,
		time.Hour,
	}

	// Mock segment reader pool so we count the number of Finalize() calls
	segmentReaders := []segmentReaderFinalizeCounter{}
	mockSegmentReaderPool := xio.NewMockSegmentReaderPool(ctrl)
	getCall := mockSegmentReaderPool.EXPECT().Get()
	getCall.DoAndReturn(func() xio.SegmentReader {
		val := 0
		reader := segmentReaderFinalizeCounter{
			xio.NewSegmentReader(ts.Segment{}),
			&val,
		}
		segmentReaders = append(segmentReaders, reader)
		return reader
	}).AnyTimes()

	// Setup
	blockOpts := NewOptions().SetSegmentReaderPool(mockSegmentReaderPool)
	encodingOpts := encoding.NewOptions()

	// Create the two blocks we plan to merge
	encoder := m3tsz.NewEncoder(data[0].TimestampNanos, nil, true, encodingOpts)
	encoder.Encode(data[0], xtime.Second, nil)
	seg := encoder.Discard()
	block1 := NewDatabaseBlock(data[0].TimestampNanos, durations[0], seg,
		blockOpts, namespace.Context{}).(*dbBlock)

	encoder.Reset(data[1].TimestampNanos, 10, nil)
	encoder.Encode(data[1], xtime.Second, nil)
	seg = encoder.Discard()
	block2 := NewDatabaseBlock(data[1].TimestampNanos, durations[1], seg,
		blockOpts, namespace.Context{}).(*dbBlock)

	// Lazily merge the two blocks
	block1.Merge(block2)

	// BlockSize should not change
	require.Equal(t, durations[0], block1.BlockSize())

	// Try and read the data back and verify it looks good
	depCtx := block1.opts.ContextPool().Get()
	stream, err := block1.Stream(depCtx)
	require.NoError(t, err)
	seg, err = stream.Segment()
	require.NoError(t, err)
	reader := xio.NewSegmentReader(seg)
	iter := m3tsz.NewReaderIterator(reader, true, encodingOpts)

	i := 0
	for iter.Next() {
		dp, _, _ := iter.Current()
		require.True(t, data[i].Equal(dp))
		i++
	}
	require.NoError(t, iter.Err())

	// Make sure the checksum was updated
	mergedChecksum, err := block1.Checksum()
	require.NoError(t, err)
	require.Equal(t, seg.CalculateChecksum(), mergedChecksum)

	// Make sure each segment reader was only finalized once
	require.Equal(t, 3, len(segmentReaders))
	depCtx.BlockingClose()
	block1.Close()
	block2.Close()
	for _, segmentReader := range segmentReaders {
		require.Equal(t, 1, *segmentReader.finalizeCount)
	}
}

// TestDatabaseBlockMergeRace is similar to TestDatabaseBlockMerge, except it
// tries to stream the data in multiple go-routines to ensure the merging isn't
// racy, this is a regression test for a known issue.
func TestDatabaseBlockMergeRace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		numRuns     = 1000
		numRoutines = 20
	)

	for i := 0; i < numRuns; i++ {
		func() {
			// Test data
			curr := xtime.Now()
			data := []ts.Datapoint{
				ts.Datapoint{
					TimestampNanos: curr,
					Value:          0,
				},
				ts.Datapoint{
					TimestampNanos: curr.Add(time.Second),
					Value:          1,
				},
			}
			durations := []time.Duration{
				time.Minute,
				time.Hour,
			}

			// Setup
			blockOpts := NewOptions()
			encodingOpts := encoding.NewOptions()

			// Create the two blocks we plan to merge
			encoder := m3tsz.NewEncoder(data[0].TimestampNanos, nil, true, encodingOpts)
			encoder.Encode(data[0], xtime.Second, nil)
			seg := encoder.Discard()
			block1 := NewDatabaseBlock(data[0].TimestampNanos, durations[0], seg, blockOpts, namespace.Context{}).(*dbBlock)

			encoder.Reset(data[1].TimestampNanos, 10, nil)
			encoder.Encode(data[1], xtime.Second, nil)
			seg = encoder.Discard()
			block2 := NewDatabaseBlock(data[1].TimestampNanos, durations[1], seg, blockOpts, namespace.Context{}).(*dbBlock)

			// Lazily merge the two blocks
			block1.Merge(block2)

			var wg sync.WaitGroup
			wg.Add(numRoutines)

			blockFn := func(block *dbBlock) {
				defer wg.Done()

				depCtx := block.opts.ContextPool().Get()
				var (
					// Make sure we shadow the top level variables
					// with the same name
					stream xio.BlockReader
					seg    ts.Segment
					err    error
				)
				stream, err = block.Stream(depCtx)
				block.Close()
				if err == errReadFromClosedBlock {
					return
				}
				require.NoError(t, err)

				seg, err = stream.Segment()
				require.NoError(t, err)
				reader := xio.NewSegmentReader(seg)
				iter := m3tsz.NewReaderIterator(reader, true, encodingOpts)

				i := 0
				for iter.Next() {
					dp, _, _ := iter.Current()
					require.True(t, data[i].Equal(dp))
					i++
				}
				require.NoError(t, iter.Err())
			}

			for i := 0; i < numRoutines; i++ {
				go blockFn(block1)
			}

			wg.Wait()
		}()
	}
}

// TestDatabaseBlockMergeChained is similar to TestDatabaseBlockMerge except
// we try chaining multiple merge calls onto the same block.
func TestDatabaseBlockMergeChained(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Test data
	curr := xtime.Now()
	data := []ts.Datapoint{
		ts.Datapoint{
			TimestampNanos: curr,
			Value:          0,
		},
		ts.Datapoint{
			TimestampNanos: curr.Add(time.Second),
			Value:          1,
		},
		ts.Datapoint{
			TimestampNanos: curr.Add(2 * time.Second),
			Value:          2,
		},
	}
	durations := []time.Duration{
		time.Second,
		time.Minute,
		time.Hour,
	}

	// Mock segment reader pool so we count the number of Finalize() calls
	segmentReaders := []segmentReaderFinalizeCounter{}
	mockSegmentReaderPool := xio.NewMockSegmentReaderPool(ctrl)
	getCall := mockSegmentReaderPool.EXPECT().Get()
	getCall.DoAndReturn(func() xio.SegmentReader {
		val := 0
		reader := segmentReaderFinalizeCounter{
			xio.NewSegmentReader(ts.Segment{}),
			&val,
		}
		segmentReaders = append(segmentReaders, reader)
		return reader
	}).AnyTimes()

	// Setup
	blockOpts := NewOptions().SetSegmentReaderPool(mockSegmentReaderPool)
	encodingOpts := encoding.NewOptions()

	// Create the two blocks we plan to merge
	encoder := m3tsz.NewEncoder(data[0].TimestampNanos, nil, true, encodingOpts)
	encoder.Encode(data[0], xtime.Second, nil)
	seg := encoder.Discard()
	block1 := NewDatabaseBlock(data[0].TimestampNanos, durations[0], seg, blockOpts, namespace.Context{}).(*dbBlock)

	encoder.Reset(data[1].TimestampNanos, 10, nil)
	encoder.Encode(data[1], xtime.Second, nil)
	seg = encoder.Discard()
	block2 := NewDatabaseBlock(data[1].TimestampNanos, durations[1], seg, blockOpts, namespace.Context{}).(*dbBlock)

	encoder.Reset(data[2].TimestampNanos, 10, nil)
	encoder.Encode(data[2], xtime.Second, nil)
	seg = encoder.Discard()
	block3 := NewDatabaseBlock(data[2].TimestampNanos, durations[2], seg, blockOpts, namespace.Context{}).(*dbBlock)

	// Lazily merge two blocks into block1
	block1.Merge(block2)
	block1.Merge(block3)

	// BlockSize should not change
	require.Equal(t, durations[0], block1.BlockSize())

	// Try and read the data back and verify it looks good
	depCtx := block1.opts.ContextPool().Get()
	stream, err := block1.Stream(depCtx)
	require.NoError(t, err)
	seg, err = stream.Segment()
	require.NoError(t, err)
	reader := xio.NewSegmentReader(seg)
	iter := m3tsz.NewReaderIterator(reader, true, encodingOpts)

	i := 0
	for iter.Next() {
		dp, _, _ := iter.Current()
		require.True(t, data[i].Equal(dp))
		i++
	}
	require.NoError(t, iter.Err())

	// Make sure the checksum was updated
	mergedChecksum, err := block1.Checksum()
	require.NoError(t, err)
	require.Equal(t, seg.CalculateChecksum(), mergedChecksum)

	// Make sure each segment reader was only finalized once
	require.Equal(t, 5, len(segmentReaders))
	depCtx.BlockingClose()
	block1.Close()
	block2.Close()
	for _, segmentReader := range segmentReaders {
		require.Equal(t, 1, *segmentReader.finalizeCount)
	}
}

func TestDatabaseBlockMergeErrorFromDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Setup
	var (
		curr      = xtime.Now()
		blockOpts = NewOptions()
	)

	// Create the two blocks we plan to merge
	block1 := NewDatabaseBlock(curr, 0, ts.Segment{}, blockOpts, namespace.Context{}).(*dbBlock)
	block2 := NewDatabaseBlock(curr, 0, ts.Segment{}, blockOpts, namespace.Context{}).(*dbBlock)

	// Mark only block 2 as retrieved from disk so we can make sure it checks
	// the block that is being merged, as well as the one that is being merged
	// into.
	block2.wasRetrievedFromDisk = true

	require.Equal(t, false, block1.wasRetrievedFromDisk)
	require.Equal(t, errTriedToMergeBlockFromDisk, block1.Merge(block2))
	require.Equal(t, errTriedToMergeBlockFromDisk, block2.Merge(block1))
}

// TestDatabaseBlockChecksumMergesAndRecalculates makes sure that the Checksum method
// will check if a lazy-merge is pending, and if so perform it and recalculate the checksum.
func TestDatabaseBlockChecksumMergesAndRecalculates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Test data
	curr := xtime.Now()
	data := []ts.Datapoint{
		ts.Datapoint{
			TimestampNanos: curr,
			Value:          0,
		},
		ts.Datapoint{
			TimestampNanos: curr.Add(time.Second),
			Value:          1,
		},
	}
	durations := []time.Duration{
		time.Minute,
		time.Hour,
	}

	// Setup
	blockOpts := NewOptions()
	encodingOpts := encoding.NewOptions()

	// Create the two blocks we plan to merge
	encoder := m3tsz.NewEncoder(data[0].TimestampNanos, nil, true, encodingOpts)
	encoder.Encode(data[0], xtime.Second, nil)
	seg := encoder.Discard()
	block1 := NewDatabaseBlock(data[0].TimestampNanos, durations[0], seg, blockOpts, namespace.Context{}).(*dbBlock)

	encoder.Reset(data[1].TimestampNanos, 10, nil)
	encoder.Encode(data[1], xtime.Second, nil)
	seg = encoder.Discard()
	block2 := NewDatabaseBlock(data[1].TimestampNanos, durations[1], seg, blockOpts, namespace.Context{}).(*dbBlock)

	// Keep track of the old checksum so we can make sure it changed
	oldChecksum, err := block1.Checksum()
	require.NoError(t, err)

	// Lazily merge the two blocks
	block1.Merge(block2)

	// BlockSize should not change
	require.Equal(t, durations[0], block1.BlockSize())

	// Make sure the checksum was updated
	newChecksum, err := block1.Checksum()
	require.NoError(t, err)
	require.NotEqual(t, oldChecksum, newChecksum)

	// Try and read the data back and verify it looks good
	depCtx := block1.opts.ContextPool().Get()
	stream, err := block1.Stream(depCtx)
	require.NoError(t, err)
	seg, err = stream.Segment()
	require.NoError(t, err)
	reader := xio.NewSegmentReader(seg)
	iter := m3tsz.NewReaderIterator(reader, true, encodingOpts)

	i := 0
	for iter.Next() {
		dp, _, _ := iter.Current()
		require.True(t, data[i].Equal(dp))
		i++
	}
	require.NoError(t, iter.Err())

	// Make sure the new checksum is correct
	mergedChecksum, err := block1.Checksum()
	require.NoError(t, err)
	require.Equal(t, seg.CalculateChecksum(), mergedChecksum)
}

func TestDatabaseBlockStreamMergePerformsCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Test data
	curr := xtime.Now()
	data := []ts.Datapoint{
		ts.Datapoint{
			TimestampNanos: curr,
			Value:          0,
		},
		ts.Datapoint{
			TimestampNanos: curr.Add(time.Second),
			Value:          1,
		},
	}
	durations := []time.Duration{
		time.Minute,
		time.Hour,
	}

	// Setup
	blockOpts := NewOptions()
	encodingOpts := encoding.NewOptions()

	// Create the two blocks we plan to merge
	encoder := m3tsz.NewEncoder(data[0].TimestampNanos, nil, true, encodingOpts)
	encoder.Encode(data[0], xtime.Second, nil)
	seg := encoder.Discard()
	block1 := NewDatabaseBlock(data[0].TimestampNanos, durations[0], seg, blockOpts, namespace.Context{}).(*dbBlock)

	encoder.Reset(data[1].TimestampNanos, 10, nil)
	encoder.Encode(data[1], xtime.Second, nil)
	seg = encoder.Discard()
	block2 := NewDatabaseBlock(data[1].TimestampNanos, durations[1], seg, blockOpts, namespace.Context{}).(*dbBlock)

	err := block1.Merge(block2)
	require.NoError(t, err)

	depCtx := block1.opts.ContextPool().Get()
	stream, err := block1.Stream(depCtx)
	require.NoError(t, err)
	block1.Close()

	seg, err = stream.Segment()
	require.NoError(t, err)
	reader := xio.NewSegmentReader(seg)
	iter := m3tsz.NewReaderIterator(reader, true, encodingOpts)

	i := 0
	for iter.Next() {
		dp, _, _ := iter.Current()
		require.True(t, data[i].Equal(dp))
		i++
	}
	require.NoError(t, iter.Err())
}

func TestDatabaseBlockCloseIfFromDisk(t *testing.T) {
	var (
		blockOpts        = NewOptions()
		blockNotFromDisk = NewDatabaseBlock(0, time.Hour, ts.Segment{}, blockOpts, namespace.Context{}).(*dbBlock)
		blockFromDisk    = NewDatabaseBlock(0, time.Hour, ts.Segment{}, blockOpts, namespace.Context{}).(*dbBlock)
	)
	blockFromDisk.wasRetrievedFromDisk = true

	require.False(t, blockNotFromDisk.CloseIfFromDisk())
	require.True(t, blockFromDisk.CloseIfFromDisk())
}

func TestDatabaseSeriesBlocksAddBlock(t *testing.T) {
	now := xtime.Now()
	blockTimes := []xtime.UnixNano{now, now.Add(time.Second),
		now.Add(time.Minute), now.Add(-time.Second), now.Add(-time.Hour)}
	blockSizes := []time.Duration{time.Minute, time.Hour, time.Second,
		time.Microsecond, time.Millisecond}
	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes, blockSizes)
	validateBlocks(t, blocks, blockTimes[4], blockTimes[2], blockTimes, blockSizes)
}

func TestDatabaseSeriesBlocksAddSeries(t *testing.T) {
	now := xtime.Now()
	blockTimes := [][]xtime.UnixNano{
		{now, now.Add(time.Second), now.Add(time.Minute), now.Add(-time.Second), now.Add(-time.Hour)},
		{now.Add(-time.Minute), now.Add(time.Hour)},
	}
	blockSizes := [][]time.Duration{
		{time.Minute, time.Hour, time.Second, time.Microsecond, time.Millisecond},
		{time.Minute * 2, time.Hour * 21},
	}
	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes[0], blockSizes[0])
	other := testDatabaseSeriesBlocksWithTimes(blockTimes[1], blockSizes[1])
	blocks.AddSeries(other)
	var expectedTimes []xtime.UnixNano
	for _, bt := range blockTimes {
		expectedTimes = append(expectedTimes, bt...)
	}
	var expectedSizes []time.Duration
	for _, bt := range blockSizes {
		expectedSizes = append(expectedSizes, bt...)
	}

	validateBlocks(t, blocks, expectedTimes[4], expectedTimes[6], expectedTimes, expectedSizes)
}

func TestDatabaseSeriesBlocksGetBlockAt(t *testing.T) {
	now := xtime.Now()
	blockTimes := []xtime.UnixNano{now, now.Add(time.Second), now.Add(-time.Hour)}
	blockSizes := []time.Duration{time.Minute, time.Hour, time.Second}

	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes, blockSizes)
	for i, bt := range blockTimes {
		b, exists := blocks.BlockAt(bt)
		require.True(t, exists)
		require.Equal(t, b.BlockSize(), blockSizes[i])
	}
	_, exists := blocks.BlockAt(now.Add(time.Minute))
	require.False(t, exists)
}

func TestDatabaseSeriesBlocksRemoveBlockAt(t *testing.T) {
	now := xtime.Now()
	blockTimes := []xtime.UnixNano{now, now.Add(-time.Second), now.Add(time.Hour)}
	blockSizes := []time.Duration{time.Minute, time.Hour, time.Second}

	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes, blockSizes)
	blocks.RemoveBlockAt(now.Add(-time.Hour))
	validateBlocks(t, blocks, blockTimes[1], blockTimes[2], blockTimes, blockSizes)

	expected := []struct {
		min      xtime.UnixNano
		max      xtime.UnixNano
		allTimes []xtime.UnixNano
	}{
		{blockTimes[1], blockTimes[2], blockTimes[1:]},
		{blockTimes[2], blockTimes[2], blockTimes[2:]},
		{timeZero, timeZero, []xtime.UnixNano{}},
	}
	for i, bt := range blockTimes {
		blocks.RemoveBlockAt(bt)
		blockSizes = blockSizes[1:]
		validateBlocks(t, blocks, expected[i].min, expected[i].max, expected[i].allTimes, blockSizes)
	}
}

func TestDatabaseSeriesBlocksRemoveAll(t *testing.T) {
	now := xtime.Now()
	blockTimes := []xtime.UnixNano{now, now.Add(-time.Second), now.Add(time.Hour)}
	blockSizes := []time.Duration{time.Minute, time.Hour, time.Second}

	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes, blockSizes)
	require.Equal(t, len(blockTimes), len(blocks.AllBlocks()))

	blocks.RemoveAll()
	require.Equal(t, 0, len(blocks.AllBlocks()))
}

func TestDatabaseSeriesBlocksClose(t *testing.T) {
	now := xtime.Now()
	blockTimes := []xtime.UnixNano{now, now.Add(-time.Second), now.Add(time.Hour)}
	blockSizes := []time.Duration{time.Minute, time.Hour, time.Second}

	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes, blockSizes)
	require.Equal(t, len(blockTimes), len(blocks.AllBlocks()))

	blocks.Close()
	require.Equal(t, 0, len(blocks.AllBlocks()))

	var nilPointer map[xtime.UnixNano]DatabaseBlock
	require.Equal(t, nilPointer, blocks.elems)
}

func TestDatabaseSeriesBlocksReset(t *testing.T) {
	now := xtime.Now()
	blockTimes := []xtime.UnixNano{now, now.Add(-time.Second), now.Add(time.Hour)}
	blockSizes := []time.Duration{time.Minute, time.Hour, time.Second}

	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes, blockSizes)
	require.Equal(t, len(blockTimes), len(blocks.AllBlocks()))

	blocks.Reset()

	require.Equal(t, 0, len(blocks.AllBlocks()))
	require.Equal(t, 0, len(blocks.elems))
	require.True(t, blocks.min.Equal(0))
	require.True(t, blocks.max.Equal(0))
}

func TestBlockResetFromDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := testDatabaseBlock(ctrl)
	now := xtime.Now()
	blockSize := 2 * time.Hour
	id := ident.StringID("testID")
	segment := ts.Segment{}
	bl.ResetFromDisk(now, blockSize, segment, id, namespace.Context{})

	assert.True(t, now.Equal(bl.StartTime()))
	assert.Equal(t, blockSize, bl.BlockSize())
	assert.Equal(t, segment, bl.segment)
	assert.Equal(t, id, bl.seriesID)
	assert.True(t, bl.WasRetrievedFromDisk())
}
