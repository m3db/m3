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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/resource"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func testDatabaseBlock(ctrl *gomock.Controller) *dbBlock {
	opts := NewOptions()
	b := NewDatabaseBlock(time.Now(), ts.Segment{}, opts).(*dbBlock)
	return b
}

func testDatabaseSeriesBlocks() *databaseSeriesBlocks {
	return NewDatabaseSeriesBlocks(0).(*databaseSeriesBlocks)
}

func testDatabaseSeriesBlocksWithTimes(times []time.Time) *databaseSeriesBlocks {
	opts := NewOptions()
	blocks := testDatabaseSeriesBlocks()
	for _, timestamp := range times {
		block := opts.DatabaseBlockPool().Get()
		block.Reset(timestamp, ts.Segment{})
		blocks.AddBlock(block)
	}
	return blocks
}

func validateBlocks(t *testing.T, blocks *databaseSeriesBlocks, minTime, maxTime time.Time, expectedTimes []time.Time) {
	require.True(t, minTime.Equal(blocks.MinTime()))
	require.True(t, maxTime.Equal(blocks.MaxTime()))
	allBlocks := blocks.elems
	require.Equal(t, len(expectedTimes), len(allBlocks))
	for _, timestamp := range expectedTimes {
		_, exists := allBlocks[xtime.ToUnixNano(timestamp)]
		require.True(t, exists)
	}
}

func closeTestDatabaseBlock(t *testing.T, block *dbBlock) {
	var finished uint32
	block.ctx = block.opts.ContextPool().Get()
	block.ctx.RegisterFinalizer(resource.FinalizerFn(func() { atomic.StoreUint32(&finished, 1) }))
	block.Close()
	// waiting for the goroutine that closes context to finish
	for atomic.LoadUint32(&finished) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
}

func TestDatabaseBlockReadFromClosedBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	block := testDatabaseBlock(ctrl)
	closeTestDatabaseBlock(t, block)
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

type testDatabaseBlockFn func(block *dbBlock)

type testDatabaseBlockAssertionFn func(t *testing.T, block *dbBlock)

func testDatabaseBlockWithDependentContext(
	t *testing.T,
	f testDatabaseBlockFn,
	af testDatabaseBlockAssertionFn,
) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block := testDatabaseBlock(ctrl)
	depCtx := block.opts.ContextPool().Get()

	// register a dependent context here
	_, err := block.Stream(depCtx)
	require.NoError(t, err)

	var finished uint32
	block.ctx.RegisterFinalizer(resource.FinalizerFn(func() {
		atomic.StoreUint32(&finished, 1)
	}))
	f(block)

	// sleep a bit to let the goroutine run
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, uint32(0), atomic.LoadUint32(&finished))

	// now closing the dependent context
	depCtx.Close()
	for atomic.LoadUint32(&finished) == 0 {
		time.Sleep(200 * time.Millisecond)
	}

	af(t, block)
}

func TestDatabaseBlockResetNormalWithDependentContext(t *testing.T) {
	f := func(block *dbBlock) { block.Reset(time.Now(), ts.Segment{}) }
	af := func(t *testing.T, block *dbBlock) { require.False(t, block.closed) }
	testDatabaseBlockWithDependentContext(t, f, af)
}

func TestDatabaseBlockCloseNormalWithDependentContext(t *testing.T) {
	f := func(block *dbBlock) { block.Close() }
	af := func(t *testing.T, block *dbBlock) { require.True(t, block.closed) }
	testDatabaseBlockWithDependentContext(t, f, af)
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
	curr := time.Now()
	data := []ts.Datapoint{
		ts.Datapoint{
			Timestamp: curr,
			Value:     0,
		},
		ts.Datapoint{
			Timestamp: curr.Add(time.Second),
			Value:     1,
		},
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
	encoder := m3tsz.NewEncoder(data[0].Timestamp, nil, true, encodingOpts)
	encoder.Encode(data[0], xtime.Second, nil)
	seg := encoder.Discard()
	block1 := NewDatabaseBlock(data[0].Timestamp, seg, blockOpts).(*dbBlock)

	encoder.Reset(data[1].Timestamp, 10)
	encoder.Encode(data[1], xtime.Second, nil)
	seg = encoder.Discard()
	block2 := NewDatabaseBlock(data[1].Timestamp, seg, blockOpts).(*dbBlock)

	// Lazily merge the two blocks
	block1.Merge(block2)

	// Try and read the data back and verify it looks good
	depCtx := block1.opts.ContextPool().Get()
	fmt.Println("streaming!")
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
	require.Equal(t, digest.SegmentChecksum(seg), mergedChecksum)

	// Make sure each segment reader was only finalized once
	require.Equal(t, 2, len(segmentReaders))
	depCtx.BlockingClose()
	for _, segmentReader := range segmentReaders {
		require.Equal(t, 1, *segmentReader.finalizeCount)
	}
}

func TestDatabaseBlockMergeErrorFromDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Setup
	var (
		curr      = time.Now()
		blockOpts = NewOptions()
	)

	// Create the two blocks we plan to merge
	block1 := NewDatabaseBlock(curr, ts.Segment{}, blockOpts).(*dbBlock)
	block2 := NewDatabaseBlock(curr, ts.Segment{}, blockOpts).(*dbBlock)

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
	curr := time.Now()
	data := []ts.Datapoint{
		ts.Datapoint{
			Timestamp: curr,
			Value:     0,
		},
		ts.Datapoint{
			Timestamp: curr.Add(time.Second),
			Value:     1,
		},
	}

	// Setup
	blockOpts := NewOptions()
	encodingOpts := encoding.NewOptions()

	// Create the two blocks we plan to merge
	encoder := m3tsz.NewEncoder(data[0].Timestamp, nil, true, encodingOpts)
	encoder.Encode(data[0], xtime.Second, nil)
	seg := encoder.Discard()
	block1 := NewDatabaseBlock(data[0].Timestamp, seg, blockOpts).(*dbBlock)

	encoder.Reset(data[1].Timestamp, 10)
	encoder.Encode(data[1], xtime.Second, nil)
	seg = encoder.Discard()
	block2 := NewDatabaseBlock(data[1].Timestamp, seg, blockOpts).(*dbBlock)

	// Keep track of the old checksum so we can make sure it changed
	oldChecksum, err := block1.Checksum()
	require.NoError(t, err)

	// Lazily merge the two blocks
	block1.Merge(block2)

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
	require.Equal(t, digest.SegmentChecksum(seg), mergedChecksum)
}

func TestDatabaseSeriesBlocksAddBlock(t *testing.T) {
	now := time.Now()
	blockTimes := []time.Time{now, now.Add(time.Second), now.Add(time.Minute), now.Add(-time.Second), now.Add(-time.Hour)}
	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes)
	validateBlocks(t, blocks, blockTimes[4], blockTimes[2], blockTimes)
}

func TestDatabaseSeriesBlocksAddSeries(t *testing.T) {
	now := time.Now()
	blockTimes := [][]time.Time{
		{now, now.Add(time.Second), now.Add(time.Minute), now.Add(-time.Second), now.Add(-time.Hour)},
		{now.Add(-time.Minute), now.Add(time.Hour)},
	}
	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes[0])
	other := testDatabaseSeriesBlocksWithTimes(blockTimes[1])
	blocks.AddSeries(other)
	var expectedTimes []time.Time
	for _, bt := range blockTimes {
		expectedTimes = append(expectedTimes, bt...)
	}
	validateBlocks(t, blocks, expectedTimes[4], expectedTimes[6], expectedTimes)
}

func TestDatabaseSeriesBlocksGetBlockAt(t *testing.T) {
	now := time.Now()
	blockTimes := []time.Time{now, now.Add(time.Second), now.Add(-time.Hour)}
	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes)
	for _, bt := range blockTimes {
		_, exists := blocks.BlockAt(bt)
		require.True(t, exists)
	}
	_, exists := blocks.BlockAt(now.Add(time.Minute))
	require.False(t, exists)
}

func TestDatabaseSeriesBlocksRemoveBlockAt(t *testing.T) {
	now := time.Now()
	blockTimes := []time.Time{now, now.Add(-time.Second), now.Add(time.Hour)}
	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes)
	blocks.RemoveBlockAt(now.Add(-time.Hour))
	validateBlocks(t, blocks, blockTimes[1], blockTimes[2], blockTimes)

	expected := []struct {
		min      time.Time
		max      time.Time
		allTimes []time.Time
	}{
		{blockTimes[1], blockTimes[2], blockTimes[1:]},
		{blockTimes[2], blockTimes[2], blockTimes[2:]},
		{timeZero, timeZero, []time.Time{}},
	}
	for i, bt := range blockTimes {
		blocks.RemoveBlockAt(bt)
		validateBlocks(t, blocks, expected[i].min, expected[i].max, expected[i].allTimes)
	}
}

func TestDatabaseSeriesBlocksRemoveAll(t *testing.T) {
	now := time.Now()
	blockTimes := []time.Time{now, now.Add(-time.Second), now.Add(time.Hour)}
	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes)
	require.Equal(t, len(blockTimes), len(blocks.AllBlocks()))

	blocks.RemoveAll()
	require.Equal(t, 0, len(blocks.AllBlocks()))
}

func TestDatabaseSeriesBlocksClose(t *testing.T) {
	now := time.Now()
	blockTimes := []time.Time{now, now.Add(-time.Second), now.Add(time.Hour)}
	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes)
	require.Equal(t, len(blockTimes), len(blocks.AllBlocks()))

	blocks.Close()
	require.Equal(t, 0, len(blocks.AllBlocks()))

	var nilPointer map[xtime.UnixNano]DatabaseBlock
	require.Equal(t, nilPointer, blocks.elems)
}

func TestDatabaseSeriesBlocksReset(t *testing.T) {
	now := time.Now()
	blockTimes := []time.Time{now, now.Add(-time.Second), now.Add(time.Hour)}
	blocks := testDatabaseSeriesBlocksWithTimes(blockTimes)
	require.Equal(t, len(blockTimes), len(blocks.AllBlocks()))

	blocks.Reset()

	require.Equal(t, 0, len(blocks.AllBlocks()))
	require.Equal(t, 0, len(blocks.elems))
	require.True(t, blocks.min.Equal(time.Time{}))
	require.True(t, blocks.max.Equal(time.Time{}))
}
