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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/ts"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func testDatabaseBlock(ctrl *gomock.Controller) *dbBlock {
	opts := NewOptions()
	b := NewDatabaseBlock(time.Now(), ts.Segment{}, opts).(*dbBlock)
	return b
}

func testDatabaseSeriesBlocks() *databaseSeriesBlocks {
	opts := NewOptions()
	return NewDatabaseSeriesBlocks(0, opts).(*databaseSeriesBlocks)
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
	require.Equal(t, minTime, blocks.MinTime())
	require.Equal(t, maxTime, blocks.MaxTime())
	allBlocks := blocks.elems
	require.Equal(t, len(expectedTimes), len(allBlocks))
	for _, timestamp := range expectedTimes {
		_, exists := allBlocks[timestamp]
		require.True(t, exists)
	}
}

func closeTestDatabaseBlock(t *testing.T, block *dbBlock) {
	var finished uint32
	block.ctx = block.opts.ContextPool().Get()
	block.ctx.RegisterFinalizer(context.FinalizerFn(func() { atomic.StoreUint32(&finished, 1) }))
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

	require.Equal(t, block.checksum, block.Checksum())
}

type testDatabaseBlockFn func(block *dbBlock)

type testDatabaseBlockExpectedFn func(encoder *encoding.MockEncoder)

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
	block.ctx.RegisterFinalizer(context.FinalizerFn(func() {
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
