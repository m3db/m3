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

package storage

import (
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/mocks"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func testDatabaseBlock(ctrl *gomock.Controller) (*dbBlock, *mocks.MockEncoder) {
	opts := testDatabaseOptions()
	encoder := mocks.NewMockEncoder(ctrl)
	b := NewDatabaseBlock(time.Now(), encoder, opts).(*dbBlock)
	return b, encoder
}

func testDatabaseSeriesBlocks() *databaseSeriesBlocks {
	opts := testDatabaseOptions()
	return NewDatabaseSeriesBlocks(opts).(*databaseSeriesBlocks)
}

func testDatabaseSeriesBlocksWithTimes(times []time.Time) *databaseSeriesBlocks {
	opts := testDatabaseOptions()
	blocks := testDatabaseSeriesBlocks()
	for _, timestamp := range times {
		block := opts.GetDatabaseBlockPool().Get()
		block.Reset(timestamp, nil)
		blocks.AddBlock(block)
	}
	return blocks
}

func validateBlocks(t *testing.T, blocks *databaseSeriesBlocks, minTime, maxTime time.Time, expectedTimes []time.Time) {
	require.Equal(t, minTime, blocks.GetMinTime())
	require.Equal(t, maxTime, blocks.GetMaxTime())
	allBlocks := blocks.elems
	require.Equal(t, len(expectedTimes), len(allBlocks))
	for _, timestamp := range expectedTimes {
		_, exists := allBlocks[timestamp]
		require.True(t, exists)
	}
}

func closeTestDatabaseBlock(t *testing.T, block *dbBlock) {
	finished := false
	block.ctx.RegisterCloser(func() { finished = true })
	block.Close()
	// waiting for the goroutine that closes context to finish
	for !finished {
		time.Sleep(100 * time.Millisecond)
	}
}

func TestDatabaseBlockWriteToClosedBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block, encoder := testDatabaseBlock(ctrl)
	encoder.EXPECT().Close()
	closeTestDatabaseBlock(t, block)
	err := block.Write(time.Now(), 1.0, xtime.Second, nil)
	require.Equal(t, errWriteToClosedBlock, err)
}

func TestDatabaseBlockWriteToSealedBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block, _ := testDatabaseBlock(ctrl)
	block.writable = false
	err := block.Write(time.Now(), 1.0, xtime.Second, nil)
	require.Equal(t, errWriteToSealedBlock, err)
}

func TestDatabaseBlockReadFromClosedBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block, encoder := testDatabaseBlock(ctrl)
	encoder.EXPECT().Close()
	closeTestDatabaseBlock(t, block)
	_, err := block.Stream(nil)
	require.Equal(t, errReadFromClosedBlock, err)
}

func TestDatabaseBlockReadFromSealedBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block, _ := testDatabaseBlock(ctrl)
	block.writable = false
	segment := m3db.Segment{Head: []byte{0x1, 0x2}, Tail: []byte{0x3, 0x4}}
	block.segment = segment
	r, err := block.Stream(nil)
	require.NoError(t, err)
	require.Equal(t, segment, r.Segment())
}

type testDatabaseBlockFn func(block *dbBlock)

type testDatabaseBlockExpectedFn func(encoder *mocks.MockEncoder)

type testDatabaseBlockAssertionFn func(t *testing.T, block *dbBlock)

func testDatabaseBlockWithDependentContext(
	t *testing.T,
	f testDatabaseBlockFn,
	ef testDatabaseBlockExpectedFn,
	af testDatabaseBlockAssertionFn,
) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block, encoder := testDatabaseBlock(ctrl)
	depCtx := block.opts.GetContextPool().Get()

	// register a dependent context here
	encoder.EXPECT().Stream().Return(nil)
	_, err := block.Stream(depCtx)
	require.NoError(t, err)

	finished := false
	block.ctx.RegisterCloser(func() { finished = true })
	f(block)

	// sleep a bit to let the goroutine run
	time.Sleep(200 * time.Millisecond)
	require.False(t, finished)

	ef(encoder)

	// now closing the dependent context
	depCtx.Close()
	for !finished {
		time.Sleep(200 * time.Millisecond)
	}

	af(t, block)
}

func TestDatabaseBlockResetNormalWithDependentContext(t *testing.T) {
	f := func(block *dbBlock) { block.Reset(time.Now(), nil) }
	ef := func(encoder *mocks.MockEncoder) { encoder.EXPECT().Close() }
	af := func(t *testing.T, block *dbBlock) { require.False(t, block.closed) }
	testDatabaseBlockWithDependentContext(t, f, ef, af)
}

func TestDatabaseBlockResetSealedWithDependentContext(t *testing.T) {
	f := func(block *dbBlock) { block.writable = false; block.Reset(time.Now(), nil) }
	ef := func(encoder *mocks.MockEncoder) {}
	af := func(t *testing.T, block *dbBlock) { require.False(t, block.closed) }
	testDatabaseBlockWithDependentContext(t, f, ef, af)
}

func TestDatabaseBlockCloseNormalWithDependentContext(t *testing.T) {
	f := func(block *dbBlock) { block.Close() }
	ef := func(encoder *mocks.MockEncoder) { encoder.EXPECT().Close() }
	af := func(t *testing.T, block *dbBlock) { require.True(t, block.closed) }
	testDatabaseBlockWithDependentContext(t, f, ef, af)
}

func TestDatabaseBlockCloseSealedWithDependentContext(t *testing.T) {
	f := func(block *dbBlock) { block.writable = false; block.Close() }
	ef := func(encoder *mocks.MockEncoder) {}
	af := func(t *testing.T, block *dbBlock) { require.True(t, block.closed) }
	testDatabaseBlockWithDependentContext(t, f, ef, af)
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
		_, exists := blocks.GetBlockAt(bt)
		require.True(t, exists)
	}
	_, exists := blocks.GetBlockAt(now.Add(time.Minute))
	require.False(t, exists)
}

func TestDatabaseSeriesBlocksGetBlockOrAdd(t *testing.T) {
	opts := testDatabaseOptions()
	blocks := testDatabaseSeriesBlocks()
	block := opts.GetDatabaseBlockPool().Get()
	now := time.Now()
	block.Reset(now, nil)
	blocks.AddBlock(block)
	res := blocks.GetBlockOrAdd(now)
	require.True(t, res == block)
	blockStart := now.Add(time.Hour)
	blocks.GetBlockOrAdd(blockStart)
	validateBlocks(t, blocks, now, blockStart, []time.Time{now, blockStart})
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
