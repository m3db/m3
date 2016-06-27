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

	"github.com/stretchr/testify/require"
)

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
