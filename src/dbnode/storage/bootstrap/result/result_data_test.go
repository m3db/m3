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

package result

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testBlockSize = 2 * time.Hour

func testResultOptions() Options {
	return NewOptions()
}

func TestDataResultSetUnfulfilledMergeShardResults(t *testing.T) {
	start := xtime.Now().Truncate(testBlockSize)
	rangeOne := shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(8 * testBlockSize),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(testBlockSize),
		}),
	}

	rangeTwo := shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start.Add(6 * testBlockSize),
			End:   start.Add(10 * testBlockSize),
		}),
		2: xtime.NewRanges(xtime.Range{
			Start: start.Add(testBlockSize),
			End:   start.Add(2 * testBlockSize),
		}),
	}

	r := NewDataBootstrapResult()
	r.SetUnfulfilled(rangeOne)
	rTwo := NewDataBootstrapResult()
	rTwo.SetUnfulfilled(rangeTwo)

	rMerged := MergedDataBootstrapResult(nil, nil)
	assert.Nil(t, rMerged)

	rMerged = MergedDataBootstrapResult(r, nil)
	assert.True(t, rMerged.Unfulfilled().Equal(rangeOne))

	rMerged = MergedDataBootstrapResult(nil, r)
	assert.True(t, rMerged.Unfulfilled().Equal(rangeOne))

	rMerged = MergedDataBootstrapResult(r, rTwo)
	expected := shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(10 * testBlockSize),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(testBlockSize),
		}),
		2: xtime.NewRanges(xtime.Range{
			Start: start.Add(testBlockSize),
			End:   start.Add(testBlockSize * 2),
		}),
	}

	assert.True(t, rMerged.Unfulfilled().Equal(expected))
}

func TestDataResultSetUnfulfilledOverwitesUnfulfilled(t *testing.T) {
	start := xtime.Now().Truncate(testBlockSize)
	r := NewDataBootstrapResult()
	r.SetUnfulfilled(shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(8 * testBlockSize),
		}),
	})

	expected := shardTimeRanges{0: xtime.NewRanges(xtime.Range{
		Start: start,
		End:   start.Add(8 * testBlockSize),
	})}

	assert.True(t, r.Unfulfilled().Equal(expected))
	r.SetUnfulfilled(shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start.Add(6 * testBlockSize),
			End:   start.Add(10 * testBlockSize),
		}),
	})

	expected = shardTimeRanges{0: xtime.NewRanges(xtime.Range{
		Start: start.Add(6 * testBlockSize),
		End:   start.Add(10 * testBlockSize),
	})}

	assert.True(t, r.Unfulfilled().Equal(expected))
}

func TestResultSetUnfulfilled(t *testing.T) {
	start := xtime.Now().Truncate(testBlockSize)

	r := NewDataBootstrapResult()
	r.SetUnfulfilled(shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
	})
	r.SetUnfulfilled(shardTimeRanges{
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
	})

	assert.True(t, r.Unfulfilled().Equal(shardTimeRanges{
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
	}))
}

func TestShardResultIsEmpty(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(opts)
	require.True(t, sr.IsEmpty())
	block := opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	block.Reset(xtime.Now(), time.Hour, ts.Segment{}, namespace.Context{})
	fooTags := ident.NewTags(ident.StringTag("foo", "foe"))
	sr.AddBlock(ident.StringID("foo"), fooTags, block)
	require.False(t, sr.IsEmpty())
}

func TestShardResultAddBlock(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(opts)
	start := xtime.Now()
	inputs := []struct {
		id        string
		tags      ident.Tags
		timestamp xtime.UnixNano
	}{
		{"foo", ident.NewTags(ident.StringTag("foo", "foe")), start},
		{"foo", ident.NewTags(ident.StringTag("foo", "foe")), start.Add(2 * time.Hour)},
		{"bar", ident.NewTags(ident.StringTag("bar", "baz")), start},
	}
	for _, input := range inputs {
		block := opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
		block.Reset(input.timestamp, time.Hour, ts.Segment{}, namespace.Context{})
		sr.AddBlock(ident.StringID(input.id), input.tags, block)
	}
	allSeries := sr.AllSeries()
	require.Equal(t, 2, allSeries.Len())
	fooBlocks, ok := allSeries.Get(ident.StringID("foo"))
	require.True(t, ok)
	require.Equal(t, 2, fooBlocks.Blocks.Len())
	barBlocks, ok := allSeries.Get(ident.StringID("bar"))
	require.True(t, ok)
	require.Equal(t, 1, barBlocks.Blocks.Len())
}

func TestShardResultAddSeries(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(opts)
	start := xtime.Now()
	inputs := []struct {
		id     string
		tags   ident.Tags
		series block.DatabaseSeriesBlocks
	}{
		{"foo", ident.NewTags(ident.StringTag("foo", "foe")), block.NewDatabaseSeriesBlocks(0)},
		{"bar", ident.NewTags(ident.StringTag("bar", "baz")), block.NewDatabaseSeriesBlocks(0)},
	}
	for _, input := range inputs {
		sr.AddSeries(ident.StringID(input.id), input.tags, input.series)
	}
	moreSeries := block.NewDatabaseSeriesBlocks(0)
	block := opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	block.Reset(start, time.Hour, ts.Segment{}, namespace.Context{})
	moreSeries.AddBlock(block)
	sr.AddSeries(ident.StringID("foo"), ident.NewTags(ident.StringTag("foo", "foe")), moreSeries)
	allSeries := sr.AllSeries()
	require.Equal(t, 2, allSeries.Len())
	fooBlocks, ok := allSeries.Get(ident.StringID("foo"))
	require.True(t, ok)
	require.Equal(t, 1, fooBlocks.Blocks.Len())
	barBlocks, ok := allSeries.Get(ident.StringID("bar"))
	require.True(t, ok)
	require.Equal(t, 0, barBlocks.Blocks.Len())
}

func TestShardResultAddResult(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(opts)
	sr.AddResult(nil)
	require.True(t, sr.IsEmpty())
	other := NewShardResult(opts)
	other.AddSeries(ident.StringID("foo"), ident.NewTags(ident.StringTag("foo", "foe")), block.NewDatabaseSeriesBlocks(0))
	other.AddSeries(ident.StringID("bar"), ident.NewTags(ident.StringTag("bar", "baz")), block.NewDatabaseSeriesBlocks(0))
	sr.AddResult(other)
	require.Equal(t, 2, sr.AllSeries().Len())
}

func TestShardResultNumSeries(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(opts)
	sr.AddResult(nil)
	require.True(t, sr.IsEmpty())
	other := NewShardResult(opts)
	other.AddSeries(ident.StringID("foo"), ident.NewTags(ident.StringTag("foo", "foe")), block.NewDatabaseSeriesBlocks(0))
	other.AddSeries(ident.StringID("bar"), ident.NewTags(ident.StringTag("bar", "baz")), block.NewDatabaseSeriesBlocks(0))
	sr.AddResult(other)
	require.Equal(t, int64(2), sr.NumSeries())
}

func TestShardResultRemoveSeries(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(opts)
	inputs := []struct {
		id     string
		tags   ident.Tags
		series block.DatabaseSeriesBlocks
	}{
		{"foo", ident.NewTags(ident.StringTag("foo", "foe")), block.NewDatabaseSeriesBlocks(0)},
		{"bar", ident.NewTags(ident.StringTag("bar", "baz")), block.NewDatabaseSeriesBlocks(0)},
	}
	for _, input := range inputs {
		sr.AddSeries(ident.StringID(input.id), input.tags, input.series)
	}
	require.Equal(t, 2, sr.AllSeries().Len())
	sr.RemoveSeries(ident.StringID("foo"))
	require.Equal(t, 1, sr.AllSeries().Len())
	sr.RemoveSeries(ident.StringID("nonexistent"))
	require.Equal(t, 1, sr.AllSeries().Len())
}

func TestShardTimeRangesIsEmpty(t *testing.T) {
	assert.True(t, shardTimeRanges{}.IsEmpty())
	assert.True(t, shardTimeRanges{0: xtime.NewRanges(), 1: xtime.NewRanges()}.IsEmpty())
	assert.True(t, shardTimeRanges{0: xtime.NewRanges(xtime.Range{})}.IsEmpty())
	now := xtime.Now()
	assert.False(t, shardTimeRanges{0: xtime.NewRanges(xtime.Range{
		Start: now,
		End:   now.Add(time.Second),
	})}.IsEmpty())
}

func TestShardTimeRangesCopy(t *testing.T) {
	now := xtime.Now()
	str := shardTimeRanges{0: xtime.NewRanges(xtime.Range{
		Start: now,
		End:   now.Add(time.Second),
	})}
	copied := str.Copy()
	// Ensure is a copy not same instance
	assert.NotEqual(t, fmt.Sprintf("%p", str), fmt.Sprintf("%p", copied))
	assert.True(t, str.Equal(copied))
}

func TestShardTimeRangesToUnfulfilledDataResult(t *testing.T) {
	now := xtime.Now()
	str := shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: now,
			End:   now.Add(time.Minute),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: now.Add(3 * time.Minute),
			End:   now.Add(4 * time.Minute),
		}),
	}
	r := str.ToUnfulfilledDataResult()
	assert.True(t, r.Unfulfilled().Equal(str))
}

func TestShardTimeRangesSubtract(t *testing.T) {
	start := xtime.Now().Truncate(testBlockSize)
	str := shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
	}
	str.Subtract(shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(testBlockSize),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: start.Add(testBlockSize),
			End:   start.Add(2 * testBlockSize),
		}),
	})

	assert.True(t, str.Equal(shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start.Add(testBlockSize),
			End:   start.Add(2 * testBlockSize),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(testBlockSize),
		}),
	}))
}

func TestShardTimeRangesMinMax(t *testing.T) {
	start := xtime.Now().Truncate(testBlockSize)
	str := shardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(testBlockSize),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: start.Add(testBlockSize),
			End:   start.Add(2 * testBlockSize),
		}),
	}

	min, max := str.MinMax()

	assert.True(t, min.Equal(start))
	assert.True(t, max.Equal(start.Add(2*testBlockSize)))
}

func TestShardTimeRangesString(t *testing.T) {
	start := xtime.FromSeconds(1472824800)
	ts := [][]xtime.UnixNano{
		{start, start.Add(testBlockSize)},
		{start.Add(2 * testBlockSize), start.Add(4 * testBlockSize)},
		{start, start.Add(2 * testBlockSize)},
	}

	str := shardTimeRanges{
		0: xtime.NewRanges(
			xtime.Range{Start: ts[0][0], End: ts[0][1]},
			xtime.Range{Start: ts[1][0], End: ts[1][1]}),
		1: xtime.NewRanges(xtime.Range{
			Start: ts[2][0],
			End:   ts[2][1],
		}),
	}

	expected := "{" +
		fmt.Sprintf("0: [(%v,%v),(%v,%v)], ", ts[0][0], ts[0][1], ts[1][0], ts[1][1]) +
		fmt.Sprintf("1: [(%v,%v)]", ts[2][0], ts[2][1]) +
		"}"

	assert.Equal(t, expected, str.String())
}

func TestShardTimeRangesSummaryString(t *testing.T) {
	start := xtime.FromSeconds(1472824800)
	str := shardTimeRanges{
		0: xtime.NewRanges(
			xtime.Range{Start: start, End: start.Add(testBlockSize)},
			xtime.Range{Start: start.Add(2 * testBlockSize), End: start.Add(4 * testBlockSize)}),
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
	}

	expected := "{0: 6h0m0s, 1: 4h0m0s}"

	assert.Equal(t, expected, str.SummaryString())
}

func TestEstimateMapBytesSize(t *testing.T) {
	opts := testResultOptions()
	blopts := opts.DatabaseBlockOptions()

	start := xtime.Now().Truncate(testBlockSize)

	threeBytes := checked.NewBytes([]byte("123"), nil)
	threeBytes.IncRef()
	blocks := []block.DatabaseBlock{
		block.NewDatabaseBlock(start, testBlockSize, ts.Segment{Head: threeBytes}, blopts, namespace.Context{}),
		block.NewDatabaseBlock(start.Add(1*testBlockSize), testBlockSize, ts.Segment{Tail: threeBytes}, blopts, namespace.Context{}),
	}

	sr := NewShardResult(opts)
	fooTags := ident.NewTags(ident.StringTag("foo", "foe"))
	barTags := ident.NewTags(ident.StringTag("bar", "baz"))

	sr.AddBlock(ident.StringID("foo"), fooTags, blocks[0])
	sr.AddBlock(ident.StringID("bar"), barTags, blocks[1])

	require.Equal(t, int64(24), EstimateMapBytesSize(sr.AllSeries()))
}

func TestEstimateMapBytesSizeEmpty(t *testing.T) {
	require.Equal(t, int64(0), EstimateMapBytesSize(nil))
}
