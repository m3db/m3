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

func TestDataResultAddMergesExistingShardResults(t *testing.T) {
	opts := testResultOptions()
	blopts := opts.DatabaseBlockOptions()

	start := time.Now().Truncate(testBlockSize)

	blocks := []block.DatabaseBlock{
		block.NewDatabaseBlock(start, testBlockSize, ts.Segment{}, blopts, namespace.Context{}),
		block.NewDatabaseBlock(start.Add(1*testBlockSize), testBlockSize, ts.Segment{}, blopts, namespace.Context{}),
		block.NewDatabaseBlock(start.Add(2*testBlockSize), testBlockSize, ts.Segment{}, blopts, namespace.Context{}),
	}

	srs := []ShardResult{
		NewShardResult(0, opts),
		NewShardResult(0, opts),
	}

	fooTags := ident.NewTags(ident.StringTag("foo", "foe"))
	barTags := ident.NewTags(ident.StringTag("bar", "baz"))

	srs[0].AddBlock(ident.StringID("foo"), fooTags, blocks[0])
	srs[0].AddBlock(ident.StringID("foo"), fooTags, blocks[1])
	srs[1].AddBlock(ident.StringID("bar"), barTags, blocks[2])

	r := NewDataBootstrapResult()
	r.Add(0, srs[0], xtime.Ranges{})
	r.Add(0, srs[1], xtime.Ranges{})

	srMerged := NewShardResult(0, opts)
	srMerged.AddBlock(ident.StringID("foo"), fooTags, blocks[0])
	srMerged.AddBlock(ident.StringID("foo"), fooTags, blocks[1])
	srMerged.AddBlock(ident.StringID("bar"), barTags, blocks[2])

	merged := NewDataBootstrapResult()
	merged.Add(0, srMerged, xtime.Ranges{})

	assert.True(t, r.ShardResults().Equal(merged.ShardResults()))
}

func TestDataResultAddMergesUnfulfilled(t *testing.T) {
	start := time.Now().Truncate(testBlockSize)

	r := NewDataBootstrapResult()

	r.Add(0, nil, xtime.NewRanges(xtime.Range{
		Start: start,
		End:   start.Add(8 * testBlockSize),
	}))

	r.Add(0, nil, xtime.NewRanges(xtime.Range{
		Start: start,
		End:   start.Add(2 * testBlockSize),
	}).AddRange(xtime.Range{
		Start: start.Add(6 * testBlockSize),
		End:   start.Add(10 * testBlockSize),
	}))

	expected := ShardTimeRanges{0: xtime.NewRanges(xtime.Range{
		Start: start,
		End:   start.Add(10 * testBlockSize),
	})}

	assert.True(t, r.Unfulfilled().Equal(expected))
}

func TestResultSetUnfulfilled(t *testing.T) {
	start := time.Now().Truncate(testBlockSize)

	r := NewDataBootstrapResult()
	r.SetUnfulfilled(ShardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
	})
	r.SetUnfulfilled(ShardTimeRanges{
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
	})

	assert.True(t, r.Unfulfilled().Equal(ShardTimeRanges{
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
	}))
}

func TestResultNumSeries(t *testing.T) {
	opts := testResultOptions()
	blopts := opts.DatabaseBlockOptions()

	start := time.Now().Truncate(testBlockSize)

	blocks := []block.DatabaseBlock{
		block.NewDatabaseBlock(start, testBlockSize, ts.Segment{}, blopts, namespace.Context{}),
		block.NewDatabaseBlock(start.Add(1*testBlockSize), testBlockSize, ts.Segment{}, blopts, namespace.Context{}),
		block.NewDatabaseBlock(start.Add(2*testBlockSize), testBlockSize, ts.Segment{}, blopts, namespace.Context{}),
	}

	srs := []ShardResult{
		NewShardResult(0, opts),
		NewShardResult(0, opts),
	}

	fooTags := ident.NewTags(ident.StringTag("foo", "foe"))
	barTags := ident.NewTags(ident.StringTag("bar", "baz"))

	srs[0].AddBlock(ident.StringID("foo"), fooTags, blocks[0])
	srs[0].AddBlock(ident.StringID("foo"), fooTags, blocks[1])
	srs[1].AddBlock(ident.StringID("bar"), barTags, blocks[2])

	r := NewDataBootstrapResult()
	r.Add(0, srs[0], xtime.Ranges{})
	r.Add(1, srs[1], xtime.Ranges{})

	require.Equal(t, int64(2), r.ShardResults().NumSeries())
}

func TestResultAddResult(t *testing.T) {
	opts := testResultOptions()
	blopts := opts.DatabaseBlockOptions()

	start := time.Now().Truncate(testBlockSize)

	blocks := []block.DatabaseBlock{
		block.NewDatabaseBlock(start, testBlockSize, ts.Segment{}, blopts, namespace.Context{}),
		block.NewDatabaseBlock(start.Add(1*testBlockSize), testBlockSize, ts.Segment{}, blopts, namespace.Context{}),
		block.NewDatabaseBlock(start.Add(2*testBlockSize), testBlockSize, ts.Segment{}, blopts, namespace.Context{}),
	}

	srs := []ShardResult{
		NewShardResult(0, opts),
		NewShardResult(0, opts),
	}
	fooTags := ident.NewTags(ident.StringTag("foo", "foe"))
	barTags := ident.NewTags(ident.StringTag("bar", "baz"))

	srs[0].AddBlock(ident.StringID("foo"), fooTags, blocks[0])
	srs[0].AddBlock(ident.StringID("foo"), fooTags, blocks[1])
	srs[1].AddBlock(ident.StringID("bar"), barTags, blocks[2])

	rs := []DataBootstrapResult{
		NewDataBootstrapResult(),
		NewDataBootstrapResult(),
	}

	rs[0].Add(0, srs[0], xtime.NewRanges(xtime.Range{
		Start: start.Add(4 * testBlockSize),
		End:   start.Add(6 * testBlockSize),
	}))

	rs[1].Add(0, srs[1], xtime.NewRanges(xtime.Range{
		Start: start.Add(6 * testBlockSize),
		End:   start.Add(8 * testBlockSize),
	}))

	r := MergedDataBootstrapResult(rs[0], rs[1])

	srMerged := NewShardResult(0, opts)
	srMerged.AddBlock(ident.StringID("foo"), fooTags, blocks[0])
	srMerged.AddBlock(ident.StringID("foo"), fooTags, blocks[1])
	srMerged.AddBlock(ident.StringID("bar"), barTags, blocks[2])

	expected := struct {
		shardResults ShardResults
		unfulfilled  ShardTimeRanges
	}{
		ShardResults{0: srMerged},
		ShardTimeRanges{0: xtime.NewRanges(xtime.Range{
			Start: start.Add(4 * testBlockSize),
			End:   start.Add(6 * testBlockSize),
		}).AddRange(xtime.Range{
			Start: start.Add(6 * testBlockSize),
			End:   start.Add(8 * testBlockSize),
		})},
	}

	assert.True(t, r.ShardResults().Equal(expected.shardResults))
	assert.True(t, r.Unfulfilled().Equal(expected.unfulfilled))
}

func TestShardResultIsEmpty(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(0, opts)
	require.True(t, sr.IsEmpty())
	block := opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	block.Reset(time.Now(), time.Hour, ts.Segment{}, namespace.Context{})
	fooTags := ident.NewTags(ident.StringTag("foo", "foe"))
	sr.AddBlock(ident.StringID("foo"), fooTags, block)
	require.False(t, sr.IsEmpty())
}

func TestShardResultAddBlock(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(0, opts)
	start := time.Now()
	inputs := []struct {
		id        string
		tags      ident.Tags
		timestamp time.Time
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
	sr := NewShardResult(0, opts)
	start := time.Now()
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
	sr := NewShardResult(0, opts)
	sr.AddResult(nil)
	require.True(t, sr.IsEmpty())
	other := NewShardResult(0, opts)
	other.AddSeries(ident.StringID("foo"), ident.NewTags(ident.StringTag("foo", "foe")), block.NewDatabaseSeriesBlocks(0))
	other.AddSeries(ident.StringID("bar"), ident.NewTags(ident.StringTag("bar", "baz")), block.NewDatabaseSeriesBlocks(0))
	sr.AddResult(other)
	require.Equal(t, 2, sr.AllSeries().Len())
}

func TestShardResultNumSeries(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(0, opts)
	sr.AddResult(nil)
	require.True(t, sr.IsEmpty())
	other := NewShardResult(0, opts)
	other.AddSeries(ident.StringID("foo"), ident.NewTags(ident.StringTag("foo", "foe")), block.NewDatabaseSeriesBlocks(0))
	other.AddSeries(ident.StringID("bar"), ident.NewTags(ident.StringTag("bar", "baz")), block.NewDatabaseSeriesBlocks(0))
	sr.AddResult(other)
	require.Equal(t, int64(2), sr.NumSeries())
}

func TestShardResultRemoveSeries(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(0, opts)
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
	assert.True(t, ShardTimeRanges{}.IsEmpty())
	assert.True(t, ShardTimeRanges{0: xtime.Ranges{}, 1: xtime.Ranges{}}.IsEmpty())
	assert.True(t, ShardTimeRanges{0: xtime.NewRanges(xtime.Range{})}.IsEmpty())
	assert.False(t, ShardTimeRanges{0: xtime.NewRanges(xtime.Range{
		Start: time.Now(),
		End:   time.Now().Add(time.Second),
	})}.IsEmpty())
}

func TestShardTimeRangesCopy(t *testing.T) {
	str := ShardTimeRanges{0: xtime.NewRanges(xtime.Range{
		Start: time.Now(),
		End:   time.Now().Add(time.Second),
	})}
	copied := str.Copy()
	// Ensure is a copy not same instance
	assert.NotEqual(t, fmt.Sprintf("%p", str), fmt.Sprintf("%p", copied))
	assert.True(t, str.Equal(copied))
}

func TestShardTimeRangesToUnfulfilledDataResult(t *testing.T) {
	str := ShardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: time.Now(),
			End:   time.Now().Add(time.Minute),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: time.Now().Add(3 * time.Minute),
			End:   time.Now().Add(4 * time.Minute),
		}),
	}
	r := str.ToUnfulfilledDataResult()
	assert.Equal(t, 0, len(r.ShardResults()))
	assert.True(t, r.Unfulfilled().Equal(str))
}

func TestShardTimeRangesSubtract(t *testing.T) {
	start := time.Now().Truncate(testBlockSize)

	str := ShardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(2 * testBlockSize),
		}),
	}
	str.Subtract(ShardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(testBlockSize),
		}),
		1: xtime.NewRanges(xtime.Range{
			Start: start.Add(testBlockSize),
			End:   start.Add(2 * testBlockSize),
		}),
	})

	assert.True(t, str.Equal(ShardTimeRanges{
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

	start := time.Now().Truncate(testBlockSize)

	str := ShardTimeRanges{
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
	start := time.Unix(1472824800, 0)

	ts := [][]time.Time{
		[]time.Time{start, start.Add(testBlockSize)},
		[]time.Time{start.Add(2 * testBlockSize), start.Add(4 * testBlockSize)},
		[]time.Time{start, start.Add(2 * testBlockSize)},
	}

	str := ShardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: ts[0][0],
			End:   ts[0][1],
		}).AddRange(xtime.Range{
			Start: ts[1][0],
			End:   ts[1][1],
		}),
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
	start := time.Unix(1472824800, 0)

	str := ShardTimeRanges{
		0: xtime.NewRanges(xtime.Range{
			Start: start,
			End:   start.Add(testBlockSize),
		}).AddRange(xtime.Range{
			Start: start.Add(2 * testBlockSize),
			End:   start.Add(4 * testBlockSize),
		}),
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

	start := time.Now().Truncate(testBlockSize)

	threeBytes := checked.NewBytes([]byte("123"), nil)
	threeBytes.IncRef()
	blocks := []block.DatabaseBlock{
		block.NewDatabaseBlock(start, testBlockSize, ts.Segment{Head: threeBytes}, blopts, namespace.Context{}),
		block.NewDatabaseBlock(start.Add(1*testBlockSize), testBlockSize, ts.Segment{Tail: threeBytes}, blopts, namespace.Context{}),
	}

	sr := NewShardResult(0, opts)
	fooTags := ident.NewTags(ident.StringTag("foo", "foe"))
	barTags := ident.NewTags(ident.StringTag("bar", "baz"))

	sr.AddBlock(ident.StringID("foo"), fooTags, blocks[0])
	sr.AddBlock(ident.StringID("bar"), barTags, blocks[1])

	require.Equal(t, 24, EstimateMapBytesSize(sr.AllSeries()))
}

func TestEstimateMapBytesSizeEmpty(t *testing.T) {
	require.Equal(t, 0, EstimateMapBytesSize(nil))
}
