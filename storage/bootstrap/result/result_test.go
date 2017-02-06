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

	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testResultOptions() Options {
	return NewOptions()
}

func TestResultAddMergesExistingShardResults(t *testing.T) {
	opts := testResultOptions()
	blopts := opts.DatabaseBlockOptions()

	blockSize := opts.RetentionOptions().BlockSize()
	start := time.Now().Truncate(blockSize)

	blocks := []block.DatabaseBlock{
		block.NewDatabaseBlock(start, ts.Segment{}, blopts),
		block.NewDatabaseBlock(start.Add(1*blockSize), ts.Segment{}, blopts),
		block.NewDatabaseBlock(start.Add(2*blockSize), ts.Segment{}, blopts),
	}

	srs := []ShardResult{
		NewShardResult(0, opts),
		NewShardResult(0, opts),
	}

	srs[0].AddBlock(ts.StringID("foo"), blocks[0])
	srs[0].AddBlock(ts.StringID("foo"), blocks[1])
	srs[1].AddBlock(ts.StringID("bar"), blocks[2])

	r := NewBootstrapResult()
	r.Add(0, srs[0], nil)
	r.Add(0, srs[1], nil)

	srMerged := NewShardResult(0, opts)
	srMerged.AddBlock(ts.StringID("foo"), blocks[0])
	srMerged.AddBlock(ts.StringID("foo"), blocks[1])
	srMerged.AddBlock(ts.StringID("bar"), blocks[2])

	merged := NewBootstrapResult()
	merged.Add(0, srMerged, nil)

	assert.True(t, r.ShardResults().Equal(merged.ShardResults()))
}

func TestResultAddMergesUnfulfilled(t *testing.T) {
	opts := testResultOptions()

	blockSize := opts.RetentionOptions().BlockSize()
	start := time.Now().Truncate(blockSize)

	r := NewBootstrapResult()

	r.Add(0, nil, xtime.NewRanges().AddRange(xtime.Range{
		Start: start,
		End:   start.Add(8 * blockSize),
	}))

	r.Add(0, nil, xtime.NewRanges().AddRange(xtime.Range{
		Start: start,
		End:   start.Add(2 * blockSize),
	}).AddRange(xtime.Range{
		Start: start.Add(6 * blockSize),
		End:   start.Add(10 * blockSize),
	}))

	expected := ShardTimeRanges{0: xtime.NewRanges().AddRange(xtime.Range{
		Start: start,
		End:   start.Add(10 * blockSize),
	})}

	assert.True(t, r.Unfulfilled().Equal(expected))
}

func TestResultSetUnfulfilled(t *testing.T) {
	opts := testResultOptions()

	blockSize := opts.RetentionOptions().BlockSize()
	start := time.Now().Truncate(blockSize)

	r := NewBootstrapResult()
	r.SetUnfulfilled(ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(2 * blockSize),
		}),
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(2 * blockSize),
		}),
	})
	r.SetUnfulfilled(ShardTimeRanges{
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(2 * blockSize),
		}),
	})

	assert.True(t, r.Unfulfilled().Equal(ShardTimeRanges{
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(2 * blockSize),
		}),
	}))
}

func TestResultAddResult(t *testing.T) {
	opts := testResultOptions()
	blopts := opts.DatabaseBlockOptions()

	blockSize := opts.RetentionOptions().BlockSize()
	start := time.Now().Truncate(blockSize)

	blocks := []block.DatabaseBlock{
		block.NewDatabaseBlock(start, ts.Segment{}, blopts),
		block.NewDatabaseBlock(start.Add(1*blockSize), ts.Segment{}, blopts),
		block.NewDatabaseBlock(start.Add(2*blockSize), ts.Segment{}, blopts),
	}

	srs := []ShardResult{
		NewShardResult(0, opts),
		NewShardResult(0, opts),
	}

	srs[0].AddBlock(ts.StringID("foo"), blocks[0])
	srs[0].AddBlock(ts.StringID("foo"), blocks[1])
	srs[1].AddBlock(ts.StringID("bar"), blocks[2])

	rs := []BootstrapResult{
		NewBootstrapResult(),
		NewBootstrapResult(),
	}

	rs[0].Add(0, srs[0], xtime.NewRanges().AddRange(xtime.Range{
		Start: start.Add(4 * blockSize),
		End:   start.Add(6 * blockSize),
	}))

	rs[1].Add(0, srs[1], xtime.NewRanges().AddRange(xtime.Range{
		Start: start.Add(6 * blockSize),
		End:   start.Add(8 * blockSize),
	}))

	r := MergedBootstrapResult(rs[0], rs[1])

	srMerged := NewShardResult(0, opts)
	srMerged.AddBlock(ts.StringID("foo"), blocks[0])
	srMerged.AddBlock(ts.StringID("foo"), blocks[1])
	srMerged.AddBlock(ts.StringID("bar"), blocks[2])

	expected := struct {
		shardResults ShardResults
		unfulfilled  ShardTimeRanges
	}{
		ShardResults{0: srMerged},
		ShardTimeRanges{0: xtime.NewRanges().AddRange(xtime.Range{
			Start: start.Add(4 * blockSize),
			End:   start.Add(6 * blockSize),
		}).AddRange(xtime.Range{
			Start: start.Add(6 * blockSize),
			End:   start.Add(8 * blockSize),
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
	block.Reset(time.Now(), ts.Segment{})
	sr.AddBlock(ts.StringID("foo"), block)
	require.False(t, sr.IsEmpty())
}

func TestShardResultAddBlock(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(0, opts)
	start := time.Now()
	inputs := []struct {
		id        string
		timestamp time.Time
	}{
		{"foo", start},
		{"foo", start.Add(2 * time.Hour)},
		{"bar", start},
	}
	for _, input := range inputs {
		block := opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
		block.Reset(input.timestamp, ts.Segment{})
		sr.AddBlock(ts.StringID(input.id), block)
	}
	allSeries := sr.AllSeries()
	require.Len(t, allSeries, 2)
	require.Equal(t, 2, allSeries[ts.StringID("foo").Hash()].Blocks.Len())
	require.Equal(t, 1, allSeries[ts.StringID("bar").Hash()].Blocks.Len())
}

func TestShardResultAddSeries(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(0, opts)
	start := time.Now()
	inputs := []struct {
		id     string
		series block.DatabaseSeriesBlocks
	}{
		{"foo", block.NewDatabaseSeriesBlocks(0, opts.DatabaseBlockOptions())},
		{"bar", block.NewDatabaseSeriesBlocks(0, opts.DatabaseBlockOptions())},
	}
	for _, input := range inputs {
		sr.AddSeries(ts.StringID(input.id), input.series)
	}
	moreSeries := block.NewDatabaseSeriesBlocks(0, opts.DatabaseBlockOptions())
	block := opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	block.Reset(start, ts.Segment{})
	moreSeries.AddBlock(block)
	sr.AddSeries(ts.StringID("foo"), moreSeries)
	allSeries := sr.AllSeries()
	require.Len(t, allSeries, 2)
	require.Equal(t, 1, allSeries[ts.StringID("foo").Hash()].Blocks.Len())
	require.Equal(t, 0, allSeries[ts.StringID("bar").Hash()].Blocks.Len())
}

func TestShardResultAddResult(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(0, opts)
	sr.AddResult(nil)
	require.True(t, sr.IsEmpty())
	other := NewShardResult(0, opts)
	other.AddSeries(ts.StringID("foo"), block.NewDatabaseSeriesBlocks(0, opts.DatabaseBlockOptions()))
	other.AddSeries(ts.StringID("bar"), block.NewDatabaseSeriesBlocks(0, opts.DatabaseBlockOptions()))
	sr.AddResult(other)
	require.Len(t, sr.AllSeries(), 2)
}

func TestShardResultRemoveSeries(t *testing.T) {
	opts := testResultOptions()
	sr := NewShardResult(0, opts)
	inputs := []struct {
		id     string
		series block.DatabaseSeriesBlocks
	}{
		{"foo", block.NewDatabaseSeriesBlocks(0, opts.DatabaseBlockOptions())},
		{"bar", block.NewDatabaseSeriesBlocks(0, opts.DatabaseBlockOptions())},
	}
	for _, input := range inputs {
		sr.AddSeries(ts.StringID(input.id), input.series)
	}
	require.Equal(t, 2, len(sr.AllSeries()))
	sr.RemoveSeries(ts.StringID("foo"))
	require.Equal(t, 1, len(sr.AllSeries()))
	sr.RemoveSeries(ts.StringID("nonexistent"))
	require.Equal(t, 1, len(sr.AllSeries()))
}

func TestShardTimeRangesIsEmpty(t *testing.T) {
	assert.True(t, ShardTimeRanges{}.IsEmpty())
	assert.True(t, ShardTimeRanges{0: xtime.NewRanges(), 1: xtime.NewRanges()}.IsEmpty())
	assert.True(t, ShardTimeRanges{0: xtime.NewRanges().AddRange(xtime.Range{})}.IsEmpty())
	assert.False(t, ShardTimeRanges{0: xtime.NewRanges().AddRange(xtime.Range{
		Start: time.Now(),
		End:   time.Now().Add(time.Second),
	})}.IsEmpty())
}

func TestShardTimeRangesCopy(t *testing.T) {
	str := ShardTimeRanges{0: xtime.NewRanges().AddRange(xtime.Range{
		Start: time.Now(),
		End:   time.Now().Add(time.Second),
	})}
	copied := str.Copy()
	// Ensure is a copy not same instance
	assert.NotEqual(t, fmt.Sprintf("%p", str), fmt.Sprintf("%p", copied))
	assert.True(t, str.Equal(copied))
}

func TestShardTimeRangesToUnfulfilledResult(t *testing.T) {
	str := ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{
			Start: time.Now(),
			End:   time.Now().Add(time.Minute),
		}),
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: time.Now().Add(3 * time.Minute),
			End:   time.Now().Add(4 * time.Minute),
		}),
	}
	r := str.ToUnfulfilledResult()
	assert.Equal(t, 0, len(r.ShardResults()))
	assert.True(t, r.Unfulfilled().Equal(str))
}

func TestShardTimeRangesSubtract(t *testing.T) {
	opts := testResultOptions()

	blockSize := opts.RetentionOptions().BlockSize()
	start := time.Now().Truncate(blockSize)

	str := ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(2 * blockSize),
		}),
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(2 * blockSize),
		}),
	}
	str.Subtract(ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(blockSize),
		}),
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: start.Add(blockSize),
			End:   start.Add(2 * blockSize),
		}),
	})

	assert.True(t, str.Equal(ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{
			Start: start.Add(blockSize),
			End:   start.Add(2 * blockSize),
		}),
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(blockSize),
		}),
	}))
}

func TestShardTimeRangesMinMax(t *testing.T) {
	opts := testResultOptions()

	blockSize := opts.RetentionOptions().BlockSize()
	start := time.Now().Truncate(blockSize)

	str := ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(blockSize),
		}),
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: start.Add(blockSize),
			End:   start.Add(2 * blockSize),
		}),
	}

	min, max := str.MinMax()

	assert.True(t, min.Equal(start))
	assert.True(t, max.Equal(start.Add(2*blockSize)))
}

func TestShardTimeRangesString(t *testing.T) {
	opts := testResultOptions()

	blockSize := opts.RetentionOptions().BlockSize()
	start := time.Unix(1472824800, 0)

	ts := [][]time.Time{
		[]time.Time{start, start.Add(blockSize)},
		[]time.Time{start.Add(2 * blockSize), start.Add(4 * blockSize)},
		[]time.Time{start, start.Add(2 * blockSize)},
	}

	str := ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{
			Start: ts[0][0],
			End:   ts[0][1],
		}).AddRange(xtime.Range{
			Start: ts[1][0],
			End:   ts[1][1],
		}),
		1: xtime.NewRanges().AddRange(xtime.Range{
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
	opts := testResultOptions()

	blockSize := opts.RetentionOptions().BlockSize()
	start := time.Unix(1472824800, 0)

	str := ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(blockSize),
		}).AddRange(xtime.Range{
			Start: start.Add(2 * blockSize),
			End:   start.Add(4 * blockSize),
		}),
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(2 * blockSize),
		}),
	}

	expected := "{0: 6h0m0s, 1: 4h0m0s}"

	assert.Equal(t, expected, str.SummaryString())
}
