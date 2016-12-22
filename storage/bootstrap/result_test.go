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

package bootstrap

import (
	"testing"
	"time"

	"github.com/m3db/m3db/client/result"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
)

func testResultOptions() result.Options {
	return result.NewOptions()
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

	srs := []result.ShardResult{
		result.NewShardResult(0, opts),
		result.NewShardResult(0, opts),
	}

	srs[0].AddBlock(ts.StringID("foo"), blocks[0])
	srs[0].AddBlock(ts.StringID("foo"), blocks[1])
	srs[1].AddBlock(ts.StringID("bar"), blocks[2])

	r := NewResult()
	r.Add(0, srs[0], nil)
	r.Add(0, srs[1], nil)

	srMerged := result.NewShardResult(0, opts)
	srMerged.AddBlock(ts.StringID("foo"), blocks[0])
	srMerged.AddBlock(ts.StringID("foo"), blocks[1])
	srMerged.AddBlock(ts.StringID("bar"), blocks[2])

	merged := NewResult()
	merged.Add(0, srMerged, nil)

	assert.True(t, r.ShardResults().Equal(merged.ShardResults()))
}

func TestResultAddMergesUnfulfilled(t *testing.T) {
	opts := testResultOptions()

	blockSize := opts.RetentionOptions().BlockSize()
	start := time.Now().Truncate(blockSize)

	r := NewResult()

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

	expected := result.ShardTimeRanges{0: xtime.NewRanges().AddRange(xtime.Range{
		Start: start,
		End:   start.Add(10 * blockSize),
	})}

	assert.True(t, r.Unfulfilled().Equal(expected))
}

func TestResultSetUnfulfilled(t *testing.T) {
	opts := testResultOptions()

	blockSize := opts.RetentionOptions().BlockSize()
	start := time.Now().Truncate(blockSize)

	r := NewResult()
	r.SetUnfulfilled(result.ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(2 * blockSize),
		}),
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(2 * blockSize),
		}),
	})
	r.SetUnfulfilled(result.ShardTimeRanges{
		1: xtime.NewRanges().AddRange(xtime.Range{
			Start: start,
			End:   start.Add(2 * blockSize),
		}),
	})

	assert.True(t, r.Unfulfilled().Equal(result.ShardTimeRanges{
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

	srs := []result.ShardResult{
		result.NewShardResult(0, opts),
		result.NewShardResult(0, opts),
	}

	srs[0].AddBlock(ts.StringID("foo"), blocks[0])
	srs[0].AddBlock(ts.StringID("foo"), blocks[1])
	srs[1].AddBlock(ts.StringID("bar"), blocks[2])

	rs := []Result{
		NewResult(),
		NewResult(),
	}

	rs[0].Add(0, srs[0], xtime.NewRanges().AddRange(xtime.Range{
		Start: start.Add(4 * blockSize),
		End:   start.Add(6 * blockSize),
	}))

	rs[1].Add(0, srs[1], xtime.NewRanges().AddRange(xtime.Range{
		Start: start.Add(6 * blockSize),
		End:   start.Add(8 * blockSize),
	}))

	r := rs[0]
	r.AddResult(rs[1])

	srMerged := result.NewShardResult(0, opts)
	srMerged.AddBlock(ts.StringID("foo"), blocks[0])
	srMerged.AddBlock(ts.StringID("foo"), blocks[1])
	srMerged.AddBlock(ts.StringID("bar"), blocks[2])

	expected := struct {
		shardResults result.ShardResults
		unfulfilled  result.ShardTimeRanges
	}{
		result.ShardResults{0: srMerged},
		result.ShardTimeRanges{0: xtime.NewRanges().AddRange(xtime.Range{
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
