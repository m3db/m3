// Copyright (c) 2020 Uber Technologies, Inc.
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

package index

import (
	"math"
	"testing"
	"time"

	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryOptions(t *testing.T) {
	opts := QueryOptions{
		DocsLimit:   10,
		SeriesLimit: 20,
	}

	assert.False(t, opts.SeriesLimitExceeded(19))
	assert.True(t, opts.SeriesLimitExceeded(20))

	assert.False(t, opts.DocsLimitExceeded(9))
	assert.True(t, opts.DocsLimitExceeded(10))

	assert.True(t, opts.LimitsExceeded(19, 10))
	assert.True(t, opts.LimitsExceeded(20, 9))
	assert.False(t, opts.LimitsExceeded(19, 9))

	assert.False(t, opts.Exhaustive(19, 10))
	assert.False(t, opts.Exhaustive(20, 9))
	assert.True(t, opts.Exhaustive(19, 9))
}

func TestInvalidWideQueryOptions(t *testing.T) {
	var (
		now      = xtime.Now().Truncate(time.Hour).Add(1)
		iterOpts = IterationOptions{}

		batchSize int
		blockSize time.Duration
	)

	_, err := NewWideQueryOptions(now, batchSize, blockSize, nil, iterOpts)
	require.EqualError(t, err, "non-positive batch size (0) for wide query")

	batchSize = 1
	_, err = NewWideQueryOptions(now, batchSize, blockSize, nil, iterOpts)
	require.EqualError(t, err, "non-positive block size (0s) for wide query")

	blockSize = time.Minute
	_, err = NewWideQueryOptions(now, batchSize, blockSize, nil, iterOpts)
	require.Error(t, err)

	now = now.Truncate(blockSize)
	_, err = NewWideQueryOptions(now, batchSize, blockSize, nil, iterOpts)
	require.NoError(t, err)
}

func TestWideQueryOptions(t *testing.T) {
	var (
		batchSize = 100
		blockSize = time.Hour * 2
		now       = xtime.Now().Truncate(blockSize)
		iterOpts  = IterationOptions{}
		shards    = []uint32{100, 23, 1}
	)

	opts, err := NewWideQueryOptions(now, batchSize, blockSize, shards, iterOpts)
	require.NoError(t, err)
	assert.Equal(t, WideQueryOptions{
		StartInclusive:   now.Truncate(blockSize),
		EndExclusive:     now.Truncate(blockSize).Add(blockSize),
		BatchSize:        batchSize,
		IterationOptions: iterOpts,
		ShardsQueried:    []uint32{1, 23, 100},
	}, opts)

	qOpts := opts.ToQueryOptions()
	assert.Equal(t, QueryOptions{
		StartInclusive:    now.Truncate(blockSize),
		EndExclusive:      now.Truncate(blockSize).Add(blockSize),
		SeriesLimit:       0,
		DocsLimit:         0,
		RequireExhaustive: false,
		IterationOptions:  iterOpts,
	}, qOpts)

	upperBound := int(math.MaxInt64)
	assert.False(t, qOpts.LimitsExceeded(upperBound, upperBound))
}
