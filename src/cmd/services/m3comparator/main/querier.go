// Copyright (c) 2019 Uber Technologies, Inc.
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

package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
)

var _ m3.Querier = (*querier)(nil)

type querier struct {
	encoderPool   encoding.EncoderPool
	iteratorPools encoding.IteratorPools
	sync.Once
}

func noop() error { return nil }

func buildDatapoints(
	seed int64,
	start time.Time,
	blockSize time.Duration,
	resolution time.Duration,
) []ts.Datapoint {
	fmt.Println("Seeding with", seed)
	rand.Seed(seed)
	numPoints := int(blockSize / resolution)
	dps := make([]ts.Datapoint, 0, numPoints)
	for i := 0; i < numPoints; i++ {
		dps = append(dps, ts.Datapoint{
			Timestamp: start.Add(resolution * time.Duration(i)),
			Value:     rand.Float64(),
		})
	}

	return dps
}

func (q *querier) buildOptions(
	start time.Time,
	blockSize time.Duration,
) iteratorOptions {
	return iteratorOptions{
		start:         start,
		blockSize:     blockSize,
		encoderPool:   q.encoderPool,
		iteratorPools: q.iteratorPools,
	}
}

// FetchCompressed fetches timeseries data based on a query.
func (q *querier) FetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (m3.SeriesFetchResult, m3.Cleanup, error) {
	var (
		blockSize  = time.Hour * 12
		resolution = time.Minute

		start = time.Now().Add(time.Hour * 24 * -7).Truncate(blockSize)
		opts  = q.buildOptions(start, blockSize)
		tags  = map[string]string{"__name__": "quail"}

		dp  = buildDatapoints(start.Unix(), start, blockSize, resolution)
		dps = [][]ts.Datapoint{dp}
	)

	q.Do(func() {
		for i, p := range dp {
			if i < 36 {
				fmt.Println(i, "Added datapoint", p)
			}
		}
	})

	iter, err := buildIterator(dps, tags, opts)
	if err != nil {
		return m3.SeriesFetchResult{}, noop, err
	}

	iters := encoding.NewSeriesIterators([]encoding.SeriesIterator{iter}, nil)
	cleanup := func() error {
		iters.Close()
		return nil
	}

	return m3.SeriesFetchResult{
		SeriesIterators: iters,
		Metadata:        block.NewResultMetadata(),
	}, cleanup, nil
}

// SearchCompressed fetches matching tags based on a query.
func (q *querier) SearchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (m3.TagResult, m3.Cleanup, error) {
	return m3.TagResult{}, noop, fmt.Errorf("not impl")
}

// CompleteTagsCompressed returns autocompleted tag results.
func (q *querier) CompleteTagsCompressed(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*storage.CompleteTagsResult, error) {
	nameOnly := query.CompleteNameOnly
	return &storage.CompleteTagsResult{
		CompleteNameOnly: nameOnly,
		CompletedTags: []storage.CompletedTag{
			storage.CompletedTag{
				Name:   []byte("__name__"),
				Values: [][]byte{[]byte("quail")},
			},
		},
	}, nil
}
