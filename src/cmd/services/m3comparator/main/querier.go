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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
)

var _ m3.Querier = (*querier)(nil)

type querier struct {
	encoderPool   encoding.EncoderPool
	iteratorPools encoding.IteratorPools
	sync.Mutex
}

func noop() error { return nil }

type seriesBlock []ts.Datapoint
type tagMap map[string]string
type series struct {
	blocks []seriesBlock
	tags   tagMap
}

func generateSeriesBlock(
	start time.Time,
	blockSize time.Duration,
	resolution time.Duration,
) seriesBlock {
	numPoints := int(blockSize / resolution)
	dps := make(seriesBlock, 0, numPoints)
	for i := 0; i < numPoints; i++ {
		dp := ts.Datapoint{
			Timestamp: start.Add(resolution * time.Duration(i)),
			Value:     rand.Float64(),
		}

		dps = append(dps, dp)
		fmt.Println(i, dp.Timestamp.Format("3:04:05PM"), dp.Value)
	}

	return dps
}

func generateSeries(
	start time.Time,
	end time.Time,
	blockSize time.Duration,
	resolution time.Duration,
	tags tagMap,
) series {
	numBlocks := int(math.Ceil(float64(end.Sub(start)) / float64(blockSize)))
	blocks := make([]seriesBlock, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blocks = append(blocks, generateSeriesBlock(start, blockSize, resolution))
		start = start.Add(blockSize)
	}

	return series{
		blocks: blocks,
		tags:   tags,
	}
}

func (q *querier) generateOptions(
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

type seriesGen struct {
	name string
	res  time.Duration
}

// FetchCompressed fetches timeseries data based on a query.
func (q *querier) FetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (m3.SeriesFetchResult, m3.Cleanup, error) {
	var (
		// TODO: take from config.
		blockSize = time.Hour * 12
		start     = query.Start.Truncate(blockSize)
		end       = query.End.Truncate(blockSize).Add(blockSize)
		opts      = q.generateOptions(start, blockSize)

		// TODO: take from config.
		gens = []seriesGen{
			seriesGen{"foo", time.Second * 15},
			seriesGen{"bar", time.Minute * 5},
			seriesGen{"quail", time.Minute},
		}
	)

	q.Lock()
	rand.Seed(start.Unix())
	for _, matcher := range query.TagMatchers {
		// filter if name, otherwise return all.
		if bytes.Equal([]byte("__name__"), matcher.Name) {
			value := string(matcher.Value)
			for _, gen := range gens {
				if value == gen.name {
					gens = []seriesGen{gen}
					break
				}
			}

			break
		}
	}

	seriesList := make([]series, 0, len(gens))
	for _, gen := range gens {
		tagMap := map[string]string{
			"__name__": gen.name,
			"foobar":   "qux",
			"name":     gen.name,
		}

		seriesList = append(
			seriesList,
			generateSeries(start, end, blockSize, gen.res, tagMap),
		)

		start = start.Add(blockSize)
	}

	q.Unlock()
	iters, err := buildSeriesIterators(seriesList, opts)
	if err != nil {
		return m3.SeriesFetchResult{}, noop, err
	}

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
				Values: [][]byte{[]byte("foo"), []byte("foo"), []byte("quail")},
			},
		},
	}, nil
}
