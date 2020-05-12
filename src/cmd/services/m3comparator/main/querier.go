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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	xtime "github.com/m3db/m3/src/x/time"
)

var _ m3.Querier = (*querier)(nil)

type querier struct {
	opts    iteratorOptions
	handler seriesLoadHandler
	sync.Mutex
}

func noop() error { return nil }

type seriesBlock []ts.Datapoint

type series struct {
	blocks []seriesBlock
	tags   parser.Tags
}

func generateSeriesBlock(
	start time.Time,
	blockSize time.Duration,
	resolution time.Duration,
) seriesBlock {
	numPoints := int(blockSize / resolution)
	dps := make(seriesBlock, 0, numPoints)
	for i := 0; i < numPoints; i++ {
		stamp := start.Add(resolution * time.Duration(i))
		dp := ts.Datapoint{
			Timestamp:      stamp,
			TimestampNanos: xtime.ToUnixNano(stamp),
			Value:          rand.Float64(),
		}

		dps = append(dps, dp)
	}

	return dps
}

func generateSeries(
	start time.Time,
	end time.Time,
	blockSize time.Duration,
	resolution time.Duration,
	tags parser.Tags,
) (series, error) {
	numBlocks := int(math.Ceil(float64(end.Sub(start)) / float64(blockSize)))
	if numBlocks == 0 {
		return series{}, fmt.Errorf("comparator querier: no blocks generated")
	}

	blocks := make([]seriesBlock, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blocks = append(blocks, generateSeriesBlock(start, blockSize, resolution))
		start = start.Add(blockSize)
	}

	return series{
		blocks: blocks,
		tags:   tags,
	}, nil
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
		iters        encoding.SeriesIterators
		randomSeries []series
		ignoreFilter bool
		err          error
		nameTagFound bool
	)

	name := q.opts.tagOptions.MetricName()
	for _, matcher := range query.TagMatchers {
		if bytes.Equal(name, matcher.Name) {
			nameTagFound = true
			iters, err = q.handler.getSeriesIterators(string(matcher.Value))
			if err != nil {
				return m3.SeriesFetchResult{}, noop, err
			}

			break
		}
	}

	const blockSize = time.Hour * 12

	if iters == nil && !nameTagFound && len(query.TagMatchers) > 0 {
		iters, err = q.handler.getSeriesIterators("")
		if err != nil {
			return m3.SeriesFetchResult{}, noop, err
		}
	}

	if iters == nil || iters.Len() == 0 {
		randomSeries, ignoreFilter, err = q.generateRandomSeries(query, blockSize)
		if err != nil {
			return m3.SeriesFetchResult{}, noop, err
		}
		iters, err = buildSeriesIterators(randomSeries, query.Start, blockSize, q.opts)
		if err != nil {
			return m3.SeriesFetchResult{}, noop, err
		}
	}

	if !ignoreFilter {
		filteredIters := filter(iters, query.TagMatchers)

		cleanup := func() error {
			iters.Close()
			filteredIters.Close()
			return nil
		}

		return m3.SeriesFetchResult{
			SeriesIterators: filteredIters,
			Metadata:        block.NewResultMetadata(),
		}, cleanup, nil
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

func (q *querier) generateRandomSeries(
	query *storage.FetchQuery,
	blockSize time.Duration,
) (series []series, ignoreFilter bool, err error) {
	var (
		start = query.Start.Truncate(blockSize)
		end   = query.End.Truncate(blockSize).Add(blockSize)
	)

	metricNameTag := q.opts.tagOptions.MetricName()
	for _, matcher := range query.TagMatchers {
		if bytes.Equal(metricNameTag, matcher.Name) {
			if matched, _ := regexp.Match(`^multi_\d+$`, matcher.Value); matched {
				series, err = q.generateMultiSeriesMetrics(string(matcher.Value), start, end, time.Second*30, blockSize)
				return
			}
		}
	}

	ignoreFilter = true
	series, err = q.generateSingleSeriesMetrics(query, start, end, blockSize)
	return
}

func (q *querier) generateSingleSeriesMetrics(
	query *storage.FetchQuery,
	start time.Time,
	end time.Time,
	blockSize time.Duration,
) ([]series, error) {
	var (
		gens = []seriesGen{
			{"foo", time.Second},
			{"bar", time.Second * 15},
			{"quail", time.Minute},
		}

		actualGens []seriesGen
	)

	q.Lock()
	defer q.Unlock()
	rand.Seed(start.Unix())

	metricNameTag := q.opts.tagOptions.MetricName()
	for _, matcher := range query.TagMatchers {
		// filter if name, otherwise return all.
		if bytes.Equal(metricNameTag, matcher.Name) {
			value := string(matcher.Value)
			for _, gen := range gens {
				if value == gen.name {
					actualGens = append(actualGens, gen)
					break
				}
			}

			break
		} else if "gen" == string(matcher.Name) {
			cStr := string(matcher.Value)
			count, err := strconv.Atoi(cStr)
			if err != nil {
				return nil, err
			}

			actualGens = make([]seriesGen, 0, count)
			for i := 0; i < count; i++ {
				actualGens = append(actualGens, seriesGen{
					res:  time.Second * 15,
					name: fmt.Sprintf("foo_%d", i),
				})
			}

			break
		}
	}

	if len(actualGens) == 0 {
		actualGens = gens
	}

	seriesList := make([]series, 0, len(actualGens))
	for _, gen := range actualGens {
		tags := parser.Tags{
			parser.NewTag("__name__", gen.name),
			parser.NewTag("foobar", "qux"),
			parser.NewTag("name", gen.name),
		}

		series, err := generateSeries(start, end, blockSize, gen.res, tags)
		if err != nil {
			return nil, err
		}

		seriesList = append(seriesList, series)
	}

	return seriesList, nil
}

func (q *querier) generateMultiSeriesMetrics(
	metricsName string,
	start time.Time,
	end time.Time,
	resolution time.Duration,
	blockSize time.Duration,
) ([]series, error) {
	suffix := strings.TrimPrefix(metricsName, "multi_")
	seriesCount, err := strconv.Atoi(suffix)
	if err != nil {
		return nil, err
	}

	q.Lock()
	defer q.Unlock()
	rand.Seed(start.Unix())

	seriesList := make([]series, 0, seriesCount)
	for i := 0; i < seriesCount; i++ {
		tags := parser.Tags{
			parser.NewTag("__name__", metricsName),
			parser.NewTag("id", strconv.Itoa(i)),
			parser.NewTag("parity", strconv.Itoa(i%2)),
			parser.NewTag("const", "x"),
		}

		series, err := generateSeries(start, end, blockSize, resolution, tags)
		if err != nil {
			return nil, err
		}

		seriesList = append(seriesList, series)
	}

	return seriesList, nil
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
	// TODO: take from config.
	return &storage.CompleteTagsResult{
		CompleteNameOnly: nameOnly,
		CompletedTags: []storage.CompletedTag{
			{
				Name:   []byte("__name__"),
				Values: [][]byte{[]byte("foo"), []byte("foo"), []byte("quail")},
			},
		},
	}, nil
}
