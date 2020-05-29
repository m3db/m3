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
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/prometheus/common/model"
)

var _ m3.Querier = (*querier)(nil)

type querier struct {
	iteratorOpts      iteratorOptions
	handler           seriesLoadHandler
	blockSize         time.Duration
	defaultResolution time.Duration
	sync.Mutex
}

func newQuerier(
	iteratorOpts iteratorOptions,
	handler seriesLoadHandler,
	blockSize time.Duration,
	defaultResolution time.Duration,
) (*querier, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("blockSize must be positive, got %d", blockSize)
	}
	if defaultResolution <= 0 {
		return nil, fmt.Errorf("defaultResolution must be positive, got %d", defaultResolution)
	}
	return &querier{
		iteratorOpts:      iteratorOpts,
		handler:           handler,
		blockSize:         blockSize,
		defaultResolution: defaultResolution,
	}, nil
}

func noop() error { return nil }

type seriesBlock []ts.Datapoint

type series struct {
	blocks []seriesBlock
	tags   parser.Tags
}

func (q *querier) generateSeriesBlock(
	start time.Time,
	resolution time.Duration,
) seriesBlock {
	numPoints := int(q.blockSize / resolution)
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

func (q *querier) generateSeries(
	start time.Time,
	end time.Time,
	resolution time.Duration,
	tags parser.Tags,
) (series, error) {
	numBlocks := int(math.Ceil(float64(end.Sub(start)) / float64(q.blockSize)))
	if numBlocks == 0 {
		return series{}, fmt.Errorf("comparator querier: no blocks generated")
	}

	blocks := make([]seriesBlock, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blocks = append(blocks, q.generateSeriesBlock(start, resolution))
		start = start.Add(q.blockSize)
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
) (consolidators.SeriesFetchResult, m3.Cleanup, error) {
	var (
		iters        encoding.SeriesIterators
		randomSeries []series
		ignoreFilter bool
		err          error
		nameTagFound bool
	)

	name := q.iteratorOpts.tagOptions.MetricName()
	for _, matcher := range query.TagMatchers {
		if bytes.Equal(name, matcher.Name) {
			nameTagFound = true
			iters, err = q.handler.getSeriesIterators(string(matcher.Value))
			if err != nil {
				return consolidators.SeriesFetchResult{}, noop, err
			}

			break
		}
	}

	if iters == nil && !nameTagFound && len(query.TagMatchers) > 0 {
		iters, err = q.handler.getSeriesIterators("")
		if err != nil {
			return m3.SeriesFetchResult{}, noop, err
		}
	}
	
	if iters == nil || iters.Len() == 0 {
		randomSeries, ignoreFilter, err = q.generateRandomSeries(query)
		if err != nil {
			return consolidators.SeriesFetchResult{}, noop, err
		}
		iters, err = buildSeriesIterators(randomSeries, query.Start, q.blockSize, q.iteratorOpts)
		if err != nil {
			return consolidators.SeriesFetchResult{}, noop, err
		}
	}

	if !ignoreFilter {
		filteredIters := filter(iters, query.TagMatchers)

		cleanup := func() error {
			iters.Close()
			filteredIters.Close()
			return nil
		}

		return consolidators.SeriesFetchResult{
			SeriesIterators: filteredIters,
			Metadata:        block.NewResultMetadata(),
		}, cleanup, nil
	}

	cleanup := func() error {
		iters.Close()
		return nil
	}

	return consolidators.SeriesFetchResult{
		SeriesIterators: iters,
		Metadata:        block.NewResultMetadata(),
	}, cleanup, nil
}

func (q *querier) generateRandomSeries(
	query *storage.FetchQuery,
) (series []series, ignoreFilter bool, err error) {
	var (
		start = query.Start.Truncate(q.blockSize)
		end   = query.End.Truncate(q.blockSize).Add(q.blockSize)
	)

	metricNameTag := q.iteratorOpts.tagOptions.MetricName()
	for _, matcher := range query.TagMatchers {
		if bytes.Equal(metricNameTag, matcher.Name) {
			if matched, _ := regexp.Match(`^multi_\d+$`, matcher.Value); matched {
				series, err = q.generateMultiSeriesMetrics(string(matcher.Value), start, end)
				return
			}
		}
	}

	ignoreFilter = true
	series, err = q.generateSingleSeriesMetrics(query, start, end)
	return
}

func (q *querier) generateSingleSeriesMetrics(
	query *storage.FetchQuery,
	start time.Time,
	end time.Time,
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

	metricNameTag := q.iteratorOpts.tagOptions.MetricName()
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
					res:  q.defaultResolution,
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
			parser.NewTag(model.MetricNameLabel, gen.name),
			parser.NewTag("foobar", "qux"),
			parser.NewTag("name", gen.name),
		}

		series, err := q.generateSeries(start, end, gen.res, tags)
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
			parser.NewTag(model.MetricNameLabel, metricsName),
			parser.NewTag("id", strconv.Itoa(i)),
			parser.NewTag("parity", strconv.Itoa(i%2)),
			parser.NewTag("const", "x"),
		}

		series, err := q.generateSeries(start, end, q.defaultResolution, tags)
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
) (consolidators.TagResult, m3.Cleanup, error) {
	return consolidators.TagResult{}, noop, fmt.Errorf("not impl")
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
