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
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/prometheus/common/model"
)

var _ m3.Querier = (*querier)(nil)

type querier struct {
	iteratorOpts         parser.Options
	handler              seriesLoadHandler
	blockSize            time.Duration
	defaultResolution    time.Duration
	histogramBucketCount uint
	sync.Mutex
}

func newQuerier(
	iteratorOpts parser.Options,
	handler seriesLoadHandler,
	blockSize time.Duration,
	defaultResolution time.Duration,
	histogramBucketCount uint,
) (*querier, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("blockSize must be positive, got %d", blockSize)
	}
	if defaultResolution <= 0 {
		return nil, fmt.Errorf("defaultResolution must be positive, got %d", defaultResolution)
	}
	return &querier{
		iteratorOpts:         iteratorOpts,
		handler:              handler,
		blockSize:            blockSize,
		defaultResolution:    defaultResolution,
		histogramBucketCount: histogramBucketCount,
	}, nil
}

func noop() error { return nil }

func (q *querier) generateSeriesBlock(
	start time.Time,
	resolution time.Duration,
	integerValues bool,
) parser.Data {
	numPoints := int(q.blockSize / resolution)
	dps := make(parser.Data, 0, numPoints)
	for i := 0; i < numPoints; i++ {
		stamp := start.Add(resolution * time.Duration(i))
		var value float64
		if integerValues {
			value = float64(rand.Intn(1000))
		} else {
			value = rand.Float64()
		}
		dp := ts.Datapoint{
			TimestampNanos: xtime.ToUnixNano(stamp),
			Value:          value,
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
	integerValues bool,
) (parser.IngestSeries, error) {
	numBlocks := int(math.Ceil(float64(end.Sub(start)) / float64(q.blockSize)))
	if numBlocks == 0 {
		return parser.IngestSeries{}, fmt.Errorf("comparator querier: no blocks generated")
	}

	blocks := make([]parser.Data, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blocks = append(blocks, q.generateSeriesBlock(start, resolution, integerValues))
		start = start.Add(q.blockSize)
	}

	return parser.IngestSeries{
		Datapoints: blocks,
		Tags:       tags,
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
		iters               encoding.SeriesIterators
		randomSeries        []parser.IngestSeries
		ignoreFilter        bool
		err                 error
		strictMetricsFilter bool
	)

	name := q.iteratorOpts.TagOptions.MetricName()
	for _, matcher := range query.TagMatchers {
		if bytes.Equal(name, matcher.Name) {

			metricsName := string(matcher.Value)

			// NB: the default behaviour of this querier is to return predefined metrics with random data if no match by
			// metrics name is found. To force it return an empty result, query the "nonexistent*" metrics.
			if match, _ := regexp.MatchString("^nonexist[ae]nt", metricsName); match {
				return consolidators.SeriesFetchResult{}, noop, nil
			}

			if matcher.Type == models.MatchEqual {
				strictMetricsFilter = true
				iters, err = q.handler.getSeriesIterators(metricsName)
				if err != nil {
					return consolidators.SeriesFetchResult{}, noop, err
				}

				break
			}
		}
	}

	if iters == nil && !strictMetricsFilter && len(query.TagMatchers) > 0 {
		iters, err = q.handler.getSeriesIterators("")
		if err != nil {
			return consolidators.SeriesFetchResult{}, noop, err
		}
	}

	if iters == nil || iters.Len() == 0 {
		randomSeries, ignoreFilter, err = q.generateRandomSeries(query)
		if err != nil {
			return consolidators.SeriesFetchResult{}, noop, err
		}
		iters, err = parser.BuildSeriesIterators(
			randomSeries, xtime.ToUnixNano(query.Start), q.blockSize, q.iteratorOpts)
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

		result, err := consolidators.NewSeriesFetchResult(
			filteredIters, nil, block.NewResultMetadata())
		return result, cleanup, err
	}

	cleanup := func() error {
		iters.Close()
		return nil
	}

	result, err := consolidators.NewSeriesFetchResult(
		iters, nil, block.NewResultMetadata())
	return result, cleanup, err
}

func (q *querier) generateRandomSeries(
	query *storage.FetchQuery,
) (series []parser.IngestSeries, ignoreFilter bool, err error) {
	var (
		start = query.Start.Truncate(q.blockSize)
		end   = query.End.Truncate(q.blockSize).Add(q.blockSize)
	)

	metricNameTag := q.iteratorOpts.TagOptions.MetricName()
	for _, matcher := range query.TagMatchers {
		if bytes.Equal(metricNameTag, matcher.Name) {
			if matched, _ := regexp.Match(`^multi_\d+$`, matcher.Value); matched {
				series, err = q.generateMultiSeriesMetrics(string(matcher.Value), start, end)
				return
			}
			if matched, _ := regexp.Match(`^histogram_\d+_bucket$`, matcher.Value); matched {
				series, err = q.generateHistogramMetrics(string(matcher.Value), start, end)
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
) ([]parser.IngestSeries, error) {
	var (
		gens = []seriesGen{
			{"foo", time.Second},
			{"bar", time.Second * 15},
			{"quail", time.Minute},
		}

		actualGens []seriesGen
	)

	unlock := q.lockAndSeed(start)
	defer unlock()

	metricNameTag := q.iteratorOpts.TagOptions.MetricName()
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

	seriesList := make([]parser.IngestSeries, 0, len(actualGens))
	for _, gen := range actualGens {
		tags := parser.Tags{
			parser.NewTag(model.MetricNameLabel, gen.name),
			parser.NewTag("foobar", "qux"),
			parser.NewTag("name", gen.name),
		}

		series, err := q.generateSeries(start, end, gen.res, tags, false)
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
) ([]parser.IngestSeries, error) {
	suffix := strings.TrimPrefix(metricsName, "multi_")
	seriesCount, err := strconv.Atoi(suffix)
	if err != nil {
		return nil, err
	}

	unlock := q.lockAndSeed(start)
	defer unlock()

	seriesList := make([]parser.IngestSeries, 0, seriesCount)
	for id := 0; id < seriesCount; id++ {
		tags := multiSeriesTags(metricsName, id)

		series, err := q.generateSeries(start, end, q.defaultResolution, tags, false)
		if err != nil {
			return nil, err
		}

		seriesList = append(seriesList, series)
	}

	return seriesList, nil
}

func (q *querier) generateHistogramMetrics(
	metricsName string,
	start time.Time,
	end time.Time,
) ([]parser.IngestSeries, error) {
	suffix := strings.TrimPrefix(metricsName, "histogram_")
	countStr := strings.TrimSuffix(suffix, "_bucket")
	seriesCount, err := strconv.Atoi(countStr)
	if err != nil {
		return nil, err
	}

	unlock := q.lockAndSeed(start)
	defer unlock()

	seriesList := make([]parser.IngestSeries, 0, seriesCount)
	for id := 0; id < seriesCount; id++ {
		le := 1.0
		var previousSeriesBlocks []parser.Data
		for bucket := uint(0); bucket < q.histogramBucketCount; bucket++ {
			tags := multiSeriesTags(metricsName, id)
			leStr := "+Inf"
			if bucket < q.histogramBucketCount-1 {
				leStr = strconv.FormatFloat(le, 'f', -1, 64)
			}
			leTag := parser.NewTag("le", leStr)
			tags = append(tags, leTag)
			le *= 10

			series, err := q.generateSeries(start, end, q.defaultResolution, tags, true)
			if err != nil {
				return nil, err
			}

			for i, prevBlock := range previousSeriesBlocks {
				for j, prevValue := range prevBlock {
					series.Datapoints[i][j].Value += prevValue.Value
				}
			}

			seriesList = append(seriesList, series)

			previousSeriesBlocks = series.Datapoints
		}
	}

	return seriesList, nil
}

func multiSeriesTags(metricsName string, id int) parser.Tags {
	return parser.Tags{
		parser.NewTag(model.MetricNameLabel, metricsName),
		parser.NewTag("id", strconv.Itoa(id)),
		parser.NewTag("parity", strconv.Itoa(id%2)),
		parser.NewTag("const", "x"),
	}
}

func (q *querier) lockAndSeed(start time.Time) func() {
	q.Lock()
	rand.Seed(start.Unix())

	return q.Unlock
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
) (*consolidators.CompleteTagsResult, error) {
	nameOnly := query.CompleteNameOnly
	// TODO: take from config.
	return &consolidators.CompleteTagsResult{
		CompleteNameOnly: nameOnly,
		CompletedTags: []consolidators.CompletedTag{
			{
				Name:   []byte("__name__"),
				Values: [][]byte{[]byte("foo"), []byte("foo"), []byte("quail")},
			},
		},
	}, nil
}
