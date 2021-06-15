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

package storage

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	xerrors "github.com/m3db/m3/src/x/errors"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
)

const initRawFetchAllocSize = 32

var rollupTags = [][]byte{
	[]byte("__rollup__"),
	[]byte("__rollup_type__"),
}

func iteratorToPromResult(
	iter encoding.SeriesIterator,
	tags models.Tags,
	tagOptions models.TagOptions,
) (*prompb.TimeSeries, error) {
	samples := make([]prompb.Sample, 0, initRawFetchAllocSize)
	for iter.Next() {
		dp, _, _ := iter.Current()
		samples = append(samples, prompb.Sample{
			Timestamp: int64(dp.TimestampNanos),
			Value:     dp.Value,
		})
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return &prompb.TimeSeries{
		Labels:  TagsToPromLabels(tags),
		Samples: samples,
	}, nil
}

func toProm(
	ctx context.Context,
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	promOptions PromOptions,
) ([]*prompb.TimeSeries, error) {
	var (
		count       = fetchResult.Count()
		seriesList  = make([]*prompb.TimeSeries, count)
		normalizing = promOptions.ShouldNormalizeAggregation()

		wg       sync.WaitGroup
		multiErr xerrors.MultiError
		mu       sync.Mutex
	)

	fastWorkerPool := readWorkerPool.FastContextCheck(100)
	for i := 0; i < count; i++ {
		i := i
		iter, tags, err := fetchResult.IterTagsAtIndex(i, tagOptions)
		if err != nil {
			return nil, err
		}

		wg.Add(1)
		available := fastWorkerPool.GoWithContext(ctx, func() {
			defer wg.Done()
			series, err := iteratorToPromResult(iter, tags, tagOptions)
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
			}

			if normalizing && shouldNormalizeSeries(tags) {
				series = normalizeSeries(
					promOptions.NormalizedAggregationStart,
					promOptions.AggregateNormalizationWindow,
					series,
				)
			}

			seriesList[i] = series
		})
		if !available {
			wg.Done()
			mu.Lock()
			multiErr = multiErr.Add(ctx.Err())
			mu.Unlock()
			break
		}
	}

	wg.Wait()

	if err := multiErr.LastError(); err != nil {
		return nil, err
	}

	return seriesList, nil
}

// filterEmpty removes all-empty series in place.
// NB: this mutates incoming slice.
func filterEmpty(seriesList []*prompb.TimeSeries) []*prompb.TimeSeries {
	filteredList := seriesList[:0]
	for _, s := range seriesList {
		if len(s.GetSamples()) > 0 {
			filteredList = append(filteredList, s)
		}
	}

	return filteredList
}

// filterEmpty removes all-empty series in place.
// NB: this mutates incoming slice.
func dropPointsBeforeAndFilterEmpty(
	seriesList []*prompb.TimeSeries,
	earliest xtime.UnixNano,
) []*prompb.TimeSeries {
	filtered := seriesList[:0]
	for idx, s := range seriesList {
		l := len(s.Samples)
		s := s
		firstPointIdx := sort.Search(l, func(idx int) bool {
			ts := xtime.UnixNano(s.Samples[idx].Timestamp)
			return ts.After(earliest)
		})

		if firstPointIdx == l {
			// NB: no points in series, can skip it.
			continue
		}

		seriesList[idx].Samples = seriesList[idx].Samples[firstPointIdx:]
		filtered = append(filtered, seriesList[idx])
	}

	return filtered
}

func seriesIteratorsToPromResult(
	ctx context.Context,
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	promOptions PromOptions,
) (PromResult, error) {
	seriesList, err := toProm(ctx, fetchResult, readWorkerPool, tagOptions, promOptions)
	if err != nil {
		return PromResult{}, err
	}

	if promOptions.InitialStart != promOptions.NormalizedAggregationStart {
		seriesList = dropPointsBeforeAndFilterEmpty(seriesList, promOptions.InitialStart)
	} else {
		seriesList = filterEmpty(seriesList)
	}

	for seriesIdx, series := range seriesList {
		for pointIdx, point := range series.Samples {
			seriesList[seriesIdx].Samples[pointIdx].Timestamp = TimeToPromTimestamp(xtime.UnixNano(point.Timestamp))
		}
	}

	return PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: seriesList,
		},
	}, nil
}

// SeriesIteratorsToPromResult converts raw series iterators directly to a
// Prometheus-compatible result.
func SeriesIteratorsToPromResult(
	ctx context.Context,
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	promOptions PromOptions,
) (PromResult, error) {
	defer fetchResult.Close()
	if err := fetchResult.Verify(); err != nil {
		return PromResult{}, err
	}

	promResult, err := seriesIteratorsToPromResult(
		ctx,
		fetchResult,
		readWorkerPool,
		tagOptions,
		promOptions,
	)
	promResult.Metadata = fetchResult.Metadata
	return promResult, err
}

func toDelta(
	values []prompb.Sample,
) []prompb.Sample {
	if len(values) < 2 {
		return values
	}

	last := values[0].Value
	for i := 1; i < len(values); i++ {
		curr := values[i].Value
		diff := curr - last
		if diff < 0 {
			diff = curr
		}

		values[i].Value = diff
		last = curr
	}

	return values
}

func bucketedDelta(
	boundary xtime.UnixNano,
	windowSize time.Duration,
	values []prompb.Sample,
) []prompb.Sample {
	var (
		numPoints                 = len(values)
		bucketBoundaryAtNextPoint = false
		runningTotal              = values[0].Value
	)

	if numPoints < 2 {
		return values
	}

	for i := 1; i < numPoints-1; i++ {
		// NB: reset values every windowSize, based from the first boundary value,
		// where the deltas are zero.
		var (
			nextPointAtOrAfterBoundary = !xtime.UnixNano(values[i+1].Timestamp).Before(boundary)
			val                        = values[i].Value
		)

		if val == 0 && bucketBoundaryAtNextPoint {
			// NB: this is the first zero point after a boundary; apply a reset here
			// and continue to the next point.
			bucketBoundaryAtNextPoint = false
			runningTotal = 0
			continue
		}

		if nextPointAtOrAfterBoundary {
			boundary = boundary.Add(windowSize)
			if val == 0 {
				// NB: if next point is after the boundary, and the current value is
				// zero, apply the reset at the current point.
				runningTotal = 0
				continue
			} else {
				// Otherwise, reset at the next zero point.
				bucketBoundaryAtNextPoint = true
			}
		}

		values[i].Value += runningTotal
		runningTotal = values[i].Value
	}

	// Now account for the last point. This should be reset iff the value of the
	// point is zero, and it crosses a bucket boundary.
	if values[numPoints-1].Value == 0 && bucketBoundaryAtNextPoint {
		values[numPoints-1].Value = 0
	} else {
		values[numPoints-1].Value += runningTotal
	}

	return values
}

func normalizeSeries(
	boundary xtime.UnixNano,
	windowSize time.Duration,
	series *prompb.TimeSeries,
) *prompb.TimeSeries {
	series.Samples = bucketedDelta(boundary, windowSize, toDelta(series.Samples))
	return series
}

func shouldNormalizeSeries(tags models.Tags) bool {
	for _, tag := range rollupTags {
		if _, found := tags.Get(tag); !found {
			return false
		}
	}

	return true
}
