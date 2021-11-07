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
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/generated/proto/annotation"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	xerrors "github.com/m3db/m3/src/x/errors"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	initRawFetchAllocSize = 32

	resolutionThresholdForCounterNormalization = time.Hour
)

func iteratorToPromResult(
	iter encoding.SeriesIterator,
	tags models.Tags,
	maxResolution time.Duration,
) (*prompb.TimeSeries, error) {
	var (
		resolution = xtime.UnixNano(maxResolution)

		firstDP           = true
		handleResets      = false
		annotationPayload annotation.Payload

		cumulativeSum float64
		prevDP        ts.Datapoint

		samples = make([]prompb.Sample, 0, initRawFetchAllocSize)
	)

	for iter.Next() {
		dp, _, _ := iter.Current()

		if firstDP && maxResolution >= resolutionThresholdForCounterNormalization {
			firstAnnotation := iter.FirstAnnotation()
			if len(firstAnnotation) > 0 {
				if err := annotationPayload.Unmarshal(firstAnnotation); err != nil {
					return nil, err
				}
				handleResets = annotationPayload.HandleValueResets
			}
		}

		if handleResets {
			if dp.TimestampNanos/resolution != prevDP.TimestampNanos/resolution && !firstDP {
				// reached next resolution window, emit previous DP
				samples = append(samples, prompb.Sample{
					Timestamp: TimeToPromTimestamp(prevDP.TimestampNanos),
					Value:     cumulativeSum,
				})
			}

			if dp.Value < prevDP.Value { // counter reset
				cumulativeSum += dp.Value
			} else {
				cumulativeSum += dp.Value - prevDP.Value
			}

			prevDP = dp
		} else {
			samples = append(samples, prompb.Sample{
				Timestamp: TimeToPromTimestamp(dp.TimestampNanos),
				Value:     dp.Value,
			})
		}

		firstDP = false
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	if handleResets {
		samples = append(samples, prompb.Sample{
			Timestamp: TimeToPromTimestamp(prevDP.TimestampNanos),
			Value:     cumulativeSum,
		})
	}

	return &prompb.TimeSeries{
		Labels:  TagsToPromLabels(tags),
		Samples: samples,
	}, nil
}

// Fall back to sequential decompression if unable to decompress concurrently.
func toPromSequentially(
	fetchResult consolidators.SeriesFetchResult,
	tagOptions models.TagOptions,
	maxResolution time.Duration,
) (PromResult, error) {
	count := fetchResult.Count()
	seriesList := make([]*prompb.TimeSeries, 0, count)
	for i := 0; i < count; i++ {
		iter, tags, err := fetchResult.IterTagsAtIndex(i, tagOptions)
		if err != nil {
			return PromResult{}, err
		}

		series, err := iteratorToPromResult(iter, tags, maxResolution)
		if err != nil {
			return PromResult{}, err
		}

		if len(series.GetSamples()) > 0 {
			seriesList = append(seriesList, series)
		}
	}

	return PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: seriesList,
		},
	}, nil
}

func toPromConcurrently(
	ctx context.Context,
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	maxResolution time.Duration,
) (PromResult, error) {
	count := fetchResult.Count()
	var (
		seriesList = make([]*prompb.TimeSeries, count)

		wg       sync.WaitGroup
		multiErr xerrors.MultiError
		mu       sync.Mutex
	)

	fastWorkerPool := readWorkerPool.FastContextCheck(100)
	for i := 0; i < count; i++ {
		i := i
		iter, tags, err := fetchResult.IterTagsAtIndex(i, tagOptions)
		if err != nil {
			return PromResult{}, err
		}

		wg.Add(1)
		available := fastWorkerPool.GoWithContext(ctx, func() {
			defer wg.Done()
			series, err := iteratorToPromResult(iter, tags, maxResolution)
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
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
		return PromResult{}, err
	}

	// Filter out empty series inplace.
	filteredList := seriesList[:0]
	for _, s := range seriesList {
		if len(s.GetSamples()) > 0 {
			filteredList = append(filteredList, s)
		}
	}

	return PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: filteredList,
		},
	}, nil
}

func seriesIteratorsToPromResult(
	ctx context.Context,
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	maxResolution time.Duration,
) (PromResult, error) {
	if readWorkerPool == nil {
		return toPromSequentially(fetchResult, tagOptions, maxResolution)
	}

	return toPromConcurrently(ctx, fetchResult, readWorkerPool, tagOptions, maxResolution)
}

// SeriesIteratorsToPromResult converts raw series iterators directly to a
// Prometheus-compatible result.
func SeriesIteratorsToPromResult(
	ctx context.Context,
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
) (PromResult, error) {
	defer fetchResult.Close()
	if err := fetchResult.Verify(); err != nil {
		return PromResult{}, err
	}

	var maxResolution time.Duration
	for _, res := range fetchResult.Metadata.Resolutions {
		if res > maxResolution {
			maxResolution = res
		}
	}

	promResult, err := seriesIteratorsToPromResult(ctx, fetchResult,
		readWorkerPool, tagOptions, maxResolution)
	promResult.Metadata = fetchResult.Metadata

	return promResult, err
}
