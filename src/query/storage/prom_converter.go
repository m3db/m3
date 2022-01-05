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
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	xerrors "github.com/m3db/m3/src/x/errors"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
)

const initRawFetchAllocSize = 32

func iteratorToPromResult(
	iter encoding.SeriesIterator,
	tags models.Tags,
	maxResolution time.Duration,
	promConvertOptions PromConvertOptions,
) (*prompb.TimeSeries, error) {
	var (
		resolution          = xtime.UnixNano(maxResolution)
		resolutionThreshold = promConvertOptions.ResolutionThresholdForCounterNormalization()

		valueDecreaseTolerance      = promConvertOptions.ValueDecreaseTolerance()
		valueDecreaseToleranceUntil = promConvertOptions.ValueDecreaseToleranceUntil()

		firstDP           = true
		handleResets      = false
		annotationPayload annotation.Payload

		cumulativeSum float64
		prevDP        ts.Datapoint

		samples = make([]prompb.Sample, 0, initRawFetchAllocSize)
	)

	for iter.Next() {
		dp, _, _ := iter.Current()

		if valueDecreaseTolerance > 0 && dp.TimestampNanos.Before(valueDecreaseToleranceUntil) {
			if !firstDP && dp.Value < prevDP.Value && dp.Value > prevDP.Value*(1-valueDecreaseTolerance) {
				dp.Value = prevDP.Value
			}
		}

		if firstDP && maxResolution >= resolutionThreshold {
			firstAnnotation := iter.FirstAnnotation()
			if len(firstAnnotation) > 0 {
				if err := annotationPayload.Unmarshal(firstAnnotation); err != nil {
					return nil, err
				}
				handleResets = annotationPayload.OpenMetricsHandleValueResets
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
		} else {
			samples = append(samples, prompb.Sample{
				Timestamp: TimeToPromTimestamp(dp.TimestampNanos),
				Value:     dp.Value,
			})
		}

		prevDP = dp
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
	promConvertOptions PromConvertOptions,
	fetchOptions *FetchOptions,
) (PromResult, error) {
	meta := block.NewResultMetadata()
	count := fetchResult.Count()
	seriesList := make([]*prompb.TimeSeries, 0, count)
	for i := 0; i < count; i++ {
		iter, tags, err := fetchResult.IterTagsAtIndex(i, tagOptions)
		if err != nil {
			return PromResult{}, err
		}

		series, err := iteratorToPromResult(iter, tags, maxResolution, promConvertOptions)
		if err != nil {
			return PromResult{}, err
		}

		if len(series.GetSamples()) > 0 {
			seriesList = append(seriesList, series)
		}

		if fetchOptions != nil && fetchOptions.MaxMetricMetadataStats > 0 {
			name, _ := tags.Get(promDefaultName)
			if len(series.GetSamples()) > 0 {
				meta.ByName(name).WithSamples++
			} else {
				meta.ByName(name).NoSamples++
			}
		}
	}

	return PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: seriesList,
		},
		Metadata: meta,
	}, nil
}

func toPromConcurrently(
	ctx context.Context,
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	maxResolution time.Duration,
	promConvertOptions PromConvertOptions,
	fetchOptions *FetchOptions,
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
			mu.Lock()
			multiErr = multiErr.Add(err)
			mu.Unlock()
			break
		}

		wg.Add(1)
		available := fastWorkerPool.GoWithContext(ctx, func() {
			defer wg.Done()
			series, err := iteratorToPromResult(iter, tags, maxResolution, promConvertOptions)
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
	meta := block.NewResultMetadata()
	filteredList := seriesList[:0]
	for _, series := range seriesList {
		if len(series.GetSamples()) > 0 {
			filteredList = append(filteredList, series)
		}

		if fetchOptions != nil && fetchOptions.MaxMetricMetadataStats > 0 {
			name := metricNameFromLabels(series.Labels)
			if len(series.GetSamples()) > 0 {
				meta.ByName(name).WithSamples++
			} else {
				meta.ByName(name).NoSamples++
			}
		}
	}

	return PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: filteredList,
		},
		Metadata: meta,
	}, nil
}

func seriesIteratorsToPromResult(
	ctx context.Context,
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	maxResolution time.Duration,
	promConvertOptions PromConvertOptions,
	fetchOptions *FetchOptions,
) (PromResult, error) {
	if readWorkerPool == nil {
		return toPromSequentially(fetchResult, tagOptions, maxResolution,
			promConvertOptions, fetchOptions)
	}

	return toPromConcurrently(ctx, fetchResult, readWorkerPool, tagOptions, maxResolution,
		promConvertOptions, fetchOptions)
}

// SeriesIteratorsToPromResult converts raw series iterators directly to a
// Prometheus-compatible result.
func SeriesIteratorsToPromResult(
	ctx context.Context,
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	promConvertOptions PromConvertOptions,
	fetchOptions *FetchOptions,
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
		readWorkerPool, tagOptions, maxResolution, promConvertOptions, fetchOptions)
	// Combine the fetchResult metadata into any metadata that was already
	// computed for this promResult.
	promResult.Metadata = promResult.Metadata.CombineMetadata(fetchResult.Metadata)

	return promResult, err
}
