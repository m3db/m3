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
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	xerrors "github.com/m3db/m3/src/x/errors"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/m3db/m3/src/x/unsafe"
)

const (
	initRawFetchAllocSize = 32

	// series with fewer than minNormalizeLength datapoints are not eligible for
	// normalization.
	minNormalizeLength = 10
	// series with fewer than minNormalizeNonZeroRatio of their values being
	// non-zero are not eligible for normalization.
	minNormalizeNonZeroRatio = 0.8
)

func iteratorToPromResult(
	iter encoding.SeriesIterator,
	tags models.Tags,
	tagOptions models.TagOptions,
) (*prompb.TimeSeries, error) {
	samples := make([]prompb.Sample, 0, initRawFetchAllocSize)
	for iter.Next() {
		dp, _, _ := iter.Current()
		samples = append(samples, prompb.Sample{
			Timestamp: TimeToPromTimestamp(dp.TimestampNanos),
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

var (
	rollupTag              = []byte("__rollup__")
	leTag                  = []byte("le")
	excludeKeysRollupAndLe = [][]byte{rollupTag, leTag}
)

func toProm(
	ctx context.Context,
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	promOptions PromOptions,
) ([]*prompb.TimeSeries, error) {
	var (
		count                 = fetchResult.Count()
		seriesList            = make([]*prompb.TimeSeries, count)
		wg                    sync.WaitGroup
		excludeRollupAndLeMap *seriesGroupMap
		multiErr              xerrors.MultiError
		mu                    sync.Mutex
		maybeNormalizeSeries  bool
	)

	fastWorkerPool := readWorkerPool.FastContextCheck(100)
	for i := 0; i < count; i++ {
		i := i
		iter, tags, err := fetchResult.IterTagsAtIndex(i, tagOptions)
		if err != nil {
			return nil, err
		}

		// If doing aggregate normalization, determine which histograms and counter
		// series require normalization. These normalizations will be applied after
		// the series are unrolled.
		if promOptions.AggregateNormalization {
			// NB: since tags are in name order, it is likely faster to search for
			// the __rollup__ tag in a linear fashion.
			_, rollupTagExists := tags.Get(rollupTag)
			if rollupTagExists {
				leTagValue, leTagExists := tags.Get(leTag)
				if leTagExists {
					var group seriesGroup
					excludeRollupAndLeKey := tags.TagsWithoutKeys(excludeKeysRollupAndLe)
					if excludeRollupAndLeMap == nil {
						excludeRollupAndLeMap = newSeriesGroupMap(count)
					} else {
						elem, ok := excludeRollupAndLeMap.Get(excludeRollupAndLeKey)
						if ok {
							group = elem
						}
					}

					sortValue, err := strconv.ParseFloat(unsafe.String(leTagValue), 64)
					if err != nil {
						return nil, err
					}

					group.entries = append(group.entries, seriesGroupEntry{
						sortValue: sortValue,
						idx:       i,
					})

					excludeRollupAndLeMap.Set(excludeRollupAndLeKey, group)
				} else {
					maybeNormalizeSeries = true
				}
			}
		}

		wg.Add(1)
		maybeNormalizeSeries := maybeNormalizeSeries
		available := fastWorkerPool.GoWithContext(ctx, func() {
			defer wg.Done()
			series, err := iteratorToPromResult(iter, tags, tagOptions)
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
			}

			if maybeNormalizeSeries {
				if shouldNormalizeSeries(series) {
					series = normalizeSeries(series)
				}
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
	// Need to now do a for loop over the groups, sort entries by .sortValue
	// then process by group using the worker pool.
	if excludeRollupAndLeMap != nil {
		var (
			// TODO: if hist series from one group are gathered into a contiguous
			// subslice in the results, can use that result slice directly here,
			// rather than allocating this temporary buffer.
			histogramGroup []*prompb.TimeSeries
			err            error
		)

		for _, hg := range excludeRollupAndLeMap.Iter() {
			group := hg.value

			if histogramGroup == nil {
				histogramGroup = make([]*prompb.TimeSeries, 0, len(group.entries))
			} else {
				histogramGroup = histogramGroup[:0]
			}

			// Sort entries by descending size, so that +Inf is highest and values
			// trickle down.
			sort.Sort(seriesGroupEntriesDesc(group.entries))
			for _, entry := range group.entries {
				histogramGroup = append(histogramGroup, seriesList[entry.idx])
			}

			histogramGroup, err = normalizeAggregatedHistograms(histogramGroup)
			if err != nil {
				return nil, err
			}

			// Now update the series list with the updated histograms.
			for _, entry := range group.entries {
				histogramGroup = append(histogramGroup, seriesList[entry.idx])
			}
		}
	}

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

	filteredList := filterEmpty(seriesList)
	return PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: filteredList,
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

func toDelta(values []prompb.Sample) []prompb.Sample {
	if len(values) < 2 {
		return values
	}

	// NB: always start from 0.
	last := values[0].Value
	values[0].Value = 0
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

func fromDelta(values []prompb.Sample) []prompb.Sample {
	if len(values) < 2 {
		return values
	}

	runningTotal := values[0].Value
	for i := 1; i < len(values); i++ {
		values[i].Value += runningTotal
		runningTotal = values[i].Value
	}

	return values
}

func normalizeSeries(series *prompb.TimeSeries) *prompb.TimeSeries {
	series.Samples = fromDelta(toDelta(series.Samples))
	return series
}

// TODO: this is fairly arbitrary, if there is a better way to detect counters
// and resets generated by aggregators, it should go here. The current strategy
// is to ensure that the series has at least minNormalizeLength points, and is
// made by an oscillating series of real values and zero values; e.g.
//     { 0, 1.0,  0, 2.5, 0, 7...  } should be normalized, but
//     { 0, 1.0, 2.0, 2.5, 0, 7... } should not.
// Furthermore, at least minNormalizeNonZeroRatio/2 of the values should be
// non-zero to be a candidate for normalization. This is divided by 2 since
// half of the values will expectedly be zero.
func shouldNormalizeSeries(series *prompb.TimeSeries) bool {
	if len(series.Samples) < minNormalizeLength {
		return false
	}

	var (
		minNonZero    = float64(len(series.Samples)) * minNormalizeNonZeroRatio / 2
		allZeroesEven = true
		allZeroesOdd  = true

		nonZeroCount int
	)

	for i, v := range series.Samples {
		if i%2 == 0 {
			if v.Value != 0 {
				allZeroesEven = false
				if !allZeroesOdd {
					// NB: shortcircuit since both conditions have failed.
					return false
				}

				nonZeroCount++
			}

			continue
		}

		if v.Value != 0 {
			allZeroesOdd = false
			if !allZeroesEven {
				// NB: shortcircuit since both conditions have failed.
				return false
			}

			nonZeroCount++
		}
	}

	return float64(nonZeroCount) > minNonZero
}

// normalizeAggregatedHistograms receives a histogram series in descending
// order, based on the size of the histogram bucket; this is required since
// prometheus style histogram buckets are necessarily monotonically decreasing
// by bucket size, and aggregation tier may bucket in such a way that a point
// in a high bucket is put in a following timestamp, and the corresponding
// low buckets in the next timestamp are not updated.
func normalizeAggregatedHistograms(
	histSeries []*prompb.TimeSeries,
) ([]*prompb.TimeSeries, error) {
	seriesLen := len(histSeries)
	if seriesLen < 2 {
		return histSeries, nil
	}

	sampleCount := len(histSeries[0].Samples)
	if sampleCount == 0 {
		// no point trying to normalize if there are no datapoints in these series
		return histSeries, nil
	}

	for _, s := range histSeries {
		// verify valid lengths
		if l := len(s.Samples); l != sampleCount {
			return nil, fmt.Errorf(
				"mismatched histogram sample counts: expected %d, got %d",
				sampleCount, l)
		}
	}

	// Now, iterate each datapoint in a step-wise fashion, by their timestamps.
	for idx := 0; idx < sampleCount; idx++ {
		var (
			first    = histSeries[0].Samples[idx]
			ts       = first.Timestamp
			minValue = first.Value
		)

		for i := 1; i < seriesLen; i++ {
			sample := histSeries[i].Samples[idx]
			if curr := sample.Timestamp; ts != curr {
				// Ensure timestamps match per-step.
				return nil, fmt.Errorf(
					"mismatched timestamps at step %d: expected %s, got %s",
					i, xtime.UnixNano(ts), xtime.UnixNano(curr))
			}

			if sample.Value < minValue {
				histSeries[i].Samples[idx].Value = minValue
			} else {
				minValue = sample.Value
			}
		}
	}

	return histSeries, nil
}

type seriesGroup struct {
	entries []seriesGroupEntry
}

type seriesGroupEntriesDesc []seriesGroupEntry

var _ sort.Interface = (*seriesGroupEntriesDesc)(nil)

func (e seriesGroupEntriesDesc) Len() int { return len(e) }

func (e seriesGroupEntriesDesc) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

func (e seriesGroupEntriesDesc) Less(i, j int) bool {
	iVal, jVal := e[i].sortValue, e[j].sortValue
	if math.IsInf(iVal, 1) {
		return true
	} else if math.IsInf(jVal, 1) {
		return false
	}

	return iVal > jVal
}

type seriesGroupEntry struct {
	sortValue float64
	idx       int
}
