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
	"sync"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	xcost "github.com/m3db/m3/src/x/cost"
	xerrors "github.com/m3db/m3/src/x/errors"
	xsync "github.com/m3db/m3/src/x/sync"
)

const initRawFetchAllocSize = 32

func iteratorToPromResult(
	iter encoding.SeriesIterator,
	tags models.Tags,
	enforcer cost.ChainedEnforcer,
	tagOptions models.TagOptions,
) (*prompb.TimeSeries, error) {
	samples := make([]prompb.Sample, 0, initRawFetchAllocSize)
	for iter.Next() {
		dp, _, _ := iter.Current()
		samples = append(samples, prompb.Sample{
			Timestamp: TimeToPromTimestamp(dp.Timestamp),
			Value:     dp.Value,
		})
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	r := enforcer.Add(xcost.Cost(len(samples)))
	if r.Error != nil {
		return nil, r.Error
	}

	return &prompb.TimeSeries{
		Labels:  TagsToPromLabels(tags),
		Samples: samples,
	}, nil
}

// Fall back to sequential decompression if unable to decompress concurrently.
func toPromSequentially(
	fetchResult consolidators.SeriesFetchResult,
	enforcer cost.ChainedEnforcer,
	tagOptions models.TagOptions,
) (PromResult, error) {
	count := fetchResult.Count()
	seriesList := make([]*prompb.TimeSeries, 0, count)
	for i := 0; i < count; i++ {
		iter, tags, err := fetchResult.IterTagsAtIndex(i, tagOptions)
		if err != nil {
			return PromResult{}, err
		}

		series, err := iteratorToPromResult(iter, tags, enforcer, tagOptions)
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
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	enforcer cost.ChainedEnforcer,
	tagOptions models.TagOptions,
) (PromResult, error) {
	count := fetchResult.Count()
	var (
		seriesList = make([]*prompb.TimeSeries, count)

		wg       sync.WaitGroup
		multiErr xerrors.MultiError
		mu       sync.Mutex
	)

	for i := 0; i < count; i++ {
		i := i
		iter, tags, err := fetchResult.IterTagsAtIndex(i, tagOptions)
		if err != nil {
			return PromResult{}, err
		}

		wg.Add(1)
		readWorkerPool.Go(func() {
			defer wg.Done()
			series, err := iteratorToPromResult(iter, tags, enforcer, tagOptions)
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
			}

			seriesList[i] = series
		})
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
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	enforcer cost.ChainedEnforcer,
	tagOptions models.TagOptions,
) (PromResult, error) {
	if readWorkerPool == nil {
		return toPromSequentially(fetchResult, enforcer, tagOptions)
	}

	return toPromConcurrently(fetchResult, readWorkerPool, enforcer, tagOptions)
}

// SeriesIteratorsToPromResult converts raw series iterators directly to a
// Prometheus-compatible result.
func SeriesIteratorsToPromResult(
	fetchResult consolidators.SeriesFetchResult,
	readWorkerPool xsync.PooledWorkerPool,
	enforcer cost.ChainedEnforcer,
	tagOptions models.TagOptions,
) (PromResult, error) {
	defer fetchResult.Close()
	if err := fetchResult.Verify(); err != nil {
		return PromResult{}, err
	}

	if enforcer == nil {
		enforcer = cost.NoopChainedEnforcer()
	}

	promResult, err := seriesIteratorsToPromResult(fetchResult,
		readWorkerPool, enforcer, tagOptions)
	promResult.Metadata = fetchResult.Metadata
	return promResult, err
}
