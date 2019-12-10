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
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	xcost "github.com/m3db/m3/src/x/cost"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
)

func cloneBytes(b []byte) []byte {
	return append(make([]byte, 0, len(b)), b...)
}

func tagIteratorToLabels(
	identTags ident.TagIterator,
) ([]prompb.Label, error) {
	labels := make([]prompb.Label, 0, identTags.Remaining())
	for identTags.Next() {
		identTag := identTags.Current()
		labels = append(labels, prompb.Label{
			Name:  cloneBytes(identTag.Name.Bytes()),
			Value: cloneBytes(identTag.Value.Bytes()),
		})
	}

	if err := identTags.Err(); err != nil {
		return nil, err
	}

	return labels, nil
}

func iteratorToPromResult(
	iter encoding.SeriesIterator,
	enforcer cost.ChainedEnforcer,
	tagOptions models.TagOptions,
) (*prompb.TimeSeries, error) {
	labels, err := tagIteratorToLabels(iter.Tags())
	if err != nil {
		return nil, err
	}

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
		Labels:  labels,
		Samples: samples,
	}, nil
}

// Fall back to sequential decompression if unable to decompress concurrently.
func toPromSequentially(
	iters []encoding.SeriesIterator,
	enforcer cost.ChainedEnforcer,
	metadata block.ResultMetadata,
	tagOptions models.TagOptions,
) (PromResult, error) {
	seriesList := make([]*prompb.TimeSeries, 0, len(iters))
	for _, iter := range iters {
		series, err := iteratorToPromResult(iter, enforcer, tagOptions)
		if err != nil {
			return PromResult{}, err
		}

		if len(series.GetSamples()) == 0 {
			continue
		}

		seriesList = append(seriesList, series)
	}

	return PromResult{
		Metadata: metadata,
		PromResult: &prompb.QueryResult{
			Timeseries: seriesList,
		},
	}, nil
}

func toPromConcurrently(
	iters []encoding.SeriesIterator,
	readWorkerPool xsync.PooledWorkerPool,
	enforcer cost.ChainedEnforcer,
	metadata block.ResultMetadata,
	tagOptions models.TagOptions,
) (PromResult, error) {
	var (
		seriesList = make([]*prompb.TimeSeries, len(iters))

		wg       sync.WaitGroup
		multiErr xerrors.MultiError
		mu       sync.Mutex
	)

	for i, iter := range iters {
		i, iter := i, iter
		wg.Add(1)
		readWorkerPool.Go(func() {
			defer wg.Done()
			series, err := iteratorToPromResult(iter, enforcer, tagOptions)
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
		Metadata: metadata,
		PromResult: &prompb.QueryResult{
			Timeseries: seriesList,
		},
	}, nil
}

// SeriesIteratorsToPromResult converts raw series iterators directly to a
// Prometheus-compatible result.
func SeriesIteratorsToPromResult(
	seriesIterators encoding.SeriesIterators,
	readWorkerPool xsync.PooledWorkerPool,
	metadata block.ResultMetadata,
	enforcer cost.ChainedEnforcer,
	tagOptions models.TagOptions,
) (PromResult, error) {
	defer seriesIterators.Close()

	iters := seriesIterators.Iters()
	if readWorkerPool == nil {
		return toPromSequentially(iters, enforcer, metadata, tagOptions)
	}

	return toPromConcurrently(iters, readWorkerPool,
		enforcer, metadata, tagOptions)
}
