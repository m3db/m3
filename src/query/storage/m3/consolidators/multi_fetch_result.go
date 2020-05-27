// Copyright (c) 2018 Uber Technologies, Inc.
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

package consolidators

import (
	"sync"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	xerrors "github.com/m3db/m3/src/x/errors"
)

type fetchDedupeMap interface {
	add(iter encoding.SeriesIterator, attrs storage.Attributes) error
	list() []multiResultSeries
}

// TODO: use a better seriesIterators merge here
type multiResult struct {
	sync.Mutex
	metadata        block.ResultMetadata
	fanout          QueryFanoutType
	seenFirstAttrs  storage.Attributes
	seenIters       []encoding.SeriesIterators // track known iterators to avoid leaking
	mergedIterators encoding.MutableSeriesIterators
	dedupeMap       fetchDedupeMap //map[string]multiResultSeries
	err             xerrors.MultiError

	pools encoding.IteratorPools
}

// NewMultiFetchResult builds a new multi fetch result.
func NewMultiFetchResult(
	fanout QueryFanoutType,
	pools encoding.IteratorPools,
) MultiFetchResult {
	return &multiResult{
		metadata: block.NewResultMetadata(),
		fanout:   fanout,
		pools:    pools,
	}
}

type multiResultSeries struct {
	attrs storage.Attributes
	iter  encoding.SeriesIterator
}

func (r *multiResult) Close() error {
	r.Lock()
	defer r.Unlock()

	for _, iters := range r.seenIters {
		iters.Close()
	}
	r.seenIters = nil

	if r.mergedIterators != nil {
		// NB(r): Since all the series iterators in the final result are held onto
		// by the original iters in the seenIters slice we allow those iterators
		// to free iterators held onto by final result, and reset the slice for
		// the final result to zero so we avoid double returning the iterators
		// themselves.
		r.mergedIterators.Reset(0)
		r.mergedIterators.Close()
		r.mergedIterators = nil
	}

	r.dedupeMap = nil
	r.err = xerrors.NewMultiError()

	return nil
}

func (r *multiResult) FinalResultWithAttrs() (SeriesFetchResult,
	[]storage.Attributes, error) {
	result, err := r.FinalResult()
	if err != nil {
		return result, nil, err
	}

	attrs := make([]storage.Attributes, result.SeriesIterators.Len())
	// TODO: add testing around here.
	if r.dedupeMap == nil {
		for i := range attrs {
			attrs[i] = r.seenFirstAttrs
		}
	} else {
		i := 0
		for _, res := range r.dedupeMap.list() {
			attrs[i] = res.attrs
			i++
		}
	}

	return result, attrs, nil
}

func (r *multiResult) FinalResult() (SeriesFetchResult, error) {
	r.Lock()
	defer r.Unlock()

	result := SeriesFetchResult{Metadata: r.metadata}
	err := r.err.LastError()
	if err != nil {
		return result, err
	}

	if r.mergedIterators != nil {
		result.SeriesIterators = r.mergedIterators
		return result, nil
	}

	if len(r.seenIters) == 0 {
		result.SeriesIterators = encoding.EmptySeriesIterators
		return result, nil
	}

	// can short-cicuit in this case
	if len(r.seenIters) == 1 {
		result.SeriesIterators = r.seenIters[0]
		return result, nil
	}

	// otherwise have to create a new seriesiters
	dedupedList := r.dedupeMap.list()
	numSeries := len(dedupedList)
	r.mergedIterators = r.pools.MutableSeriesIterators().Get(numSeries)
	r.mergedIterators.Reset(numSeries)

	for i, res := range dedupedList {
		r.mergedIterators.SetAt(i, res.iter)
	}

	result.SeriesIterators = r.mergedIterators
	return result, nil
}

func (r *multiResult) Add(
	fetchResult SeriesFetchResult,
	attrs storage.Attributes,
	err error,
) {
	r.Lock()
	defer r.Unlock()

	if err != nil {
		r.err = r.err.Add(err)
		return
	}

	if len(r.seenIters) == 0 {
		// store the first attributes seen
		r.seenFirstAttrs = attrs
		r.metadata = fetchResult.Metadata
	} else {
		// NB: any non-exhaustive result set added makes the entire
		// result non-exhaustive
		r.metadata = r.metadata.CombineMetadata(fetchResult.Metadata)
	}

	newIterators := fetchResult.SeriesIterators
	r.seenIters = append(r.seenIters, newIterators)
	// Need to check the error to bail early after accumulating the iterators
	// otherwise when we close the the multi fetch result
	if !r.err.Empty() {
		// don't need to do anything if the final result is going to be an error
		return
	}

	if len(r.seenIters) < 2 {
		// don't need to create the de-dupe map until we need to actually need to
		// dedupe between two results
		return
	}

	if len(r.seenIters) == 2 {
		// need to backfill the dedupe map from the first result first
		first := r.seenIters[0]
		// FIXME: split on config whether to use ID map or Tag map.
		r.dedupeMap = newIDDedupeMap(r.fanout, first.Len())
		r.addOrUpdateDedupeMap(r.seenFirstAttrs, first)
	}

	// Now de-duplicate
	r.addOrUpdateDedupeMap(attrs, newIterators)
}

func (r *multiResult) addOrUpdateDedupeMap(
	attrs storage.Attributes,
	newIterators encoding.SeriesIterators,
) {
	for _, iter := range newIterators.Iters() {
		if err := r.dedupeMap.add(iter, attrs); err != nil {
			r.err = r.err.Add(err)
		}
	}
}
