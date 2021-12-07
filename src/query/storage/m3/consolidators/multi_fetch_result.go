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
	"errors"
	"fmt"
	"sync"

	"github.com/m3db/m3/src/dbnode/encoding"
	terrors "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/errors"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
)

type fetchDedupeMap interface {
	add(iter encoding.SeriesIterator, attrs storagemetadata.Attributes, narrowing Narrowing) error
	update(iter encoding.SeriesIterator, attrs storagemetadata.Attributes, narrowing Narrowing) (bool, error)
	list() []multiResultSeries
	len() int
	close()
}

type multiResult struct {
	sync.Mutex
	metadata           block.ResultMetadata
	fanout             QueryFanoutType
	seenFirstAttrs     storagemetadata.Attributes
	seenFirstNarrowing Narrowing

	seenIters       []encoding.SeriesIterators // track known iterators to avoid leaking
	mergedIterators encoding.MutableSeriesIterators
	mergedTags      []*models.Tags
	dedupeMap       fetchDedupeMap
	err             xerrors.MultiError
	matchOpts       MatchOptions
	tagOpts         models.TagOptions
	limitOpts       LimitOptions

	all []MultiFetchResults

	pools encoding.IteratorPools
}

// LimitOptions specifies the limits when accumulating results in consolidators.
type LimitOptions struct {
	Limit             int
	RequireExhaustive bool
}

// NewMultiFetchResult builds a new multi fetch result.
func NewMultiFetchResult(
	fanout QueryFanoutType,
	pools encoding.IteratorPools,
	opts MatchOptions,
	tagOpts models.TagOptions,
	limitOpts LimitOptions,
) MultiFetchResult {
	return &multiResult{
		metadata:  block.NewResultMetadata(),
		fanout:    fanout,
		pools:     pools,
		matchOpts: opts,
		tagOpts:   tagOpts,
		limitOpts: limitOpts,
	}
}

type multiResultSeries struct {
	attrs     storagemetadata.Attributes
	iter      encoding.SeriesIterator
	tags      models.Tags
	narrowing Narrowing
}

func (r *multiResult) Close() error {
	r.Lock()
	defer r.Unlock()

	for _, iters := range r.seenIters {
		if iters != nil {
			iters.Close()
		}
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

func (r *multiResult) FinalResultWithAttrs() (
	SeriesFetchResult, []storagemetadata.Attributes, error,
) {
	result, err := r.FinalResult()
	if err != nil {
		return result, nil, err
	}

	var attrs []storagemetadata.Attributes
	seriesData := result.seriesData
	if iters := seriesData.seriesIterators; iters != nil {
		l := iters.Len()
		attrs = make([]storagemetadata.Attributes, 0, l)
		if r.dedupeMap == nil {
			for i := 0; i < l; i++ {
				attrs = append(attrs, r.seenFirstAttrs)
			}
		} else {
			for _, res := range r.dedupeMap.list() {
				attrs = append(attrs, res.attrs)
			}
		}
	}

	return result, attrs, nil
}

func (r *multiResult) FinalResult() (SeriesFetchResult, error) {
	r.Lock()
	defer r.Unlock()

	err := r.err.LastError()
	if err != nil {
		return NewEmptyFetchResult(r.metadata), err
	}

	if r.mergedIterators != nil {
		return NewSeriesFetchResult(r.mergedIterators, nil, r.metadata)
	}

	if len(r.seenIters) == 0 {
		return NewSeriesFetchResult(encoding.EmptySeriesIterators, nil, r.metadata)
	}

	// otherwise have to create a new seriesiters
	dedupedList := r.dedupeMap.list()
	numSeries := len(dedupedList)
	r.mergedIterators = r.pools.MutableSeriesIterators().Get(numSeries)
	r.mergedIterators.Reset(numSeries)
	if r.mergedTags == nil {
		r.mergedTags = make([]*models.Tags, numSeries)
	}

	lenCurr, lenNext := len(r.mergedTags), len(dedupedList)
	if lenCurr < lenNext {
		// If incoming list is longer, expand the stored list.
		r.mergedTags = append(r.mergedTags, make([]*models.Tags, lenNext-lenCurr)...)
	} else if lenCurr > lenNext {
		// If incoming list somehow shorter, shrink stored list.
		r.mergedTags = r.mergedTags[:lenNext]
	}

	for i, res := range dedupedList {
		r.mergedIterators.SetAt(i, res.iter)
		r.mergedTags[i] = &dedupedList[i].tags
	}

	return NewSeriesFetchResult(r.mergedIterators, r.mergedTags, r.metadata)
}

func (r *multiResult) Results() []MultiFetchResults {
	r.Lock()
	defer r.Unlock()
	return r.all
}

func (r *multiResult) Add(add MultiFetchResults) {
	var (
		newIterators = add.SeriesIterators
		metadata     = add.Metadata
		attrs        = add.Attrs
		narrowing    = add.Narrowing
	)

	r.Lock()
	defer r.Unlock()

	r.all = append(r.all, add)

	if err := add.Err; err != nil {
		r.err = r.err.Add(err)
		return
	}

	if newIterators == nil || newIterators.Len() == 0 {
		return
	}

	nsID := ""
	if newIterators.Iters()[0].Namespace() != nil {
		nsID = newIterators.Iters()[0].Namespace().String() // sometimes the namespace ID is empty
	}

	// the series limit was reached within this namespace.
	if !metadata.Exhaustive && r.limitOpts.RequireExhaustive {
		r.err = r.err.Add(NewLimitError(fmt.Sprintf("series limit exceeded for namespace %s", nsID)))
		return
	}

	if len(r.seenIters) == 0 {
		// store the first attributes seen
		r.seenFirstAttrs = attrs
		r.seenFirstNarrowing = narrowing
	} else if !r.metadata.Exhaustive {
		// a previous namespace result already hit the limit, so bail. this handles the case of RequireExhaustive=false
		// and there is no error to short circuit.
		return
	}

	// NB: any non-exhaustive result set added makes the entire
	// result non-exhaustive
	// Note: must never override metadata and always use CombineMetadata
	// in case warnings were first set with call to AddWarnings(..) and
	// then must be combined before first result is ever set.
	r.metadata = r.metadata.CombineMetadata(metadata)

	r.seenIters = append(r.seenIters, newIterators)
	// Need to check the error to bail early after accumulating the iterators
	// otherwise when we close the the multi fetch result
	if !r.err.Empty() {
		// don't need to do anything if the final result is going to be an error
		return
	}

	var added bool
	if len(r.seenIters) == 1 {
		// need to backfill the dedupe map from the first result first
		first := r.seenIters[0]
		opts := tagMapOpts{
			fanout:  r.fanout,
			size:    first.Len(),
			tagOpts: r.tagOpts,
		}

		if r.matchOpts.MatchType == MatchIDs {
			r.dedupeMap = newIDDedupeMap(opts)
		} else {
			r.dedupeMap = newTagDedupeMap(opts)
		}

		added = r.addOrUpdateDedupeMap(r.seenFirstAttrs, r.seenFirstNarrowing, first)
	} else {
		// Now de-duplicate
		added = r.addOrUpdateDedupeMap(attrs, narrowing, newIterators)
	}

	// the series limit was reached by adding the results of this namespace to the existing results.
	if !added && r.err.Empty() {
		r.metadata.Exhaustive = false
		if r.limitOpts.RequireExhaustive {
			r.err = r.err.Add(
				NewLimitError(fmt.Sprintf("series limit exceeded adding namespace %s to results", nsID)))
		}
	}
}

func (r *multiResult) AddWarnings(warnings ...block.Warning) {
	r.Lock()
	defer r.Unlock()
	r.metadata.AddWarnings(warnings...)
}

// NewLimitError returns a limit error so that it's the same type as the query
// limit error returned from a single database instance to receive the same
// error behavior as a database limit error.
func NewLimitError(msg string) error {
	return terrors.NewResourceExhaustedError(errors.New(msg))
}

func (r *multiResult) addOrUpdateDedupeMap(
	attrs storagemetadata.Attributes,
	narrowing Narrowing,
	newIterators encoding.SeriesIterators,
) bool {
	for _, iter := range newIterators.Iters() {
		tagIter := iter.Tags()
		shouldFilter, err := filterTagIterator(tagIter, r.tagOpts.Filters())
		if err != nil {
			r.err = r.err.Add(err)
			return false
		}

		if shouldFilter {
			// NB: skip here, the closer will free the series iterator regardless.
			continue
		}

		if r.dedupeMap.len() == r.limitOpts.Limit {
			updated, err := r.dedupeMap.update(iter, attrs, narrowing)
			if err != nil {
				r.err = r.err.Add(err)
				return false
			}
			if !updated {
				return false
			}
		} else if err := r.dedupeMap.add(iter, attrs, narrowing); err != nil {
			r.err = r.err.Add(err)
			return false
		}
	}
	return true
}
