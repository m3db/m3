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

package index

import (
	"fmt"
	"math"
	"sync"

	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
)

const missingDocumentFields = "invalid document fields: empty %s"

// NB: emptyValues is an AggregateValues with no values, used for tracking
// terms only rather than terms and values.
var emptyValues = AggregateValues{hasValues: false}

type aggregatedResults struct {
	sync.RWMutex

	nsID          ident.ID
	aggregateOpts AggregateResultsOptions

	resultsMap     *AggregateResultsMap
	size int
	totalDocsCount int

	idPool    ident.Pool
	bytesPool pool.CheckedBytesPool

	pool       AggregateResultsPool
	valuesPool AggregateValuesPool

	encodedDocReader docs.EncodedDocumentReader
}

// NewAggregateResults returns a new AggregateResults object.
func NewAggregateResults(
	namespaceID ident.ID,
	aggregateOpts AggregateResultsOptions,
	opts Options,
) AggregateResults {
	return &aggregatedResults{
		nsID:          namespaceID,
		aggregateOpts: aggregateOpts,
		resultsMap:    newAggregateResultsMap(opts.IdentifierPool()),
		idPool:        opts.IdentifierPool(),
		bytesPool:     opts.CheckedBytesPool(),
		pool:          opts.AggregateResultsPool(),
		valuesPool:    opts.AggregateValuesPool(),
	}
}

func (r *aggregatedResults) EnforceLimits() bool { return true }

func (r *aggregatedResults) Reset(
	nsID ident.ID,
	aggregateOpts AggregateResultsOptions,
) {
	r.Lock()

	r.aggregateOpts = aggregateOpts

	// finalize existing held nsID
	if r.nsID != nil {
		r.nsID.Finalize()
	}

	// make an independent copy of the new nsID
	if nsID != nil {
		nsID = r.idPool.Clone(nsID)
	}
	r.nsID = nsID

	// reset all values from map first
	for _, entry := range r.resultsMap.Iter() {
		valueMap := entry.Value()
		valueMap.finalize()
	}

	// reset all keys in the map next
	r.resultsMap.Reset()
	r.totalDocsCount = 0
	r.size = 0

	// NB: could do keys+value in one step but I'm trying to avoid
	// using an internal method of a code-gen'd type.
	r.Unlock()
}

func (r *aggregatedResults) AggregateResultsOptions() AggregateResultsOptions {
	return r.aggregateOpts
}

func (r *aggregatedResults) AddFields(batch []AggregateResultsEntry) (int, int) {
	r.Lock()
	maxInsertions := int(math.MaxInt64)
	if r.aggregateOpts.SizeLimit != 0 {
		maxInsertions = r.aggregateOpts.SizeLimit - r.totalDocsCount
	}

	valueInsertions := 0
	for _, entry := range batch {
		f := entry.Field
		aggValues, ok := r.resultsMap.Get(f)
		if !ok {
			aggValues = r.valuesPool.Get()
			// we can avoid the copy because we assume ownership of the passed ident.ID,
			// but still need to finalize it.
			r.resultsMap.SetUnsafe(f, aggValues, AggregateResultsMapSetUnsafeOptions{
				NoCopyKey:     true,
				NoFinalizeKey: false,
			})
		} else {
			// because we already have a entry for this field, we release the ident back to
			// the underlying pool.
			f.Finalize()
		}
		valuesMap := aggValues.Map()
		for _, t := range entry.Terms {
			if !valuesMap.Contains(t) {
				fmt.Println(maxInsertions, valueInsertions, t)
				// we can avoid the copy because we assume ownership of the passed ident.ID,
				// but still need to finalize it.
				if maxInsertions > valueInsertions {
					valuesMap.SetUnsafe(t, struct{}{}, AggregateValuesMapSetUnsafeOptions{
						NoCopyKey:     true,
						NoFinalizeKey: false,
					})
					valueInsertions++
				} else {
					// this value exceeds the limit, so should be released to the underling
					// pool without adding to the map.
					t.Finalize()
				}
			} else {
				// because we already have a entry for this term, we release the ident back to
				// the underlying pool.
				t.Finalize()
			}
		}
	}

	docsCount := r.totalDocsCount + valueInsertions
	r.totalDocsCount = docsCount
	r.Unlock()
	return r.resultsMap.Len(), docsCount
}

func (r *aggregatedResults) Namespace() ident.ID {
	r.RLock()
	ns := r.nsID
	r.RUnlock()
	return ns
}

func (r *aggregatedResults) Map() *AggregateResultsMap {
	r.RLock()
	m := r.resultsMap
	r.RUnlock()
	return m
}

func (r *aggregatedResults) Size() int {
	r.RLock()
	l := r.resultsMap.Len()
	r.RUnlock()
	return l
}

func (r *aggregatedResults) TotalDocsCount() int {
	r.RLock()
	count := r.totalDocsCount
	r.RUnlock()
	return count
}

func (r *aggregatedResults) Finalize() {
	r.Reset(nil, AggregateResultsOptions{})
	if r.pool == nil {
		return
	}

	r.pool.Put(r)
}
