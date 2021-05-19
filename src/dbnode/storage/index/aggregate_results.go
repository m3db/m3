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
	"math"
	"sync"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

type aggregatedResults struct {
	sync.RWMutex

	nsID          ident.ID
	aggregateOpts AggregateResultsOptions

	resultsMap     *AggregateResultsMap
	size           int
	totalDocsCount int

	idPool    ident.Pool
	bytesPool pool.CheckedBytesPool

	pool             AggregateResultsPool
	valuesPool       AggregateValuesPool
	encodedDocReader docs.EncodedDocumentReader

	iOpts instrument.Options
}

var _ AggregateUsageMetrics = (*usageMetrics)(nil)

type usageMetrics struct {
	total tally.Counter

	totalTerms   tally.Counter
	dedupedTerms tally.Counter

	totalFields   tally.Counter
	dedupedFields tally.Counter
}

func (m *usageMetrics) IncTotal(val int64) {
	// NB: if metrics not set, to valid values, no-op.
	if m.total != nil {
		m.total.Inc(val)
	}
}

func (m *usageMetrics) IncTotalTerms(val int64) {
	// NB: if metrics not set, to valid values, no-op.
	if m.totalTerms != nil {
		m.totalTerms.Inc(val)
	}
}

func (m *usageMetrics) IncDedupedTerms(val int64) {
	// NB: if metrics not set, to valid values, no-op.
	if m.dedupedTerms != nil {
		m.dedupedTerms.Inc(val)
	}
}

func (m *usageMetrics) IncTotalFields(val int64) {
	// NB: if metrics not set, to valid values, no-op.
	if m.totalFields != nil {
		m.totalFields.Inc(val)
	}
}

func (m *usageMetrics) IncDedupedFields(val int64) {
	// NB: if metrics not set, to valid values, no-op.
	if m.dedupedFields != nil {
		m.dedupedFields.Inc(val)
	}
}

// NewAggregateUsageMetrics builds a new aggregated usage metrics.
func NewAggregateUsageMetrics(ns ident.ID, iOpts instrument.Options) AggregateUsageMetrics {
	if ns == nil {
		return &usageMetrics{}
	}

	scope := iOpts.MetricsScope()
	buildCounter := func(val string) tally.Counter {
		return scope.
			Tagged(map[string]string{"type": val, "namespace": ns.String()}).
			Counter("aggregated-results")
	}

	return &usageMetrics{
		total:         buildCounter("total"),
		totalTerms:    buildCounter("total-terms"),
		dedupedTerms:  buildCounter("deduped-terms"),
		totalFields:   buildCounter("total-fields"),
		dedupedFields: buildCounter("deduped-fields"),
	}
}

// NewAggregateResults returns a new AggregateResults object.
func NewAggregateResults(
	namespaceID ident.ID,
	aggregateOpts AggregateResultsOptions,
	opts Options,
) AggregateResults {
	if aggregateOpts.AggregateUsageMetrics == nil {
		aggregateOpts.AggregateUsageMetrics = &usageMetrics{}
	}

	return &aggregatedResults{
		nsID:          namespaceID,
		aggregateOpts: aggregateOpts,
		iOpts:         opts.InstrumentOptions(),
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

	if aggregateOpts.AggregateUsageMetrics == nil {
		aggregateOpts.AggregateUsageMetrics = NewAggregateUsageMetrics(nsID, r.iOpts)
	}

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
	defer r.Unlock()

	// NB: init total count with batch length, since each aggregated entry
	// will have one field.
	totalCount := len(batch)
	for idx := 0; idx < len(batch); idx++ {
		totalCount += len(batch[idx].Terms)
	}

	r.aggregateOpts.AggregateUsageMetrics.IncTotal(int64(totalCount))
	remainingDocs := math.MaxInt64
	if r.aggregateOpts.DocsLimit != 0 {
		remainingDocs = r.aggregateOpts.DocsLimit - r.totalDocsCount
	}

	// NB: already hit doc limit.
	if remainingDocs <= 0 {
		for idx := 0; idx < len(batch); idx++ {
			batch[idx].Field.Finalize()
			r.aggregateOpts.AggregateUsageMetrics.IncTotalFields(1)
			for _, term := range batch[idx].Terms {
				r.aggregateOpts.AggregateUsageMetrics.IncTotalTerms(1)
				term.Finalize()
			}
		}

		return r.size, r.totalDocsCount
	}

	// NB: cannot insert more than max docs, so that acts as the upper bound here.
	remainingInserts := remainingDocs
	if r.aggregateOpts.SizeLimit != 0 {
		if remaining := r.aggregateOpts.SizeLimit - r.size; remaining < remainingInserts {
			remainingInserts = remaining
		}
	}

	var (
		docs       int
		numInserts int
		entry      AggregateResultsEntry
	)

	for idx := 0; idx < len(batch); idx++ {
		entry = batch[idx]
		r.aggregateOpts.AggregateUsageMetrics.IncTotalFields(1)

		if docs >= remainingDocs || numInserts >= remainingInserts {
			entry.Field.Finalize()
			for _, term := range entry.Terms {
				r.aggregateOpts.AggregateUsageMetrics.IncTotalTerms(1)
				term.Finalize()
			}

			r.size += numInserts
			r.totalDocsCount += docs
			return r.size, r.totalDocsCount
		}

		docs++
		f := entry.Field
		aggValues, ok := r.resultsMap.Get(f)
		if !ok {
			if remainingInserts > numInserts {
				r.aggregateOpts.AggregateUsageMetrics.IncDedupedFields(1)

				numInserts++
				aggValues = r.valuesPool.Get()
				// we can avoid the copy because we assume ownership of the passed ident.ID,
				// but still need to finalize it.
				r.resultsMap.SetUnsafe(f, aggValues, AggregateResultsMapSetUnsafeOptions{
					NoCopyKey:     true,
					NoFinalizeKey: false,
				})
			} else {
				// this value exceeds the limit, so should be released to the underling
				// pool without adding to the map.
				f.Finalize()
			}
		} else {
			// because we already have a entry for this field, we release the ident back to
			// the underlying pool.
			f.Finalize()
		}

		valuesMap := aggValues.Map()
		for _, t := range entry.Terms {
			r.aggregateOpts.AggregateUsageMetrics.IncTotalTerms(1)
			if remainingDocs > docs {
				docs++
				if !valuesMap.Contains(t) {
					// we can avoid the copy because we assume ownership of the passed ident.ID,
					// but still need to finalize it.
					if remainingInserts > numInserts {
						r.aggregateOpts.AggregateUsageMetrics.IncDedupedTerms(1)
						valuesMap.SetUnsafe(t, struct{}{}, AggregateValuesMapSetUnsafeOptions{
							NoCopyKey:     true,
							NoFinalizeKey: false,
						})
						numInserts++
						continue
					}
				}
			}

			t.Finalize()
		}
	}

	r.size += numInserts
	r.totalDocsCount += docs
	return r.size, r.totalDocsCount
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
	size := r.size
	r.RUnlock()
	return size
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
