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
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

type aggregatedResults struct {
	sync.RWMutex

	nsID               ident.ID
	queryOpts          QueryResultsOptions
	aggregateQueryOpts AggregateResultsOptions

	resultsMap *AggregateResultsMap

	idPool    ident.Pool
	bytesPool pool.CheckedBytesPool

	pool       AggregateResultsPool
	valuesPool AggregateValuesPool
}

// NewAggregateResults returns a new AggregateResults object.
func NewAggregateResults(
	namespaceID ident.ID,
	queryOpts QueryResultsOptions,
	aggregateQueryOpts AggregateResultsOptions,
	opts Options,
) AggregateResults {
	return &aggregatedResults{
		nsID:               namespaceID,
		queryOpts:          queryOpts,
		aggregateQueryOpts: aggregateQueryOpts,
		resultsMap:         newAggregateResultsMap(opts.IdentifierPool()),
		idPool:             opts.IdentifierPool(),
		bytesPool:          opts.CheckedBytesPool(),
		pool:               opts.AggregateResultsPool(),
		valuesPool:         opts.AggregateValuesPool(),
	}
}

func (r *aggregatedResults) Reset(
	nsID ident.ID,
	queryOpts QueryResultsOptions,
	aggregateQueryOpts AggregateResultsOptions,
) {
	r.Lock()

	r.queryOpts = queryOpts
	r.aggregateQueryOpts = aggregateQueryOpts

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
		valueMap.reset()
	}

	// reset all keys in the map next
	r.resultsMap.Reset()

	// NB: could do keys+value in one step but I'm trying to avoid
	// using an internal method of a code-gen'd type.
	r.Unlock()
}

func (r *aggregatedResults) AddDocuments(batch []doc.Document) (int, error) {
	r.Lock()
	err := r.addDocumentsBatchWithLock(batch)
	size := r.resultsMap.Len()
	r.Unlock()
	return size, err
}

func (r *aggregatedResults) addDocumentsBatchWithLock(
	batch []doc.Document,
) error {
	for i := range batch {
		err := r.addDocumentWithLock(batch[i])
		if err != nil {
			return err
		}

		if r.queryOpts.SizeLimit > 0 && r.resultsMap.Len() >= r.queryOpts.SizeLimit {
			// Early return if limit enforced and we hit our limit.
			break
		}
	}

	return nil
}

func (r *aggregatedResults) addDocumentWithLock(
	document doc.Document,
) error {
	for _, field := range document.Fields {
		if err := r.addFieldWithLock(field.Name, field.Value); err != nil {
			return err
		}
	}

	return nil
}

func (r *aggregatedResults) addFieldWithLock(
	term []byte,
	value []byte,
) error {
	// NB: can cast the []byte -> ident.ID to avoid an alloc
	// before we're sure we need it.
	var termID ident.ID = ident.BytesID(term)

	// if this term hasn't been seen, ensure it should be included in output.
	if !r.aggregateQueryOpts.TermFilter.Contains(termID) {
		return nil
	}

	valueBytes := r.bytesPool.Get(len(value))
	valueBytes.IncRef()
	valueBytes.AppendAll(value)
	valueBytes.DecRef()
	valueID := r.idPool.BinaryID(valueBytes)
	valueMap, found := r.resultsMap.Get(termID)
	if found {
		return valueMap.addValue(valueID)
	}

	aggValues := r.valuesPool.Get()
	if err := aggValues.addValue(valueID); err != nil {
		// Return these values to the pool.
		r.valuesPool.Put(aggValues)
		return err
	}

	termBytes := r.bytesPool.Get(len(value))
	termBytes.IncRef()
	termBytes.AppendAll(term)
	termBytes.DecRef()
	termID = r.idPool.BinaryID(termBytes)
	r.resultsMap.Set(termID, aggValues)
	return nil
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

func (r *aggregatedResults) Finalize() {
	r.Reset(nil, QueryResultsOptions{}, AggregateResultsOptions{})
	if r.pool == nil {
		return
	}

	r.pool.Put(r)
}
