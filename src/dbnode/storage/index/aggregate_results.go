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
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

type aggregatedResults struct {
	nsID       ident.ID
	resultsMap *AggregateResultsMap

	idPool    ident.Pool
	bytesPool pool.CheckedBytesPool

	pool       AggregateResultsPool
	valuesPool AggregateValuesPool
	noFinalize bool
}

// NewAggregateResults returns a new AggregateResults object.
func NewAggregateResults(opts Options) AggregateResults {
	return &aggregatedResults{
		resultsMap: newAggregateResultsMap(opts.IdentifierPool()),
		idPool:     opts.IdentifierPool(),
		bytesPool:  opts.CheckedBytesPool(),
		pool:       opts.AggregateResultsPool(),
		valuesPool: opts.AggregateValuesPool(),
	}
}

func (r *aggregatedResults) addField(
	term []byte,
	value []byte,
	opts AggregateQueryOptions,
) error {
	// NB: can cast the []byte -> ident.ID to avoid an alloc
	// before we're sure we need it.
	termID := ident.BytesID(term)

	// NB: if it is already in the map, this is a valid ID to add and it's not
	// necessary to check against the filter.
	valueMap, found := r.resultsMap.Get(termID)
	if found {
		valueID := ident.BytesID(value)
		return valueMap.addValue(valueID)
	}

	// if this term hasn't been seen, ensure it should be included in output.
	if !opts.TermFilter.Contains(termID) {
		return nil
	}

	aggValues := r.valuesPool.Get()
	valueID := ident.BytesID(value)
	if err := aggValues.addValue(valueID); err != nil {
		// Return these values to the pool.
		r.valuesPool.Put(aggValues)
		return err
	}

	r.resultsMap.Set(termID, aggValues)
	return nil
}

func (r *aggregatedResults) AggregateDocument(
	document doc.Document,
	opts AggregateQueryOptions,
) error {
	for _, field := range document.Fields {
		if err := r.addField(field.Name, field.Value, opts); err != nil {
			return err
		}
	}

	return nil
}

func (r *aggregatedResults) AddIDAndValues(
	termID ident.ID,
	values AggregateValues,
) error {
	valueIt := values.Map().Iter()

	valueMap, found := r.resultsMap.Get(termID)
	if found {
		for _, value := range valueIt {
			if err := valueMap.addValue(value.Key()); err != nil {
				return err
			}
		}

		return nil
	}

	aggValues := r.valuesPool.Get()
	for _, value := range valueIt {
		if err := aggValues.addValue(value.Key()); err != nil {
			// Return these values to the pool.
			r.valuesPool.Put(aggValues)
			return err
		}
	}

	r.resultsMap.Set(termID, aggValues)
	return nil
}

func (r *aggregatedResults) Namespace() ident.ID {
	return r.nsID
}

func (r *aggregatedResults) Map() *AggregateResultsMap {
	return r.resultsMap
}

func (r *aggregatedResults) Size() int {
	return r.resultsMap.Len()
}

func (r *aggregatedResults) Reset(nsID ident.ID) {
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
}

func (r *aggregatedResults) Finalize() {
	if r.noFinalize {
		return
	}

	r.Reset(nil)
	if r.pool == nil {
		return
	}

	r.pool.Put(r)
}
