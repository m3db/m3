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

package index

import (
	"errors"
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

var (
	errUnableToAddResultMissingID          = errors.New("no id for result")
	errResultAlreadyExistsNoPartialUpdates = errors.New("id already exists for result and partial updates not allowed")
)

type results struct {
	sync.RWMutex
	nsID       ident.ID
	resultsMap *ResultsMap

	idPool    ident.Pool
	bytesPool pool.CheckedBytesPool

	pool       ResultsPool
	noFinalize bool
}

// NewResults returns a new results object.
func NewResults(opts Options) Results {
	return &results{
		resultsMap: newResultsMap(opts.IdentifierPool()),
		idPool:     opts.IdentifierPool(),
		bytesPool:  opts.CheckedBytesPool(),
		pool:       opts.ResultsPool(),
	}
}

func (r *results) AddDocument(
	d doc.Document,
) (added bool, size int, err error) {
	r.Lock()
	added, size, err = r.addDocumentWithLock(d)
	r.Unlock()
	return
}

func (r *results) addDocumentWithLock(
	d doc.Document,
) (added bool, size int, err error) {
	added = false
	if len(d.ID) == 0 {
		return added, r.resultsMap.Len(), errUnableToAddResultMissingID
	}

	// NB: can cast the []byte -> ident.ID to avoid an alloc
	// before we're sure we need it.
	tsID := ident.BytesID(d.ID)

	// check if it already exists in the map.
	if r.resultsMap.Contains(tsID) {
		return added, r.resultsMap.Len(), nil
	}

	// i.e. it doesn't exist in the map, so we create the tags wrapping
	// fields prodided by the document.
	tags := r.cloneTagsFromFields(d.Fields)

	// We use Set() instead of SetUnsafe to ensure we're taking a copy of
	// the tsID's bytes.
	r.resultsMap.Set(tsID, tags)

	added = true
	return added, r.resultsMap.Len(), nil
}

func (r *results) AddDocumentsBatch(
	batch []doc.Document,
	opts AddDocumentsBatchResultsOptions,
) (numPartialUpdates int, size int, err error) {
	r.Lock()
	numPartialUpdates, size, err = r.addDocumentsBatchWithLock(batch, opts)
	r.Unlock()
	return
}

func (r *results) addDocumentsBatchWithLock(
	batch []doc.Document,
	opts AddDocumentsBatchResultsOptions,
) (numPartialUpdates int, size int, err error) {
	numPartialUpdates = 0
	size = r.resultsMap.Len()
	for i := range batch {
		var added bool
		added, size, err = r.addDocumentWithLock(batch[i])
		if err != nil {
			return numPartialUpdates, size, err
		}
		if !added {
			if !opts.AllowPartialUpdates {
				return numPartialUpdates, size, errResultAlreadyExistsNoPartialUpdates
			}
			numPartialUpdates++
		}
		if opts.LimitSize > 0 && size >= opts.LimitSize {
			// Early return if limit enforced and we hit our limit
			break
		}
	}
	return numPartialUpdates, size, nil
}

func (r *results) AddIDAndTags(
	id ident.ID,
	tags ident.Tags,
) (added bool, size int, err error) {
	r.Lock()
	added, size, err = r.addIDAndTagsWithLock(id, tags)
	r.Unlock()
	return
}

func (r *results) addIDAndTagsWithLock(
	id ident.ID,
	tags ident.Tags,
) (added bool, size int, err error) {
	added = false
	bytesID := ident.BytesID(id.Bytes())
	if len(bytesID) == 0 {
		return added, r.resultsMap.Len(), errUnableToAddResultMissingID
	}

	// check if it already exists in the map.
	if r.resultsMap.Contains(bytesID) {
		return added, r.resultsMap.Len(), nil
	}

	// We use Set() instead of SetUnsafe to ensure we're taking a copy of
	// the tsID's bytes.
	r.resultsMap.Set(bytesID, r.cloneTags(tags))

	added = true
	return added, r.resultsMap.Len(), nil
}

func (r *results) cloneTags(tags ident.Tags) ident.Tags {
	return r.idPool.CloneTags(tags)
}

func (r *results) cloneTagsFromFields(fields doc.Fields) ident.Tags {
	tags := r.idPool.Tags()
	for _, f := range fields {
		tags.Append(r.idPool.CloneTag(ident.Tag{
			Name:  ident.BytesID(f.Name),
			Value: ident.BytesID(f.Value),
		}))
	}
	return tags
}

func (r *results) Namespace() ident.ID {
	r.RLock()
	v := r.nsID
	r.RUnlock()
	return v
}

func (r *results) WithMap(fn func(results *ResultsMap)) {
	r.RLock()
	fn(r.resultsMap)
	r.RUnlock()
}

func (r *results) Size() int {
	r.RLock()
	v := r.resultsMap.Len()
	r.RUnlock()
	return v
}

func (r *results) Reset(nsID ident.ID) {
	r.Lock()
	defer r.Unlock()

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
		tags := entry.Value()
		tags.Finalize()
	}

	// reset all keys in the map next
	r.resultsMap.Reset()

	// NB: could do keys+value in one step but I'm trying to avoid
	// using an internal method of a code-gen'd type.
}

func (r *results) Finalize() {
	r.RLock()
	noFinalize := r.noFinalize
	r.RUnlock()

	if noFinalize {
		return
	}

	r.Reset(nil)

	if r.pool == nil {
		return
	}
	r.pool.Put(r)
}

func (r *results) NoFinalize() {
	r.Lock()
	defer r.Unlock()

	// Ensure neither the results object itself, or any of its underlying
	// IDs and tags will be finalized.
	r.noFinalize = true
	for _, entry := range r.resultsMap.Iter() {
		id, tags := entry.Key(), entry.Value()
		id.NoFinalize()
		tags.NoFinalize()
	}
}
