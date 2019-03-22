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
	errUnableToAddResultMissingID = errors.New("no id for result")
)

type results struct {
	sync.RWMutex

	nsID ident.ID
	opts ResultsOptions

	resultsMap *ResultsMap

	idPool    ident.Pool
	bytesPool pool.CheckedBytesPool

	pool       ResultsPool
	noFinalize bool
}

// NewResults returns a new results object.
func NewResults(
	namespaceID ident.ID,
	opts ResultsOptions,
	indexOpts Options,
) Results {
	return &results{
		nsID:       namespaceID,
		opts:       opts,
		resultsMap: newResultsMap(indexOpts.IdentifierPool()),
		idPool:     indexOpts.IdentifierPool(),
		bytesPool:  indexOpts.CheckedBytesPool(),
		pool:       indexOpts.ResultsPool(),
	}
}

func (r *results) Reset(nsID ident.ID, opts ResultsOptions) {
	r.Lock()

	r.opts = opts

	// Finalize existing held nsID.
	if r.nsID != nil {
		r.nsID.Finalize()
	}
	// Make an independent copy of the new nsID.
	if nsID != nil {
		nsID = r.idPool.Clone(nsID)
	}
	r.nsID = nsID

	// Reset all values from map first
	for _, entry := range r.resultsMap.Iter() {
		tags := entry.Value()
		tags.Finalize()
	}

	// Reset all keys in the map next, this will finalize the keys.
	r.resultsMap.Reset()

	// NB: could do keys+value in one step but I'm trying to avoid
	// using an internal method of a code-gen'd type.

	r.Unlock()
}

// NB: If documents with duplicate IDs are added, they are simply ignored and
// the first document added with an ID is returned.
func (r *results) AddDocuments(batch []doc.Document) (int, error) {
	r.Lock()
	err := r.addDocumentsBatchWithLock(batch)
	size := r.resultsMap.Len()
	r.Unlock()
	return size, err
}

func (r *results) addDocumentsBatchWithLock(batch []doc.Document) error {
	for i := range batch {
		_, size, err := r.addDocumentWithLock(batch[i])
		if err != nil {
			return err
		}
		if r.opts.SizeLimit > 0 && size >= r.opts.SizeLimit {
			// Early return if limit enforced and we hit our limit.
			break
		}
	}
	return nil
}

func (r *results) addDocumentWithLock(
	d doc.Document,
) (bool, int, error) {
	if len(d.ID) == 0 {
		return false, r.resultsMap.Len(), errUnableToAddResultMissingID
	}

	// NB: can cast the []byte -> ident.ID to avoid an alloc
	// before we're sure we need it.
	tsID := ident.BytesID(d.ID)

	// check if it already exists in the map.
	if r.resultsMap.Contains(tsID) {
		return false, r.resultsMap.Len(), nil
	}

	// i.e. it doesn't exist in the map, so we create the tags wrapping
	// fields prodided by the document.
	tags := r.cloneTagsFromFields(d.Fields)

	// We use Set() instead of SetUnsafe to ensure we're taking a copy of
	// the tsID's bytes.
	r.resultsMap.Set(tsID, tags)

	return true, r.resultsMap.Len(), nil
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

func (r *results) Map() *ResultsMap {
	r.RLock()
	v := r.resultsMap
	r.RUnlock()
	return v
}

func (r *results) Size() int {
	r.RLock()
	v := r.resultsMap.Len()
	r.RUnlock()
	return v
}

func (r *results) Finalize() {
	r.RLock()
	noFinalize := r.noFinalize
	r.RUnlock()

	if noFinalize {
		return
	}

	// Reset locks so cannot hold onto lock for call to Finalize.
	r.Reset(nil, ResultsOptions{})

	if r.pool == nil {
		return
	}
	r.pool.Put(r)
}

func (r *results) NoFinalize() {
	r.Lock()

	// Ensure neither the results object itself, nor any of its underlying
	// IDs and tags will be finalized.
	r.noFinalize = true
	for _, entry := range r.resultsMap.Iter() {
		id, tags := entry.Key(), entry.Value()
		id.NoFinalize()
		tags.NoFinalize()
	}

	r.Unlock()
}
