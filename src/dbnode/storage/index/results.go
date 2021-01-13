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
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
)

var (
	errUnableToAddResultMissingID = errors.New("no id for result")
	resultMapNoFinalizeOpts       = ResultsMapSetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	}
)

type results struct {
	sync.RWMutex

	nsID ident.ID
	opts QueryResultsOptions

	reusableID     *ident.ReusableBytesID
	resultsMap     *ResultsMap
	totalDocsCount int

	idPool    ident.Pool
	bytesPool pool.CheckedBytesPool

	pool       QueryResultsPool
	noFinalize bool
}

// NewQueryResults returns a new query results object.
func NewQueryResults(
	namespaceID ident.ID,
	opts QueryResultsOptions,
	indexOpts Options,
) QueryResults {
	return &results{
		nsID:       namespaceID,
		opts:       opts,
		resultsMap: newResultsMap(),
		idPool:     indexOpts.IdentifierPool(),
		bytesPool:  indexOpts.CheckedBytesPool(),
		pool:       indexOpts.QueryResultsPool(),
		reusableID: ident.NewReusableBytesID(),
	}
}

func (r *results) EnforceLimits() bool { return true }

func (r *results) Reset(nsID ident.ID, opts QueryResultsOptions) {
	r.Lock()

	// Finalize existing held nsID.
	if r.nsID != nil {
		r.nsID.Finalize()
	}
	// Make an independent copy of the new nsID.
	if nsID != nil {
		nsID = r.idPool.Clone(nsID)
	}
	r.nsID = nsID

	// Reset all keys in the map next, this will finalize the keys.
	r.resultsMap.Reset()
	r.totalDocsCount = 0

	r.opts = opts

	r.Unlock()
}

// NB: If documents with duplicate IDs are added, they are simply ignored and
// the first document added with an ID is returned.
func (r *results) AddDocuments(batch []doc.Document) (int, int, error) {
	r.Lock()
	err := r.addDocumentsBatchWithLock(batch)
	size := r.resultsMap.Len()
	docsCount := r.totalDocsCount + len(batch)
	r.totalDocsCount = docsCount
	r.Unlock()
	return size, docsCount, err
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

func (r *results) addDocumentWithLock(w doc.Document) (bool, int, error) {
	id, err := docs.ReadIDFromDocument(w)
	if err != nil {
		return false, r.resultsMap.Len(), err
	}

	if len(id) == 0 {
		return false, r.resultsMap.Len(), errUnableToAddResultMissingID
	}

	// Need to apply filter if set first.
	r.reusableID.Reset(id)
	if r.opts.FilterID != nil && !r.opts.FilterID(r.reusableID) {
		return false, r.resultsMap.Len(), nil
	}

	// check if it already exists in the map.
	if r.resultsMap.Contains(id) {
		return false, r.resultsMap.Len(), nil
	}

	// It is assumed that the document is valid for the lifetime of the index
	// results.
	r.resultsMap.SetUnsafe(id, w, resultMapNoFinalizeOpts)

	return true, r.resultsMap.Len(), nil
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

func (r *results) TotalDocsCount() int {
	r.RLock()
	count := r.totalDocsCount
	r.RUnlock()
	return count
}

func (r *results) Finalize() {
	// Reset locks so cannot hold onto lock for call to Finalize.
	r.Reset(nil, QueryResultsOptions{})

	if r.pool == nil {
		return
	}
	r.pool.Put(r)
}
