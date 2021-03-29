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

	parent *results

	nsID      ident.ID
	opts      QueryResultsOptions
	indexOpts Options

	subResults []*results

	reusableID     *ident.ReusableBytesID
	resultsMap     *ResultsMap
	totalDocsCount int

	statsLock      sync.RWMutex
	statsSize      int
	statsDocsCount int

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
	return newQueryResults(namespaceID, opts, indexOpts)
}

func newQueryResults(
	namespaceID ident.ID,
	opts QueryResultsOptions,
	indexOpts Options,
) *results {
	return &results{
		nsID:       namespaceID,
		opts:       opts,
		indexOpts:  indexOpts,
		resultsMap: newResultsMap(),
		idPool:     indexOpts.IdentifierPool(),
		bytesPool:  indexOpts.CheckedBytesPool(),
		pool:       indexOpts.QueryResultsPool(),
		reusableID: ident.NewReusableBytesID(),
	}
}

func (r *results) EnforceLimits() bool {
	return true
}

func (r *results) Reset(nsID ident.ID, opts QueryResultsOptions) {
	r.reset(nil, nsID, opts)
}

func (r *results) reset(parent *results, nsID ident.ID, opts QueryResultsOptions) {
	r.Lock()

	// Set parent.
	r.parent = parent

	// Return all subresults to pools.
	for _, subResult := range r.subResults {
		subResult.Finalize()
	}
	r.subResults = nil

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

func (r *results) NonConcurrentBuilder() (BaseResultsBuilder, bool) {
	subResult := r.pool.Get().(*results)
	subResult.reset(r, r.nsID, r.opts)

	r.Lock()
	r.subResults = append(r.subResults, subResult)
	r.Unlock()

	return subResult, true
}

// NB: If documents with duplicate IDs are added, they are simply ignored and
// the first document added with an ID is returned.
func (r *results) AddDocuments(batch []doc.Document) (int, int, error) {
	r.Lock()
	err := r.addDocumentsBatchWithLock(batch)
	parent := r.parent
	size, docsCount := r.resultsMap.Len(), r.totalDocsCount
	r.Unlock()

	// Update stats using just the stats lock to avoid contention.
	r.statsLock.Lock()
	r.statsSize = size
	r.statsDocsCount = docsCount
	r.statsLock.Unlock()

	if parent == nil {
		return size, docsCount, err
	}

	// If a child, need to aggregate the size and docs count.
	size, docsCount = parent.statsNoLock()

	return size, docsCount, err
}

func (r *results) statsNoLock() (size int, docsCount int) {
	r.statsLock.RLock()
	size = r.statsSize
	docsCount = r.statsDocsCount
	r.statsLock.RUnlock()

	// Take snapshot of subresults with RLock.
	r.RLock()
	subResults := r.subResults[:]
	r.RUnlock()

	for _, subResult := range subResults {
		subResult.statsLock.RLock()
		size += subResult.statsSize
		docsCount += subResult.statsDocsCount
		subResult.statsLock.RUnlock()
	}

	return
}

func (r *results) addDocumentsBatchWithLock(batch []doc.Document) error {
	for i := range batch {
		_, size, err := r.addDocumentWithLock(batch[i])
		if err != nil {
			return err
		}
		r.totalDocsCount++
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

func (r *results) mergeSubResultWithLock(subResult *results) {
	subResult.Lock()
	defer subResult.Unlock()

	if r.resultsMap.Len() == 0 {
		// Just swap ownership of this results map since this subresult
		// has results and the current results does not.
		currResultsMap := r.resultsMap
		r.resultsMap = subResult.resultsMap
		subResult.resultsMap = currResultsMap
		return
	}

	for _, elem := range subResult.resultsMap.Iter() {
		key := elem.Key()
		if r.resultsMap.Contains(key) {
			// Already contained.
			continue
		}
		// It is assumed that the document is valid for the lifetime of the
		// index results.
		r.resultsMap.SetUnsafe(key, elem.Value(), resultMapNoFinalizeOpts)
	}

	// Reset all keys in the subresult map next, this will finalize the keys
	// and make sure the values are not closed on next reset.
	subResult.resultsMap.Reset()
}

func (r *results) Map() *ResultsMap {
	r.Lock()

	// Copy any subresults into final result.
	for _, subResult := range r.subResults {
		r.mergeSubResultWithLock(subResult)
	}

	// Finalize and reset sub results now merged.
	for _, subResult := range r.subResults {
		subResult.Finalize()
	}
	r.subResults = nil

	v := r.resultsMap

	r.Unlock()
	return v
}

func (r *results) Size() int {
	size, _ := r.statsNoLock()
	return size
}

func (r *results) TotalDocsCount() int {
	_, docsCount := r.statsNoLock()
	return docsCount
}

func (r *results) Finalize() {
	// Reset locks so cannot hold onto lock for call to Finalize.
	r.Reset(nil, QueryResultsOptions{})

	if r.pool == nil {
		return
	}
	r.pool.Put(r)
}
