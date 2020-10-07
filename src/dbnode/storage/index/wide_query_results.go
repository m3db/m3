// Copyright (c) 2020 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/ident"
)

// ErrWideQueryResultsExhausted is used to short circuit additional document
// entries being added if these wide results will no longer accept documents,
// e.g. if the results are closed, or if no further documents will pass the
// shard filter.
var ErrWideQueryResultsExhausted = errors.New("no more values to add to wide query results")

type shardFilterFn func(ident.ID) (uint32, bool)

type wideResults struct {
	nsID   ident.ID
	idPool ident.Pool

	closed      bool
	idsOverflow []ident.ID
	batch       *ident.IDBatch
	batchCh     chan<- *ident.IDBatch
	batchSize   int

	shardFilter shardFilterFn
	shards      []uint32
	shardIdx    int
	// NB: pastLastShard will mark this reader as exhausted after a
	// document is discovered whose shard exceeds the last shard this results
	// is responsible for, using the fact that incoming documents are sorted by
	// shard then by ID.
	pastLastShard bool
}

// NewWideQueryResults returns a new wide query results object.
// NB: Reader must read results from `batchCh` in a goroutine, and call
// batch.Done() after the result is used, and the writer must close the
// channel after no more Documents are available.
func NewWideQueryResults(
	namespaceID ident.ID,
	idPool ident.Pool,
	shardFilter shardFilterFn,
	opts WideQueryOptions,
) BaseResults {
	batchSize := opts.BatchSize
	results := &wideResults{
		nsID:        namespaceID,
		idPool:      idPool,
		batchSize:   batchSize,
		idsOverflow: make([]ident.ID, 0, batchSize),
		batch: &ident.IDBatch{
			IDs: make([]ident.ID, 0, batchSize),
		},
		batchCh: opts.IndexBatchCollector,
		shards:  opts.ShardsQueried,
	}

	if len(opts.ShardsQueried) > 0 {
		// Only apply filter if there are shards to filter against.
		results.shardFilter = shardFilter
	}

	return results
}

func (r *wideResults) AddDocuments(batch []doc.Document) (int, int, error) {
	if r.closed || r.pastLastShard {
		return 0, 0, ErrWideQueryResultsExhausted
	}

	err := r.addDocumentsBatchWithLock(batch)
	release := len(r.batch.IDs) == r.batchSize
	if release {
		r.releaseAndWait()
		r.releaseOverflow(false)
	}

	return 0, 0, err
}

func (r *wideResults) releaseOverflow(forceRelease bool) {
	var (
		incomplete bool
		size       int
		overflow   int
	)
	for {
		size = r.batchSize
		overflow = len(r.idsOverflow)
		if overflow == 0 {
			// NB: no overflow elements.
			return
		}

		if overflow < size {
			size = overflow
			incomplete = true
		}

		r.batch.IDs = append(r.batch.IDs, r.idsOverflow[0:size]...)
		r.batch.IDs = r.batch.IDs[:size]
		copy(r.idsOverflow, r.idsOverflow[size:])
		r.idsOverflow = r.idsOverflow[:overflow-size]
		if !forceRelease && incomplete {
			return
		}

		r.releaseAndWait()
	}
}

func (r *wideResults) addDocumentsBatchWithLock(batch []doc.Document) error {
	for i := range batch {
		err := r.addDocumentWithLock(batch[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *wideResults) addDocumentWithLock(d doc.Document) error {
	if len(d.ID) == 0 {
		return errUnableToAddResultMissingID
	}

	var tsID ident.ID = ident.BytesID(d.ID)

	// Need to apply filter if set first.
	if r.shardFilter != nil {
		filteringShard := r.shards[r.shardIdx]
		shard, shardOwned := r.shardFilter(tsID)
		// NB: Check to see if shard is exceeded first (to short circuit earlier if
		// the current shard is not owned by this node, but shard exceeds filtered).
		if filteringShard > shard {
			// this element is from a shard lower than the next shard allowed.
			return nil
		}

		for filteringShard < shard {
			// this element is from a shard higher than the next shard allowed;
			// advance to the next shard, then try again.
			r.shardIdx = r.shardIdx + 1
			if r.shardIdx >= len(r.shards) {
				// shard is past the final shard allowed by filter, no more results
				// will be accepted.
				r.pastLastShard = true
				return ErrWideQueryResultsExhausted
			}

			filteringShard = r.shards[r.shardIdx]
			if filteringShard > shard {
				// this element is from a shard lower than the next shard allowed.
				return nil
			}
		}

		if !shardOwned {
			return nil
		}
	}

	// Pool IDs after filter is passed.
	tsID = r.idPool.Clone(tsID)
	if len(r.batch.IDs) < r.batchSize {
		r.batch.IDs = append(r.batch.IDs, tsID)
	} else {
		r.idsOverflow = append(r.idsOverflow, tsID)
	}

	return nil
}

func (r *wideResults) Namespace() ident.ID {
	return r.nsID
}

func (r *wideResults) Size() int {
	return 0
}

func (r *wideResults) TotalDocsCount() int {
	return 0
}

// NB: Finalize should be called after all documents have been consumed.
func (r *wideResults) Finalize() {
	if r.closed {
		return
	}

	// NB: release current
	r.releaseAndWait()
	r.closed = true
	close(r.batchCh)
}

func (r *wideResults) releaseAndWait() {
	if r.closed || len(r.batch.IDs) == 0 {
		return
	}

	r.batch.Add(1)
	r.batchCh <- r.batch
	r.batch.Wait()
	r.batch.IDs = r.batch.IDs[:0]
}
