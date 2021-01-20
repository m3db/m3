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
	"fmt"
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/x/ident"
)

// ErrWideQueryResultsExhausted is used to short circuit additional document
// entries being added if these wide results will no longer accept documents,
// e.g. if the results are closed, or if no further documents will pass the
// shard filter.
var ErrWideQueryResultsExhausted = errors.New("no more values to add to wide query results")

type shardFilterFn func(ident.ID) (uint32, bool)

type wideResults struct {
	sync.RWMutex
	size           int
	totalDocsCount int

	nsID   ident.ID
	idPool ident.Pool

	closed      bool
	idsOverflow []ident.ShardID
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
	collector chan *ident.IDBatch,
	opts WideQueryOptions,
) BaseResults {
	batchSize := opts.BatchSize
	results := &wideResults{
		nsID:        namespaceID,
		idPool:      idPool,
		batchSize:   batchSize,
		idsOverflow: make([]ident.ShardID, 0, batchSize),
		batch: &ident.IDBatch{
			ShardIDs: make([]ident.ShardID, 0, batchSize),
		},
		batchCh:     collector,
		shardFilter: shardFilter,
		shards:      opts.ShardsQueried,
	}

	return results
}

func (r *wideResults) EnforceLimits() bool {
	// NB: wide results should not enforce limits, as they may span an entire
	// block in a memory constrained batch-wise fashion.
	return false
}

func (r *wideResults) AddDocuments(batch []doc.Document) (int, int, error) {
	var size, totalDocsCount int
	r.RLock()
	size, totalDocsCount = r.size, r.totalDocsCount
	r.RUnlock()

	if r.closed || r.pastLastShard {
		return size, totalDocsCount, ErrWideQueryResultsExhausted
	}

	r.Lock()
	defer r.Unlock()

	err := r.addDocumentsBatchWithLock(batch)
	size, totalDocsCount = r.size, r.totalDocsCount
	if err != nil && err != ErrWideQueryResultsExhausted {
		// NB: if exhausted, drain the current batch and overflows.
		return size, totalDocsCount, err
	}

	release := len(r.batch.ShardIDs) == r.batchSize
	if release {
		r.releaseAndWaitWithLock()
		r.releaseOverflowWithLock()
	}

	size = r.size
	return size, totalDocsCount, err
}

func (r *wideResults) addDocumentsBatchWithLock(batch []doc.Document) error {
	for i := range batch {
		if err := r.addDocumentWithLock(batch[i]); err != nil {
			return err
		}
	}

	return nil
}

func (r *wideResults) addDocumentWithLock(w doc.Document) error {
	docID, err := docs.ReadIDFromDocument(w)
	if err != nil {
		return fmt.Errorf("unable to decode document ID: %w", err)
	}
	if len(docID) == 0 {
		return errUnableToAddResultMissingID
	}

	var tsID ident.ID = ident.BytesID(docID)

	documentShard, documentShardOwned := r.shardFilter(tsID)
	if !documentShardOwned {
		// node is no longer responsible for this document's shard.
		return nil
	}

	if len(r.shards) > 0 {
		// Need to apply filter if shard set provided.
		filteringShard := r.shards[r.shardIdx]
		// NB: Check to see if shard is exceeded first (to short circuit earlier if
		// the current shard is not owned by this node, but shard exceeds filtered).
		if filteringShard > documentShard {
			// this document is from a shard lower than the next shard allowed.
			return nil
		}

		for filteringShard < documentShard {
			// this document is from a shard higher than the next shard allowed;
			// advance to the next shard, then try again.
			r.shardIdx++
			if r.shardIdx >= len(r.shards) {
				// shard is past the final shard allowed by filter, no more results
				// will be accepted.
				r.pastLastShard = true
				return ErrWideQueryResultsExhausted
			}

			filteringShard = r.shards[r.shardIdx]
			if filteringShard > documentShard {
				// this document is from a shard lower than the next shard allowed.
				return nil
			}
		}
	}

	r.size++
	r.totalDocsCount++

	shardID := ident.ShardID{
		Shard: documentShard,
		ID:    tsID,
	}

	if len(r.batch.ShardIDs) < r.batchSize {
		r.batch.ShardIDs = append(r.batch.ShardIDs, shardID)
	} else {
		// NB: Add any IDs overflowing the batch size to the overflow slice.
		r.idsOverflow = append(r.idsOverflow, shardID)
	}

	return nil
}

func (r *wideResults) Namespace() ident.ID {
	return r.nsID
}

func (r *wideResults) Size() int {
	r.RLock()
	v := r.size
	r.RUnlock()
	return v
}

func (r *wideResults) TotalDocsCount() int {
	r.RLock()
	v := r.totalDocsCount
	r.RUnlock()
	return v
}

// NB: Finalize should be called after all documents have been consumed.
func (r *wideResults) Finalize() {
	r.Lock()
	defer r.Unlock()

	if r.closed {
		return
	}

	// NB: release current
	r.releaseAndWaitWithLock()
	r.closed = true
	close(r.batchCh)
}

func (r *wideResults) releaseAndWaitWithLock() {
	if r.closed || len(r.batch.ShardIDs) == 0 {
		return
	}

	r.batch.ReadyForProcessing()
	r.batchCh <- r.batch
	r.batch.WaitUntilProcessed()
	r.batch.ShardIDs = r.batch.ShardIDs[:0]
	r.size = len(r.idsOverflow)
}

func (r *wideResults) releaseOverflowWithLock() {
	if len(r.batch.ShardIDs) != 0 {
		// If still some IDs in the batch, noop. Theoretically this should not
		// happen, since releaseAndWaitWithLock should be called before
		// releaseOverflowWithLock, which should drain the channel.
		return
	}

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

		// NB: move overflow IDs to the batch itself.
		r.batch.ShardIDs = append(r.batch.ShardIDs, r.idsOverflow[0:size]...)
		copy(r.idsOverflow, r.idsOverflow[size:])
		r.idsOverflow = r.idsOverflow[:overflow-size]
		if incomplete {
			// NB: not enough overflow IDs to create a new batch; seed the existing
			// batch and return.
			return
		}

		r.releaseAndWaitWithLock()
	}
}
