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
	"bytes"
	"fmt"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/ident"
)

type shardFilterFn func(ident.ID) (uint32, bool)

type wideResults struct {
	nsID        ident.ID
	shardFilter shardFilterFn
	idPool      ident.Pool

	closed      bool
	idsOverflow []ident.ID
	batch       *ident.IDBatch
	batchCh     chan<- *ident.IDBatch
	batchSize   int

	// debug only, remove after
	lastSet   bool
	lastID    []byte
	lastShard uint32
}

// NewWideQueryResults returns a new wide query results object.
// NB: Reader must read results from `batchCh` in a goroutine, and call
// batch.Done() after the result is used, and the writer must close the
// channel after no more Documents are available.
func NewWideQueryResults(
	namespaceID ident.ID,
	batchSize int,
	idPool ident.Pool,
	batchCh chan<- *ident.IDBatch,
	shardFilter shardFilterFn,
) BaseResults {
	return &wideResults{
		nsID:        namespaceID,
		idPool:      idPool,
		batchSize:   batchSize,
		idsOverflow: make([]ident.ID, 0, batchSize),
		batch: &ident.IDBatch{
			IDs: make([]ident.ID, 0, batchSize),
		},
		batchCh:     batchCh,
		shardFilter: shardFilter,
	}
}

func (r *wideResults) AddDocuments(batch []doc.Document) (int, int, error) {
	if r.closed {
		return 0, 0, nil
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
		shard, shardOwned := r.shardFilter(tsID)
		if !shardOwned {
			return nil
		}

		// Debug below.
		if r.lastSet {
			if r.lastShard == shard {
				// ID sorted check
				if bytes.Compare(r.lastID, d.ID) != -1 {
					return fmt.Errorf("IDs within shard %d are unsorted: %s appeared in shard before %s",
						shard, string(r.lastID), string(d.ID))
				}
			}

			// Shard sorted check
			// if r.lastShard > shard {
			// 	return fmt.Errorf("Shards are unsorted: shard %d comes before shard %d",
			// 		int(r.lastShard), int(shard))
			// }
		}

		r.lastSet = true
		r.lastID = append(make([]byte, 0, len(d.ID)), d.ID...)
		r.lastShard = shard
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
