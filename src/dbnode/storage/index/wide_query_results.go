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
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/ident"
)

type wideResults struct {
	sync.RWMutex

	nsID   ident.ID
	opts   QueryResultsOptions
	idPool ident.Pool

	closed        bool
	batch         ident.IDBatch
	batchOverflow ident.IDBatch
	batchCh       chan<- ident.IDBatch
	closeCh       chan struct{}
	batchSize     int
}

// NewWideQueryResults returns a new wide query results object.
func NewWideQueryResults(
	namespaceID ident.ID,
	batchSize int,
	idPool ident.Pool,
	batchCh chan<- ident.IDBatch,
	opts QueryResultsOptions,
) BaseResults {
	return &wideResults{
		nsID:          namespaceID,
		idPool:        idPool,
		batchSize:     batchSize,
		batch:         make(ident.IDBatch, 0, batchSize),
		batchOverflow: make(ident.IDBatch, 0, batchSize),
		batchCh:       batchCh,
		closeCh:       make(chan struct{}),
	}
}

// NB: If documents with duplicate IDs are added, they are simply ignored and
// the first document added with an ID is returned.
func (r *wideResults) AddDocuments(batch []doc.Document) (int, int, error) {
	r.Lock()
	if r.closed {
		r.Unlock()
		return 0, 0, nil
	}

	err := r.addDocumentsBatchWithLock(batch)
	release := len(r.batch) > r.batchSize
	r.Unlock()

	if release {
		select {
		case r.batchCh <- r.batch:
			r.releaseOverflow()
		case <-r.closeCh:
		}
	}

	return 0, 0, err
}

func (r *wideResults) releaseOverflow() {
	for {
		r.Lock()
		r.batch = r.batchOverflow[0:r.batchSize]
		copy(r.batchOverflow, r.batchOverflow[r.batchSize:])
		r.batchOverflow = r.batchOverflow[:len(r.batchOverflow)-r.batchSize]
		release := len(r.batch) > r.batchSize
		r.Unlock()
		if !release {
			return
		}

		select {
		case r.batchCh <- r.batch:
			r.releaseOverflow()
		case <-r.closeCh:
		}
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
	if r.opts.FilterID != nil && !r.opts.FilterID(tsID) {
		return nil
	}

	// Pool IDs after filter is passed.
	tsID = r.idPool.Clone(tsID)

	if len(r.batch) > r.batchSize {
		r.batch = append(r.batch, tsID)
	} else {
		r.batchOverflow = append(r.batchOverflow, tsID)
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
	r.Lock()
	if r.closed {
		return
	}

	r.closed = true
	r.Unlock()
	r.closeCh <- struct{}{}
}
