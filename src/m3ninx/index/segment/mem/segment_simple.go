// Copyright (c) 2017 Uber Technologies, Inc.
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

package mem

import (
	"errors"
	"sync"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"

	"github.com/uber-go/atomic"
)

var (
	errUnknownDocID = errors.New("unknown DocID specified")
)

// TODO(prateek): investigate impact of native heap
type simpleSegment struct {
	opts     Options
	id       segment.ID
	docIDGen *atomic.Uint32

	// internal docID -> document
	docs struct {
		sync.RWMutex
		values []document
	}

	// field (Name+Value) -> postingsManagerOffset
	termsDict *simpleTermsDictionary

	searcher searcher
	// TODO(prateek): add a delete documents bitmap to optimise fetch
}

type document struct {
	doc.Document
	docID      segment.DocID
	tombstoned bool
}

// New returns a new in-memory index.
func New(id segment.ID, opts Options) (Segment, error) {
	seg := &simpleSegment{
		opts:      opts,
		id:        id,
		docIDGen:  atomic.NewUint32(0),
		termsDict: newSimpleTermsDictionary(opts),
	}
	seg.docs.values = make([]document, opts.InitialCapacity())
	searcher := newSequentialSearcher(seg, differenceNegationFn, opts.PostingsListPool())
	seg.searcher = searcher
	return seg, nil
}

// TODO(prateek): consider copy semantics for input data, esp if we can store it on the native heap
func (i *simpleSegment) Insert(d doc.Document) error {
	newDoc := document{
		Document: d,
		docID:    segment.DocID(i.docIDGen.Inc()),
	}
	i.insertDocument(newDoc)
	return i.insertTerms(newDoc)
}

func (i *simpleSegment) insertDocument(doc document) {
	docID := int64(doc.docID)
	// can early terminate if we have sufficient capacity
	i.docs.RLock()
	size := len(i.docs.values)
	if int64(size) > docID {
		// NB(prateek): only need a Read-lock here despite an insert operation because
		// we're guranteed to never have conflicts with docID (it's monotonoic increasing),
		// and have checked `i.docs.values` is large enough.
		i.docs.values[doc.docID] = doc
		i.docs.RUnlock()
		return
	}
	i.docs.RUnlock()

	// need to expand capacity
	i.docs.Lock()
	size = len(i.docs.values)
	// expanded since we released the lock
	if int64(size) > docID {
		i.docs.values[doc.docID] = doc
		i.docs.Unlock()
		return
	}

	docs := make([]document, 2*(size+1))
	copy(docs, i.docs.values)
	i.docs.values = docs
	i.docs.values[doc.docID] = doc
	i.docs.Unlock()
}

func (i *simpleSegment) insertTerms(doc document) error {
	// insert each of the indexed fields into the reverse index
	// TODO: current implementation allows for partial indexing. Evaluate perf impact of not doing that.
	for _, field := range doc.Fields {
		if err := i.termsDict.Insert(field, doc.docID); err != nil {
			return err
		}
	}

	return nil
}

func (i *simpleSegment) Query(query segment.Query) (segment.ResultsIter, error) {
	ids, pendingFn, err := i.searcher.Query(query)
	if err != nil {
		return nil, err
	}

	return newResultsIter(ids, pendingFn, i), nil
}

func (i *simpleSegment) Filter(
	f segment.Filter,
) (segment.PostingsList, matchPredicate, error) {
	docs, err := i.termsDict.Fetch(f.FieldName, f.FieldValueFilter, termFetchOptions{isRegexp: f.Regexp})
	return docs, nil, err
}

func (i *simpleSegment) Delete(d doc.Document) error {
	panic("not implemented")
}

func (i *simpleSegment) Size() uint32 {
	return i.docIDGen.Load()
}

func (i *simpleSegment) ID() segment.ID {
	return i.id
}

func (i *simpleSegment) Options() Options {
	return i.opts
}

func (i *simpleSegment) FetchDocument(id segment.DocID) (document, error) {
	i.docs.RLock()
	if int(id) >= len(i.docs.values) {
		i.docs.RUnlock()
		return document{}, errUnknownDocID
	}
	d := i.docs.values[id]
	i.docs.RUnlock()
	return d, nil
}
