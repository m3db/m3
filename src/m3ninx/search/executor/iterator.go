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

package executor

import (
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/errors"
)

// iterator is a wrapper around many doc.Iterators (one per segment) that provides a stream of docs for a index block.
//
// all segments are eagerly searched on the first call to Next(). eagerly searching all the segments for a block allows
// the iterator to yield without holding locks when processing the results set. yielding allows the goroutine to
// yield the index worker to another goroutine waiting and then can resume the iterator when it acquires a worker again.
// this prevents large queries with many result docs from starving small queries.
//
// for each segment, the searcher gets the postings list. the postings list are lazily processed by the returned
// doc iterators. for each posting in the list, the encoded document is retrieved.
type iterator struct {
	// immutable state
	searcher search.Searcher
	query    search.Query
	readers  index.Readers
	ctx      context.Context

	// immutable state after the first call to Next()
	iters []doc.Iterator

	// mutable state
	idx     int
	currDoc doc.Document
	nextDoc doc.Document
	done    bool
	err     error
}

func newIterator(
	ctx context.Context,
	s search.Searcher,
	q search.Query,
	rs index.Readers,
) doc.QueryDocIterator {
	return &iterator{
		ctx:      ctx,
		searcher: s,
		query:    q,
		readers:  rs,
	}
}

func (it *iterator) Done() bool {
	return it.err != nil || it.done
}

func (it *iterator) Next() bool {
	if it.Done() {
		return false
	}
	if it.iters == nil {
		if err := it.initIters(); err != nil {
			it.err = err
			return false
		}
		if !it.next() {
			it.done = true
			return false
		}
		it.nextDoc = it.current()
	}

	it.currDoc = it.nextDoc
	if it.next() {
		it.nextDoc = it.current()
	} else {
		it.done = true
	}
	return true
}

func (it *iterator) next() bool {
	if it.idx == len(it.iters) {
		return false
	}
	currIter := it.iters[it.idx]
	for !currIter.Next() {
		// Check if the current iterator encountered an error.
		if err := currIter.Err(); err != nil {
			it.err = err
			return false
		}
		// move to next iterator so we don't try to Close twice.
		it.idx++

		// Close current iterator now that we are finished with it.
		if err := currIter.Close(); err != nil {
			it.err = err
			return false
		}

		if it.idx == len(it.iters) {
			return false
		}
		currIter = it.iters[it.idx]
	}
	return true
}

func (it *iterator) current() doc.Document {
	return it.iters[it.idx].Current()
}

func (it *iterator) Current() doc.Document {
	return it.currDoc
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Close() error {
	if it.iters == nil {
		return nil
	}
	var multiErr errors.MultiError
	// close any iterators that weren't closed in Next.
	for i := it.idx; i < len(it.iters); i++ {
		multiErr = multiErr.Add(it.iters[i].Close())
	}
	return multiErr.FinalError()
}

func (it *iterator) initIters() error {
	it.iters = make([]doc.Iterator, len(it.readers))
	for i, reader := range it.readers {
		_, sp := it.ctx.StartTraceSpan(tracepoint.SearchExecutorIndexSearch)
		var (
			pl  postings.List
			err error
		)
		if readThrough, ok := reader.(search.ReadThroughSegmentSearcher); ok {
			pl, err = readThrough.Search(it.query, it.searcher)
		} else {
			pl, err = it.searcher.Search(reader)
		}
		sp.Finish()
		if err != nil {
			return err
		}
		iter, err := reader.Docs(pl)
		if err != nil {
			return err
		}
		it.iters[i] = iter
	}
	return nil
}
