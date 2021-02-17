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
	"time"

	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
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
	iters               []doc.Iterator
	totalSearchDuration time.Duration

	// mutable state
	idx     int
	currDoc doc.Document
	err     error
}

func newIterator(ctx context.Context, s search.Searcher, rs index.Readers) (doc.QueryDocIterator, error) {
	start := time.Now()
	docIters, err := newDocIters(ctx, s, rs)
	if err != nil {
		return nil, err
	}
	return &iterator{
		iters:               docIters,
		totalSearchDuration: time.Since(start),
	}, nil
}

func (it *iterator) SearchDuration() time.Duration {
	return it.totalSearchDuration
}

func (it *iterator) Next() bool {
	if it.err != nil || it.idx == len(it.iters) {
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

	it.currDoc = currIter.Current()
	return true
}

func (it *iterator) Current() doc.Document {
	return it.currDoc
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Close() error {
	var multiErr errors.MultiError
	// close any iterators that weren't closed in Next.
	for i := it.idx; i < len(it.iters); i++ {
		multiErr = multiErr.Add(it.iters[it.idx].Close())
	}
	return multiErr.FinalError()
}

func newDocIters(ctx context.Context, searcher search.Searcher, readers index.Readers) ([]doc.Iterator, error) {
	iters := make([]doc.Iterator, len(readers))
	for i, reader := range readers {
		_, sp := ctx.StartTraceSpan(tracepoint.SearchExecutorIndexSearch)
		//start := time.Now()
		pl, err := searcher.Search(reader)
		sp.Finish()
		if err != nil {
			return nil, err
		}
		iter, err := reader.Docs(pl)
		if err != nil {
			return nil, err
		}
		iters[i] = iter
	}
	return iters, nil
}
