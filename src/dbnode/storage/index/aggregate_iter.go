// Copyright (c) 2021 Uber Technologies, Inc.
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
	"time"

	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
)

var _ AggregateIterator = &aggregateIter{}

type aggregateIter struct {
	// immutable state
	readers     []segment.Reader
	iterateOpts fieldsAndTermsIteratorOpts
	newIterFn   newFieldsAndTermsIteratorFn

	// immutable state after first Next() call
	iters          []fieldsAndTermsIterator
	searchDuration time.Duration

	// mutable state
	idx                    int
	err                    error
	done                   bool
	currField, currTerm    []byte
	nextField, nextTerm    []byte
	docsCount, seriesCount int
}

func (it *aggregateIter) Next(ctx context.Context) bool {
	if it.Done() {
		return false
	}
	if it.iters == nil {
		for _, reader := range it.readers {
			iter, err := it.newIterFn(ctx, reader, it.iterateOpts)
			if err != nil {
				it.err = err
				return false
			}
			it.searchDuration += iter.SearchDuration()
			it.iters = append(it.iters, iter)
		}
		if !it.next() {
			it.done = true
			return false
		}
		it.nextField, it.nextTerm = it.current()
	}
	// the fieldAndTermsIterator mutates the underlying byte slice, so we need to copy to preserve the value.
	it.currField = append(it.currField[:0], it.nextField...)
	it.currTerm = append(it.currTerm[:0], it.nextTerm...)

	if it.next() {
		it.nextField, it.nextTerm = it.current()
	} else {
		// the iterators have been exhausted. mark done so the next call Done returns true. Still return true from
		// this call so the caller can retrieve the last element with Current.
		it.done = true
	}
	return true
}

func (it *aggregateIter) next() bool {
	if it.idx == len(it.iters) {
		return false
	}
	currIter := it.iters[it.idx]

	for !currIter.Next() {
		if err := currIter.Err(); err != nil {
			it.err = err
			return false
		}
		// move to next iterator so we don't try to Close twice.
		it.idx++

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

func (it *aggregateIter) current() (field, term []byte) {
	return it.iters[it.idx].Current()
}

func (it *aggregateIter) Err() error {
	return it.err
}

func (it *aggregateIter) Done() bool {
	return it.err != nil || it.done
}

func (it *aggregateIter) Current() (field, term []byte) {
	return it.currField, it.currTerm
}

func (it *aggregateIter) fieldsAndTermsIteratorOpts() fieldsAndTermsIteratorOpts {
	return it.iterateOpts
}

func (it *aggregateIter) Close() error {
	if it.iters == nil {
		return nil
	}
	var multiErr xerrors.MultiError
	// close any iterators that weren't closed in Next.
	for i := it.idx; i < len(it.iters); i++ {
		multiErr = multiErr.Add(it.iters[i].Close())
	}
	return multiErr.FinalError()
}

func (it *aggregateIter) SearchDuration() time.Duration {
	return it.searchDuration
}

func (it *aggregateIter) AddSeries(count int) {
	it.seriesCount += count
}

func (it *aggregateIter) AddDocs(count int) {
	it.docsCount += count
}

func (it *aggregateIter) Counts() (series, docs int) {
	return it.seriesCount, it.docsCount
}
