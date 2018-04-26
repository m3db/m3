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

package mem

import (
	"errors"
	"regexp"
	"sync"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index"
	"github.com/m3db/m3ninx/postings"
)

var (
	errSegmentReaderClosed = errors.New("segment reader is closed")
)

type reader struct {
	sync.RWMutex

	segment ReadableSegment
	limit   postings.ID

	closed bool
}

func newReader(s ReadableSegment, limit postings.ID) index.Reader {
	return &reader{
		segment: s,
		limit:   limit,
	}
}

func (r *reader) MatchTerm(field, term []byte) (postings.List, error) {
	r.RLock()
	if r.closed {
		r.RUnlock()
		return nil, errSegmentReaderClosed
	}

	// A reader can return IDs in the posting list which are greater than its limit.
	// The reader only guarantees that when fetching the documents associated with a
	// postings list through a call to Docs, IDs greater than or equal to the limit
	// will be filtered out.
	pl, err := r.segment.matchTerm(field, term)
	r.RUnlock()
	return pl, err
}

func (r *reader) MatchRegexp(field, regexp []byte, compiled *regexp.Regexp) (postings.List, error) {
	r.RLock()
	if r.closed {
		r.RUnlock()
		return nil, errSegmentReaderClosed
	}

	// A reader can return IDs in the posting list which are greater than its maximum
	// permitted ID. The reader only guarantees that when fetching the documents associated
	// with a postings list through a call to Docs will IDs greater than the maximum be
	// filtered out.
	pl, err := r.segment.matchRegexp(field, regexp, compiled)
	r.RUnlock()
	return pl, err
}

func (r *reader) Docs(pl postings.List) (doc.Iterator, error) {
	return newIterator(r.segment, pl.Iterator(), r.limit), nil
}

func (r *reader) Close() error {
	r.Lock()
	if r.closed {
		r.Unlock()
		return errSegmentReaderClosed
	}
	r.closed = true
	r.Unlock()
	return nil
}
