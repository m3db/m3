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

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/context"
)

type queryIter struct {
	// immutable state
	docIter doc.QueryDocIterator

	// mutable state
	seriesCount, docCount int
}

var _ QueryIterator = &queryIter{}

// NewQueryIter wraps the provided QueryDocIterator as a QueryIterator
func NewQueryIter(docIter doc.QueryDocIterator) QueryIterator {
	return &queryIter{
		docIter: docIter,
	}
}

func (q *queryIter) Done() bool {
	return q.docIter.Done()
}

func (q *queryIter) Next(_ context.Context) bool {
	return q.docIter.Next()
}

func (q *queryIter) Err() error {
	return q.docIter.Err()
}

func (q *queryIter) SearchDuration() time.Duration {
	return q.docIter.SearchDuration()
}

func (q *queryIter) Close() error {
	return q.docIter.Close()
}

func (q *queryIter) AddSeries(count int) {
	q.seriesCount += count
}

func (q *queryIter) AddDocs(count int) {
	q.docCount += count
}

func (q *queryIter) Counts() (series, docs int) {
	return q.seriesCount, q.docCount
}

func (q *queryIter) Current() doc.Document {
	return q.docIter.Current()
}
