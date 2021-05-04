// Copyright (c) 2021  Uber Technologies, Inc.
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

// Package read is responsible for read logic shared by query engines.
package read

import (
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/ts"
)

// M3QueryResultIterator is an iterator for iterating through
// read query results via the M3 query engine.
type M3QueryResultIterator struct {
	result  Result
	current int
}

// NewM3QueryResultIterator creates a new M3QueryResultIterator
func NewM3QueryResultIterator(result Result) ResultIterator {
	return &M3QueryResultIterator{
		result: result,
	}
}

// Next returns a boolean indicating whether there are more results in this
// iterator.
func (m *M3QueryResultIterator) Next() bool {
	return m.current < len(m.result.Series)
}

// Current returns the current ts.Series at the head of the iterator.
func (m *M3QueryResultIterator) Current() *ts.Series {
	if !m.Next() {
		return nil
	}

	m.current++
	return m.result.Series[m.current-1]
}

// Metadata returns metadata from the read query.
func (m *M3QueryResultIterator) Metadata() block.ResultMetadata {
	return m.result.Meta
}

// BlockType returns the block type for the result.
func (m *M3QueryResultIterator) BlockType() block.BlockType {
	return m.result.BlockType
}

// Reset starts the iterator over from the beginning so that it can be
// re-used.
func (m *M3QueryResultIterator) Reset() {
	m.current = 0
}

// Size returns the total number of results in the iterator.
func (m *M3QueryResultIterator) Size() int {
	return len(m.result.Series)
}
