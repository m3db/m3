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

package multiresults

import (
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/storage"
)

// QueryFanoutType dictates the fanout range tyoe of a multi fetch merge
type QueryFanoutType uint

// Possible QueryFanoutTypes
const (
	NamespaceCoversAllQueryRange QueryFanoutType = iota
	NamespaceCoversPartialQueryRange
)

// MultiFetchResultBuilder is a builder for combining series iterators with a strategy
type MultiFetchResultBuilder interface {
	// Add adds series iterators to the builder
	Add(
		attrs storage.Attributes,
		iterators encoding.SeriesIterators,
		err error,
	)
	// Build builds a combined set of series iterators
	Build() (encoding.SeriesIterators, error)
	// Close releases resources held by the builder
	Close() error
}

// MultiSearchResultBuilder is a builder for combining search results
type MultiSearchResultBuilder interface {
	// Add adds series iterators to the builder
	Add(
		result *storage.SearchResults,
		err error,
	)
	// Build builds a combined set of series iterators
	Build() (*storage.SearchResults, error)
	// Close releases resources held by the builder
	Close() error
}
