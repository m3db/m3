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
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3x/ident"
)

// QueryFanoutType dictates the fanout range tyoe of a multi fetch merge
type QueryFanoutType uint

// Possible QueryFanoutTypes
const (
	NamespaceCoversAllQueryRange QueryFanoutType = iota
	NamespaceCoversPartialQueryRange
)

// MultiFetchResultBuilder is a deduping accumalator for series iterators
// that allows merging using a given strategy
type MultiFetchResultBuilder interface {
	// Add adds series iterators with corresponding attributes to the accumulator
	Add(
		attrs storage.Attributes,
		iterators encoding.SeriesIterators,
		err error,
	)
	// Build returns a series iterators object containing
	// deduplciated series values
	Build() (encoding.SeriesIterators, error)
	// Close releases all resources held by this accumulator
	Close() error
}

// MultiSearchResultBuilder is a deduping accumalator for tag iterators
type MultiSearchResultBuilder interface {
	// Add adds tagged ID iterators to the accumulator
	Add(
		newIterator client.TaggedIDsIterator,
		err error,
	)
	// Build returns a deduped list of tag iterators with
	// corresponding series ids
	Build() ([]MultiTagResult, error)
	// Close releases resources held by the builder
	Close() error
}

// MultiTagResult represents a tag iterator with its string ID
type MultiTagResult struct {
	// ID is the series ID
	ID ident.ID
	// Iter is the tag iterator for the series
	Iter ident.TagIterator
}
