// Copyright (c) 2016 Uber Technologies, Inc.
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

	inx "github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/ident"
)

// Query is a rich end user query to describe a set of constraints on required IDs.
type Query struct {
	inx.Query
}

// QueryOptions enables users to specify constraints on query execution.
type QueryOptions struct {
	StartInclusive time.Time
	EndExclusive   time.Time
	Limit          int
	PageToken      QueryResultsPageToken
}

// QueryResultsPageToken is an opaque token for query result pagination.
type QueryResultsPageToken []byte

// QueryResults is the collection of results for a query.
type QueryResults struct {
	Iter      TaggedIDsIter
	HasMore   bool
	PageToken QueryResultsPageToken
}

// TaggedIDsIter iterates over a collection of IDs with associated
// tags and namespace
type TaggedIDsIter interface {
	// Next returns whether there are more items in the collection
	Next() bool

	// Current returns the ID, Tags and Namespace for a single timeseries.
	// These remain valid until Next() is called again.
	Current() (namespaceID ident.ID, seriesID ident.ID, tags ident.TagIterator)

	// Err returns any error encountered
	Err() error
}
