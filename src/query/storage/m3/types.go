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

package m3

import (
	"context"

	"github.com/m3db/m3/src/dbnode/encoding"
	genericstorage "github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3x/ident"
)

// Cleanup is a cleanup function to be called after resources are freed
type Cleanup func() error

func noop() error {
	return nil
}

// Storage provides an interface for reading and writing to the tsdb
type Storage interface {
	genericstorage.Storage
	Querier
}

// Querier handles queries against an M3 instance.
type Querier interface {
	// FetchRaw fetches timeseries data based on a query
	FetchRaw(
		ctx context.Context,
		query *genericstorage.FetchQuery,
		options *genericstorage.FetchOptions,
	) (encoding.SeriesIterators, Cleanup, error)

	SearchRaw(
		ctx context.Context,
		query *genericstorage.FetchQuery,
		options *genericstorage.FetchOptions,
	) ([]MultiTagResult, Cleanup, error)
}

// MultiTagResult represents a tag iterator with its string ID
type MultiTagResult struct {
	// ID is the series ID
	ID ident.ID
	// Iter is the tag iterator for the series
	Iter ident.TagIterator
}
