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

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	genericstorage "github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/ident"
)

// Cleanup is a cleanup function to be called after resources are freed.
type Cleanup func() error

func noop() error {
	return nil
}

// Storage provides an interface for reading and writing to the TSDB.
type Storage interface {
	genericstorage.Storage
	Querier
}

// Querier handles queries against an M3 instance.
type Querier interface {
	// FetchCompressed fetches timeseries data based on a query.
	FetchCompressed(
		ctx context.Context,
		query *genericstorage.FetchQuery,
		options *genericstorage.FetchOptions,
	) (SeriesFetchResult, Cleanup, error)

	// SearchCompressed fetches matching tags based on a query.
	SearchCompressed(
		ctx context.Context,
		query *genericstorage.FetchQuery,
		options *genericstorage.FetchOptions,
	) (TagResult, Cleanup, error)

	// CompleteTagsCompressed returns autocompleted tag results.
	CompleteTagsCompressed(
		ctx context.Context,
		query *genericstorage.CompleteTagsQuery,
		options *genericstorage.FetchOptions,
	) (*genericstorage.CompleteTagsResult, error)
}

// SeriesFetchResult is a fetch result with associated metadata.
type SeriesFetchResult struct {
	// Metadata is the set of metadata associated with the fetch result.
	Metadata block.ResultMetadata
	// SeriesIterators is the list of series iterators for the result.
	SeriesIterators encoding.SeriesIterators
}

// MultiFetchResult is a deduping accumalator for series iterators
// that allows merging using a given strategy.
type MultiFetchResult interface {
	// Add appends series fetch results to the accumulator.
	Add(
		fetchResult SeriesFetchResult,
		attrs genericstorage.Attributes,
		err error,
	)

	// FinalResult returns a series fetch result containing deduplicated series
	// iterators and their metadata, and any errors encountered.
	FinalResult() (SeriesFetchResult, error)

	// FinalResult returns a series fetch result containing deduplicated series
	// iterators and their metadata, as well as any attributes corresponding to
	// these results, and any errors encountered.
	FinalResultWithAttrs() (SeriesFetchResult, []genericstorage.Attributes, error)

	// Close releases all resources held by this accumulator.
	Close() error
}

// TagResult is a fetch tag result with associated metadata.
type TagResult struct {
	// Metadata is the set of metadata associated with the fetch result.
	Metadata block.ResultMetadata
	// Tags is the list of tags for the result.
	Tags []MultiTagResult
}

// MultiFetchTagsResult is a deduping accumalator for tag iterators.
type MultiFetchTagsResult interface {
	// Add adds tagged ID iterators to the accumulator.
	Add(
		newIterator client.TaggedIDsIterator,
		meta block.ResultMetadata,
		err error,
	)
	// FinalResult returns a deduped list of tag iterators with
	// corresponding series IDs.
	FinalResult() (TagResult, error)
	// Close releases all resources held by this accumulator.
	Close() error
}

// MultiTagResult represents a tag iterator with its string ID.
type MultiTagResult struct {
	// ID is the series ID.
	ID ident.ID
	// Iter is the tag iterator for the series.
	Iter ident.TagIterator
}
