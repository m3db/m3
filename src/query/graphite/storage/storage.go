// Copyright (c) 2019 Uber Technologies, Inc.
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

package storage

import (
	stdcontext "context"
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/ts"
	querystorage "github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
)

// FetchOptions provides context to a fetch expression.
type FetchOptions struct {
	// StartTime is the start time for the fetch.
	StartTime time.Time
	// EndTime is the end time for the fetch.
	EndTime time.Time
	// DataOptions are the options for the fetch.
	DataOptions
	// Source is the query source.
	Source []byte
	// QueryFetchOpts are the query storage fetch options.
	QueryFetchOpts *querystorage.FetchOptions
}

// DataOptions provide data context.
type DataOptions struct {
	// Timeout determines a custom timeout for the context. If set to 0, uses
	// the default timeout.
	Timeout time.Duration
	// Limit is the limit for number of datapoints to retrieve.
	Limit int
}

// Storage provides an interface for retrieving timeseries values or names
// based upon a query or path.
type Storage interface {
	// FetchByQuery fetches timeseries data based on a query.
	FetchByQuery(
		ctx context.Context,
		query string,
		opts FetchOptions,
	) (*FetchResult, error)

	// CompleteTags fetches tag data based on a request.
	CompleteTags(
		ctx stdcontext.Context,
		query *querystorage.CompleteTagsQuery,
		opts *querystorage.FetchOptions,
	) (*consolidators.CompleteTagsResult, error)
}

// FetchResult provides a fetch result and meta information.
type FetchResult struct {
	// SeriesList is the aggregated list of results across all underlying storage
	// calls.
	SeriesList []*ts.Series
	// Metadata contains any additional metadata indicating information about
	// series execution.
	Metadata block.ResultMetadata
}

// Close will return the fetch result to the pool.
func (fr *FetchResult) Close() error {
	fr.SeriesList = nil
	fr.Metadata = block.NewResultMetadata()
	fetchResultPool.Put(fr)
	return nil
}

// Reset will wipe out existing fetch result data.
func (fr *FetchResult) Reset() {
	fr.SeriesList = nil
	fr.Metadata = block.NewResultMetadata()
}

var (
	fetchResultPool = &sync.Pool{
		New: func() interface{} {
			return &FetchResult{
				Metadata: block.NewResultMetadata(),
			}
		},
	}
)

// NewFetchResult is a convenience method for creating a FetchResult.
func NewFetchResult(
	ctx context.Context,
	seriesList []*ts.Series,
	resultMeta block.ResultMetadata,
) *FetchResult {
	fetchResult := fetchResultPool.Get().(*FetchResult)
	fetchResult.Reset()
	fetchResult.SeriesList = seriesList
	fetchResult.Metadata = resultMeta
	ctx.RegisterCloser(fetchResult)
	return fetchResult
}
