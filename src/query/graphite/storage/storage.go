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
	"sync"
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/ts"
)

// FetchOptions provide context to a fetch
type FetchOptions struct {
	StartTime time.Time // The start time for the fetch
	EndTime   time.Time // The end time for the fetch
	DataOptions
}

// DataOptions provide data context
type DataOptions struct {
	Timeout time.Duration // Whether to use a custom timeout, zero if no or positive if yes
}

// Storage provides an interface for retrieving timeseries values or names based upon
// a query or path
type Storage interface {
	// FetchByQuery fetches timeseries data based on a query
	FetchByQuery(
		ctx context.Context, query string, opts FetchOptions,
	) (*FetchResult, error)
}

// FetchResult provides a fetch result and meta information
type FetchResult struct {
	SeriesList []*ts.Series // The aggregated list of results across all underlying storage calls
	LocalOnly  bool
}

// Close will return the fetch result to the pool.
func (fr *FetchResult) Close() error {
	fr.SeriesList = nil
	fetchResultPool.Put(fr)
	return nil
}

// Reset will wipe out existing fetch result data.
func (fr *FetchResult) Reset() {
	fr.SeriesList = nil
}

var (
	fetchResultPool = &sync.Pool{
		New: func() interface{} {
			return &FetchResult{}
		},
	}
)

// NewFetchResult is a convenience method for creating a FetchResult
func NewFetchResult(ctx context.Context, seriesList []*ts.Series) *FetchResult {
	fetchResult := fetchResultPool.Get().(*FetchResult)
	fetchResult.Reset()

	fetchResult.SeriesList = seriesList

	ctx.RegisterCloser(fetchResult)

	return fetchResult
}
