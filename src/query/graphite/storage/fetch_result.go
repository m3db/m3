package storage

import (
	"sync"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/ts"
)

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
