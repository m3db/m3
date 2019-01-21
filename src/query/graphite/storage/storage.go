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

// Type describes the type of storage
// type Type int

const (
// TypeLocalDC is for storages that reside in the local datacenter
// TypeLocalDC Type = iota
// TypeRemoteDC is for storages that reside in a remote datacenter
// TypeRemoteDC
// TypeMultiDC is for storages that will aggregate multiple datacenters
// TypeMultiDC
)

// Storage provides an interface for retrieving timeseries values or names based upon
// a query or path
type Storage interface {
	// FetchByQuery fetches timeseries data based on a query
	FetchByQuery(
		ctx context.Context, query string, opts FetchOptions,
	) (*FetchResult, error)

	// Name identifies a friendly name for the underlying storage
	// Name() string

	// Type identifies the type of the underlying storage
	// Type() Type
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
