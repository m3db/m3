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
	LocalOnly                bool          // Whether to fetch from local dc only
	UseCache                 bool          // Whether to fetch from data from the cache
	UseM3DB                  bool          // Whether to fetch from data from M3DB
	RequestCompressed        bool          // Whether to request compressed data from remote storage
	RequestCompressedDeduped bool          // Whether to request compressed deduped data from remote storage
	Timeout                  time.Duration // Whether to use a custom timeout, zero if no or positive if yes
}

// FetchResult provides a fetch result and meta information
type FetchResult struct {
	SeriesList []*ts.Series // The aggregated list of results across all underlying storage calls
	LocalOnly  bool
}

// Type describes the type of storage
type Type int

const (
	// TypeLocalDC is for storages that reside in the local datacenter
	TypeLocalDC Type = iota
	// TypeRemoteDC is for storages that reside in a remote datacenter
	TypeRemoteDC
	// TypeMultiDC is for storages that will aggregate multiple datacenters
	TypeMultiDC
)

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
