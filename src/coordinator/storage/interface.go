package storage

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/ts"

	xtime "github.com/m3db/m3x/time"
)

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

// Storage provides an interface for reading and writing to the tsdb
type Storage interface {
	Querier
	Appender
	// Type identifies the type of the underlying storage
	Type() Type
}

// ReadQuery represents the input query which is fetched from M3DB
type ReadQuery struct {
	TagMatchers models.Matchers
	Start       time.Time
	End         time.Time
}

// Querier handles queries against a storage.
type Querier interface {
	// Fetch fetches timeseries data based on a query
	Fetch(
		ctx context.Context, query *ReadQuery) (*FetchResult, error)
}

// WriteQuery represents the input timeseries that is written to M3DB
type WriteQuery struct {
	Tags       models.Tags
	Datapoints ts.Datapoints
	Unit       xtime.Unit
	Annotation []byte
}

// Appender provides batched appends against a storage.
type Appender interface {
	// Write value to the database for an ID
	Write(ctx context.Context, query *WriteQuery) error
}

// FetchResult provides a fetch result and meta information
type FetchResult struct {
	SeriesList []*ts.Series // The aggregated list of results across all underlying storage calls
	LocalOnly  bool
}
