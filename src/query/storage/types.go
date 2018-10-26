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

package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
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
	// TypeDebug is for storages that are used for debugging purposes
	TypeDebug
)

// Storage provides an interface for reading and writing to the tsdb
type Storage interface {
	Querier
	Appender
	// Type identifies the type of the underlying storage
	Type() Type
	// Close is used to close the underlying storage and free up resources
	Close() error
}

// Query is an interface for a M3DB query
type Query interface {
	fmt.Stringer
	// nolint
	query()
}

func (q *FetchQuery) query() {}
func (q *WriteQuery) query() {}

// FetchQuery represents the input query which is fetched from M3DB
type FetchQuery struct {
	Raw         string
	TagMatchers models.Matchers `json:"matchers"`
	Start       time.Time       `json:"start"`
	End         time.Time       `json:"end"`
	Interval    time.Duration   `json:"interval"`
}

func (q *FetchQuery) String() string {
	return q.Raw
}

// FetchOptions represents the options for fetch query.
type FetchOptions struct {
	// Limit is the maximum number of series to return.
	Limit     int
	BlockType models.FetchedBlockType
	Enforcer  cost.ChainedEnforcer
	Scope     tally.Scope
}

// NewFetchOptions creates a new fetch options.
func NewFetchOptions() *FetchOptions {
	return &FetchOptions{
		Limit:    0,
		Enforcer: cost.NoopChainedEnforcer(),
	}
}

// Querier handles queries against a storage.
type Querier interface {
	// Fetch fetches timeseries data based on a query
	Fetch(
		ctx context.Context,
		query *FetchQuery,
		options *FetchOptions,
	) (*FetchResult, error)

	// FetchBlocks converts fetch results to storage blocks
	FetchBlocks(
		ctx context.Context,
		query *FetchQuery,
		options *FetchOptions,
	) (block.Result, error)

	// FetchTags returns search results for tags
	FetchTags(
		ctx context.Context,
		query *FetchQuery,
		options *FetchOptions,
	) (*SearchResults, error)

	// CompleteTags returns autocompleted tag results
	CompleteTags(
		ctx context.Context,
		query *CompleteTagsQuery,
		options *FetchOptions,
	) (*CompleteTagsResult, error)
}

// WriteQuery represents the input timeseries that is written to the db
type WriteQuery struct {
	Tags       models.Tags
	Datapoints ts.Datapoints
	Unit       xtime.Unit
	Annotation []byte
	Attributes Attributes
}

func (q *WriteQuery) String() string {
	return q.Tags.ID()
}

// CompleteTagsQuery represents a query that returns an autocompleted
// set of tags that exist in the db
type CompleteTagsQuery struct {
	CompleteNameOnly bool
	FilterNameTags   [][]byte
	TagMatchers      models.Matchers
}

// SeriesMatchQuery represents a query that returns a set of series
// that match the query
type SeriesMatchQuery struct {
	TagMatchers []models.Matchers
	Start       time.Time
	End         time.Time
}

func (q *CompleteTagsQuery) String() string {
	if q.CompleteNameOnly {
		return fmt.Sprintf("completing tag name for query %s", q.TagMatchers)
	}

	return fmt.Sprintf("completing tag values for query %s", q.TagMatchers)
}

// CompletedTag is an autocompleted tag with a name and a list of possible values
type CompletedTag struct {
	Name   []byte
	Values [][]byte
}

// CompleteTagsResult represents a set of autocompleted tag names and values
type CompleteTagsResult struct {
	CompleteNameOnly bool
	CompletedTags    []CompletedTag
}

// CompleteTagsResultBuilder is a builder that accumulates and deduplicates
// incoming CompleteTagsResult values
type CompleteTagsResultBuilder interface {
	Add(*CompleteTagsResult) error
	Build() CompleteTagsResult
}

// Appender provides batched appends against a storage.
type Appender interface {
	// Write value to the database for an ID
	Write(ctx context.Context, query *WriteQuery) error
}

// SearchResults is the result from a search
type SearchResults struct {
	Metrics models.Metrics
}

// FetchResult provides a fetch result and meta information
type FetchResult struct {
	SeriesList ts.SeriesList // The aggregated list of results across all underlying storage calls
	LocalOnly  bool
	HasNext    bool
}

// QueryResult is the result from a query
type QueryResult struct {
	FetchResult *FetchResult
	Err         error
}

// MetricsType is a type of stored metrics.
type MetricsType uint

const (
	// UnaggregatedMetricsType is an unaggregated metrics type.
	UnaggregatedMetricsType MetricsType = iota
	// AggregatedMetricsType is an aggregated metrics type.
	AggregatedMetricsType

	// DefaultMetricsType is the default metrics type value.
	DefaultMetricsType = UnaggregatedMetricsType
)

var (
	validMetricsTypes = []MetricsType{
		UnaggregatedMetricsType,
		AggregatedMetricsType,
	}
)

func (t MetricsType) String() string {
	switch t {
	case UnaggregatedMetricsType:
		return "unaggregated"
	case AggregatedMetricsType:
		return "aggregated"
	default:
		return "unknown"
	}
}

// Attributes is a set of stored metrics attributes.
type Attributes struct {
	MetricsType MetricsType
	Retention   time.Duration
	Resolution  time.Duration
}
