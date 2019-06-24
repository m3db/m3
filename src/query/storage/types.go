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
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/generated/proto/rpcpb"

	"github.com/m3db/m3/src/metrics/policy"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
)

var (
	errNoRestrictFetchOptionsProtoMsg = errors.New("no restrict fetch options proto message")
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
	Limit int
	// BlockType is the block type that the fetch function returns.
	BlockType models.FetchedBlockType
	// FanoutOptions are the options for the fetch namespace fanout.
	FanoutOptions *FanoutOptions
	// RestrictFetchOptions restricts the fetch to a specific set of
	// conditions.
	RestrictFetchOptions *RestrictFetchOptions
	// Enforcer is used to enforce resource limits on the number of datapoints
	// used by a given query. Limits are imposed at time of decompression.
	Enforcer cost.ChainedEnforcer
	// Scope is used to report metrics about the fetch.
	Scope tally.Scope
}

// FanoutOptions describes which namespaces should be fanned out to for
// the query.
type FanoutOptions struct {
	// FanoutUnaggregated describes the fanout options for
	// unaggregated namespaces.
	FanoutUnaggregated FanoutOption
	// FanoutAggregated describes the fanout options for
	// aggregated namespaces.
	FanoutAggregated FanoutOption
	// FanoutAggregatedOptimized describes the fanout options for the
	// aggregated namespace optimization.
	FanoutAggregatedOptimized FanoutOption
}

// FanoutOption describes the fanout option.
type FanoutOption uint

const (
	// FanoutDefault defaults to the fanout option.
	FanoutDefault FanoutOption = iota
	// FanoutForceDisable forces disabling fanout.
	FanoutForceDisable
	// FanoutForceEnable forces enabling fanout.
	FanoutForceEnable
)

// NewFetchOptions creates a new fetch options.
func NewFetchOptions() *FetchOptions {
	return &FetchOptions{
		Limit:     0,
		BlockType: models.TypeSingleBlock,
		FanoutOptions: &FanoutOptions{
			FanoutUnaggregated:        FanoutDefault,
			FanoutAggregated:          FanoutDefault,
			FanoutAggregatedOptimized: FanoutDefault,
		},
		Enforcer: cost.NoopChainedEnforcer(),
		Scope:    tally.NoopScope,
	}
}

// Clone will clone and return the fetch options.
func (o *FetchOptions) Clone() *FetchOptions {
	result := *o
	return &result
}

// RestrictFetchOptions restricts the fetch to a specific set of conditions.
type RestrictFetchOptions struct {
	// MetricsType restricts the type of metrics being returned.
	MetricsType MetricsType
	// StoragePolicy is required if metrics type is not unaggregated
	// to specify which storage policy metrics should be returned from.
	StoragePolicy policy.StoragePolicy
}

// NewRestrictFetchOptionsFromProto returns a restrict fetch options from
// protobuf message.
func NewRestrictFetchOptionsFromProto(
	p *rpcpb.RestrictFetchOptions,
) (RestrictFetchOptions, error) {
	var result RestrictFetchOptions

	if p == nil {
		return result, errNoRestrictFetchOptionsProtoMsg
	}

	switch p.MetricsType {
	case rpcpb.MetricsType_UNAGGREGATED_METRICS_TYPE:
		result.MetricsType = UnaggregatedMetricsType
	case rpcpb.MetricsType_AGGREGATED_METRICS_TYPE:
		result.MetricsType = AggregatedMetricsType
	}

	if p.MetricsStoragePolicy != nil {
		storagePolicy, err := policy.NewStoragePolicyFromProto(
			p.MetricsStoragePolicy)
		if err != nil {
			return result, err
		}

		result.StoragePolicy = storagePolicy
	}

	// Validate the resulting options.
	if err := result.Validate(); err != nil {
		return result, err
	}

	return result, nil
}

// Validate will validate the restrict fetch options.
func (o RestrictFetchOptions) Validate() error {
	switch o.MetricsType {
	case UnaggregatedMetricsType:
		if o.StoragePolicy != policy.EmptyStoragePolicy {
			return fmt.Errorf(
				"expected no storage policy for unaggregated metrics type, "+
					"instead got: %v", o.StoragePolicy.String())
		}
	case AggregatedMetricsType:
		if v := o.StoragePolicy.Resolution().Window; v <= 0 {
			return fmt.Errorf(
				"expected positive resolution window, instead got: %v", v)
		}
		if v := o.StoragePolicy.Resolution().Precision; v <= 0 {
			return fmt.Errorf(
				"expected positive resolution precision, instead got: %v", v)
		}
		if v := o.StoragePolicy.Retention().Duration(); v <= 0 {
			return fmt.Errorf(
				"expected positive retention, instead got: %v", v)
		}
	default:
		return fmt.Errorf(
			"unknown metrics type: %v", o.MetricsType)
	}
	return nil
}

// Proto returns the protobuf message that corresponds to RestrictFetchOptions.
func (o RestrictFetchOptions) Proto() (*rpcpb.RestrictFetchOptions, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}

	result := &rpcpb.RestrictFetchOptions{}

	switch o.MetricsType {
	case UnaggregatedMetricsType:
		result.MetricsType = rpcpb.MetricsType_UNAGGREGATED_METRICS_TYPE
	case AggregatedMetricsType:
		result.MetricsType = rpcpb.MetricsType_AGGREGATED_METRICS_TYPE

		storagePolicyProto, err := o.StoragePolicy.Proto()
		if err != nil {
			return nil, err
		}

		result.MetricsStoragePolicy = storagePolicyProto
	}

	return result, nil
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

	// SearchSeries returns series IDs matching the current query
	SearchSeries(
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
	return string(q.Tags.ID())
}

// CompleteTagsQuery represents a query that returns an autocompleted
// set of tags that exist in the db
type CompleteTagsQuery struct {
	CompleteNameOnly bool
	FilterNameTags   [][]byte
	TagMatchers      models.Matchers
	Start            time.Time
	End              time.Time
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
	// UnknownMetricsType is the unknown metrics type and is invalid.
	UnknownMetricsType MetricsType = iota
	// UnaggregatedMetricsType is an unaggregated metrics type.
	UnaggregatedMetricsType
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
