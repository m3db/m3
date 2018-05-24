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

	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/ts"
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
}

func (q *FetchQuery) String() string {
	return q.Raw
}

// FetchOptions represents the options for fetch query
type FetchOptions struct {
	Limit    int
	KillChan chan struct{}
}

// Querier handles queries against a storage.
type Querier interface {
	// Fetch fetches timeseries data based on a query
	Fetch(
		ctx context.Context, query *FetchQuery, options *FetchOptions) (*FetchResult, error)
	FetchTags(
		ctx context.Context, query *FetchQuery, options *FetchOptions) (*SearchResults, error)
	FetchBlocks(
		ctx context.Context, query *FetchQuery, options *FetchOptions) (BlockResult, error)
}

// WriteQuery represents the input timeseries that is written to M3DB
type WriteQuery struct {
	Raw        string
	Tags       models.Tags
	Datapoints ts.Datapoints
	Unit       xtime.Unit
	Annotation []byte
}

func (q *WriteQuery) String() string {
	return q.Raw
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
	SeriesList []*ts.Series // The aggregated list of results across all underlying storage calls
	LocalOnly  bool
	HasNext    bool
}

// QueryResult is the result from a query
type QueryResult struct {
	FetchResult *FetchResult
	Err         error
}

// BlockResult is the result from a block query
type BlockResult struct {
	Blocks []Block
}
