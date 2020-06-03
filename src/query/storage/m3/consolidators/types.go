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

package consolidators

import (
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/ident"
)

// MatchOptions are multi fetch matching options.
type MatchOptions struct {
	// MatchType is the equality matching type by which to compare series.
	MatchType MatchType
}

// MatchType is a equality match type.
type MatchType uint

const (
	// MatchIDs matches series based on ID only.
	MatchIDs MatchType = iota
	// MatchTags matcher series based on tags.
	MatchTags
)

// QueryFanoutType is a query fanout type.
type QueryFanoutType uint

const (
	// NamespaceInvalid indicates there is no valid namespace.
	NamespaceInvalid QueryFanoutType = iota
	// NamespaceCoversAllQueryRange indicates the given namespace covers
	// the entire query range.
	NamespaceCoversAllQueryRange
	// NamespaceCoversPartialQueryRange indicates the given namespace covers
	// a partial query range.
	NamespaceCoversPartialQueryRange
)

func (t QueryFanoutType) String() string {
	switch t {
	case NamespaceCoversAllQueryRange:
		return "coversAllQueryRange"
	case NamespaceCoversPartialQueryRange:
		return "coversPartialQueryRange"
	default:
		return "unknown"
	}
}

// MultiFetchResult is a deduping accumalator for series iterators
// that allows merging using a given strategy.
type MultiFetchResult interface {
	// Add appends series fetch results to the accumulator.
	Add(
		fetchResult SeriesFetchResult,
		attrs Attributes,
		err error,
	)

	// FinalResult returns a series fetch result containing deduplicated series
	// iterators and their metadata, and any errors encountered.
	FinalResult() (SeriesFetchResult, error)

	// FinalResult returns a series fetch result containing deduplicated series
	// iterators and their metadata, as well as any attributes corresponding to
	// these results, and any errors encountered.
	FinalResultWithAttrs() (SeriesFetchResult, []Attributes, error)

	// Close releases all resources held by this accumulator.
	Close() error
}

// SeriesFetchResult is a fetch result with associated metadata.
type SeriesFetchResult struct {
	// Metadata is the set of metadata associated with the fetch result.
	Metadata block.ResultMetadata
	// seriesData is the list of series data for the result.
	seriesData seriesData
}

// SeriesData is fetched series data.
type seriesData struct {
	// seriesIterators are the series iterators for the series.
	seriesIterators encoding.SeriesIterators
	// tags are the decoded tags for the series.
	tags []*models.Tags
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

// Attributes is a set of stored metrics attributes.
type Attributes struct {
	// MetricsType indicates the type of namespace this metric originated from.
	MetricsType MetricsType
	// Retention indicates the retention of the namespace this metric originated
	// from.
	Retention time.Duration
	// Resolution indicates the retention of the namespace this metric originated
	// from.
	Resolution time.Duration
}

// Validate validates a storage attributes.
func (a Attributes) Validate() error {
	return ValidateMetricsType(a.MetricsType)
}
