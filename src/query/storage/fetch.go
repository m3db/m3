// Copyright (c) 2020 Uber Technologies, Inc.
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
	"time"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
)

const (
	defaultMaxMetricMetadataStats = 4
)

// NewFetchOptions creates a new fetch options.
func NewFetchOptions() *FetchOptions {
	return &FetchOptions{
		SeriesLimit:            0,
		DocsLimit:              0,
		MaxMetricMetadataStats: defaultMaxMetricMetadataStats,
		BlockType:              models.TypeSingleBlock,
		FanoutOptions: &FanoutOptions{
			FanoutUnaggregated:        FanoutDefault,
			FanoutAggregated:          FanoutDefault,
			FanoutAggregatedOptimized: FanoutDefault,
		},
		Scope: tally.NoopScope,
	}
}

// LookbackDurationOrDefault returns either the default lookback duration or
// overridden lookback duration if set.
func (o *FetchOptions) LookbackDurationOrDefault(
	defaultValue time.Duration,
) time.Duration {
	if o.LookbackDuration == nil {
		return defaultValue
	}
	return *o.LookbackDuration
}

// QueryFetchOptions returns fetch options for a given query.
func (o *FetchOptions) QueryFetchOptions(
	queryCtx *models.QueryContext,
	blockType models.FetchedBlockType,
) (*FetchOptions, error) {
	r := o.Clone()
	if r.SeriesLimit <= 0 {
		r.SeriesLimit = queryCtx.Options.LimitMaxTimeseries
	}
	if r.DocsLimit <= 0 {
		r.DocsLimit = queryCtx.Options.LimitMaxDocs
	}

	// Use inbuilt options for type restriction if none found.
	if r.RestrictQueryOptions.GetRestrictByType() == nil &&
		queryCtx.Options.RestrictFetchType != nil {
		v := queryCtx.Options.RestrictFetchType
		restrict := &RestrictByType{
			MetricsType:   storagemetadata.MetricsType(v.MetricsType),
			StoragePolicy: v.StoragePolicy,
		}

		if err := restrict.Validate(); err != nil {
			return nil, err
		}

		if r.RestrictQueryOptions == nil {
			r.RestrictQueryOptions = &RestrictQueryOptions{}
		}

		r.RestrictQueryOptions.RestrictByType = restrict
	}

	return r, nil
}

// Clone will clone and return the fetch options.
func (o *FetchOptions) Clone() *FetchOptions {
	result := *o
	return &result
}
