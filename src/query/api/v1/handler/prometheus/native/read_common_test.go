// Copyright (c) 2021  Uber Technologies, Inc.
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

package native

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestRewriteRangeDuration(t *testing.T) {
	ctrl := xtest.NewController(t)

	queryTests := []struct {
		name     string
		params   models.RequestParams
		attrs    []storagemetadata.Attributes
		mult     int
		newQuery string
	}{
		{
			name: "query with range to unagg",
			params: models.RequestParams{
				Query: "rate(foo[1m])",
				Start: xtime.ToUnixNano(time.Now().Add(-10 * time.Minute)),
				End:   xtime.Now(),
			},
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.UnaggregatedMetricsType,
					Retention:   7 * 24 * time.Hour,
				},
			},
			mult: 2,
		},
		{
			name: "query with no range",
			params: models.RequestParams{
				Query: "foo",
				Start: xtime.Now(),
				End:   xtime.Now(),
			},
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.UnaggregatedMetricsType,
					Retention:   7 * 24 * time.Hour,
				},
			},
			mult: 2,
		},
		{
			name: "query with range to agg; no rewrite",
			params: models.RequestParams{
				Query: "rate(foo[5m])",
				Start: xtime.ToUnixNano(time.Now().Add(14 * 24 * time.Hour)),
				End:   xtime.Now(),
			},
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   30 * 24 * time.Hour,
					Resolution:  1 * time.Minute,
				},
			},
			mult: 2,
		},
		{
			name: "query with rewriteable range; rewrite disabled",
			params: models.RequestParams{
				Query: "rate(foo[1m])",
				Start: xtime.ToUnixNano(time.Now().Add(14 * 24 * time.Hour)),
				End:   xtime.Now(),
			},
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   30 * 24 * time.Hour,
					Resolution:  5 * time.Minute,
				},
			},
			mult: 0,
		},
		{
			name: "query with range to agg; rewrite",
			params: models.RequestParams{
				Query: "rate(foo[1m])",
				Start: xtime.ToUnixNano(time.Now().Add(14 * 24 * time.Hour)),
				End:   xtime.Now(),
			},
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   30 * 24 * time.Hour,
					Resolution:  5 * time.Minute,
				},
			},
			newQuery: "rate(foo[10m])",
			mult:     2,
		},
		{
			name: "query with range to multiple aggs; rewrite to largest",
			params: models.RequestParams{
				Query: "rate(foo[1m])",
				Start: xtime.ToUnixNano(time.Now().Add(14 * 24 * time.Hour)),
				End:   xtime.Now(),
			},
			attrs: []storagemetadata.Attributes{
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   30 * 24 * time.Hour,
					Resolution:  2 * time.Minute,
				},
				{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   60 * 24 * time.Hour,
					Resolution:  4 * time.Minute,
				},
			},
			newQuery: "rate(foo[12m])",
			mult:     3,
		},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			store := storage.NewMockStorage(ctrl)
			opts := ParsedOptions{
				Params:    tt.params,
				FetchOpts: storage.NewFetchOptions(),
			}
			hOpts := options.EmptyHandlerOptions().
				SetStorage(store)

			cfg := hOpts.Config()
			cfg.Query.RewriteRangesLessThanResolutionMultiplier = tt.mult
			hOpts = hOpts.SetConfig(cfg)

			store.EXPECT().QueryStorageMetadataAttributes(
				context.Background(), opts.Params.Start.ToTime(), opts.Params.End.ToTime(), opts.FetchOpts,
			).Return(tt.attrs, nil)

			updated, params, err := RewriteRangeDuration(context.Background(), opts, hOpts)
			require.NoError(t, err)

			if tt.newQuery == "" {
				require.False(t, updated)
			} else {
				require.True(t, updated)
				opts.Params.Query = tt.newQuery
			}
			require.Equal(t, opts.Params, params)
		})
	}
}
