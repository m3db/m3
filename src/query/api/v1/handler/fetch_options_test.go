// Copyright (c) 2019 Uber Technologies, Inc.
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

package handler

import (
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/storage"

	"github.com/stretchr/testify/require"
)

func TestFetchOptionsBuilder(t *testing.T) {
	tests := []struct {
		name             string
		defaultLimit     int
		headers          map[string]string
		expectedLimit    int
		expectedRestrict *storage.RestrictFetchOptions
		expectedErr      bool
	}{
		{
			name:          "default limit with no headers",
			defaultLimit:  42,
			headers:       map[string]string{},
			expectedLimit: 42,
		},
		{
			name:         "limit with header",
			defaultLimit: 42,
			headers: map[string]string{
				LimitMaxSeriesHeader: "4242",
			},
			expectedLimit: 4242,
		},
		{
			name:         "bad header",
			defaultLimit: 42,
			headers: map[string]string{
				LimitMaxSeriesHeader: "not_a_number",
			},
			expectedErr: true,
		},
		{
			name: "unaggregated metrics type",
			headers: map[string]string{
				MetricsTypeHeader: storage.UnaggregatedMetricsType.String(),
			},
			expectedRestrict: &storage.RestrictFetchOptions{
				MetricsType: storage.UnaggregatedMetricsType,
			},
		},
		{
			name: "aggregated metrics type",
			headers: map[string]string{
				MetricsTypeHeader:          storage.AggregatedMetricsType.String(),
				MetricsStoragePolicyHeader: "1m:14d",
			},
			expectedRestrict: &storage.RestrictFetchOptions{
				MetricsType:   storage.AggregatedMetricsType,
				StoragePolicy: policy.MustParseStoragePolicy("1m:14d"),
			},
		},
		{
			name: "unaggregated metrics type with storage policy",
			headers: map[string]string{
				MetricsTypeHeader:          storage.UnaggregatedMetricsType.String(),
				MetricsStoragePolicyHeader: "1m:14d",
			},
			expectedErr: true,
		},
		{
			name: "aggregated metrics type without storage policy",
			headers: map[string]string{
				MetricsTypeHeader: storage.AggregatedMetricsType.String(),
			},
			expectedErr: true,
		},
		{
			name: "unrecognized metrics type",
			headers: map[string]string{
				MetricsTypeHeader: "foo",
			},
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewFetchOptionsBuilder(FetchOptionsBuilderOptions{
				Limit: test.defaultLimit,
			})

			req := httptest.NewRequest("GET", "/foo", nil)
			for k, v := range test.headers {
				req.Header.Add(k, v)
			}

			opts, err := builder.NewFetchOptions(req)

			if !test.expectedErr {
				require.NoError(t, err)
				require.Equal(t, test.expectedLimit, opts.Limit)
				if test.expectedRestrict == nil {
					require.Nil(t, opts.RestrictFetchOptions)
				} else {
					require.NotNil(t, opts.RestrictFetchOptions)
					require.Equal(t, *test.expectedRestrict, *opts.RestrictFetchOptions)
				}
			} else {
				require.Error(t, err)
			}
		})
	}
}
