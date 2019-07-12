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
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchOptionsBuilder(t *testing.T) {
	type expectedLookback struct {
		value time.Duration
	}

	tests := []struct {
		name             string
		defaultLimit     int
		headers          map[string]string
		query            string
		expectedLimit    int
		expectedRestrict *storage.RestrictFetchOptions
		expectedLookback *expectedLookback
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
		{
			name:  "can set lookback duration",
			query: "lookback=10s",
			expectedLookback: &expectedLookback{
				value: 10 * time.Second,
			},
		},
		{
			name:  "can set lookback duration based on step",
			query: "lookback=step&step=10s",
			expectedLookback: &expectedLookback{
				value: 10 * time.Second,
			},
		},
		{
			name:        "bad lookback returns error",
			query:       "lookback=foo",
			expectedErr: true,
		},
		{
			name:        "loookback step but step is bad",
			query:       "lookback=step&step=invalid",
			expectedErr: true,
		},
		{
			name:        "lookback step but step is negative",
			query:       "lookback=step&step=-1",
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := NewFetchOptionsBuilder(FetchOptionsBuilderOptions{
				Limit: test.defaultLimit,
			})

			url := "/foo"
			if test.query != "" {
				url += "?" + test.query
			}
			req := httptest.NewRequest("GET", url, nil)
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
				if test.expectedLookback == nil {
					require.Nil(t, opts.LookbackDuration)
				} else {
					require.NotNil(t, opts.LookbackDuration)
					require.Equal(t, test.expectedLookback.value, *opts.LookbackDuration)
				}
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestInvalidStep(t *testing.T) {
	req := httptest.NewRequest("GET", "/foo", nil)
	vals := make(url.Values)
	vals.Del(StepParam)
	vals.Add(StepParam, "-10.50s")
	req.URL.RawQuery = vals.Encode()
	_, _, err := ParseStep(req)
	require.NotNil(t, err, "unable to parse request")
}

func TestParseLookbackDuration(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/foo", nil)
	_, ok, err := ParseLookbackDuration(r)
	require.NoError(t, err)
	require.False(t, ok)

	r = httptest.NewRequest(http.MethodGet, "/foo?step=60s&lookback=step", nil)
	v, ok, err := ParseLookbackDuration(r)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, time.Minute, v)

	r = httptest.NewRequest(http.MethodGet, "/foo?step=60s&lookback=120s", nil)
	v, ok, err = ParseLookbackDuration(r)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 2*time.Minute, v)

	r = httptest.NewRequest(http.MethodGet, "/foo?step=60s&lookback=foobar", nil)
	_, _, err = ParseLookbackDuration(r)
	require.Error(t, err)
}

func TestParseDuration(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/foo?step=10s", nil)
	require.NoError(t, err)
	v, err := ParseDuration(r, StepParam)
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, v)
}

func TestParseFloatDuration(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/foo?step=10.50m", nil)
	require.NoError(t, err)
	v, err := ParseDuration(r, StepParam)
	require.NoError(t, err)
	assert.Equal(t, 10*time.Minute+30*time.Second, v)

	r = httptest.NewRequest(http.MethodGet, "/foo?step=10.00m", nil)
	require.NoError(t, err)
	v, err = ParseDuration(r, StepParam)
	require.NoError(t, err)
	assert.Equal(t, 10*time.Minute, v)
}

func TestParseDurationParsesIntAsSeconds(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/foo?step=30", nil)
	require.NoError(t, err)
	v, err := ParseDuration(r, StepParam)
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, v)
}

func TestParseDurationParsesFloatAsSeconds(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/foo?step=30.00", nil)
	require.NoError(t, err)
	v, err := ParseDuration(r, StepParam)
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, v)
}

func TestParseDurationError(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/foo?step=bar10", nil)
	require.NoError(t, err)
	_, err = ParseDuration(r, StepParam)
	assert.Error(t, err)
}

func TestParseDurationOverflowError(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, fmt.Sprintf("/foo?step=%f", float64(math.MaxInt64)), nil)
	require.NoError(t, err)
	_, err = ParseDuration(r, StepParam)
	assert.Error(t, err)
}
