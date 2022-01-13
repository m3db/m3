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

package handleroptions

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchOptionsBuilder(t *testing.T) {
	type expectedLookback struct {
		value time.Duration
	}

	tests := []struct {
		name                                  string
		defaultLimit                          int
		defaultRangeLimit                     time.Duration
		defaultRestrictByTag                  *storage.RestrictByTag
		headers                               map[string]string
		query                                 string
		expectedLimit                         int
		expectedRangeLimit                    time.Duration
		expectedRestrict                      *storage.RestrictQueryOptions
		expectedLookback                      *expectedLookback
		expectedReadConsistencyLevel          *topology.ReadConsistencyLevel
		expectedIterateEqualTimestampStrategy *encoding.IterateEqualTimestampStrategy
		expectedErr                           bool
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
				headers.LimitMaxSeriesHeader: "4242",
			},
			expectedLimit: 4242,
		},
		{
			name:         "bad limit header",
			defaultLimit: 42,
			headers: map[string]string{
				headers.LimitMaxSeriesHeader: "not_a_number",
			},
			expectedErr: true,
		},
		{
			name:               "default range limit with no headers",
			defaultRangeLimit:  42 * time.Hour,
			headers:            map[string]string{},
			expectedRangeLimit: 42 * time.Hour,
		},
		{
			name:              "range limit with header",
			defaultRangeLimit: 42 * time.Hour,
			headers: map[string]string{
				headers.LimitMaxRangeHeader: "84h",
			},
			expectedRangeLimit: 84 * time.Hour,
		},
		{
			name:              "bad range limit header",
			defaultRangeLimit: 42 * time.Hour,
			headers: map[string]string{
				// Not a parseable time range string.
				headers.LimitMaxRangeHeader: "4242",
			},
			expectedErr: true,
		},
		{
			name: "unaggregated metrics type",
			headers: map[string]string{
				headers.MetricsTypeHeader: storagemetadata.UnaggregatedMetricsType.String(),
			},
			expectedRestrict: &storage.RestrictQueryOptions{
				RestrictByType: &storage.RestrictByType{
					MetricsType: storagemetadata.UnaggregatedMetricsType,
				},
			},
		},
		{
			name: "aggregated metrics type",
			headers: map[string]string{
				headers.MetricsTypeHeader:          storagemetadata.AggregatedMetricsType.String(),
				headers.MetricsStoragePolicyHeader: "1m:14d",
			},
			expectedRestrict: &storage.RestrictQueryOptions{
				RestrictByType: &storage.RestrictByType{
					MetricsType:   storagemetadata.AggregatedMetricsType,
					StoragePolicy: policy.MustParseStoragePolicy("1m:14d"),
				},
			},
		},
		{
			name: "unaggregated metrics type with storage policy",
			headers: map[string]string{
				headers.MetricsTypeHeader:          storagemetadata.UnaggregatedMetricsType.String(),
				headers.MetricsStoragePolicyHeader: "1m:14d",
			},
			expectedErr: true,
		},
		{
			name: "aggregated metrics type without storage policy",
			headers: map[string]string{
				headers.MetricsTypeHeader: storagemetadata.AggregatedMetricsType.String(),
			},
			expectedErr: true,
		},
		{
			name: "unrecognized metrics type",
			headers: map[string]string{
				headers.MetricsTypeHeader: "foo",
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
		{
			name: "restrict by tags json header",
			headers: map[string]string{
				headers.RestrictByTagsJSONHeader: stripSpace(`{
					"match":[{"name":"foo", "value":"bar", "type":"EQUAL"}],
					"strip":["foo"]
				}`),
			},
			expectedRestrict: &storage.RestrictQueryOptions{
				RestrictByTag: &storage.RestrictByTag{
					Restrict: models.Matchers{
						mustMatcher("foo", "bar", models.MatchEqual),
					},
					Strip: toStrip("foo"),
				},
			},
		},
		{
			name: "restrict by tags json defaults",
			defaultRestrictByTag: &storage.RestrictByTag{
				Restrict: models.Matchers{
					mustMatcher("foo", "bar", models.MatchEqual),
				},
				Strip: toStrip("foo"),
			},
			expectedRestrict: &storage.RestrictQueryOptions{
				RestrictByTag: &storage.RestrictByTag{
					Restrict: models.Matchers{
						mustMatcher("foo", "bar", models.MatchEqual),
					},
					Strip: toStrip("foo"),
				},
			},
		},
		{
			name: "restrict by tags json default override by header",
			defaultRestrictByTag: &storage.RestrictByTag{
				Restrict: models.Matchers{
					mustMatcher("foo", "bar", models.MatchEqual),
				},
				Strip: toStrip("foo"),
			},
			headers: map[string]string{
				headers.RestrictByTagsJSONHeader: stripSpace(`{
					"match":[{"name":"qux", "value":"qaz", "type":"EQUAL"}],
					"strip":["qux"]
				}`),
			},
			expectedRestrict: &storage.RestrictQueryOptions{
				RestrictByTag: &storage.RestrictByTag{
					Restrict: models.Matchers{
						mustMatcher("qux", "qaz", models.MatchEqual),
					},
					Strip: toStrip("qux"),
				},
			},
		},
		{
			name: "restrict by policies with metrics type",
			headers: map[string]string{
				headers.MetricsTypeHeader:                      "aggregated",
				headers.MetricsRestrictByStoragePoliciesHeader: "10m:60d",
			},
			expectedErr: true,
		},
		{
			name: "restrict by policies with storage policy",
			headers: map[string]string{
				headers.MetricsStoragePolicyHeader:             "10m:60d",
				headers.MetricsRestrictByStoragePoliciesHeader: "10m:60d",
			},
			expectedErr: true,
		},
		{
			name: "restrict by policies - invalid policy",
			headers: map[string]string{
				headers.MetricsRestrictByStoragePoliciesHeader: "10m",
			},
			expectedErr: true,
		},
		{
			name: "restrict by policies - invalid delimiter",
			headers: map[string]string{
				headers.MetricsRestrictByStoragePoliciesHeader: "10m:60d,5m:30d",
			},
			expectedErr: true,
		},
		{
			name: "restrict by policies - single policy",
			headers: map[string]string{
				headers.MetricsRestrictByStoragePoliciesHeader: "10m:60d",
			},
			expectedRestrict: &storage.RestrictQueryOptions{
				RestrictByTypes: []*storage.RestrictByType{
					{
						MetricsType:   storagemetadata.AggregatedMetricsType,
						StoragePolicy: policy.MustParseStoragePolicy("10m:60d"),
					},
				},
			},
		},
		{
			name: "restrict by policies - multiple policy",
			headers: map[string]string{
				headers.MetricsRestrictByStoragePoliciesHeader: "10m:60d;5m:30d",
			},
			expectedRestrict: &storage.RestrictQueryOptions{
				RestrictByTypes: []*storage.RestrictByType{
					{
						MetricsType:   storagemetadata.AggregatedMetricsType,
						StoragePolicy: policy.MustParseStoragePolicy("10m:60d"),
					},
					{
						MetricsType:   storagemetadata.AggregatedMetricsType,
						StoragePolicy: policy.MustParseStoragePolicy("5m:30d"),
					},
				},
			},
		},
		{
			name: "read overrides",
			headers: map[string]string{
				headers.ReadConsistencyLevelHeader:          "2",
				headers.IterateEqualTimestampStrategyHeader: "2",
			},
			expectedReadConsistencyLevel:          &topology.ValidReadConsistencyLevels()[2],
			expectedIterateEqualTimestampStrategy: &encoding.ValidIterateEqualTimestampStrategies()[2],
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder, err := NewFetchOptionsBuilder(FetchOptionsBuilderOptions{
				Limits: FetchOptionsBuilderLimitsOptions{
					SeriesLimit: test.defaultLimit,
				},
				RestrictByTag: test.defaultRestrictByTag,
				Timeout:       10 * time.Second,
			})
			require.NoError(t, err)

			url := "/foo"
			if test.query != "" {
				url += "?" + test.query
			}
			req := httptest.NewRequest("GET", url, nil)
			for k, v := range test.headers {
				req.Header.Add(k, v)
			}

			ctx, opts, err := builder.NewFetchOptions(context.Background(), req)
			if !test.expectedErr {
				require.NoError(t, err)
				require.Equal(t, test.expectedLimit, opts.SeriesLimit)
				if test.expectedRestrict == nil {
					require.Nil(t, opts.RestrictQueryOptions)
				} else {
					require.NotNil(t, opts.RestrictQueryOptions)
					require.Equal(t, *test.expectedRestrict, *opts.RestrictQueryOptions)
				}
				if test.expectedLookback == nil {
					require.Nil(t, opts.LookbackDuration)
				} else {
					require.NotNil(t, opts.LookbackDuration)
					require.Equal(t, test.expectedLookback.value, *opts.LookbackDuration)
				}
				if test.expectedReadConsistencyLevel == nil {
					require.Nil(t, opts.ReadConsistencyLevel)
				} else {
					require.NotNil(t, opts.ReadConsistencyLevel)
					require.Equal(t, *test.expectedReadConsistencyLevel, *opts.ReadConsistencyLevel)
				}
				if test.expectedIterateEqualTimestampStrategy == nil {
					require.Nil(t, opts.IterateEqualTimestampStrategy)
				} else {
					require.NotNil(t, opts.IterateEqualTimestampStrategy)
					require.Equal(t, *test.expectedIterateEqualTimestampStrategy, *opts.IterateEqualTimestampStrategy)
				}
				require.Equal(t, 10*time.Second, opts.Timeout)
				// Check context has deadline and headers from
				// the request.
				_, ok := ctx.Deadline()
				require.True(t, ok)
				headers := ctx.Value(RequestHeaderKey)
				require.NotNil(t, headers)
				_, ok = headers.(http.Header)
				require.True(t, ok)
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

func TestFetchOptionsWithHeader(t *testing.T) {
	type expectedLookback struct {
		value time.Duration
	}

	headers := map[string]string{
		headers.MetricsTypeHeader:          storagemetadata.AggregatedMetricsType.String(),
		headers.MetricsStoragePolicyHeader: "1m:14d",
		headers.RestrictByTagsJSONHeader: `{
			"match":[
				{"name":"a", "value":"b", "type":"EQUAL"},
				{"name":"c", "value":"d", "type":"NOTEQUAL"},
				{"name":"e", "value":"f", "type":"REGEXP"},
				{"name":"g", "value":"h", "type":"NOTREGEXP"},
				{"name":"i", "value":"j", "type":"EXISTS"},
				{"name":"k", "value":"l", "type":"NOTEXISTS"}
			],
			"strip":["foo"]
		}`,
		headers.ReadConsistencyLevelHeader:          "all",
		headers.IterateEqualTimestampStrategyHeader: "iterate_lowest_value",
	}

	builder, err := NewFetchOptionsBuilder(FetchOptionsBuilderOptions{
		Limits: FetchOptionsBuilderLimitsOptions{
			SeriesLimit: 5,
		},
		Timeout: 10 * time.Second,
	})
	require.NoError(t, err)

	req := httptest.NewRequest("GET", "/", nil)
	for k, v := range headers {
		req.Header.Add(k, v)
	}

	_, opts, err := builder.NewFetchOptions(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, opts.RestrictQueryOptions)
	ex := &storage.RestrictQueryOptions{
		RestrictByType: &storage.RestrictByType{
			MetricsType:   storagemetadata.AggregatedMetricsType,
			StoragePolicy: policy.MustParseStoragePolicy("1m:14d"),
		},
		RestrictByTag: &storage.RestrictByTag{
			Restrict: models.Matchers{
				mustMatcher("a", "b", models.MatchEqual),
				mustMatcher("c", "d", models.MatchNotEqual),
				mustMatcher("e", "f", models.MatchRegexp),
				mustMatcher("g", "h", models.MatchNotRegexp),
				mustMatcher("i", "j", models.MatchField),
				mustMatcher("k", "l", models.MatchNotField),
			},
			Strip: toStrip("foo"),
		},
	}

	require.Equal(t, ex, opts.RestrictQueryOptions)
	require.Equal(t, topology.ReadConsistencyLevelAll, *opts.ReadConsistencyLevel)
	require.Equal(t, encoding.IterateLowestValue, *opts.IterateEqualTimestampStrategy)
}

func stripSpace(str string) string {
	return regexp.MustCompile(`\s+`).ReplaceAllString(str, "")
}

func TestParseRequestTimeout(t *testing.T) {
	req := httptest.NewRequest("GET", "/read?timeout=2m", nil)
	dur, err := ParseRequestTimeout(req, time.Second)
	require.NoError(t, err)
	assert.Equal(t, 2*time.Minute, dur)
}

func TestParseRelatedQueryOptions(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		expectErr      bool
		expectedOk     bool
		headers        []string
		expectedResult *storage.RelatedQueryOptions
	}{
		"simple": {
			headers:    []string{"1635160222:1635166222"},
			expectErr:  false,
			expectedOk: true,
			expectedResult: &storage.RelatedQueryOptions{
				Timespans: []storage.QueryTimespan{
					{Start: 1635160222000000000, End: 1635166222000000000},
				},
			},
		},
		"multiple queries (second header ignored)": {
			headers:    []string{"1635160222:1635166222", "1635161222:1635165222"},
			expectErr:  false,
			expectedOk: true,
			expectedResult: &storage.RelatedQueryOptions{
				Timespans: []storage.QueryTimespan{
					{Start: 1635160222000000000, End: 1635166222000000000},
				},
			},
		},
		"multiple queries same header.": {
			headers:    []string{"1635160222:1635166222;1635161222:1635165222"},
			expectErr:  false,
			expectedOk: true,
			expectedResult: &storage.RelatedQueryOptions{
				Timespans: []storage.QueryTimespan{
					{Start: 1635160222000000000, End: 1635166222000000000},
					{Start: 1635161222000000000, End: 1635165222000000000},
				},
			},
		},
		"no related_queries": {
			headers:        []string{},
			expectErr:      false,
			expectedOk:     false,
			expectedResult: nil,
		},
		"incomplete pair": {
			headers:        []string{"1635160222"},
			expectErr:      true,
			expectedOk:     false,
			expectedResult: nil,
		},
		"invalid pair (start time)": {
			headers:        []string{"2m:6m"},
			expectErr:      true,
			expectedOk:     false,
			expectedResult: nil,
		},
		"invalid pair (end time)": {
			headers:        []string{"1635160222:6m"},
			expectErr:      true,
			expectedOk:     false,
			expectedResult: nil,
		},
		"invalid pair (end time after start time)": {
			headers:        []string{"1635166222:1635160222"},
			expectErr:      true,
			expectedOk:     false,
			expectedResult: nil,
		},
	}

	for name, tc := range tests {
		name, tc := name, tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("GET", "/read", nil)
			for _, header := range tc.headers {
				req.Header.Add(headers.RelatedQueriesHeader, header)
			}
			options, ok, err := ParseRelatedQueryOptions(req)
			assert.Equal(t, tc.expectedOk, ok,
				"Expected result of ok to be %v got %v", tc.expectedOk, ok)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tc.expectedResult == nil {
				assert.Nil(t, options)
			} else {
				assert.NotNil(t, options)
				assert.Equal(t, tc.expectedResult, options)
			}
		})
	}
}

func TestTimeoutParseWithHeader(t *testing.T) {
	req := httptest.NewRequest("POST", "/dummy", nil)
	req.Header.Add("timeout", "1ms")

	timeout, err := ParseRequestTimeout(req, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, timeout, time.Millisecond)

	req.Header.Add(headers.TimeoutHeader, "1s")
	timeout, err = ParseRequestTimeout(req, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, timeout, time.Second)

	req.Header.Del("timeout")
	req.Header.Del(headers.TimeoutHeader)
	timeout, err = ParseRequestTimeout(req, 2*time.Minute)
	assert.NoError(t, err)
	assert.Equal(t, timeout, 2*time.Minute)

	req.Header.Add("timeout", "invalid")
	_, err = ParseRequestTimeout(req, 15*time.Second)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
}

func TestTimeoutParseWithPostRequestParam(t *testing.T) {
	params := url.Values{}
	params.Add("timeout", "1ms")

	buff := bytes.NewBuffer(nil)
	form := multipart.NewWriter(buff)
	form.WriteField("timeout", "1ms")
	require.NoError(t, form.Close())

	req := httptest.NewRequest("POST", "/dummy", buff)
	req.Header.Set(xhttp.HeaderContentType, form.FormDataContentType())

	timeout, err := ParseRequestTimeout(req, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, timeout, time.Millisecond)
}

func TestTimeoutParseWithGetRequestParam(t *testing.T) {
	params := url.Values{}
	params.Add("timeout", "1ms")

	req := httptest.NewRequest("GET", "/dummy?"+params.Encode(), nil)

	timeout, err := ParseRequestTimeout(req, time.Second)
	assert.NoError(t, err)
	assert.Equal(t, timeout, time.Millisecond)
}

func TestInstanceMultiple(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	m, err := ParseInstanceMultiple(req, 2.0)
	require.NoError(t, err)
	require.Equal(t, float32(2.0), m)

	req.Header.Set(headers.LimitInstanceMultipleHeader, "3.0")
	m, err = ParseInstanceMultiple(req, 2.0)
	require.NoError(t, err)
	require.Equal(t, float32(3.0), m)

	req.Header.Set(headers.LimitInstanceMultipleHeader, "blah")
	_, err = ParseInstanceMultiple(req, 2.0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not parse instance multiple")
}
