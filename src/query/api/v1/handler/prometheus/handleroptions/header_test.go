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
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/headers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddDBResultResponseHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()
	meta := block.NewResultMetadata()
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, nil))
	assert.Equal(t, 0, len(recorder.Header()))

	recorder = httptest.NewRecorder()
	meta.Exhaustive = false
	ex := headers.LimitHeaderSeriesLimitApplied
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, ex, recorder.Header().Get(headers.LimitHeader))

	recorder = httptest.NewRecorder()
	meta.AddWarning("foo", "bar")
	ex = fmt.Sprintf("%s,%s_%s", headers.LimitHeaderSeriesLimitApplied, "foo", "bar")
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, ex, recorder.Header().Get(headers.LimitHeader))

	recorder = httptest.NewRecorder()
	meta.Exhaustive = true
	ex = "foo_bar"
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, ex, recorder.Header().Get(headers.LimitHeader))

	recorder = httptest.NewRecorder()
	meta = block.NewResultMetadata()
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, &storage.FetchOptions{
		Timeout: 5 * time.Second,
	}))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, "5s", recorder.Header().Get(headers.TimeoutHeader))

	recorder = httptest.NewRecorder()
	meta = block.NewResultMetadata()
	meta.WaitedIndex = 3
	meta.WaitedSeriesRead = 42
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, "{\"waitedIndex\":3,\"waitedSeriesRead\":42}",
		recorder.Header().Get(headers.WaitedHeader))
}

func TestAddDBResultResponseHeadersFetched(t *testing.T) {
	recorder := httptest.NewRecorder()
	meta := block.NewResultMetadata()
	meta.FetchedSeriesCount = 42
	meta.FetchedMetadataCount = 142
	meta.FetchedResponses = 99
	meta.FetchedBytesEstimate = 1072
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, nil))
	assert.Equal(t, 4, len(recorder.Header()))
	assert.Equal(t, "99", recorder.Header().Get(headers.FetchedResponsesHeader))
	assert.Equal(t, "1072", recorder.Header().Get(headers.FetchedBytesEstimateHeader))
	assert.Equal(t, "42", recorder.Header().Get(headers.FetchedSeriesCount))
	assert.Equal(t, "142", recorder.Header().Get(headers.FetchedMetadataCount))
}

func TestAddDBResultResponseHeadersNamespaces(t *testing.T) {
	recorder := httptest.NewRecorder()
	meta := block.NewResultMetadata()
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, nil))
	assert.Equal(t, 0, len(recorder.Header()))

	recorder = httptest.NewRecorder()
	meta = block.NewResultMetadata()
	meta.AddNamespace("default")
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, "default", recorder.Header().Get(headers.NamespacesHeader))

	recorder = httptest.NewRecorder()
	meta = block.NewResultMetadata()
	meta.AddNamespace("default")
	meta.AddNamespace("myfavoritens")
	meta.AddNamespace("myfavoritens")
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, "default,myfavoritens", recorder.Header().Get(headers.NamespacesHeader))
}

func TestAddDBResultResponseHeadersMetadataByName(t *testing.T) {
	recorder := httptest.NewRecorder()
	meta := block.NewResultMetadata()
	*(meta.ByName([]byte("mymetric"))) = block.ResultMetricMetadata{
		NoSamples:    1,
		WithSamples:  2,
		Aggregated:   3,
		Unaggregated: 4,
	}
	fetchOpts := storage.NewFetchOptions()
	fetchOpts.MaxMetricMetadataStats = 10
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, fetchOpts))
	assert.Equal(t, 6, len(recorder.Header()))
	assert.Equal(t, "0s", recorder.Header().Get(headers.TimeoutHeader))
	assert.Equal(t, "1", recorder.Header().Get(headers.FetchedSeriesNoSamplesCount))
	assert.Equal(t, "2", recorder.Header().Get(headers.FetchedSeriesWithSamplesCount))
	assert.Equal(t, "3", recorder.Header().Get(headers.FetchedAggregatedSeriesCount))
	assert.Equal(t, "4", recorder.Header().Get(headers.FetchedUnaggregatedSeriesCount))
	assert.Equal(t,
		"{\"mymetric\":{\"NoSamples\":1,\"WithSamples\":2,\"Aggregated\":3,\"Unaggregated\":4}}",
		recorder.Header().Get(headers.MetricStats))

	recorder = httptest.NewRecorder()
	meta = block.NewResultMetadata()
	*(meta.ByName([]byte("metric_a"))) = block.ResultMetricMetadata{
		NoSamples:    1,
		WithSamples:  2,
		Aggregated:   3,
		Unaggregated: 4,
	}
	*(meta.ByName([]byte("metric_b"))) = block.ResultMetricMetadata{
		NoSamples:    10,
		WithSamples:  20,
		Aggregated:   30,
		Unaggregated: 40,
	}
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, fetchOpts))
	assert.Equal(t, 6, len(recorder.Header()))
	assert.Equal(t, "0s", recorder.Header().Get(headers.TimeoutHeader))
	assert.Equal(t, "11", recorder.Header().Get(headers.FetchedSeriesNoSamplesCount))
	assert.Equal(t, "22", recorder.Header().Get(headers.FetchedSeriesWithSamplesCount))
	assert.Equal(t, "33", recorder.Header().Get(headers.FetchedAggregatedSeriesCount))
	assert.Equal(t, "44", recorder.Header().Get(headers.FetchedUnaggregatedSeriesCount))
	assert.Equal(t,
		"{\"metric_a\":{\"NoSamples\":1,\"WithSamples\":2,\"Aggregated\":3,\"Unaggregated\":4},"+
			"\"metric_b\":{\"NoSamples\":10,\"WithSamples\":20,\"Aggregated\":30,\"Unaggregated\":40}}",
		recorder.Header().Get(headers.MetricStats))

	recorder = httptest.NewRecorder()
	meta = block.NewResultMetadata()
	numStats := fetchOpts.MaxMetricMetadataStats + 2
	totalCount := 0
	for i := 0; i < numStats; i++ {
		count := i + 1
		meta.ByName([]byte(fmt.Sprintf("metric_%v", i))).Unaggregated = count
		totalCount += count
	}
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, fetchOpts))
	assert.Equal(t, 3, len(recorder.Header()))
	assert.Equal(t, "0s", recorder.Header().Get(headers.TimeoutHeader))
	assert.Equal(t, fmt.Sprint(totalCount), recorder.Header().Get(headers.FetchedUnaggregatedSeriesCount))

	parsed := make(map[string]*block.ResultMetricMetadata)
	metricStatsHeader := recorder.Header().Get(headers.MetricStats)
	assert.NotEmpty(t, metricStatsHeader)
	err := json.Unmarshal([]byte(metricStatsHeader), &parsed)
	assert.NoError(t, err)
	assert.Equal(t, fetchOpts.MaxMetricMetadataStats, len(parsed))
	observedCount := 0
	for _, stat := range parsed {
		observedCount += stat.Unaggregated
	}
	// The total count includes `max+2` counters. The bottom two values of those 12 are 1 and 2.
	// So we want to see the total minus (1 + 2), which means the count contains only the
	// top `max` counts.
	wantCount := totalCount - (1 + 2)
	assert.Equal(t, observedCount, wantCount)
}

func TestAddDBResultResponseHeadersMetadataByNameMaxConfig(t *testing.T) {
	recorder := httptest.NewRecorder()
	meta := block.NewResultMetadata()
	*(meta.ByName([]byte("mymetric"))) = block.ResultMetricMetadata{
		NoSamples:    1,
		WithSamples:  2,
		Aggregated:   3,
		Unaggregated: 4,
	}

	// Disable metric metadata stats using a header
	req := httptest.NewRequest("GET", "/api/v1/query", nil)
	req.Header.Add(headers.LimitMaxMetricMetadataStatsHeader, "0")
	fetchOptsBuilder, err := NewFetchOptionsBuilder(
		FetchOptionsBuilderOptions{
			Timeout: 5 * time.Second,
		},
	)
	require.NoError(t, err)
	_, fetchOpts, err := fetchOptsBuilder.NewFetchOptions(context.Background(), req)
	require.NoError(t, err)
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, fetchOpts))
	assert.Equal(t, 5, len(recorder.Header()))
	assert.Equal(t, "5s", recorder.Header().Get(headers.TimeoutHeader))
	assert.Equal(t, "1", recorder.Header().Get(headers.FetchedSeriesNoSamplesCount))
	assert.Equal(t, "2", recorder.Header().Get(headers.FetchedSeriesWithSamplesCount))
	assert.Equal(t, "3", recorder.Header().Get(headers.FetchedAggregatedSeriesCount))
	assert.Equal(t, "4", recorder.Header().Get(headers.FetchedUnaggregatedSeriesCount))
	assert.Empty(t, recorder.Header().Get(headers.MetricStats))

	// Disable metric metadata stats using config
	recorder = httptest.NewRecorder()
	fetchOpts.MaxMetricMetadataStats = 0
	require.NoError(t, AddDBResultResponseHeaders(recorder, meta, fetchOpts))
	assert.Equal(t, 5, len(recorder.Header()))
	assert.Equal(t, "5s", recorder.Header().Get(headers.TimeoutHeader))
	assert.Equal(t, "1", recorder.Header().Get(headers.FetchedSeriesNoSamplesCount))
	assert.Equal(t, "2", recorder.Header().Get(headers.FetchedSeriesWithSamplesCount))
	assert.Equal(t, "3", recorder.Header().Get(headers.FetchedAggregatedSeriesCount))
	assert.Equal(t, "4", recorder.Header().Get(headers.FetchedUnaggregatedSeriesCount))
	assert.Empty(t, recorder.Header().Get(headers.MetricStats))
}

func TestAddReturnedLimitResponseHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()
	require.NoError(t, AddReturnedLimitResponseHeaders(recorder, &ReturnedDataLimited{
		Series:      3,
		Datapoints:  6,
		TotalSeries: 3,
		Limited:     false,
	}, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, "{\"Series\":3,\"Datapoints\":6,\"TotalSeries\":3,\"Limited\":false}",
		recorder.Header().Get(headers.ReturnedDataLimitedHeader))

	recorder = httptest.NewRecorder()
	require.NoError(t, AddReturnedLimitResponseHeaders(recorder, nil, &ReturnedMetadataLimited{
		Results:      3,
		TotalResults: 3,
		Limited:      false,
	}))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, "{\"Results\":3,\"TotalResults\":3,\"Limited\":false}",
		recorder.Header().Get(headers.ReturnedMetadataLimitedHeader))
}
