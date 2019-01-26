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

package graphite

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/query/ts"

	"github.com/stretchr/testify/require"
)

func TestNativeParseNoQuery(t *testing.T) {
	mockStorage := mock.NewMockStorage()
	handler := NewNativeRenderHandler(mockStorage)

	req, _ := http.NewRequest(GraphiteReadHTTPMethod, GraphiteReadURL, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 400, res.StatusCode)
}

func TestNativeParseQueryNoResults(t *testing.T) {
	mockStorage := mock.NewMockStorage()
	mockStorage.SetFetchResult(&storage.FetchResult{}, nil)
	handler := NewNativeRenderHandler(mockStorage)

	req, _ := http.NewRequest(GraphiteReadHTTPMethod, GraphiteReadURL, nil)
	req.URL.RawQuery = "target=foo.bar&from=-2h&until=now"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, []byte("[]"), buf)
}

func TestNativeParseQueryResults(t *testing.T) {
	mockStorage := mock.NewMockStorage()
	start := time.Now()
	vals := ts.NewFixedStepValues(10*time.Second, 3, 3, start)
	seriesList := ts.SeriesList{
		ts.NewSeries("a", vals, models.NewTags(0, nil)),
	}

	mockStorage.SetFetchResult(&storage.FetchResult{SeriesList: seriesList}, nil)
	handler := NewNativeRenderHandler(mockStorage)

	req, _ := http.NewRequest(GraphiteReadHTTPMethod, GraphiteReadURL, nil)
	req.URL.RawQuery = "target=foo.bar&from=-30min"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	startSeconds := start.Add(time.Minute*-30).UnixNano() / 1000000000
	expected := fmt.Sprintf(
		`[{"target":"a","datapoints":[[3.000000,%d],`+
			`[3.000000,%d],[3.000000,%d]],"step_size_ms":1000}]`,
		startSeconds, startSeconds+1, startSeconds+2,
	)

	require.Equal(t, []byte(expected), buf)
}

func TestNativeParseQueryResultsMaxDatapoints(t *testing.T) {
	mockStorage := mock.NewMockStorage()
	start, err := time.Parse(time.RFC3339, "2014-03-07T00:30:00Z")
	require.NoError(t, err)

	vals := ts.NewFixedStepValues(10*time.Second, 4, 4, start)
	seriesList := ts.SeriesList{
		ts.NewSeries("a", vals, models.NewTags(0, nil)),
	}

	mockStorage.SetFetchResult(&storage.FetchResult{SeriesList: seriesList}, nil)
	handler := NewNativeRenderHandler(mockStorage)

	req, _ := http.NewRequest(GraphiteReadHTTPMethod, GraphiteReadURL, nil)
	req.URL.RawQuery = "target=foo.bar&from=03/07/14&until=03/07/15&maxDataPoints=1"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	startSeconds := start.Add(time.Minute*-30).UnixNano() / 1000000000
	expected := fmt.Sprintf(
		`[{"target":"a","datapoints":[[4.000000,%d]],"step_size_ms":4000}]`,
		startSeconds,
	)

	require.Equal(t, []byte(expected), buf)
}

func TestNativeParseQueryResultsMultiTarget(t *testing.T) {
	mockStorage := mock.NewMockStorage()
	start := time.Now()
	vals := ts.NewFixedStepValues(10*time.Second, 3, 3, start)
	seriesList := ts.SeriesList{
		ts.NewSeries("a", vals, models.NewTags(0, nil)),
	}

	mockStorage.SetFetchResult(&storage.FetchResult{SeriesList: seriesList}, nil)
	handler := NewNativeRenderHandler(mockStorage)

	req, _ := http.NewRequest(GraphiteReadHTTPMethod, GraphiteReadURL, nil)
	req.URL.RawQuery = "target=foo.bar&from=-12min&target=baz.qux"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)

	startSeconds := start.Add(time.Minute*-12).UnixNano() / 1000000000
	expected := fmt.Sprintf(
		`[{"target":"a","datapoints":[[3.000000,%d],`+
			`[3.000000,%d],[3.000000,%d]],"step_size_ms":1000},`+
			`{"target":"a","datapoints":[[3.000000,%d],`+
			`[3.000000,%d],[3.000000,%d]],"step_size_ms":1000}]`,
		startSeconds, startSeconds+1, startSeconds+2,
		startSeconds, startSeconds+1, startSeconds+2,
	)

	require.Equal(t, expected, string(buf))
}
