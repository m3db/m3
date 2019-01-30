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

	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/query/ts"

	"github.com/stretchr/testify/require"
)

func TestParseNoQuery(t *testing.T) {
	mockStorage := mock.NewMockStorage()
	handler := NewRenderHandler(mockStorage)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, newGraphiteReadHTTPRequest(t))

	res := recorder.Result()
	require.Equal(t, 400, res.StatusCode)
}

func TestParseQueryNoResults(t *testing.T) {
	mockStorage := mock.NewMockStorage()
	mockStorage.SetFetchResult(&storage.FetchResult{}, nil)
	handler := NewRenderHandler(mockStorage)

	req := newGraphiteReadHTTPRequest(t)
	req.URL.RawQuery = "target=foo.bar&from=-2h&until=now"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, []byte("[]"), buf)
}

func TestParseQueryResults(t *testing.T) {
	mockStorage := mock.NewMockStorage()
	start := time.Now().Add(-30 * time.Minute)
	resolution := 10 * time.Second
	vals := ts.NewFixedStepValues(resolution, 3, 3, start)
	tags := models.NewTags(0, nil)
	tags = tags.AddTag(models.Tag{Name: graphite.TagName(0), Value: []byte("foo")})
	tags = tags.AddTag(models.Tag{Name: graphite.TagName(1), Value: []byte("bar")})
	seriesList := ts.SeriesList{
		ts.NewSeries([]byte("series_name"), vals, tags),
	}
	for _, series := range seriesList {
		series.SetResolution(resolution)
	}

	mockStorage.SetFetchResult(&storage.FetchResult{SeriesList: seriesList}, nil)
	handler := NewRenderHandler(mockStorage)

	req := newGraphiteReadHTTPRequest(t)
	req.URL.RawQuery = fmt.Sprintf("target=foo.bar&from=%d&until=%d",
		start.Unix(), start.Unix()+30)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	expected := fmt.Sprintf(
		`[{"target":"series_name","datapoints":[[3.000000,%d],`+
			`[3.000000,%d],[3.000000,%d]],"step_size_ms":%d}]`,
		start.Unix(), start.Unix()+10, start.Unix()+20, resolution/time.Millisecond)

	require.Equal(t, expected, string(buf))
}

func TestParseQueryResultsMaxDatapoints(t *testing.T) {
	mockStorage := mock.NewMockStorage()

	startStr := "03/07/14"
	endStr := "03/07/15"
	start, err := graphite.ParseTime(startStr, time.Now(), 0)
	require.NoError(t, err)
	end, err := graphite.ParseTime(endStr, time.Now(), 0)
	require.NoError(t, err)

	resolution := 10 * time.Second
	vals := ts.NewFixedStepValues(resolution, 4, 4, start)
	seriesList := ts.SeriesList{
		ts.NewSeries([]byte("a"), vals, models.NewTags(0, nil)),
	}
	for _, series := range seriesList {
		series.SetResolution(resolution)
	}

	mockStorage.SetFetchResult(&storage.FetchResult{SeriesList: seriesList}, nil)
	handler := NewRenderHandler(mockStorage)

	req := newGraphiteReadHTTPRequest(t)
	req.URL.RawQuery = "target=foo.bar&from=" + startStr + "&until=" + endStr + "&maxDataPoints=1"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)

	expected := fmt.Sprintf(
		`[{"target":"a","datapoints":[[4.000000,%d]],"step_size_ms":%d}]`,
		start.Unix(), end.Sub(start)/time.Millisecond)

	require.Equal(t, expected, string(buf))
}

func TestParseQueryResultsMultiTarget(t *testing.T) {
	mockStorage := mock.NewMockStorage()
	minsAgo := 12
	start := time.Now().Add(-1 * time.Duration(minsAgo) * time.Minute)

	resolution := 10 * time.Second
	vals := ts.NewFixedStepValues(resolution, 3, 3, start)
	seriesList := ts.SeriesList{
		ts.NewSeries([]byte("a"), vals, models.NewTags(0, nil)),
	}
	for _, series := range seriesList {
		series.SetResolution(resolution)
	}

	mockStorage.SetFetchResult(&storage.FetchResult{SeriesList: seriesList}, nil)
	handler := NewRenderHandler(mockStorage)

	req := newGraphiteReadHTTPRequest(t)
	req.URL.RawQuery = fmt.Sprintf("target=foo.bar&target=baz.qux&from=%d&until=%d",
		start.Unix(), start.Unix()+30)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	res := recorder.Result()
	require.Equal(t, 200, res.StatusCode)

	buf, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)

	expected := fmt.Sprintf(
		`[{"target":"a","datapoints":[[3.000000,%d],`+
			`[3.000000,%d],[3.000000,%d]],"step_size_ms":%d},`+
			`{"target":"a","datapoints":[[3.000000,%d],`+
			`[3.000000,%d],[3.000000,%d]],"step_size_ms":%d}]`,
		start.Unix(), start.Unix()+10, start.Unix()+20, resolution/time.Millisecond,
		start.Unix(), start.Unix()+10, start.Unix()+20, resolution/time.Millisecond)

	require.Equal(t, expected, string(buf))
}

func newGraphiteReadHTTPRequest(t *testing.T) *http.Request {
	req, err := http.NewRequest(ReadHTTPMethods[0], ReadURL, nil)
	require.NoError(t, err)
	return req
}
