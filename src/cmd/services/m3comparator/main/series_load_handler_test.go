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

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"
	"github.com/m3db/m3/src/dbnode/encoding"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NB: this is regression test data that used to cause issues.
const seriesStr = `
[
    {
        "start": "2020-03-30T11:39:45Z",
        "end": "2020-03-30T11:58:00Z",
        "tags": [
            ["__name__", "series_name"],
            ["abc", "def"],
            ["tag_a", "foo"]
        ],
        "datapoints": [
            { "val": "7076", "ts": "2020-03-30T11:39:51.288Z" },
            { "val": "7076", "ts": "2020-03-30T11:39:57.478Z" },
            { "val": "7076", "ts": "2020-03-30T11:40:07.478Z" },
            { "val": "7076", "ts": "2020-03-30T11:40:18.886Z" },
            { "val": "7076", "ts": "2020-03-30T11:40:31.135Z" },
            { "val": "7077", "ts": "2020-03-30T11:40:40.047Z" },
            { "val": "7077", "ts": "2020-03-30T11:40:54.893Z" },
            { "val": "7077", "ts": "2020-03-30T11:40:57.478Z" },
            { "val": "7077", "ts": "2020-03-30T11:41:07.478Z" },
            { "val": "7077", "ts": "2020-03-30T11:41:17.478Z" },
            { "val": "7077", "ts": "2020-03-30T11:41:29.323Z" },
            { "val": "7078", "ts": "2020-03-30T11:41:43.873Z" },
            { "val": "7078", "ts": "2020-03-30T11:41:54.375Z" },
            { "val": "7078", "ts": "2020-03-30T11:41:58.053Z" },
            { "val": "7078", "ts": "2020-03-30T11:42:09.250Z" },
            { "val": "7078", "ts": "2020-03-30T11:42:20.793Z" },
            { "val": "7078", "ts": "2020-03-30T11:42:34.915Z" },
            { "val": "7079", "ts": "2020-03-30T11:42:43.467Z" },
            { "val": "7079", "ts": "2020-03-30T11:42:50.364Z" },
            { "val": "7079", "ts": "2020-03-30T11:43:02.376Z" },
            { "val": "7079", "ts": "2020-03-30T11:43:07.478Z" },
            { "val": "7079", "ts": "2020-03-30T11:43:20.807Z" },
            { "val": "7079", "ts": "2020-03-30T11:43:29.432Z" },
            { "val": "7079", "ts": "2020-03-30T11:43:37.478Z" },
            { "val": "7080", "ts": "2020-03-30T11:43:47.478Z" },
            { "val": "7080", "ts": "2020-03-30T11:44:01.078Z" },
            { "val": "7080", "ts": "2020-03-30T11:44:07.478Z" },
            { "val": "7080", "ts": "2020-03-30T11:44:17.478Z" },
            { "val": "7080", "ts": "2020-03-30T11:44:28.444Z" },
            { "val": "7080", "ts": "2020-03-30T11:44:37.478Z" },
            { "val": "7081", "ts": "2020-03-30T11:44:49.607Z" },
            { "val": "7081", "ts": "2020-03-30T11:45:02.758Z" },
            { "val": "7081", "ts": "2020-03-30T11:45:16.740Z" },
            { "val": "7081", "ts": "2020-03-30T11:45:27.813Z" },
            { "val": "7081", "ts": "2020-03-30T11:45:38.141Z" },
            { "val": "7082", "ts": "2020-03-30T11:45:53.850Z" },
            { "val": "7082", "ts": "2020-03-30T11:46:00.954Z" },
            { "val": "7082", "ts": "2020-03-30T11:46:08.814Z" },
            { "val": "7082", "ts": "2020-03-30T11:46:17.478Z" },
            { "val": "7082", "ts": "2020-03-30T11:46:27.478Z" },
            { "val": "7082", "ts": "2020-03-30T11:46:38.152Z" },
            { "val": "7083", "ts": "2020-03-30T11:46:48.192Z" },
            { "val": "7084", "ts": "2020-03-30T11:47:40.871Z" },
            { "val": "7084", "ts": "2020-03-30T11:47:49.966Z" },
            { "val": "7084", "ts": "2020-03-30T11:47:57.478Z" },
            { "val": "7084", "ts": "2020-03-30T11:48:07.478Z" },
            { "val": "7084", "ts": "2020-03-30T11:48:23.279Z" },
            { "val": "7084", "ts": "2020-03-30T11:48:29.018Z" },
            { "val": "7084", "ts": "2020-03-30T11:48:37.478Z" },
            { "val": "7085", "ts": "2020-03-30T11:48:47.478Z" },
            { "val": "7085", "ts": "2020-03-30T11:48:57.478Z" },
            { "val": "7085", "ts": "2020-03-30T11:49:07.478Z" },
            { "val": "7085", "ts": "2020-03-30T11:49:17.478Z" },
            { "val": "7085", "ts": "2020-03-30T11:49:27.478Z" },
            { "val": "7085", "ts": "2020-03-30T11:49:37.478Z" },
            { "val": "7086", "ts": "2020-03-30T11:49:47.478Z" },
            { "val": "7086", "ts": "2020-03-30T11:49:57.850Z" },
            { "val": "7086", "ts": "2020-03-30T11:50:07.478Z" },
            { "val": "7086", "ts": "2020-03-30T11:50:20.887Z" },
            { "val": "7087", "ts": "2020-03-30T11:51:12.729Z" },
            { "val": "7087", "ts": "2020-03-30T11:51:19.914Z" },
            { "val": "7087", "ts": "2020-03-30T11:51:27.478Z" },
            { "val": "7087", "ts": "2020-03-30T11:51:37.478Z" },
            { "val": "7088", "ts": "2020-03-30T11:51:47.478Z" },
            { "val": "7088", "ts": "2020-03-30T11:51:57.478Z" },
            { "val": "7088", "ts": "2020-03-30T11:52:07.478Z" },
            { "val": "7088", "ts": "2020-03-30T11:52:17.478Z" },
            { "val": "7088", "ts": "2020-03-30T11:52:29.869Z" },
            { "val": "7088", "ts": "2020-03-30T11:52:38.976Z" },
            { "val": "7089", "ts": "2020-03-30T11:52:47.478Z" },
            { "val": "7089", "ts": "2020-03-30T11:52:57.478Z" },
            { "val": "7089", "ts": "2020-03-30T11:53:07.478Z" },
            { "val": "7089", "ts": "2020-03-30T11:53:17.906Z" },
            { "val": "7089", "ts": "2020-03-30T11:53:27.478Z" },
            { "val": "7090", "ts": "2020-03-30T11:54:17.478Z" },
            { "val": "7090", "ts": "2020-03-30T11:54:27.478Z" },
            { "val": "7090", "ts": "2020-03-30T11:54:37.478Z" },
            { "val": "7091", "ts": "2020-03-30T11:54:51.214Z" },
            { "val": "7091", "ts": "2020-03-30T11:54:58.985Z" },
            { "val": "7091", "ts": "2020-03-30T11:55:08.548Z" },
            { "val": "7091", "ts": "2020-03-30T11:55:19.762Z" },
            { "val": "7091", "ts": "2020-03-30T11:55:27.478Z" },
            { "val": "7091", "ts": "2020-03-30T11:55:39.009Z" },
            { "val": "7092", "ts": "2020-03-30T11:55:47.478Z" },
            { "val": "7092", "ts": "2020-03-30T11:56:01.507Z" },
            { "val": "7092", "ts": "2020-03-30T11:56:12.995Z" },
            { "val": "7092", "ts": "2020-03-30T11:56:24.892Z" },
            { "val": "7092", "ts": "2020-03-30T11:56:38.410Z" },
            { "val": "7093", "ts": "2020-03-30T11:56:47.478Z" },
            { "val": "7093", "ts": "2020-03-30T11:56:58.786Z" },
            { "val": "7093", "ts": "2020-03-30T11:57:07.478Z" },
            { "val": "7093", "ts": "2020-03-30T11:57:17.478Z" },
            { "val": "7093", "ts": "2020-03-30T11:57:31.283Z" },
            { "val": "7093", "ts": "2020-03-30T11:57:39.113Z" },
            { "val": "7094", "ts": "2020-03-30T11:57:48.864Z" },
            { "val": "7094", "ts": "2020-03-30T11:57:57.478Z" }
        ]
    }
]`

func TestIngestSeries(t *testing.T) {
	opts := parser.Options{
		EncoderPool:       encoderPool,
		IteratorPools:     iterPools,
		TagOptions:        tagOptions,
		InstrumentOptions: iOpts,
	}

	req, err := http.NewRequest(http.MethodPost, "", strings.NewReader(seriesStr))
	require.NoError(t, err)

	recorder := httptest.NewRecorder()

	handler := newHTTPSeriesLoadHandler(opts)
	handler.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusOK, recorder.Code)

	iters, err := handler.getSeriesIterators("series_name")
	require.NoError(t, err)
	require.NotNil(t, iters)

	expectedList := make([]parser.Series, 0, 10)
	err = json.Unmarshal([]byte(seriesStr), &expectedList)
	require.NoError(t, err)
	require.Equal(t, 1, len(expectedList))
	expected := expectedList[0]

	require.Equal(t, 1, len(iters.Iters()))
	it := iters.Iters()[0]
	j := 0
	for it.Next() {
		c, _, _ := it.Current()
		ts := c.TimestampNanos.ToTime().UTC()
		ex := expected.Datapoints[j]
		assert.Equal(t, ex.Timestamp, ts)
		assert.Equal(t, float64(ex.Value), c.Value)
		j++
	}

	assert.NoError(t, it.Err())
	assert.Equal(t, expected.Tags, readTags(it))
	assert.Equal(t, j, len(expected.Datapoints))
}

func TestClearData(t *testing.T) {
	opts := parser.Options{
		EncoderPool:       encoderPool,
		IteratorPools:     iterPools,
		TagOptions:        tagOptions,
		InstrumentOptions: iOpts,
	}

	req, err := http.NewRequest(http.MethodPost, "", strings.NewReader(seriesStr))
	require.NoError(t, err)

	recorder := httptest.NewRecorder()

	handler := newHTTPSeriesLoadHandler(opts)
	handler.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusOK, recorder.Code)

	iters, err := handler.getSeriesIterators("series_name")
	require.NoError(t, err)
	require.Equal(t, 1, len(iters.Iters()))

	// Call clear data
	req, err = http.NewRequest(http.MethodDelete, "", nil)
	require.NoError(t, err)

	handler.ServeHTTP(recorder, req)
	assert.Equal(t, http.StatusOK, recorder.Code)

	iters, err = handler.getSeriesIterators("series_name")
	require.NoError(t, err)
	require.Nil(t, iters)
}

func readTags(it encoding.SeriesIterator) parser.Tags {
	tagIter := it.Tags()
	tags := make(parser.Tags, 0, tagIter.Len())
	for tagIter.Next() {
		tag := tagIter.Current()
		newTag := parser.NewTag(tag.Name.String(), tag.Value.String())
		tags = append(tags, newTag)
	}

	return tags
}
