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

func TestAddResponseHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()
	meta := block.NewResultMetadata()
	require.NoError(t, AddResponseHeaders(recorder, meta, nil, nil, nil))
	assert.Equal(t, 0, len(recorder.Header()))

	recorder = httptest.NewRecorder()
	meta.Exhaustive = false
	ex := headers.LimitHeaderSeriesLimitApplied
	require.NoError(t, AddResponseHeaders(recorder, meta, nil, nil, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, ex, recorder.Header().Get(headers.LimitHeader))

	recorder = httptest.NewRecorder()
	meta.AddWarning("foo", "bar")
	ex = fmt.Sprintf("%s,%s_%s", headers.LimitHeaderSeriesLimitApplied, "foo", "bar")
	require.NoError(t, AddResponseHeaders(recorder, meta, nil, nil, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, ex, recorder.Header().Get(headers.LimitHeader))

	recorder = httptest.NewRecorder()
	meta.Exhaustive = true
	ex = "foo_bar"
	require.NoError(t, AddResponseHeaders(recorder, meta, nil, nil, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, ex, recorder.Header().Get(headers.LimitHeader))

	recorder = httptest.NewRecorder()
	meta = block.NewResultMetadata()
	require.NoError(t, AddResponseHeaders(recorder, meta, &storage.FetchOptions{
		Timeout: 5 * time.Second,
	}, nil, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, "5s", recorder.Header().Get(headers.TimeoutHeader))

	recorder = httptest.NewRecorder()
	meta = block.NewResultMetadata()
	require.NoError(t, AddResponseHeaders(recorder, meta, nil, &ReturnedDataLimited{
		Series:      3,
		Datapoints:  6,
		TotalSeries: 3,
		Limited:     false,
	}, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, "{\"Series\":3,\"Datapoints\":6,\"TotalSeries\":3,\"Limited\":false}",
		recorder.Header().Get(headers.ReturnedDataLimitedHeader))

	recorder = httptest.NewRecorder()
	meta = block.NewResultMetadata()
	require.NoError(t, AddResponseHeaders(recorder, meta, nil, nil, &ReturnedMetadataLimited{
		Results:      3,
		TotalResults: 3,
		Limited:      false,
	}))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, "{\"Results\":3,\"TotalResults\":3,\"Limited\":false}",
		recorder.Header().Get(headers.ReturnedMetadataLimitedHeader))

	recorder = httptest.NewRecorder()
	meta = block.NewResultMetadata()
	meta.ThrottledIndex = 3
	meta.ThrottledSeriesRead = 42
	require.NoError(t, AddResponseHeaders(recorder, meta, nil, nil, nil))
	assert.Equal(t, 1, len(recorder.Header()))
	assert.Equal(t, "{\"throttledIndex\":3,\"throttledSeriesRead\":42}",
		recorder.Header().Get(headers.ThrottledHeader))
}
