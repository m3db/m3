// Copyright (c) 2018 Uber Technologies, Inc.
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
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/test/local"
	"github.com/m3db/m3db/src/coordinator/test/seriesiter"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromReadWithFetchOnly(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, mockSession := local.NewStorageAndSession(t, ctrl)
	testTags := seriesiter.GenerateTag()
	mockSession.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).Return(seriesiter.NewMockSeriesIters(ctrl, testTags, 1, 10), true, nil)

	promRead := &PromReadHandler{engine: executor.NewEngine(storage)}
	req, _ := http.NewRequest("GET", PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()

	r, parseErr := parseParams(req)
	require.Nil(t, parseErr)
	seriesList, err := promRead.read(context.TODO(), httptest.NewRecorder(), r)
	require.NoError(t, err)
	require.Len(t, seriesList, 1)
	s := seriesList[0]
	assert.Equal(t, 361, s.Values().Len(), "10 second resolution for 1 hour, including the start time")
	assert.Equal(t, float64(0), s.Values().ValueAt(0), "first value is zero since db returns values starting from start + 10ms")
	assert.Equal(t, float64(0), s.Values().ValueAt(1))
	for i := 2; i < 10; i++ {
		assert.Equal(t, float64(i-1), s.Values().ValueAt(i))
	}

	for i := 11; i < s.Values().Len(); i++ {
		require.True(t, math.IsNaN(s.Values().ValueAt(i)), "all remaining are nans")
	}
}

func TestPromReadWithFetchAndCount(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, mockSession := local.NewStorageAndSession(t, ctrl)
	testTags := seriesiter.GenerateTag()
	numSeries := 2
	mockSession.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).Return(seriesiter.NewMockSeriesIters(ctrl, testTags, numSeries, 10), true, nil)

	promRead := &PromReadHandler{engine: executor.NewEngine(storage)}
	req, _ := http.NewRequest("GET", PromReadURL, nil)
	params := defaultParams()
	params.Set(targetParam, `count(http_requests_total{job="prometheus",group="canary"})`)
	req.URL.RawQuery = params.Encode()

	r, parseErr := parseParams(req)
	require.Nil(t, parseErr)
	seriesList, err := promRead.read(context.TODO(), httptest.NewRecorder(), r)
	require.NoError(t, err)
	require.Len(t, seriesList, 1)
	s := seriesList[0]
	assert.Equal(t, 361, s.Values().Len(), "10 second resolution for 1 hour including start time")
	for i := 0; i < 10; i++ {
		assert.Equal(t, float64(numSeries), s.Values().ValueAt(i))
	}

	for i := 11; i < s.Values().Len(); i++ {
		assert.Equal(t, float64(0), s.Values().ValueAt(i))
	}
}

func TestPromReadWithFetchAndAbs(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, mockSession := local.NewStorageAndSession(t, ctrl)
	testTags := seriesiter.GenerateTag()
	mockSession.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).Return(seriesiter.NewMockSeriesIters(ctrl, testTags, 1, 10), true, nil)

	promRead := &PromReadHandler{engine: executor.NewEngine(storage)}
	req, _ := http.NewRequest("GET", PromReadURL, nil)
	params := defaultParams()
	params.Set(targetParam, `abs(http_requests_total{job="prometheus",group="canary"})`)
	req.URL.RawQuery = params.Encode()

	r, parseErr := parseParams(req)
	require.NoError(t, parseErr)
	seriesList, err := promRead.read(context.TODO(), httptest.NewRecorder(), r)
	require.NoError(t, err)
	require.Len(t, seriesList, 1)
	s := seriesList[0]
	assert.Equal(t, 361, s.Values().Len(), "10 second resolution for 1 hour including start time")
	assert.Equal(t, float64(0), s.Values().ValueAt(0), "first value is zero since db returns values starting from start + 10ms")
	assert.Equal(t, float64(0), s.Values().ValueAt(1))
	for i := 2; i < 10; i++ {
		assert.Equal(t, float64(i-1), s.Values().ValueAt(i))
	}

	for i := 11; i < s.Values().Len(); i++ {
		require.True(t, math.IsNaN(s.Values().ValueAt(i)), "all remaining are nans")
	}
}
