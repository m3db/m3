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

package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote/test"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/util/logging"
	xclock "github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestPromWriteParsing(t *testing.T) {
	logging.InitWithCores(nil)

	promWrite := &PromWriteHandler{}

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req, _ := http.NewRequest("POST", PromWriteURL, promReqBody)

	r, err := promWrite.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, len(r.Timeseries), 2)
}

func TestPromWrite(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	mockDownsamplerAndWriter.EXPECT().WriteBatch(gomock.Any(), gomock.Any())

	promWrite := &PromWriteHandler{downsamplerAndWriter: mockDownsamplerAndWriter}

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req := httptest.NewRequest("POST", PromWriteURL, promReqBody)

	r, err := promWrite.parseRequest(req)
	require.Nil(t, err, "unable to parse request")

	writeErr := promWrite.write(context.TODO(), r)
	require.NoError(t, writeErr)
}

func TestPromWriteError(t *testing.T) {
	logging.InitWithCores(nil)

	multiErr := xerrors.NewMultiError().Add(errors.New("an error"))
	batchErr := ingest.BatchError(multiErr)

	ctrl := gomock.NewController(t)
	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	mockDownsamplerAndWriter.EXPECT().
		WriteBatch(gomock.Any(), gomock.Any()).
		Return(batchErr)

	promWrite, err := NewPromWriteHandler(mockDownsamplerAndWriter,
		models.NewTagOptions(), tally.NoopScope)
	require.NoError(t, err)

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req, err := http.NewRequest("POST", PromWriteURL, promReqBody)
	require.NoError(t, err)

	writer := httptest.NewRecorder()
	promWrite.ServeHTTP(writer, req)
	resp := writer.Result()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.True(t, bytes.Contains(body, []byte(batchErr.Error())))
}

func TestWriteErrorMetricCount(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	mockDownsamplerAndWriter.EXPECT().WriteBatch(gomock.Any(), gomock.Any())

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	defer closer.Close()
	writeMetrics, err := newPromWriteMetrics(scope)
	require.NoError(t, err)

	promWrite := &PromWriteHandler{
		downsamplerAndWriter: mockDownsamplerAndWriter,
		promWriteMetrics:     writeMetrics,
	}
	req, _ := http.NewRequest("POST", PromWriteURL, nil)
	promWrite.ServeHTTP(httptest.NewRecorder(), req)

	foundMetric := xclock.WaitUntil(func() bool {
		found := reporter.Counters()["write.errors"]
		return found == 1
	}, 5*time.Second)
	require.True(t, foundMetric)
}

func TestWriteDatapointDelayMetric(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	mockDownsamplerAndWriter.EXPECT().WriteBatch(gomock.Any(), gomock.Any())

	scope := tally.NewTestScope("", map[string]string{"test": "delay-metric-test"})

	handler, err := NewPromWriteHandler(mockDownsamplerAndWriter,
		models.NewTagOptions(), scope)
	require.NoError(t, err)

	writeHandler, ok := handler.(*PromWriteHandler)
	require.True(t, ok)

	buckets := writeHandler.promWriteMetrics.datapointDelayBuckets
	expected := "[0s 100ms 200ms 300ms 400ms 500ms 600ms 700ms 800ms 900ms 1s 1.25s 1.5s 1.75s 2s 2.25s 2.5s 2.75s 3s 3.25s 3.5s 3.75s 4s 4.25s 4.5s 4.75s 5s 5.25s 5.5s 5.75s 6s 6.25s 6.5s 6.75s 7s 7.25s 7.5s 7.75s 8s 8.25s 8.5s 8.75s 9s 9.25s 9.5s 9.75s 10s 10.25s 10.5s 10.75s 10s 11s 12s 13s 14s 15s 16s 17s 18s 19s 20s 21s 22s 23s 24s 25s 26s 27s 28s 29s 30s 31s 32s 33s 34s 35s 36s 37s 38s 39s 40s 41s 42s 43s 44s 45s 46s 47s 48s 49s 50s 51s 52s 53s 54s 55s 56s 57s 58s 59s 1m0s 1m1s 1m2s 1m3s 1m4s 1m5s 1m6s 1m7s 1m8s 1m9s 1m0s 2m0s 3m0s 4m0s 5m0s 6m0s 7m0s 8m0s 9m0s 10m0s 11m0s 12m0s 13m0s 14m0s 15m0s 16m0s 17m0s 18m0s 19m0s 20m0s 21m0s 22m0s 23m0s 24m0s 25m0s 26m0s 27m0s 28m0s 29m0s 30m0s 31m0s 32m0s 33m0s 34m0s 35m0s 36m0s 37m0s 38m0s 39m0s 40m0s 41m0s 42m0s 43m0s 44m0s 45m0s 46m0s 47m0s 48m0s 49m0s 50m0s 51m0s 52m0s 53m0s 54m0s 55m0s 56m0s 57m0s 58m0s 59m0s 1h0m0s]"
	actual := fmt.Sprintf("%v", buckets.AsDurations())
	require.Equal(t, expected, actual)

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req := httptest.NewRequest("POST", PromWriteURL, promReqBody)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	foundMetric := xclock.WaitUntil(func() bool {
		value, found := scope.Snapshot().Histograms()["datapoint.delay+test=delay-metric-test"]
		for k, v := range scope.Snapshot().Histograms() {
			fmt.Printf("%s = %v\n", k, len(v.Durations()))
		}
		return found && len(value.Durations()) >= 1
	}, 5*time.Second)
	require.True(t, foundMetric)
}
