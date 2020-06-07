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
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote/test"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xclock "github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func makeOptions(ds ingest.DownsamplerAndWriter) options.HandlerOptions {
	return options.EmptyHandlerOptions().
		SetNowFn(time.Now).
		SetDownsamplerAndWriter(ds).
		SetTagOptions(models.NewTagOptions()).
		SetConfig(config.Configuration{
			WriteForwarding: config.WriteForwardingConfiguration{
				PromRemoteWrite: handleroptions.PromWriteHandlerForwardingOptions{},
			},
		})
}

func TestPromWriteParsing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	handlerOpts := makeOptions(mockDownsamplerAndWriter)
	handler, err := NewPromWriteHandler(handlerOpts)
	require.NoError(t, err)

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req := httptest.NewRequest(PromWriteHTTPMethod, PromWriteURL, promReqBody)

	r, opts, _, err := handler.(*PromWriteHandler).parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, len(r.Timeseries), 2)
	require.Equal(t, ingest.WriteOptions{}, opts)
}

func TestPromWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	mockDownsamplerAndWriter.
		EXPECT().
		WriteBatch(gomock.Any(), gomock.Any(), gomock.Any())

	opts := makeOptions(mockDownsamplerAndWriter)
	handler, err := NewPromWriteHandler(opts)
	require.NoError(t, err)

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req := httptest.NewRequest(PromWriteHTTPMethod, PromWriteURL, promReqBody)

	writer := httptest.NewRecorder()
	handler.ServeHTTP(writer, req)
	resp := writer.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestPromWriteError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	multiErr := xerrors.NewMultiError().Add(errors.New("an error"))
	batchErr := ingest.BatchError(multiErr)

	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	mockDownsamplerAndWriter.EXPECT().
		WriteBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(batchErr)

	opts := makeOptions(mockDownsamplerAndWriter)
	handler, err := NewPromWriteHandler(opts)
	require.NoError(t, err)

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req := httptest.NewRequest(PromWriteHTTPMethod, PromWriteURL, promReqBody)
	require.NoError(t, err)

	writer := httptest.NewRecorder()
	handler.ServeHTTP(writer, req)
	resp := writer.Result()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.True(t, bytes.Contains(body, []byte(batchErr.Error())))
}

func TestWriteErrorMetricCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)

	scope := tally.NewTestScope("",
		map[string]string{"test": "error-metric-test"})

	iopts := instrument.NewOptions().SetMetricsScope(scope)
	opts := makeOptions(mockDownsamplerAndWriter).SetInstrumentOpts(iopts)
	handler, err := NewPromWriteHandler(opts)
	require.NoError(t, err)

	req := httptest.NewRequest(PromWriteHTTPMethod, PromWriteURL, nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	foundMetric := xclock.WaitUntil(func() bool {
		found, ok := scope.Snapshot().Counters()["write.errors+code=4XX,handler=remote-write,test=error-metric-test"]
		return ok && found.Value() == 1
	}, 5*time.Second)
	require.True(t, foundMetric)
}

func TestWriteDatapointDelayMetric(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	mockDownsamplerAndWriter.
		EXPECT().
		WriteBatch(gomock.Any(), gomock.Any(), gomock.Any())

	scope := tally.NewTestScope("",
		map[string]string{"test": "delay-metric-test"})

	iopts := instrument.NewOptions().SetMetricsScope(scope)
	opts := makeOptions(mockDownsamplerAndWriter).SetInstrumentOpts(iopts)
	handler, err := NewPromWriteHandler(opts)
	require.NoError(t, err)

	writeHandler, ok := handler.(*PromWriteHandler)
	require.True(t, ok)

	buckets := writeHandler.metrics.ingestLatencyBuckets

	// NB(r): Bucket length is tested just to sanity check how many buckets we are creating
	require.Equal(t, 80, len(buckets.AsDurations()))

	// NB(r): Bucket values are tested to sanity check they look right
	expected := "[0s 100ms 200ms 300ms 400ms 500ms 600ms 700ms 800ms 900ms 1s 1.5s 2s 2.5s 3s 3.5s 4s 4.5s 5s 5.5s 6s 6.5s 7s 7.5s 8s 8.5s 9s 9.5s 10s 15s 20s 25s 30s 35s 40s 45s 50s 55s 1m0s 5m0s 10m0s 15m0s 20m0s 25m0s 30m0s 35m0s 40m0s 45m0s 50m0s 55m0s 1h0m0s 1h30m0s 2h0m0s 2h30m0s 3h0m0s 3h30m0s 4h0m0s 4h30m0s 5h0m0s 5h30m0s 6h0m0s 6h30m0s 7h0m0s 8h0m0s 9h0m0s 10h0m0s 11h0m0s 12h0m0s 13h0m0s 14h0m0s 15h0m0s 16h0m0s 17h0m0s 18h0m0s 19h0m0s 20h0m0s 21h0m0s 22h0m0s 23h0m0s 24h0m0s]"
	actual := fmt.Sprintf("%v", buckets.AsDurations())
	require.Equal(t, expected, actual)

	// Ensure buckets increasing in order
	lastValue := time.Duration(math.MinInt64)
	for _, value := range buckets.AsDurations() {
		require.True(t, value > lastValue,
			fmt.Sprintf("%s must be greater than last bucket value %s", value, lastValue))
		lastValue = value
	}

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req := httptest.NewRequest(PromWriteHTTPMethod, PromWriteURL, promReqBody)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	foundMetric := xclock.WaitUntil(func() bool {
		values, found := scope.Snapshot().Histograms()["ingest.latency+handler=remote-write,test=delay-metric-test"]
		if !found {
			return false
		}
		for _, valuesInBucket := range values.Durations() {
			if valuesInBucket > 0 {
				return true
			}
		}
		return false
	}, 5*time.Second)
	require.True(t, foundMetric)
}

func TestPromWriteUnaggregatedMetricsWithHeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expectedIngestWriteOptions := ingest.WriteOptions{
		DownsampleOverride:     true,
		DownsampleMappingRules: nil,
		WriteOverride:          false,
		WriteStoragePolicies:   nil,
	}

	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	mockDownsamplerAndWriter.
		EXPECT().
		WriteBatch(gomock.Any(), gomock.Any(), expectedIngestWriteOptions)

	opts := makeOptions(mockDownsamplerAndWriter)
	handler, err := NewPromWriteHandler(opts)
	require.NoError(t, err)

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req := httptest.NewRequest(PromWriteHTTPMethod, PromWriteURL, promReqBody)
	req.Header.Add(handleroptions.MetricsTypeHeader,
		storagemetadata.UnaggregatedMetricsType.String())

	writer := httptest.NewRecorder()
	handler.ServeHTTP(writer, req)
	resp := writer.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestPromWriteAggregatedMetricsWithHeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expectedIngestWriteOptions := ingest.WriteOptions{
		DownsampleOverride:     true,
		DownsampleMappingRules: nil,
		WriteOverride:          true,
		WriteStoragePolicies: policy.StoragePolicies{
			policy.MustParseStoragePolicy("1m:21d"),
		},
	}

	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	mockDownsamplerAndWriter.
		EXPECT().
		WriteBatch(gomock.Any(), gomock.Any(), expectedIngestWriteOptions)

	opts := makeOptions(mockDownsamplerAndWriter)
	writeHandler, err := NewPromWriteHandler(opts)
	require.NoError(t, err)

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req := httptest.NewRequest(PromWriteHTTPMethod, PromWriteURL, promReqBody)
	req.Header.Add(handleroptions.MetricsTypeHeader,
		storagemetadata.AggregatedMetricsType.String())
	req.Header.Add(handleroptions.MetricsStoragePolicyHeader,
		"1m:21d")

	writer := httptest.NewRecorder()
	writeHandler.ServeHTTP(writer, req)
	resp := writer.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func BenchmarkWriteDatapoints(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	mockDownsamplerAndWriter := ingest.NewMockDownsamplerAndWriter(ctrl)
	mockDownsamplerAndWriter.
		EXPECT().
		WriteBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes()

	opts := makeOptions(mockDownsamplerAndWriter)
	handler, err := NewPromWriteHandler(opts)
	require.NoError(b, err)

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBodyBytes(b, promReq)
	promReqBodyReader := bytes.NewReader(nil)

	for i := 0; i < b.N; i++ {
		promReqBodyReader.Reset(promReqBody)
		req := httptest.NewRequest(PromWriteHTTPMethod, PromWriteURL, promReqBodyReader)
		handler.ServeHTTP(httptest.NewRecorder(), req)
	}
}
