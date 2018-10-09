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

package json

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3/src/collector/reporter"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	reportRequestJSON = `
{
	"metrics": [
		{
			"type": "counter",
			"tags": {"foo": "bar"},
			"value": 1
		},
		{
			"type": "gauge",
			"tags": {"foo": "baz"},
			"value": 2.42
		},
		{
			"type": "timer",
			"tags": {"foo": "qux"},
			"value": 3.42
		}
	]
}
`
)

func TestReportHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	test := newTestReportHandler(ctrl)
	handler := test.handler
	reporter := test.reporter

	req := newTestReportHandlerRequest(t, reportRequestJSON)

	reporter.EXPECT().
		ReportCounter(gomock.Any(), gomock.Any()).
		DoAndReturn(func(id id.ID, value int64) error {
			tagValue, ok := id.TagValue([]byte("foo"))
			require.True(t, ok)
			assert.Equal(t, "bar", string(tagValue))
			assert.Equal(t, int64(1), value)
			return nil
		})

	reporter.EXPECT().
		ReportGauge(gomock.Any(), gomock.Any()).
		DoAndReturn(func(id id.ID, value float64) error {
			tagValue, ok := id.TagValue([]byte("foo"))
			require.True(t, ok)
			assert.Equal(t, "baz", string(tagValue))
			assert.Equal(t, 2.42, value)
			return nil
		})

	reporter.EXPECT().
		ReportBatchTimer(gomock.Any(), gomock.Any()).
		DoAndReturn(func(id id.ID, values []float64) error {
			tagValue, ok := id.TagValue([]byte("foo"))
			require.True(t, ok)
			assert.Equal(t, "qux", string(tagValue))
			require.Equal(t, 1, len(values))
			assert.Equal(t, 3.42, values[0])
			return nil
		})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	resp := decodeParseReportResponse(t, http.StatusOK, recorder)
	assert.Equal(t, 3, resp.Reported)
}

func TestReportHandlerInvalidCounterValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	test := newTestReportHandler(ctrl)
	handler := test.handler

	req := newTestReportHandlerRequest(t, `
	{
		"metrics": [
			{
				"type": "counter",
				"tags": {"foo": "bar"},
				"value": 1.42
			}
		]
	}
`)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code,
		fmt.Sprintf("not 400 status: status=%d\nbody=%s\n",
			recorder.Code, recorder.Body.String()))
}

func TestReportHandlerUnknownMetricType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	test := newTestReportHandler(ctrl)
	handler := test.handler

	req := newTestReportHandlerRequest(t, `
	{
		"metrics": [
			{
				"type": "invalid",
				"tags": {"foo": "bar"},
				"value": 1.42
			}
		]
	}
`)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code,
		fmt.Sprintf("not 400 status: status=%d\nbody=%s\n",
			recorder.Code, recorder.Body.String()))
}

func TestReportHandlerInvalidJSONBody(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	test := newTestReportHandler(ctrl)
	handler := test.handler

	req := newTestReportHandlerRequest(t, "plain text")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code,
		fmt.Sprintf("not 400 status: status=%d\nbody=%s\n",
			recorder.Code, recorder.Body.String()))
}

type testReportHandler struct {
	handler  *reportHandler
	reporter *reporter.MockReporter
}

func newTestReportHandler(ctrl *gomock.Controller) testReportHandler {
	reporter := reporter.NewMockReporter(ctrl)
	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	tagEncoderPool := serialize.NewTagEncoderPool(
		serialize.NewTagEncoderOptions(), poolOpts)
	tagEncoderPool.Init()
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(), poolOpts)
	tagDecoderPool.Init()
	instrumentOpts := instrument.NewOptions()

	handler := NewReportHandler(reporter,
		tagEncoderPool, tagDecoderPool, instrumentOpts)
	return testReportHandler{
		handler:  handler.(*reportHandler),
		reporter: reporter,
	}
}

func newTestReportHandlerRequest(t *testing.T, body string) *http.Request {
	req, err := http.NewRequest(ReportHTTPMethod, ReportURL,
		strings.NewReader(body))
	require.NoError(t, err)
	return req
}

func decodeParseReportResponse(
	t *testing.T,
	expectStatusCode int,
	recorder *httptest.ResponseRecorder,
) reportResponse {
	assert.Equal(t, expectStatusCode, recorder.Code,
		fmt.Sprintf("not %d status: status=%d\nbody=%s\n",
			expectStatusCode, recorder.Code, recorder.Body.String()))

	var resp reportResponse
	err := json.NewDecoder(recorder.Body).Decode(&resp)
	require.NoError(t, err)
	return resp
}
