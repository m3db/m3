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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3db/src/coordinator/generated/proto/prompb"
	"github.com/m3db/m3db/src/coordinator/test/local"
	"github.com/m3db/m3db/src/coordinator/util/logging"
	"github.com/m3db/m3db/src/dbnode/x/metrics"
	xclock "github.com/m3db/m3x/clock"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func generatePromWriteRequest() *prompb.WriteRequest {
	req := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{{
			Labels: []*prompb.Label{
				{Name: "foo", Value: "bar"},
				{Name: "biz", Value: "baz"},
			},
			Samples: []*prompb.Sample{
				{Value: 1.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
				{Value: 2.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
			},
		},
			{
				Labels: []*prompb.Label{
					{Name: "foo", Value: "qux"},
					{Name: "bar", Value: "baz"},
				},
				Samples: []*prompb.Sample{
					{Value: 3.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
					{Value: 4.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
				},
			}},
	}
	return req
}

func generatePromWriteBody(t *testing.T) io.Reader {
	req := generatePromWriteRequest()
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}

	compressed := snappy.Encode(nil, data)
	b := bytes.NewReader(compressed)
	return b
}

func TestPromWriteParsing(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, _ := local.NewStorageAndSession(t, ctrl)

	promWrite := &PromWriteHandler{store: storage}

	req, _ := http.NewRequest("POST", PromWriteURL, generatePromWriteBody(t))

	r, err := promWrite.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, len(r.Timeseries), 2)
}

func TestPromWrite(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	storage, session := local.NewStorageAndSession(t, ctrl)
	session.EXPECT().WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	promWrite := &PromWriteHandler{store: storage}

	req, _ := http.NewRequest("POST", PromWriteURL, generatePromWriteBody(t))

	r, err := promWrite.parseRequest(req)
	require.Nil(t, err, "unable to parse request")

	writeErr := promWrite.write(context.TODO(), r)
	require.NoError(t, writeErr)
}

func TestWriteErrorMetricCount(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	storage, session := local.NewStorageAndSession(t, ctrl)
	session.EXPECT().WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	defer closer.Close()
	writeMetrics := newPromWriteMetrics(scope)

	promWrite := &PromWriteHandler{store: storage, promWriteMetrics: writeMetrics}
	req, _ := http.NewRequest("POST", PromWriteURL, nil)
	promWrite.ServeHTTP(httptest.NewRecorder(), req)

	foundMetric := xclock.WaitUntil(func() bool {
		found := reporter.Counters()["write.errors"]
		return found == 1
	}, 5*time.Second)
	require.True(t, foundMetric)
}
