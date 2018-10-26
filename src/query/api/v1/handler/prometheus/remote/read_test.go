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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/query/util/logging"
	xclock "github.com/m3db/m3x/clock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	promReadTestMetrics = newPromReadMetrics(tally.NewTestScope("", nil))
)

func setupServer(t *testing.T) *httptest.Server {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	// No calls expected on session object
	lstore, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().
		FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, false, fmt.Errorf("not initialized"))
	storage := test.NewSlowStorage(lstore, 10*time.Millisecond)
	promRead := readHandler(storage)
	server := httptest.NewServer(test.NewSlowHandler(promRead, 10*time.Millisecond))
	return server
}

func readHandler(store storage.Storage) *PromReadHandler {
	return &PromReadHandler{engine: executor.NewEngine(store, tally.NewTestScope("test", nil), cost.NoopChainedEnforcer()), promReadMetrics: promReadTestMetrics}
}

func TestPromReadParsing(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	promRead := &PromReadHandler{engine: executor.NewEngine(storage, tally.NewTestScope("test", nil), cost.NoopChainedEnforcer()), promReadMetrics: promReadTestMetrics}
	req, _ := http.NewRequest("POST", PromReadURL, test.GeneratePromReadBody(t))

	r, err := promRead.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, len(r.Queries), 1)
}

func TestPromReadParsingBad(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	promRead := readHandler(storage)
	req, _ := http.NewRequest("POST", PromReadURL, strings.NewReader("bad body"))
	_, err := promRead.parseRequest(req)
	require.NotNil(t, err, "unable to parse request")
}

func TestPromReadStorageWithFetchError(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, true, fmt.Errorf("unable to get data"))
	session.EXPECT().IteratorPools().
		Return(nil, nil)
	promRead := readHandler(storage)
	req := test.GeneratePromReadRequest()
	_, err := promRead.read(context.TODO(), httptest.NewRecorder(), req, time.Hour)
	require.NotNil(t, err, "unable to read from storage")
}

func TestQueryMatchMustBeEqual(t *testing.T) {
	logging.InitWithCores(nil)

	req := test.GeneratePromReadRequest()
	matchers, err := storage.PromMatchersToM3(req.Queries[0].Matchers)
	require.NoError(t, err)

	_, err = matchers.ToTags(models.NewTagOptions())
	assert.NoError(t, err)
}

func TestQueryKillOnClientDisconnect(t *testing.T) {
	server := setupServer(t)
	defer server.Close()

	c := &http.Client{
		Timeout: 1 * time.Millisecond,
	}

	_, err := c.Post(server.URL, "application/x-protobuf", test.GeneratePromReadBody(t))
	assert.Error(t, err)
}

func TestQueryKillOnTimeout(t *testing.T) {
	server := setupServer(t)
	defer server.Close()

	req, _ := http.NewRequest("POST", server.URL, test.GeneratePromReadBody(t))
	req.Header.Add("Content-Type", "application/x-protobuf")
	req.Header.Add("timeout", "1ms")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotNil(t, resp)
	assert.Equal(t, resp.StatusCode, 500, "Status code not 500")
}

func TestReadErrorMetricsCount(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, true, fmt.Errorf("unable to get data"))
	session.EXPECT().IteratorPools().
		Return(nil, nil)

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	defer closer.Close()
	readMetrics := newPromReadMetrics(scope)

	promRead := &PromReadHandler{engine: executor.NewEngine(storage, scope, cost.NoopChainedEnforcer()), promReadMetrics: readMetrics}
	req, _ := http.NewRequest("POST", PromReadURL, test.GeneratePromReadBody(t))
	promRead.ServeHTTP(httptest.NewRecorder(), req)

	foundMetric := xclock.WaitUntil(func() bool {
		found := reporter.Counters()["fetch.errors"]
		return found == 1
	}, 5*time.Second)
	require.True(t, foundMetric)
}
