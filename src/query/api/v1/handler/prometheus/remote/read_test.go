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

	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/query/util/logging"
	xclock "github.com/m3db/m3/src/x/clock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	promReadTestMetrics     = newPromReadMetrics(tally.NewTestScope("", nil))
	defaultLookbackDuration = time.Minute

	timeoutOpts = &prometheus.TimeoutOpts{
		FetchTimeout: 15 * time.Second,
	}
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
	promRead := readHandler(storage, timeoutOpts)
	server := httptest.NewServer(test.NewSlowHandler(promRead, 10*time.Millisecond))
	return server
}

func readHandler(store storage.Storage, timeoutOpts *prometheus.TimeoutOpts) *PromReadHandler {
	opts := handler.FetchOptionsBuilderOptions{Limit: 100}
	engineOpts := executor.NewEngineOpts().SetStore(store).SetCostScope(tally.NewTestScope("test", nil)).
		SetLookbackDuration(defaultLookbackDuration).SetGlobalEnforcer(nil)
	return &PromReadHandler{
		engine:              executor.NewEngine(engineOpts),
		promReadMetrics:     promReadTestMetrics,
		timeoutOpts:         timeoutOpts,
		fetchOptionsBuilder: handler.NewFetchOptionsBuilder(opts),
	}
}

func TestPromReadParsing(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	opts := handler.FetchOptionsBuilderOptions{Limit: 100}
	engineOpts := executor.NewEngineOpts().SetStore(storage).SetCostScope(tally.NewTestScope("test", nil)).
		SetLookbackDuration(defaultLookbackDuration).SetGlobalEnforcer(nil)
	promRead := &PromReadHandler{
		engine:              executor.NewEngine(engineOpts),
		promReadMetrics:     promReadTestMetrics,
		fetchOptionsBuilder: handler.NewFetchOptionsBuilder(opts),
	}
	req, _ := http.NewRequest("POST", PromReadURL, test.GeneratePromReadBody(t))

	r, err := promRead.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, len(r.Queries), 1)
}

func TestPromFetchTimeoutParsing(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	opts := handler.FetchOptionsBuilderOptions{Limit: 100}
	engineOpts := executor.NewEngineOpts().SetStore(storage).SetCostScope(tally.NewTestScope("test", nil)).
		SetLookbackDuration(defaultLookbackDuration).SetGlobalEnforcer(nil)
	promRead := &PromReadHandler{
		engine:          executor.NewEngine(engineOpts),
		promReadMetrics: promReadTestMetrics,
		timeoutOpts: &prometheus.TimeoutOpts{
			FetchTimeout: 2 * time.Minute,
		},
		fetchOptionsBuilder: handler.NewFetchOptionsBuilder(opts),
	}

	req, _ := http.NewRequest("POST", PromReadURL, test.GeneratePromReadBody(t))
	dur, err := prometheus.ParseRequestTimeout(req, promRead.timeoutOpts.FetchTimeout)
	require.NoError(t, err)
	assert.Equal(t, 2*time.Minute, dur)
}

func TestPromReadParsingBad(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	promRead := readHandler(storage, timeoutOpts)
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
	promRead := readHandler(storage, timeoutOpts)
	req := test.GeneratePromReadRequest()
	_, err := promRead.read(context.TODO(), httptest.NewRecorder(),
		req, time.Hour, 100)
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
	opts := handler.FetchOptionsBuilderOptions{Limit: 100}
	engineOpts := executor.NewEngineOpts().SetStore(storage).SetCostScope(scope).
		SetLookbackDuration(defaultLookbackDuration).SetGlobalEnforcer(nil)
	promRead := &PromReadHandler{
		engine:              executor.NewEngine(engineOpts),
		promReadMetrics:     readMetrics,
		timeoutOpts:         timeoutOpts,
		fetchOptionsBuilder: handler.NewFetchOptionsBuilder(opts),
	}

	req, _ := http.NewRequest("POST", PromReadURL, test.GeneratePromReadBody(t))
	promRead.ServeHTTP(httptest.NewRecorder(), req)
	foundMetric := xclock.WaitUntil(func() bool {
		found := reporter.Counters()["fetch.errors"]
		return found == 1
	}, 5*time.Second)
	require.True(t, foundMetric)
}
