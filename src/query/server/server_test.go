//
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

package server

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote/test"
	"github.com/m3db/m3/src/query/cost"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/storage/m3"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xtest "github.com/m3db/m3x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var queryPort = 25123

var configYAML = `
listenAddress:
  type: "config"
  value: "127.0.0.1:25123"

metrics:
  scope:
    prefix: "coordinator"
  prometheus:
    handlerPath: /metrics
    listenAddress: "127.0.0.1:18202"
  sanitization: prometheus
  samplingRate: 1.0

clusters:
  - namespaces:
      - namespace: prometheus_metrics
        type: unaggregated
        retention: 48h

tagOptions:
  metricName: "_new"
  idScheme: quoted

readWorkerPoolPolicy:
  grow: true
  size: 100
  shards: 1000
  killProbability: 0.3

writeWorkerPoolPolicy:
  grow: true
  size: 100
  shards: 1000
  killProbability: 0.3

`

//TODO: Use randomly assigned port here
func TestRun(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	configFile, close := newTestFile(t, "config.yaml", configYAML)
	defer close()

	var cfg config.Configuration
	err := xconfig.LoadFile(&cfg, configFile.Name(), xconfig.Options{})
	require.NoError(t, err)

	// Override the client creation
	require.Equal(t, 1, len(cfg.Clusters))

	session := client.NewMockSession(ctrl)
	for _, value := range []float64{1, 2} {
		session.EXPECT().WriteTagged(ident.NewIDMatcher("prometheus_metrics"),
			ident.NewIDMatcher(`{_new="first",biz="baz",foo="bar"}`),
			gomock.Any(),
			gomock.Any(),
			value,
			gomock.Any(),
			nil)
	}
	for _, value := range []float64{3, 4} {
		session.EXPECT().WriteTagged(ident.NewIDMatcher("prometheus_metrics"),
			ident.NewIDMatcher(`{_new="second",bar="baz",foo="qux"}`),
			gomock.Any(),
			gomock.Any(),
			value,
			gomock.Any(),
			nil)
	}
	session.EXPECT().Close()

	dbClient := client.NewMockClient(ctrl)
	dbClient.EXPECT().DefaultSession().Return(session, nil)

	cfg.Clusters[0].NewClientFromConfig = m3.NewClientFromConfig(
		func(
			cfg client.Configuration,
			params client.ConfigurationParameters,
			custom ...client.CustomOption,
		) (client.Client, error) {
			return dbClient, nil
		})

	interruptCh := make(chan error)
	doneCh := make(chan struct{})
	go func() {
		Run(RunOptions{
			Config:      cfg,
			InterruptCh: interruptCh,
		})
		doneCh <- struct{}{}
	}()

	// Wait for server to come up
	waitForServerHealthy(t, queryPort)

	// Send Prometheus write request
	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d", queryPort)+remote.PromWriteURL, promReqBody)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	// Ensure close server performs as expected
	interruptCh <- fmt.Errorf("interrupt")
	<-doneCh
}

type closeFn func()

func newTestFile(t *testing.T, fileName, contents string) (*os.File, closeFn) {
	tmpFile, err := ioutil.TempFile("", fileName)
	require.NoError(t, err)

	_, err = tmpFile.Write([]byte(contents))
	require.NoError(t, err)

	return tmpFile, func() {
		assert.NoError(t, tmpFile.Close())
		assert.NoError(t, os.Remove(tmpFile.Name()))
	}
}

func waitForServerHealthy(t *testing.T, port int) {
	maxWait := 10 * time.Second
	startAt := time.Now()
	for time.Since(startAt) < maxWait {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/health", port), nil)
		require.NoError(t, err)
		res, err := http.DefaultClient.Do(req)
		if err != nil || res.StatusCode != http.StatusOK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
	require.FailNow(t, "waited for server healthy longer than limit: "+
		maxWait.String())
}

type queryServer struct {
	reads, searches, tagCompletes int
	mu                            sync.Mutex
}

func (s *queryServer) Fetch(
	*rpc.FetchRequest,
	rpc.Query_FetchServer,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	return nil
}

func (s *queryServer) Search(
	*rpc.SearchRequest,
	rpc.Query_SearchServer,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.searches++
	return nil
}

func (s *queryServer) CompleteTags(
	*rpc.CompleteTagsRequest,
	rpc.Query_CompleteTagsServer,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tagCompletes++
	return nil
}

func TestGRPCBackend(t *testing.T) {
	var grpcConfigYAML = `
listenAddress:
  type: "config"
  value: "127.0.0.1:17201"

metrics:
  scope:
    prefix: "coordinator"
  prometheus:
    handlerPath: /metrics
    listenAddress: "127.0.0.1:17203"
    onError: stderr
  sanitization: prometheus
  samplingRate: 1.0

rpc:
  remoteListenAddresses:
    - "127.0.0.1:17202"

backend: grpc

tagOptions:
  metricName: "bar"
  idScheme: prepend_meta

readWorkerPoolPolicy:
  grow: true
  size: 100
  shards: 1000
  killProbability: 0.3

writeWorkerPoolPolicy:
  grow: true
  size: 100
  shards: 1000
  killProbability: 0.3
`

	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	port := "127.0.0.1:17202"
	lis, err := net.Listen("tcp", port)
	require.NoError(t, err)
	s := grpc.NewServer()
	defer s.GracefulStop()
	qs := &queryServer{}
	rpc.RegisterQueryServer(s, qs)
	go func() {
		s.Serve(lis)
	}()

	configFile, close := newTestFile(t, "config_backend.yaml", grpcConfigYAML)
	defer close()

	var cfg config.Configuration
	err = xconfig.LoadFile(&cfg, configFile.Name(), xconfig.Options{})
	require.NoError(t, err)

	// No clusters
	require.Equal(t, 0, len(cfg.Clusters))
	require.Equal(t, config.GRPCStorageType, cfg.Backend)

	interruptCh := make(chan error)
	doneCh := make(chan struct{})
	go func() {
		Run(RunOptions{
			Config:      cfg,
			InterruptCh: interruptCh,
		})
		doneCh <- struct{}{}
	}()

	// Wait for server to come up
	waitForServerHealthy(t, 17201)

	// Send Prometheus read request
	promReq := test.GeneratePromReadRequest()
	promReqBody := test.GeneratePromReadRequestBody(t, promReq)
	req, err := http.NewRequest(http.MethodPost,
		"http://127.0.0.1:17201"+remote.PromReadURL, promReqBody)
	require.NoError(t, err)

	_, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, qs.reads, 1)

	// Ensure close server performs as expected
	interruptCh <- fmt.Errorf("interrupt")
	<-doneCh
}

func TestNewPerQueryEnforcer(t *testing.T) {
	type testContext struct {
		Global cost.ChainedEnforcer
		Query  cost.ChainedEnforcer
		Block  cost.ChainedEnforcer
	}

	setup := func(t *testing.T, globalLimit, queryLimit int) testContext {
		cfg := &config.Configuration{
			Limits: config.LimitsConfiguration{
				Global: config.GlobalLimitsConfiguration{
					MaxFetchedDatapoints: 100,
				},
				PerQuery: config.PerQueryLimitsConfiguration{
					MaxFetchedDatapoints: 10,
				},
			},
		}

		global, err := newConfiguredChainedEnforcer(cfg, instrument.NewOptions())
		require.NoError(t, err)

		queryLvl := global.Child(cost.QueryLevel)
		blockLvl := queryLvl.Child(cost.BlockLevel)

		return testContext{
			Global: global,
			Query:  queryLvl,
			Block:  blockLvl,
		}
	}

	tctx := setup(t, 100, 10)

	// spot check that limits are setup properly for each level
	r := tctx.Block.Add(11)
	require.Error(t, r.Error)

	floatsEqual := func(f1, f2 float64) {
		assert.InDelta(t, f1, f2, 0.0000001)
	}

	floatsEqual(float64(r.Cost), 11)

	r, _ = tctx.Query.State()
	floatsEqual(float64(r.Cost), 11)

	r, _ = tctx.Global.State()
	floatsEqual(float64(r.Cost), 11)
	require.NoError(t, r.Error)
}
