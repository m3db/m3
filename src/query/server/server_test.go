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
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	remotetest "github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote/test/remote"
	"github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/storage/local"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"sync"
)

var configYAML = `
listenAddress:
  type: "config"
  value: "127.0.0.1:7201"

metrics:
  scope:
    prefix: "coordinator"
  prometheus:
    handlerPath: /metrics
    listenAddress: "127.0.0.1:7202"
  sanitization: prometheus
  samplingRate: 1.0

clusters:
  - namespaces:
      - namespace: prometheus_metrics
        storageMetricsType: unaggregated
        retention: 48h
`

func TestRun(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
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
			ident.NewIDMatcher("__name__=first,biz=baz,foo=bar,"),
			gomock.Any(),
			gomock.Any(),
			value,
			gomock.Any(),
			nil)
	}
	for _, value := range []float64{3, 4} {
		session.EXPECT().WriteTagged(ident.NewIDMatcher("prometheus_metrics"),
			ident.NewIDMatcher("__name__=second,bar=baz,foo=qux,"),
			gomock.Any(),
			gomock.Any(),
			value,
			gomock.Any(),
			nil)
	}
	session.EXPECT().Close()

	dbClient := client.NewMockClient(ctrl)
	dbClient.EXPECT().DefaultSession().Return(session, nil)

	cfg.Clusters[0].NewClientFromConfig = local.NewClientFromConfig(
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
	waitForServerHealthy(t, 7201)

	// Send Prometheus write request
	promReq := remotetest.GeneratePromWriteRequest()
	promReqBody := remotetest.GeneratePromWriteRequestBody(t, promReq)
	req, err := http.NewRequest(http.MethodPost,
		"http://127.0.0.1:7201"+remote.PromWriteURL, promReqBody)
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
	writes, reads int
	mu sync.Mutex
}

func (s *queryServer) Fetch(*rpcpb.FetchMessage, rpcpb.Query_FetchServer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	return nil
}

func (s *queryServer) Write(rpcpb.Query_WriteServer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
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
`

	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	port := "127.0.0.1:17202"
	lis, err := net.Listen("tcp", port)
	require.NoError(t, err)
	s := grpc.NewServer()
	defer s.GracefulStop()
	qs := &queryServer{}
	rpcpb.RegisterQueryServer(s, qs)
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

	// Send Prometheus write request
	promReq := remotetest.GeneratePromWriteRequest()
	promReqBody := remotetest.GeneratePromWriteRequestBody(t, promReq)
	req, err := http.NewRequest(http.MethodPost,
		"http://127.0.0.1:17201"+remote.PromWriteURL, promReqBody)
	require.NoError(t, err)

	_, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, qs.writes, 2)
	// Ensure close server performs as expected
	interruptCh <- fmt.Errorf("interrupt")
	<-doneCh
}
