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
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	m3msgproto "github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote/test"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/promremote/promremotetest"
	xclock "github.com/m3db/m3/src/x/clock"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/serialize"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

var configYAML = `
listenAddress: 127.0.0.1:0

logging:
  level: info

metrics:
  scope:
    prefix: "coordinator"
  prometheus:
    handlerPath: /metrics
    listenAddress: "127.0.0.1:0"
  sanitization: prometheus
  samplingRate: 1.0

clusters:
  - namespaces:
      - namespace: prometheus_metrics
        type: unaggregated
        retention: 48h
      - namespace: prometheus_metrics_1m_aggregated
        type: aggregated
        retention: 120h
        resolution: 1m
        downsample:
          all: false

ingest:
  ingester:
    workerPoolSize: 100
    opPool:
      size: 100
    retry:
      maxRetries: 3
      jitter: true
    logSampleRate: 0.01
  m3msg:
    server:
      listenAddress: "0.0.0.0:0"
      retry:
        maxBackoff: 10s
        jitter: true

tagOptions:
  metricName: "_new"
  idScheme: quoted

readWorkerPoolPolicy:
  grow: true
  size: 100
  shards: 100
  killProbability: 0.3

writeWorkerPoolPolicy:
  grow: true
  size: 100
  shards: 100
  killProbability: 0.3
`

func TestMultiProcessSetsProcessLabel(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	configFile, c := newTestFile(t, "config.yaml", configYAML)
	defer c()

	var cfg config.Configuration
	err := xconfig.LoadFile(&cfg, configFile.Name(), xconfig.Options{})
	require.NoError(t, err)

	metricsPort := 8765
	cfg.Metrics.PrometheusReporter.ListenAddress = fmt.Sprintf("127.0.0.1:%d", metricsPort)
	cfg.MultiProcess = config.MultiProcessConfiguration{
		Enabled: true,
		Count:   2,
	}
	detailedMetrics := instrument.DetailedExtendedMetrics
	cfg.Metrics.ExtendedMetrics = &detailedMetrics

	multiProcessInstance := multiProcessProcessID()
	if multiProcessInstance == "" {
		// Parent process just needs to ensure that it spawns the child and
		// that the child exits cleanly.
		// The child process will ensure that everything comes up properly
		// and that the metrics have the correct labels on it.
		result := Run(RunOptions{Config: cfg})
		assert.True(t, result.MultiProcessRun)
		assert.True(t, result.MultiProcessIsParentCleanExit)
		return
	}

	// Override the client creation
	require.Equal(t, 1, len(cfg.Clusters))

	session := client.NewMockSession(ctrl)
	session.EXPECT().Close().AnyTimes()

	dbClient := client.NewMockClient(ctrl)
	dbClient.EXPECT().DefaultSession().Return(session, nil).AnyTimes()

	cfg.Clusters[0].NewClientFromConfig = m3.NewClientFromConfig(
		func(
			cfg client.Configuration,
			params client.ConfigurationParameters,
			custom ...client.CustomAdminOption,
		) (client.Client, error) {
			return dbClient, nil
		})

	interruptCh := make(chan error, 1)
	doneCh := make(chan struct{}, 1)
	listenerCh := make(chan net.Listener, 1)

	rulesNamespacesValue := kv.NewMockValue(ctrl)
	rulesNamespacesValue.EXPECT().Version().Return(0).AnyTimes()
	rulesNamespacesValue.EXPECT().Unmarshal(gomock.Any()).DoAndReturn(func(v proto.Message) error {
		msg := v.(*rulepb.Namespaces)
		*msg = rulepb.Namespaces{}
		return nil
	})
	rulesNamespacesWatchable := kv.NewValueWatchable()
	_ = rulesNamespacesWatchable.Update(rulesNamespacesValue)
	_, rulesNamespacesWatch, err := rulesNamespacesWatchable.Watch()
	require.NoError(t, err)
	kvClient := kv.NewMockStore(ctrl)
	kvClient.EXPECT().Watch(gomock.Any()).Return(rulesNamespacesWatch, nil).AnyTimes()
	clusterClient := clusterclient.NewMockClient(ctrl)
	clusterClient.EXPECT().KV().Return(kvClient, nil).AnyTimes()
	clusterClientCh := make(chan clusterclient.Client, 1)
	clusterClientCh <- clusterClient

	downsamplerReadyCh := make(chan struct{}, 1)

	go func() {
		result := Run(RunOptions{
			Config:             cfg,
			InterruptCh:        interruptCh,
			ListenerCh:         listenerCh,
			ClusterClient:      clusterClientCh,
			DownsamplerReadyCh: downsamplerReadyCh,
		})
		assert.True(t, result.MultiProcessRun)
		assert.False(t, result.MultiProcessIsParentCleanExit)
		doneCh <- struct{}{}
	}()

	// Wait for downsampler to be ready.
	<-downsamplerReadyCh

	// Wait for listener
	listener := <-listenerCh
	addr := listener.Addr().String()

	// Wait for server to come up
	waitForServerHealthy(t, addr)

	r, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", metricsPort)) //nolint
	require.NoError(t, err)
	defer r.Body.Close()
	bodyBytes, err := ioutil.ReadAll(r.Body)
	require.NoError(t, err)
	metricsResponse := string(bodyBytes)
	assert.Contains(t, metricsResponse, "coordinator_runtime_memory_allocated{multiprocess_id=\"1\"}")
	assert.Contains(t, metricsResponse, "coordinator_ingest_success{multiprocess_id=\"1\"}")
	// Ensure close server performs as expected
	interruptCh <- fmt.Errorf("interrupt")
	<-doneCh
}

func TestWriteH1(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	configFile, closeFn := newTestFile(t, "config.yaml", configYAML)
	defer closeFn()

	var cfg config.Configuration
	err := xconfig.LoadFile(&cfg, configFile.Name(), xconfig.Options{})
	require.NoError(t, err)

	testWrite(t, cfg, ctrl)
}

func TestWriteH2C(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	configFile, closer := newTestFile(t, "config.yaml", configYAML)
	defer closer()

	var cfg config.Configuration
	err := xconfig.LoadFile(&cfg, configFile.Name(), xconfig.Options{})
	require.NoError(t, err)

	cfg.HTTP.EnableH2C = true

	testWrite(t, cfg, ctrl)
}

func TestIngestH1(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	configFile, closer := newTestFile(t, "config.yaml", configYAML)
	defer closer()

	var cfg config.Configuration
	err := xconfig.LoadFile(&cfg, configFile.Name(), xconfig.Options{})
	require.NoError(t, err)

	testIngest(t, cfg, ctrl)
}

func TestIngestH2C(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	configFile, closer := newTestFile(t, "config.yaml", configYAML)
	defer closer()

	var cfg config.Configuration
	err := xconfig.LoadFile(&cfg, configFile.Name(), xconfig.Options{})
	require.NoError(t, err)

	cfg.HTTP.EnableH2C = true

	testIngest(t, cfg, ctrl)
}

func TestGRPCBackend(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcAddr := lis.Addr().String()
	grpcConfigYAML := fmt.Sprintf(`
listenAddress: 127.0.0.1:0

logging:
  level: info

metrics:
  scope:
    prefix: "coordinator"
  prometheus:
    handlerPath: /metrics
    listenAddress: "127.0.0.1:0"
    onError: stderr
  sanitization: prometheus
  samplingRate: 1.0

rpc:
  remoteListenAddresses: ["%s"]

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
`, grpcAddr)

	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	s := grpc.NewServer()
	defer s.GracefulStop()
	qs := newQueryServer()
	rpc.RegisterQueryServer(s, qs)
	go func() {
		_ = s.Serve(lis)
	}()

	configFile, closeFn := newTestFile(t, "config_backend.yaml", grpcConfigYAML)
	defer closeFn()

	var cfg config.Configuration
	err = xconfig.LoadFile(&cfg, configFile.Name(), xconfig.Options{})
	require.NoError(t, err)

	// No clusters
	require.Equal(t, 0, len(cfg.Clusters))
	require.Equal(t, config.GRPCStorageType, cfg.Backend)

	interruptCh := make(chan error)
	doneCh := make(chan struct{})
	listenerCh := make(chan net.Listener, 1)
	go func() {
		Run(RunOptions{
			Config:      cfg,
			InterruptCh: interruptCh,
			ListenerCh:  listenerCh,
		})
		doneCh <- struct{}{}
	}()

	// Wait for listener
	listener := <-listenerCh
	addr := listener.Addr().String()

	// Wait for server to come up
	waitForServerHealthy(t, addr)

	// Send Prometheus read request
	promReq := test.GeneratePromReadRequest()
	promReqBody := test.GeneratePromReadRequestBody(t, promReq)
	req, err := http.NewRequestWithContext(
		context.TODO(),
		http.MethodPost,
		fmt.Sprintf("http://%s%s", addr, remote.PromReadURL),
		promReqBody,
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer require.NoError(t, resp.Body.Close())
	assert.Equal(t, qs.reads, 1)

	// Ensure close server performs as expected
	interruptCh <- fmt.Errorf("interrupt")
	<-doneCh
}

func TestPromRemoteBackend(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()
	externalFakePromServer, closeFn := promremotetest.NewServer(t)
	defer closeFn()
	externalPromAddr := externalFakePromServer.HTTPAddr()
	configYAML := fmt.Sprintf(`
listenAddress: 127.0.0.1:0

logging:
  level: info

metrics:
  scope:
    prefix: "coordinator"
  prometheus:
    handlerPath: /metrics
    listenAddress: "127.0.0.1:0"
    onError: stderr
  sanitization: prometheus
  samplingRate: 1.0

prometheusRemoteBackend:
  endpoints: 
  - address: "%s"

backend: prom-remote

tagOptions:
  allowTagNameDuplicates: true
`, externalPromAddr)

	configFile, closeFn := newTestFile(t, "config_backend.yaml", configYAML)
	defer closeFn()

	var cfg config.Configuration
	err := xconfig.LoadFile(&cfg, configFile.Name(), xconfig.Options{})
	require.NoError(t, err)

	require.Equal(t, config.PromRemoteStorageType, cfg.Backend)

	clusterClientCh := make(chan clusterclient.Client, 1)
	clusterClientCh <- clusterclient.NewMockClient(ctrl)
	interruptCh := make(chan error)
	doneCh := make(chan struct{})
	listenerCh := make(chan net.Listener, 1)
	go func() {
		Run(RunOptions{
			Config:        cfg,
			InterruptCh:   interruptCh,
			ListenerCh:    listenerCh,
			ClusterClient: clusterClientCh,
		})
		doneCh <- struct{}{}
	}()

	// Wait for listener
	listener := <-listenerCh
	addr := listener.Addr().String()

	// Wait for server to come up
	waitForServerHealthy(t, addr)

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req, err := http.NewRequestWithContext(
		context.TODO(),
		http.MethodPost,
		fmt.Sprintf("http://%s%s", addr, remote.PromWriteURL),
		promReqBody,
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer require.NoError(t, resp.Body.Close())
	assert.NotNil(t, externalFakePromServer.GetLastRequest())

	// Ensure close server performs as expected
	interruptCh <- fmt.Errorf("interrupt")
	<-doneCh
}

func testWrite(t *testing.T, cfg config.Configuration, ctrl *gomock.Controller) {
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
	session.EXPECT().Close().AnyTimes()

	dbClient := client.NewMockClient(ctrl)
	dbClient.EXPECT().DefaultSession().Return(session, nil).AnyTimes()

	cfg.Clusters[0].NewClientFromConfig = m3.NewClientFromConfig(
		func(
			cfg client.Configuration,
			params client.ConfigurationParameters,
			custom ...client.CustomAdminOption,
		) (client.Client, error) {
			return dbClient, nil
		})

	interruptCh := make(chan error, 1)
	doneCh := make(chan struct{}, 1)
	listenerCh := make(chan net.Listener, 1)

	rulesNamespacesValue := kv.NewMockValue(ctrl)
	rulesNamespacesValue.EXPECT().Version().Return(0).AnyTimes()
	rulesNamespacesValue.EXPECT().Unmarshal(gomock.Any()).DoAndReturn(func(v proto.Message) error {
		msg := v.(*rulepb.Namespaces)
		*msg = rulepb.Namespaces{}
		return nil
	})
	rulesNamespacesWatchable := kv.NewValueWatchable()
	rulesNamespacesWatchable.Update(rulesNamespacesValue)
	_, rulesNamespacesWatch, err := rulesNamespacesWatchable.Watch()
	require.NoError(t, err)
	kvClient := kv.NewMockStore(ctrl)
	kvClient.EXPECT().Watch(gomock.Any()).Return(rulesNamespacesWatch, nil).AnyTimes()
	clusterClient := clusterclient.NewMockClient(ctrl)
	clusterClient.EXPECT().KV().Return(kvClient, nil).AnyTimes()
	clusterClientCh := make(chan clusterclient.Client, 1)
	clusterClientCh <- clusterClient

	downsamplerReadyCh := make(chan struct{}, 1)

	go func() {
		Run(RunOptions{
			Config:             cfg,
			InterruptCh:        interruptCh,
			ListenerCh:         listenerCh,
			ClusterClient:      clusterClientCh,
			DownsamplerReadyCh: downsamplerReadyCh,
		})
		doneCh <- struct{}{}
	}()

	// Wait for downsampler to be ready.
	<-downsamplerReadyCh

	// Wait for listener
	listener := <-listenerCh
	addr := listener.Addr().String()

	// Wait for server to come up
	waitForServerHealthy(t, addr)

	// Send Prometheus write request
	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("http://%s%s", addr, remote.PromWriteURL), promReqBody)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	// Ensure close server performs as expected
	interruptCh <- fmt.Errorf("interrupt")
	<-doneCh
}

// testIngest will test an M3Msg being ingested by the coordinator, it also
// makes sure that the tag options is correctly propagated from the config
// all the way to the M3Msg ingester and when written to the DB will include
// the correctly formed ID.
func testIngest(t *testing.T, cfg config.Configuration, ctrl *gomock.Controller) {
	// Override the client creation
	require.Equal(t, 1, len(cfg.Clusters))

	numWrites := atomic.NewInt32(0)

	session := client.NewMockSession(ctrl)
	session.EXPECT().
		WriteTagged(ident.NewIDMatcher("prometheus_metrics_1m_aggregated"),
			ident.NewIDMatcher(`{_new="first",biz="baz",foo="bar"}`),
			gomock.Any(),
			gomock.Any(),
			42.0,
			gomock.Any(),
			nil).
		Do(func(_, _, _, _, _, _, _ interface{}) {
			numWrites.Add(1)
		})
	session.EXPECT().Close().AnyTimes()

	dbClient := client.NewMockClient(ctrl)
	dbClient.EXPECT().DefaultSession().Return(session, nil).AnyTimes()

	cfg.Clusters[0].NewClientFromConfig = m3.NewClientFromConfig(
		func(
			cfg client.Configuration,
			params client.ConfigurationParameters,
			custom ...client.CustomAdminOption,
		) (client.Client, error) {
			return dbClient, nil
		})

	interruptCh := make(chan error, 1)
	doneCh := make(chan struct{}, 1)
	listenerCh := make(chan net.Listener, 1)
	m3msgListenerCh := make(chan net.Listener, 1)

	rulesNamespacesValue := kv.NewMockValue(ctrl)
	rulesNamespacesValue.EXPECT().Version().Return(0).AnyTimes()
	rulesNamespacesValue.EXPECT().Unmarshal(gomock.Any()).DoAndReturn(func(v proto.Message) error {
		msg := v.(*rulepb.Namespaces)
		*msg = rulepb.Namespaces{}
		return nil
	})
	rulesNamespacesWatchable := kv.NewValueWatchable()
	rulesNamespacesWatchable.Update(rulesNamespacesValue)
	_, rulesNamespacesWatch, err := rulesNamespacesWatchable.Watch()
	require.NoError(t, err)
	kvClient := kv.NewMockStore(ctrl)
	kvClient.EXPECT().Watch(gomock.Any()).Return(rulesNamespacesWatch, nil).AnyTimes()
	clusterClient := clusterclient.NewMockClient(ctrl)
	clusterClient.EXPECT().KV().Return(kvClient, nil).AnyTimes()
	clusterClientCh := make(chan clusterclient.Client, 1)
	clusterClientCh <- clusterClient

	downsamplerReadyCh := make(chan struct{}, 1)

	go func() {
		Run(RunOptions{
			Config:             cfg,
			InterruptCh:        interruptCh,
			ListenerCh:         listenerCh,
			M3MsgListenerCh:    m3msgListenerCh,
			ClusterClient:      clusterClientCh,
			DownsamplerReadyCh: downsamplerReadyCh,
		})
		doneCh <- struct{}{}
	}()

	// Wait for downsampler to be ready.
	<-downsamplerReadyCh

	// Wait for listener
	listener := <-listenerCh
	addr := listener.Addr().String()

	// Wait for server to come up
	waitForServerHealthy(t, addr)

	// Send ingest message.
	tagEncoderPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(), nil)
	tagEncoderPool.Init()
	tagEncoder := tagEncoderPool.Get()
	err = tagEncoder.Encode(ident.MustNewTagStringsIterator(
		"_new", "first",
		"biz", "baz",
		"foo", "bar"))
	require.NoError(t, err)
	id, ok := tagEncoder.Data()
	require.True(t, ok)
	sp, err := policy.MustParseStoragePolicy("1m:120h").Proto()
	require.NoError(t, err)

	// Copy message.
	message, err := proto.Marshal(&metricpb.AggregatedMetric{
		Metric: metricpb.TimedMetricWithStoragePolicy{
			TimedMetric: metricpb.TimedMetric{
				Type:      metricpb.MetricType_GAUGE,
				Id:        id.Bytes(),
				TimeNanos: time.Now().UnixNano(),
				Value:     42,
			},
			StoragePolicy: *sp,
		},
	})
	require.NoError(t, err)

	// Encode as m3msg protobuf message.
	encoder := m3msgproto.NewEncoder(m3msgproto.NewOptions())
	err = encoder.Encode(&msgpb.Message{
		Value: message,
	})
	require.NoError(t, err)
	m3msgListener := <-m3msgListenerCh
	conn, err := net.Dial("tcp", m3msgListener.Addr().String())
	require.NoError(t, err)
	_, err = conn.Write(encoder.Bytes())
	require.NoError(t, err)

	// Now wait for write.
	xclock.WaitUntil(func() bool {
		return numWrites.Load() == 1
	}, 30*time.Second)

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

func waitForServerHealthy(t *testing.T, addr string) {
	maxWait := 10 * time.Second
	startAt := time.Now()
	for time.Since(startAt) < maxWait {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/health", addr), nil)
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

var _ rpc.QueryServer = &queryServer{}

type queryServer struct {
	up                            time.Time
	reads, searches, tagCompletes int
	mu                            sync.Mutex
}

func newQueryServer() *queryServer {
	return &queryServer{up: time.Now()}
}

func (s *queryServer) Health(
	ctx context.Context,
	req *rpc.HealthRequest,
) (*rpc.HealthResponse, error) {
	up := time.Since(s.up)
	return &rpc.HealthResponse{
		UptimeDuration:    up.String(),
		UptimeNanoseconds: int64(up),
	}, nil
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
