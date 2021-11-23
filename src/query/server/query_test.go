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

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
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
)

var configYAML = `
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

	cfg := configFromYAML(t, configYAML)

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

	cfg.Clusters[0].NewClientFromConfig = func(
		cfg client.Configuration,
		params client.ConfigurationParameters,
		custom ...client.CustomAdminOption,
	) (client.Client, error) {
		return dbClient, nil
	}

	downsamplerReadyCh := make(chan struct{}, 1)
	resultCh := make(chan RunResult, 1)
	opts := runServerOpts{cfg: cfg, ctrl: ctrl, downsamplerReadyCh: downsamplerReadyCh, runResultCh: resultCh}
	_, stopServer := runServer(t, opts)
	defer func() {
		stopServer()
		result := <-resultCh
		assert.True(t, result.MultiProcessRun)
		assert.False(t, result.MultiProcessIsParentCleanExit)
	}()

	r, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", metricsPort)) //nolint
	require.NoError(t, err)
	defer r.Body.Close()
	bodyBytes, err := ioutil.ReadAll(r.Body)
	require.NoError(t, err)
	metricsResponse := string(bodyBytes)
	assert.Contains(t, metricsResponse, "coordinator_runtime_memory_allocated{multiprocess_id=\"1\"}")
	assert.Contains(t, metricsResponse, "coordinator_ingest_success{multiprocess_id=\"1\"}")
}

func TestWriteH1(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	cfg := configFromYAML(t, configYAML)

	testWrite(t, cfg, ctrl)
}

func TestWriteH2C(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	cfg := configFromYAML(t, configYAML)

	cfg.HTTP.EnableH2C = true

	testWrite(t, cfg, ctrl)
}

func TestIngestH1(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	cfg := configFromYAML(t, configYAML)

	testIngest(t, cfg, ctrl)
}

func TestIngestH2C(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	cfg := configFromYAML(t, configYAML)

	cfg.HTTP.EnableH2C = true

	testIngest(t, cfg, ctrl)
}

func TestPromRemoteBackend(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	externalFakePromServer := promremotetest.NewServer(t)
	defer externalFakePromServer.Close()

	cfg := configFromYAML(t, fmt.Sprintf(`
prometheusRemoteBackend:
  endpoints: 
  - name: defaultEndpointForTests
    address: "%s"

backend: prom-remote

tagOptions:
  allowTagNameDuplicates: true
`, externalFakePromServer.WriteAddr()))

	require.Equal(t, config.PromRemoteStorageType, cfg.Backend)

	addr, stopServer := runServer(t, runServerOpts{cfg: cfg, ctrl: ctrl})
	defer stopServer()

	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	requestURL := fmt.Sprintf("http://%s%s", addr, remote.PromWriteURL)
	newRequest := func() *http.Request {
		req, err := http.NewRequestWithContext(
			context.TODO(),
			http.MethodPost,
			requestURL,
			promReqBody,
		)
		require.NoError(t, err)
		return req
	}

	t.Run("write request", func(t *testing.T) {
		defer externalFakePromServer.Reset()
		resp, err := http.DefaultClient.Do(newRequest())
		require.NoError(t, err)

		assert.NotNil(t, externalFakePromServer.GetLastWriteRequest())
		require.NoError(t, resp.Body.Close())
	})

	t.Run("bad request propagates", func(t *testing.T) {
		defer externalFakePromServer.Reset()
		externalFakePromServer.SetError("badRequest", http.StatusBadRequest)

		resp, err := http.DefaultClient.Do(newRequest())
		require.NoError(t, err)

		assert.Equal(t, 400, resp.StatusCode)
		require.NoError(t, resp.Body.Close())
	})
}

func TestGRPCBackend(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcAddr := lis.Addr().String()
	cfg := configFromYAML(t, fmt.Sprintf(`
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
`, grpcAddr))

	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	s := grpc.NewServer()
	defer s.GracefulStop()
	qs := newQueryServer()
	rpc.RegisterQueryServer(s, qs)
	go func() {
		_ = s.Serve(lis)
	}()

	// No clusters
	require.Equal(t, 0, len(cfg.Clusters))
	require.Equal(t, config.GRPCStorageType, cfg.Backend)

	addr, stopServer := runServer(t, runServerOpts{cfg: cfg, ctrl: ctrl})
	defer stopServer()

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
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, qs.reads, 1)
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

	cfg.Clusters[0].NewClientFromConfig = func(
		cfg client.Configuration,
		params client.ConfigurationParameters,
		custom ...client.CustomAdminOption,
	) (client.Client, error) {
		return dbClient, nil
	}

	downsamplerReadyCh := make(chan struct{}, 1)
	addr, stopServer := runServer(t, runServerOpts{cfg: cfg, ctrl: ctrl, downsamplerReadyCh: downsamplerReadyCh})
	defer stopServer()

	// Send Prometheus write request
	promReq := test.GeneratePromWriteRequest()
	promReqBody := test.GeneratePromWriteRequestBody(t, promReq)
	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("http://%s%s", addr, remote.PromWriteURL), promReqBody)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
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

	var (
		m3msgListenerCh    = make(chan net.Listener, 1)
		downsamplerReadyCh = make(chan struct{}, 1)
		runOpts            = runServerOpts{
			cfg:                cfg,
			ctrl:               ctrl,
			downsamplerReadyCh: downsamplerReadyCh,
			m3msgListenerCh:    m3msgListenerCh,
		}
	)

	_, stopServer := runServer(t, runOpts)
	defer stopServer()

	// Send ingest message.
	tagEncoderPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(), nil)
	tagEncoderPool.Init()
	tagEncoder := tagEncoderPool.Get()
	err := tagEncoder.Encode(ident.MustNewTagStringsIterator(
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

}

func TestCreateEnginesWithResolutionBasedLookbacks(t *testing.T) {
	var (
		defaultLookback      = 10 * time.Minute
		resolutionMultiplier = 2
		clusters             = m3.ClustersStaticConfiguration{
			{
				Namespaces: []m3.ClusterStaticNamespaceConfiguration{
					{Resolution: 5 * time.Minute},
					{Resolution: 10 * time.Minute},
				},
			},
			{
				Namespaces: []m3.ClusterStaticNamespaceConfiguration{
					{Resolution: 5 * time.Minute},
					{Resolution: 15 * time.Minute},
				},
			},
		}
		newEngineFn = func(lookback time.Duration) (*promql.Engine, error) {
			return promql.NewEngine(promql.EngineOpts{}), nil
		}

		expecteds = []time.Duration{defaultLookback, 20 * time.Minute, 30 * time.Minute}
	)
	defaultEngine, err := newEngineFn(defaultLookback)
	require.NoError(t, err)

	enginesByLookback, err := createEnginesWithResolutionBasedLookbacks(
		defaultLookback,
		defaultEngine,
		clusters,
		resolutionMultiplier,
		newEngineFn,
	)
	require.NoError(t, err)

	engine, ok := enginesByLookback[defaultLookback]
	require.True(t, ok)
	assert.Equal(t, defaultEngine, engine)

	for _, expected := range expecteds {
		engine, ok = enginesByLookback[expected]
		require.True(t, ok)
		assert.NotNil(t, engine)
	}
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

func configFromYAML(t *testing.T, partYAML string) config.Configuration {
	cfgYAML := fmt.Sprintf(`
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

%s
`, partYAML)

	configFile, closeFile := newTestFile(t, "config_backend.yaml", cfgYAML)
	defer closeFile()
	var cfg config.Configuration
	err := xconfig.LoadFile(&cfg, configFile.Name(), xconfig.Options{})
	require.NoError(t, err)
	return cfg
}

type runServerOpts struct {
	cfg                config.Configuration
	ctrl               *gomock.Controller
	downsamplerReadyCh chan struct{}
	m3msgListenerCh    chan net.Listener
	runResultCh        chan RunResult
}

func runServer(t *testing.T, opts runServerOpts) (string, closeFn) {
	var (
		interruptCh     = make(chan error)
		doneCh          = make(chan struct{})
		listenerCh      = make(chan net.Listener, 1)
		clusterClient   = clusterclient.NewMockClient(opts.ctrl)
		clusterClientCh chan clusterclient.Client
	)

	if len(opts.cfg.Clusters) > 0 || opts.cfg.ClusterManagement.Etcd != nil {
		clusterClientCh = make(chan clusterclient.Client, 1)
		store := mem.NewStore()
		clusterClient.EXPECT().KV().Return(store, nil).MaxTimes(2)
		clusterClientCh <- clusterClient
	}

	go func() {
		r := Run(RunOptions{
			Config:             opts.cfg,
			InterruptCh:        interruptCh,
			ListenerCh:         listenerCh,
			ClusterClient:      clusterClientCh,
			DownsamplerReadyCh: opts.downsamplerReadyCh,
			M3MsgListenerCh:    opts.m3msgListenerCh,
		})
		doneCh <- struct{}{}
		if opts.runResultCh != nil {
			opts.runResultCh <- r
		}
	}()

	if opts.downsamplerReadyCh != nil {
		// Wait for downsampler to be ready.
		<-opts.downsamplerReadyCh
	}

	// Wait for listener
	listener := <-listenerCh
	addr := listener.Addr().String()

	// Wait for server to come up
	waitForServerHealthy(t, addr)

	return addr, func() {
		// Ensure close server performs as expected
		interruptCh <- fmt.Errorf("interrupt")
		<-doneCh
	}
}

func waitForServerHealthy(t *testing.T, addr string) {
	maxWait := 10 * time.Second
	startAt := time.Now()
	for time.Since(startAt) < maxWait {
		req, err := http.NewRequestWithContext(context.TODO(), "GET", fmt.Sprintf("http://%s/health", addr), nil)
		require.NoError(t, err)
		res, err := http.DefaultClient.Do(req)
		if res != nil {
			require.NoError(t, res.Body.Close())
		}
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
