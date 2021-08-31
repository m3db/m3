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
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/m3db/m3/src/aggregator/server"
	clusterclient "github.com/m3db/m3/src/cluster/client"
	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/kv"
	handleroptions3 "github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/serve"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	ingestcarbon "github.com/m3db/m3/src/cmd/services/m3coordinator/ingest/carbon"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/httpd"
	"github.com/m3db/m3/src/query/api/v1/options"
	m3dbcluster "github.com/m3db/m3/src/query/cluster/m3db"
	"github.com/m3db/m3/src/query/executor"
	graphite "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/policy/filter"
	"github.com/m3db/m3/src/query/pools"
	tsdbremote "github.com/m3db/m3/src/query/remote"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/fanout"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/remote"
	"github.com/m3db/m3/src/query/stores/m3db"
	"github.com/m3db/m3/src/x/clock"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	xnet "github.com/m3db/m3/src/x/net"
	xos "github.com/m3db/m3/src/x/os"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xserver "github.com/m3db/m3/src/x/server"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/go-kit/kit/log"
	kitlogzap "github.com/go-kit/kit/log/zap"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	extprom "github.com/prometheus/client_golang/prometheus"
	prometheuspromql "github.com/prometheus/prometheus/promql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	serviceName        = "m3query"
	cpuProfileDuration = 5 * time.Second
)

var (
	defaultCarbonIngesterWorkerPoolSize = 1024
	defaultPerCPUMultiProcess           = 0.5
)

type cleanupFn func() error

// RunOptions provides options for running the server
// with backwards compatibility if only solely adding fields.
type RunOptions struct {
	// Config is an alternate way to provide configuration and will be used
	// instead of parsing ConfigFile if ConfigFile is not specified.
	Config config.Configuration

	// DBConfig is the local M3DB config when running embedded.
	DBConfig *dbconfig.DBConfiguration

	// DBClient is the local M3DB client when running embedded.
	DBClient <-chan client.Client

	// ClusterClient is the local M3DB cluster client when running embedded.
	ClusterClient <-chan clusterclient.Client

	// InterruptCh is a programmatic interrupt channel to supply to
	// interrupt and shutdown the server.
	InterruptCh <-chan error

	// ShutdownCh is an optional channel to supply if interested in receiving
	// a notification that the server has shutdown.
	ShutdownCh chan<- struct{}

	// ListenerCh is a programmatic channel to receive the server listener
	// on once it has opened.
	ListenerCh chan<- net.Listener

	// M3MsgListenerCh is a programmatic channel to receive the M3Msg server
	// listener on once it has opened.
	M3MsgListenerCh chan<- net.Listener

	// DownsamplerReadyCh is a programmatic channel to receive the downsampler
	// ready signal once it is open.
	DownsamplerReadyCh chan<- struct{}

	// InstrumentOptionsReadyCh is a programmatic channel to receive a set of
	// instrument options and metric reporters that is delivered when
	// constructed.
	InstrumentOptionsReadyCh chan<- InstrumentOptionsReady

	// ClockOptions is an optional clock to use instead of the default one.
	ClockOptions clock.Options

	// CustomHandlerOptions contains custom handler options.
	CustomHandlerOptions options.CustomHandlerOptions

	// CustomPromQLParseFunction is a custom PromQL parsing function.
	CustomPromQLParseFunction promql.ParseFn

	// ApplyCustomTSDBOptions is a transform that allows for custom tsdb options.
	ApplyCustomTSDBOptions CustomTSDBOptionsFn

	// BackendStorageTransform is a custom backend storage transform.
	BackendStorageTransform BackendStorageTransform

	// AggregatorServerOptions are server options for aggregator.
	AggregatorServerOptions []server.AdminOption

	// CustomBuildTags are additional tags to be added to the instrument build
	// reporter.
	CustomBuildTags map[string]string

	// ApplyCustomRuleStore provides an option to swap the backend used for the rule stores.
	ApplyCustomRuleStore downsample.CustomRuleStoreFn
}

// InstrumentOptionsReady is a set of instrument options
// and metric reporters that is delivered when constructed.
type InstrumentOptionsReady struct {
	InstrumentOptions instrument.Options
	MetricsReporters  instrument.MetricsConfigurationReporters
}

// CustomTSDBOptionsFn is a transformation function for TSDB Options.
type CustomTSDBOptionsFn func(m3.Options) m3.Options

// BackendStorageTransform is a transformation function for backend storage.
type BackendStorageTransform func(
	storage.Storage,
	m3.Options,
	instrument.Options,
) storage.Storage

// RunResult returns metadata about the process run.
type RunResult struct {
	MultiProcessRun               bool
	MultiProcessIsParentCleanExit bool
}

// Run runs the server programmatically given a filename for the configuration file.
func Run(runOpts RunOptions) RunResult {
	rand.Seed(time.Now().UnixNano())

	var (
		cfg          = runOpts.Config
		listenerOpts = xnet.NewListenerOptions()
		runResult    RunResult
	)

	logger, err := cfg.LoggingOrDefault().BuildLogger()
	if err != nil {
		// NB(r): Use fmt.Fprintf(os.Stderr, ...) to avoid etcd.SetGlobals()
		// sending stdlib "log" to black hole. Don't remove unless with good reason.
		fmt.Fprintf(os.Stderr, "unable to create logger: %v", err)
		os.Exit(1)
	}

	// NB(nate): Register shutdown notification defer function first so that
	// it's the last defer to fire before terminating. This allows other defer methods
	// that clean up resources to execute first.
	if runOpts.ShutdownCh != nil {
		defer func() {
			select {
			case runOpts.ShutdownCh <- struct{}{}:
				break
			default:
				logger.Warn("could not send shutdown notification as channel was full")
			}
		}()
	}

	defer logger.Sync()

	cfg.Debug.SetRuntimeValues(logger)

	xconfig.WarnOnDeprecation(cfg, logger)

	var commonLabels map[string]string
	if cfg.MultiProcess.Enabled {
		// Mark as a multi-process run result.
		runResult.MultiProcessRun = true

		// Execute multi-process parent spawn or child setup code path.
		multiProcessRunResult, err := multiProcessRun(cfg, logger, listenerOpts)
		if err != nil {
			logger = logger.With(zap.String("processID", multiProcessProcessID()))
			logger.Fatal("failed to run", zap.Error(err))
		}
		if multiProcessRunResult.isParentCleanExit {
			// Parent process clean exit.
			runResult.MultiProcessIsParentCleanExit = true
			return runResult
		}

		cfg = multiProcessRunResult.cfg
		logger = multiProcessRunResult.logger
		listenerOpts = multiProcessRunResult.listenerOpts
		commonLabels = multiProcessRunResult.commonLabels
	}

	prometheusEngineRegistry := extprom.NewRegistry()
	scope, closer, reporters, err := cfg.MetricsOrDefault().NewRootScopeAndReporters(
		instrument.NewRootScopeAndReportersOptions{
			PrometheusExternalRegistries: []instrument.PrometheusExternalRegistry{
				{
					Registry: prometheusEngineRegistry,
					SubScope: "coordinator_prometheus_engine",
				},
			},
			PrometheusOnError: func(err error) {
				// NB(r): Required otherwise collisions when registering metrics will
				// cause a panic.
				logger.Error("register metric error", zap.Error(err))
			},
			CommonLabels: commonLabels,
		})
	if err != nil {
		logger.Fatal("could not connect to metrics", zap.Error(err))
	}

	tracer, traceCloser, err := cfg.Tracing.NewTracer(serviceName, scope, logger)
	if err != nil {
		logger.Fatal("could not initialize tracing", zap.Error(err))
	}

	defer traceCloser.Close()

	if _, ok := tracer.(opentracing.NoopTracer); ok {
		logger.Info("tracing disabled for m3query; set `tracing.backend` to enable")
	}

	opentracing.SetGlobalTracer(tracer)

	clockOpts := clock.NewOptions()
	if runOpts.ClockOptions != nil {
		clockOpts = runOpts.ClockOptions
	}

	instrumentOptions := instrument.NewOptions().
		SetMetricsScope(scope).
		SetLogger(logger).
		SetTracer(tracer).
		SetCustomBuildTags(runOpts.CustomBuildTags)

	if runOpts.InstrumentOptionsReadyCh != nil {
		runOpts.InstrumentOptionsReadyCh <- InstrumentOptionsReady{
			InstrumentOptions: instrumentOptions,
			MetricsReporters:  reporters,
		}
	}

	// Close metrics scope
	defer func() {
		logger.Info("closing metrics scope")
		if err := closer.Close(); err != nil {
			logger.Error("unable to close metrics scope", zap.Error(err))
		}
	}()

	buildReporter := instrument.NewBuildReporter(instrumentOptions)
	if err := buildReporter.Start(); err != nil {
		logger.Fatal("could not start build reporter", zap.Error(err))
	}

	defer buildReporter.Stop()

	storageRestrictByTags, _, err := cfg.Query.RestrictTagsAsStorageRestrictByTag()
	if err != nil {
		logger.Fatal("could not parse query restrict tags config", zap.Error(err))
	}

	timeout := cfg.Query.TimeoutOrDefault()
	if runOpts.DBConfig != nil &&
		runOpts.DBConfig.Client.FetchTimeout != nil &&
		*runOpts.DBConfig.Client.FetchTimeout > timeout {
		timeout = *runOpts.DBConfig.Client.FetchTimeout
	}

	fetchOptsBuilderLimitsOpts := cfg.Limits.PerQuery.AsFetchOptionsBuilderLimitsOptions()
	fetchOptsBuilder, err := handleroptions.NewFetchOptionsBuilder(
		handleroptions.FetchOptionsBuilderOptions{
			Limits:        fetchOptsBuilderLimitsOpts,
			RestrictByTag: storageRestrictByTags,
			Timeout:       timeout,
		})
	if err != nil {
		logger.Fatal("could not set fetch options parser", zap.Error(err))
	}

	var (
		clusterNamespacesWatcher m3.ClusterNamespacesWatcher
		backendStorage           storage.Storage
		clusterClient            clusterclient.Client
		downsampler              downsample.Downsampler
		queryCtxOpts             = models.QueryContextOptions{
			LimitMaxTimeseries:             fetchOptsBuilderLimitsOpts.SeriesLimit,
			LimitMaxDocs:                   fetchOptsBuilderLimitsOpts.DocsLimit,
			LimitMaxReturnedSeries:         fetchOptsBuilderLimitsOpts.ReturnedSeriesLimit,
			LimitMaxReturnedDatapoints:     fetchOptsBuilderLimitsOpts.ReturnedDatapointsLimit,
			LimitMaxReturnedSeriesMetadata: fetchOptsBuilderLimitsOpts.ReturnedSeriesMetadataLimit,
			RequireExhaustive:              fetchOptsBuilderLimitsOpts.RequireExhaustive,
		}

		matchOptions = consolidators.MatchOptions{
			MatchType: cfg.Query.ConsolidationConfiguration.MatchType,
		}
	)

	tagOptions, err := config.TagOptionsFromConfig(cfg.TagOptions)
	if err != nil {
		logger.Fatal("could not create tag options", zap.Error(err))
	}

	lookbackDuration, err := cfg.LookbackDurationOrDefault()
	if err != nil {
		logger.Fatal("error validating LookbackDuration", zap.Error(err))
	}
	cfg.LookbackDuration = &lookbackDuration

	readWorkerPool, writeWorkerPool, err := pools.BuildWorkerPools(
		instrumentOptions,
		cfg.ReadWorkerPool,
		cfg.WriteWorkerPoolOrDefault(),
		scope)
	if err != nil {
		logger.Fatal("could not create worker pools", zap.Error(err))
	}

	var (
		m3dbClusters    m3.Clusters
		m3dbPoolWrapper *pools.PoolWrapper
	)

	tsdbOpts := m3.NewOptions().
		SetTagOptions(tagOptions).
		SetLookbackDuration(lookbackDuration).
		SetConsolidationFunc(consolidators.TakeLast).
		SetReadWorkerPool(readWorkerPool).
		SetWriteWorkerPool(writeWorkerPool).
		SetSeriesConsolidationMatchOptions(matchOptions)

	if runOpts.ApplyCustomTSDBOptions != nil {
		tsdbOpts = runOpts.ApplyCustomTSDBOptions(tsdbOpts)
	}

	serveOptions := serve.NewOptions(instrumentOptions)
	for i, transform := range runOpts.AggregatorServerOptions {
		if opts, err := transform(serveOptions); err != nil {
			logger.Fatal("could not apply transform",
				zap.Int("index", i), zap.Error(err))
		} else {
			serveOptions = opts
		}
	}

	rwOpts := serveOptions.RWOptions()
	switch cfg.Backend {
	case config.GRPCStorageType:
		// For grpc backend, we need to setup only the grpc client and a storage
		// accompanying that client.
		poolWrapper := pools.NewPoolsWrapper(
			pools.BuildIteratorPools(pools.BuildIteratorPoolsOptions{}))
		remoteOpts := config.RemoteOptionsFromConfig(cfg.RPC)
		remotes, enabled, err := remoteClient(poolWrapper, remoteOpts,
			tsdbOpts, instrumentOptions)
		if err != nil {
			logger.Fatal("unable to setup grpc backend", zap.Error(err))
		}
		if !enabled {
			logger.Fatal("need remote clients for grpc backend")
		}

		var (
			r = filter.AllowAll
			w = filter.AllowAll
			c = filter.CompleteTagsAllowAll
		)

		backendStorage = fanout.NewStorage(remotes, r, w, c,
			tagOptions, tsdbOpts, instrumentOptions)
		logger.Info("setup grpc backend")

	case config.NoopEtcdStorageType:
		backendStorage = storage.NewNoopStorage()
		etcd := cfg.ClusterManagement.Etcd

		if etcd == nil || len(etcd.ETCDClusters) == 0 {
			logger.Fatal("must specify cluster management config and at least one etcd cluster")
		}

		opts := etcd.NewOptions()
		clusterClient, err = etcdclient.NewConfigServiceClient(opts)
		if err != nil {
			logger.Fatal("error constructing etcd client", zap.Error(err))
		}
		logger.Info("setup noop storage backend with etcd")

	// Empty backend defaults to M3DB.
	case "":
		// For m3db backend, we need to make connections to the m3db cluster
		// which generates a session and use the storage with the session.
		m3dbClusters, clusterNamespacesWatcher, m3dbPoolWrapper, err = initClusters(cfg,
			runOpts.DBConfig, runOpts.DBClient, instrumentOptions, tsdbOpts.CustomAdminOptions())
		if err != nil {
			logger.Fatal("unable to init clusters", zap.Error(err))
		}

		var cleanup cleanupFn
		backendStorage, clusterClient, downsampler, cleanup, err = newM3DBStorage(
			cfg, m3dbClusters, m3dbPoolWrapper,
			runOpts, queryCtxOpts, tsdbOpts,
			runOpts.DownsamplerReadyCh, clusterNamespacesWatcher, rwOpts, clockOpts, instrumentOptions)

		if err != nil {
			logger.Fatal("unable to setup m3db backend", zap.Error(err))
		}
		defer cleanup()

	default:
		logger.Fatal("unrecognized backend", zap.String("backend", string(cfg.Backend)))
	}

	if fn := runOpts.BackendStorageTransform; fn != nil {
		backendStorage = fn(backendStorage, tsdbOpts, instrumentOptions)
	}

	engineOpts := executor.NewEngineOptions().
		SetStore(backendStorage).
		SetLookbackDuration(*cfg.LookbackDuration).
		SetInstrumentOptions(instrumentOptions.
			SetMetricsScope(instrumentOptions.MetricsScope().SubScope("engine")))
	if fn := runOpts.CustomPromQLParseFunction; fn != nil {
		engineOpts = engineOpts.
			SetParseOptions(engineOpts.ParseOptions().SetParseFn(fn))
	}

	engine := executor.NewEngine(engineOpts)
	downsamplerAndWriter, err := newDownsamplerAndWriter(
		backendStorage,
		downsampler,
		cfg.WriteWorkerPoolOrDefault(),
		instrumentOptions,
	)
	if err != nil {
		logger.Fatal("unable to create new downsampler and writer", zap.Error(err))
	}

	var serviceOptionDefaults []handleroptions3.ServiceOptionsDefault
	if dbCfg := runOpts.DBConfig; dbCfg != nil {
		hostID, err := dbCfg.HostIDOrDefault().Resolve()
		if err != nil {
			logger.Fatal("could not resolve hostID",
				zap.Error(err))
		}

		discoveryCfg := dbCfg.DiscoveryOrDefault()
		envCfg, err := discoveryCfg.EnvironmentConfig(hostID)
		if err != nil {
			logger.Fatal("could not get env config from discovery config",
				zap.Error(err))
		}

		cluster, err := envCfg.Services.SyncCluster()
		if err != nil {
			logger.Fatal("could not resolve embedded db cluster info",
				zap.Error(err))
		}
		if svcCfg := cluster.Service; svcCfg != nil {
			serviceOptionDefaults = append(serviceOptionDefaults,
				handleroptions3.WithDefaultServiceEnvironment(svcCfg.Env))
			serviceOptionDefaults = append(serviceOptionDefaults,
				handleroptions3.WithDefaultServiceZone(svcCfg.Zone))
		}
	}

	var (
		graphiteFindFetchOptsBuilder   = fetchOptsBuilder
		graphiteRenderFetchOptsBuilder = fetchOptsBuilder
		graphiteStorageOpts            graphite.M3WrappedStorageOptions
	)
	if cfg.Carbon != nil {
		graphiteStorageOpts.AggregateNamespacesAllData =
			cfg.Carbon.AggregateNamespacesAllData
		graphiteStorageOpts.ShiftTimeStart = cfg.Carbon.ShiftTimeStart
		graphiteStorageOpts.ShiftTimeEnd = cfg.Carbon.ShiftTimeEnd
		graphiteStorageOpts.ShiftStepsStart = cfg.Carbon.ShiftStepsStart
		graphiteStorageOpts.ShiftStepsEnd = cfg.Carbon.ShiftStepsEnd
		graphiteStorageOpts.ShiftStepsStartWhenAtResolutionBoundary = cfg.Carbon.ShiftStepsStartWhenAtResolutionBoundary
		graphiteStorageOpts.ShiftStepsEndWhenAtResolutionBoundary = cfg.Carbon.ShiftStepsEndWhenAtResolutionBoundary
		graphiteStorageOpts.ShiftStepsEndWhenStartAtResolutionBoundary = cfg.Carbon.ShiftStepsEndWhenStartAtResolutionBoundary
		graphiteStorageOpts.ShiftStepsStartWhenEndAtResolutionBoundary = cfg.Carbon.ShiftStepsStartWhenEndAtResolutionBoundary
		graphiteStorageOpts.RenderPartialStart = cfg.Carbon.RenderPartialStart
		graphiteStorageOpts.RenderPartialEnd = cfg.Carbon.RenderPartialEnd
		graphiteStorageOpts.RenderSeriesAllNaNs = cfg.Carbon.RenderSeriesAllNaNs
		graphiteStorageOpts.CompileEscapeAllNotOnlyQuotes = cfg.Carbon.CompileEscapeAllNotOnlyQuotes

		if limits := cfg.Carbon.LimitsFind; limits != nil {
			fetchOptsBuilderLimitsOpts := limits.PerQuery.AsFetchOptionsBuilderLimitsOptions()
			graphiteFindFetchOptsBuilder, err = handleroptions.NewFetchOptionsBuilder(
				handleroptions.FetchOptionsBuilderOptions{
					Limits:        fetchOptsBuilderLimitsOpts,
					RestrictByTag: storageRestrictByTags,
					Timeout:       timeout,
				})
			if err != nil {
				logger.Fatal("could not set graphite find fetch options parser", zap.Error(err))
			}
		}
		if limits := cfg.Carbon.LimitsRender; limits != nil {
			fetchOptsBuilderLimitsOpts := limits.PerQuery.AsFetchOptionsBuilderLimitsOptions()
			graphiteRenderFetchOptsBuilder, err = handleroptions.NewFetchOptionsBuilder(
				handleroptions.FetchOptionsBuilderOptions{
					Limits:        fetchOptsBuilderLimitsOpts,
					RestrictByTag: storageRestrictByTags,
					Timeout:       timeout,
				})
			if err != nil {
				logger.Fatal("could not set graphite find fetch options parser", zap.Error(err))
			}
		}
	}

	prometheusEngine, err := newPromQLEngine(cfg, prometheusEngineRegistry,
		instrumentOptions)
	if err != nil {
		logger.Fatal("unable to create PromQL engine", zap.Error(err))
	}

	handlerOptions, err := options.NewHandlerOptions(downsamplerAndWriter,
		tagOptions, engine, prometheusEngine, m3dbClusters, clusterClient, cfg,
		runOpts.DBConfig, fetchOptsBuilder, graphiteFindFetchOptsBuilder, graphiteRenderFetchOptsBuilder,
		queryCtxOpts, instrumentOptions, cpuProfileDuration, []string{handleroptions3.M3DBServiceName},
		serviceOptionDefaults, httpd.NewQueryRouter(), httpd.NewQueryRouter(),
		graphiteStorageOpts, tsdbOpts)
	if err != nil {
		logger.Fatal("unable to set up handler options", zap.Error(err))
	}

	if fn := runOpts.CustomHandlerOptions.OptionTransformFn; fn != nil {
		handlerOptions = fn(handlerOptions)
	}

	customHandlers := runOpts.CustomHandlerOptions.CustomHandlers
	handler := httpd.NewHandler(handlerOptions, cfg.Middleware, customHandlers...)
	if err := handler.RegisterRoutes(); err != nil {
		logger.Fatal("unable to register routes", zap.Error(err))
	}

	listenAddress := cfg.ListenAddressOrDefault()
	srvHandler := handler.Router()
	if cfg.HTTP.EnableH2C {
		srvHandler = h2c.NewHandler(handler.Router(), &http2.Server{})
	}
	srv := &http.Server{Addr: listenAddress, Handler: srvHandler}
	defer func() {
		logger.Info("closing server")
		if err := srv.Shutdown(context.Background()); err != nil {
			logger.Error("error closing server", zap.Error(err))
		}
	}()

	listener, err := listenerOpts.Listen("tcp", listenAddress)
	if err != nil {
		logger.Fatal("unable to listen on listen address",
			zap.String("address", listenAddress),
			zap.Error(err))
	}
	if runOpts.ListenerCh != nil {
		runOpts.ListenerCh <- listener
	}
	go func() {
		logger.Info("starting API server", zap.Stringer("address", listener.Addr()))
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server serve error",
				zap.String("address", listenAddress),
				zap.Error(err))
		}
	}()

	if cfg.Ingest != nil {
		logger.Info("starting m3msg server",
			zap.String("address", cfg.Ingest.M3Msg.Server.ListenAddress))
		ingester, err := cfg.Ingest.Ingester.NewIngester(backendStorage,
			tagOptions, instrumentOptions)
		if err != nil {
			logger.Fatal("unable to create ingester", zap.Error(err))
		}

		server, err := cfg.Ingest.M3Msg.NewServer(
			ingester.Ingest, rwOpts,
			instrumentOptions.SetMetricsScope(scope.SubScope("ingest-m3msg")))
		if err != nil {
			logger.Fatal("unable to create m3msg server", zap.Error(err))
		}

		listener, err := listenerOpts.Listen("tcp", cfg.Ingest.M3Msg.Server.ListenAddress)
		if err != nil {
			logger.Fatal("unable to open m3msg server", zap.Error(err))
		}

		if runOpts.M3MsgListenerCh != nil {
			runOpts.M3MsgListenerCh <- listener
		}

		if err := server.Serve(listener); err != nil {
			logger.Fatal("unable to listen on ingest server", zap.Error(err))
		}

		logger.Info("started m3msg server", zap.Stringer("addr", listener.Addr()))
		defer server.Close()
	} else {
		logger.Info("no m3msg server configured")
	}

	if cfg.Carbon != nil && cfg.Carbon.Ingester != nil {
		server := startCarbonIngestion(*cfg.Carbon.Ingester, listenerOpts,
			instrumentOptions, logger, m3dbClusters, clusterNamespacesWatcher,
			downsamplerAndWriter)
		defer server.Close()
	}

	// Wait for process interrupt.
	xos.WaitForInterrupt(logger, xos.InterruptOptions{
		InterruptCh: runOpts.InterruptCh,
	})

	return runResult
}

// make connections to the m3db cluster(s) and generate sessions for those clusters along with the storage
func newM3DBStorage(
	cfg config.Configuration,
	clusters m3.Clusters,
	poolWrapper *pools.PoolWrapper,
	runOpts RunOptions,
	queryContextOptions models.QueryContextOptions,
	tsdbOpts m3.Options,
	downsamplerReadyCh chan<- struct{},
	clusterNamespacesWatcher m3.ClusterNamespacesWatcher,
	rwOpts xio.Options,
	clockOpts clock.Options,
	instrumentOptions instrument.Options,
) (storage.Storage, clusterclient.Client, downsample.Downsampler, cleanupFn, error) {
	var (
		logger              = instrumentOptions.Logger()
		clusterClient       clusterclient.Client
		clusterClientWaitCh <-chan struct{}
	)
	if clusterClientCh := runOpts.ClusterClient; clusterClientCh != nil {
		// Only use a cluster client if we are going to receive one, that
		// way passing nil to httpd NewHandler disables the endpoints entirely
		clusterClientDoneCh := make(chan struct{}, 1)
		clusterClientWaitCh = clusterClientDoneCh
		clusterClient = m3dbcluster.NewAsyncClient(func() (clusterclient.Client, error) {
			return <-clusterClientCh, nil
		}, clusterClientDoneCh)
	} else {
		var etcdCfg *etcdclient.Configuration
		switch {
		case cfg.ClusterManagement.Etcd != nil:
			etcdCfg = cfg.ClusterManagement.Etcd
		case len(cfg.Clusters) == 1 &&
			cfg.Clusters[0].Client.EnvironmentConfig != nil:
			syncCfg, err := cfg.Clusters[0].Client.EnvironmentConfig.Services.SyncCluster()
			if err != nil {
				return nil, nil, nil, nil, errors.Wrap(err, "unable to get etcd sync cluster config")
			}
			etcdCfg = syncCfg.Service
		}

		if etcdCfg != nil {
			// We resolved an etcd configuration for cluster management endpoints
			var (
				clusterSvcClientOpts = etcdCfg.NewOptions()
				err                  error
			)
			clusterClient, err = etcdclient.NewConfigServiceClient(clusterSvcClientOpts)
			if err != nil {
				return nil, nil, nil, nil, errors.Wrap(err, "unable to create cluster management etcd client")
			}
		}
	}

	fanoutStorage, storageCleanup, err := newStorages(clusters, cfg,
		poolWrapper, queryContextOptions, tsdbOpts, instrumentOptions)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "unable to set up storages")
	}

	var (
		namespaces  = clusters.ClusterNamespaces()
		downsampler downsample.Downsampler
	)
	logger.Info("configuring downsampler to use with aggregated cluster namespaces",
		zap.Int("numAggregatedClusterNamespaces", len(namespaces)))

	newDownsamplerFn := func() (downsample.Downsampler, error) {
		ds, err := newDownsampler(
			cfg.Downsample, clusterClient,
			fanoutStorage, clusterNamespacesWatcher,
			tsdbOpts.TagOptions(), clockOpts, instrumentOptions, rwOpts, runOpts.ApplyCustomRuleStore)
		if err != nil {
			return nil, err
		}

		// Notify the downsampler ready channel that
		// the downsampler has now been created and is ready.
		if downsamplerReadyCh != nil {
			downsamplerReadyCh <- struct{}{}
		}

		return ds, nil
	}

	if clusterClientWaitCh != nil {
		// Need to wait before constructing and instead return an async downsampler
		// since the cluster client will return errors until it's initialized itself
		// and will fail constructing the downsampler consequently
		downsampler = downsample.NewAsyncDownsampler(func() (downsample.Downsampler, error) {
			<-clusterClientWaitCh
			return newDownsamplerFn()
		}, nil)
	} else {
		// Otherwise we already have a client and can immediately construct the downsampler
		downsampler, err = newDownsamplerFn()
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	cleanup := func() error {
		lastErr := storageCleanup()
		// Don't want to quit on the first error since the full cleanup is important
		if lastErr != nil {
			logger.Error("error during storage cleanup", zap.Error(lastErr))
		}

		clusterNamespacesWatcher.Close()

		if err := clusters.Close(); err != nil {
			lastErr = errors.Wrap(err, "unable to close M3DB cluster sessions")
			// Make sure the previous error is at least logged
			logger.Error("error during cluster cleanup", zap.Error(err))
		}

		return lastErr
	}

	return fanoutStorage, clusterClient, downsampler, cleanup, nil
}

func newDownsampler(
	cfg downsample.Configuration,
	clusterManagementClient clusterclient.Client,
	storage storage.Storage,
	clusterNamespacesWatcher m3.ClusterNamespacesWatcher,
	tagOptions models.TagOptions,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
	rwOpts xio.Options,
	applyCustomRuleStore downsample.CustomRuleStoreFn,
) (downsample.Downsampler, error) {
	// Namespace the downsampler metrics.
	instrumentOpts = instrumentOpts.SetMetricsScope(
		instrumentOpts.MetricsScope().SubScope("downsampler"))

	if clusterManagementClient == nil {
		return nil, fmt.Errorf("no configured cluster management config, " +
			"must set this config for downsampler")
	}

	var kvStore kv.Store
	var err error

	if applyCustomRuleStore == nil {
		kvStore, err = clusterManagementClient.KV()
		if err != nil {
			return nil, errors.Wrap(err, "unable to create KV store from the "+
				"cluster management config client")
		}
	} else {
		kvStore, err = applyCustomRuleStore(clusterManagementClient, instrumentOpts)
		if err != nil {
			return nil, errors.Wrap(err, "unable to apply custom rule store")
		}
	}

	tagEncoderOptions := serialize.NewTagEncoderOptions()
	tagDecoderOptions := serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{})
	tagEncoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-encoder-pool")))
	tagDecoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-decoder-pool")))
	metricsAppenderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("metrics-appender-pool")))

	downsampler, err := cfg.NewDownsampler(downsample.DownsamplerOptions{
		Storage:                    storage,
		ClusterClient:              clusterManagementClient,
		RulesKVStore:               kvStore,
		ClusterNamespacesWatcher:   clusterNamespacesWatcher,
		ClockOptions:               clockOpts,
		InstrumentOptions:          instrumentOpts,
		TagEncoderOptions:          tagEncoderOptions,
		TagDecoderOptions:          tagDecoderOptions,
		TagEncoderPoolOptions:      tagEncoderPoolOptions,
		TagDecoderPoolOptions:      tagDecoderPoolOptions,
		TagOptions:                 tagOptions,
		MetricsAppenderPoolOptions: metricsAppenderPoolOptions,
		RWOptions:                  rwOpts,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create downsampler: %v", err)
	}

	return downsampler, nil
}

func initClusters(
	cfg config.Configuration,
	dbCfg *dbconfig.DBConfiguration,
	dbClientCh <-chan client.Client,
	instrumentOpts instrument.Options,
	customAdminOptions []client.CustomAdminOption,
) (m3.Clusters, m3.ClusterNamespacesWatcher, *pools.PoolWrapper, error) {
	instrumentOpts = instrumentOpts.
		SetMetricsScope(instrumentOpts.MetricsScope().SubScope("m3db-client"))

	var (
		logger                   = instrumentOpts.Logger()
		clusterNamespacesWatcher = m3.NewClusterNamespacesWatcher()
		clusters                 m3.Clusters
		poolWrapper              *pools.PoolWrapper
		staticNamespaceConfig    bool
		err                      error
	)
	if len(cfg.Clusters) > 0 {
		for _, clusterCfg := range cfg.Clusters {
			if len(clusterCfg.Namespaces) > 0 {
				staticNamespaceConfig = true
				break
			}
		}
		opts := m3.ClustersStaticConfigurationOptions{
			AsyncSessions:      true,
			CustomAdminOptions: customAdminOptions,
		}
		if staticNamespaceConfig {
			clusters, err = cfg.Clusters.NewStaticClusters(instrumentOpts, opts, clusterNamespacesWatcher)
		} else {
			// No namespaces defined in config -- pull namespace data from etcd.
			clusters, err = cfg.Clusters.NewDynamicClusters(instrumentOpts, opts, clusterNamespacesWatcher)
		}
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "unable to connect to clusters")
		}

		poolWrapper = pools.NewPoolsWrapper(
			pools.BuildIteratorPools(pools.BuildIteratorPoolsOptions{}))
	} else {
		localCfg := cfg.Local
		if localCfg == nil {
			localCfg = &config.LocalConfiguration{}
		}
		if len(localCfg.Namespaces) > 0 {
			staticNamespaceConfig = true
		}

		if dbClientCh == nil {
			return nil, nil, nil, errors.New("no clusters configured and not running local cluster")
		}

		sessionInitChan := make(chan struct{})
		session := m3db.NewAsyncSession(func() (client.Client, error) {
			return <-dbClientCh, nil
		}, sessionInitChan)

		clusterStaticConfig := m3.ClusterStaticConfiguration{
			Namespaces: localCfg.Namespaces,
		}
		if !staticNamespaceConfig {
			if dbCfg == nil {
				return nil, nil, nil, errors.New("environment config required when dynamically fetching namespaces")
			}

			hostID, err := dbCfg.HostIDOrDefault().Resolve()
			if err != nil {
				logger.Fatal("could not resolve hostID",
					zap.Error(err))
			}

			discoveryCfg := dbCfg.DiscoveryOrDefault()
			envCfg, err := discoveryCfg.EnvironmentConfig(hostID)
			if err != nil {
				logger.Fatal("could not get env config from discovery config",
					zap.Error(err))
			}

			clusterStaticConfig.Client = client.Configuration{EnvironmentConfig: &envCfg}
		}

		clustersCfg := m3.ClustersStaticConfiguration{clusterStaticConfig}
		cfgOptions := m3.ClustersStaticConfigurationOptions{
			ProvidedSession:    session,
			CustomAdminOptions: customAdminOptions,
		}

		if staticNamespaceConfig {
			clusters, err = clustersCfg.NewStaticClusters(instrumentOpts, cfgOptions, clusterNamespacesWatcher)
		} else {
			clusters, err = clustersCfg.NewDynamicClusters(instrumentOpts, cfgOptions, clusterNamespacesWatcher)
		}
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "unable to connect to clusters")
		}

		poolWrapper = pools.NewAsyncPoolsWrapper()
		go func() {
			<-sessionInitChan
			poolWrapper.Init(session.IteratorPools())
		}()
	}

	for _, namespace := range clusters.ClusterNamespaces() {
		logger.Info("resolved cluster namespace",
			zap.String("namespace", namespace.NamespaceID().String()))
	}

	return clusters, clusterNamespacesWatcher, poolWrapper, nil
}

func newStorages(
	clusters m3.Clusters,
	cfg config.Configuration,
	poolWrapper *pools.PoolWrapper,
	queryContextOptions models.QueryContextOptions,
	opts m3.Options,
	instrumentOpts instrument.Options,
) (storage.Storage, cleanupFn, error) {
	var (
		logger  = instrumentOpts.Logger()
		cleanup = func() error { return nil }
	)

	localStorage, err := m3.NewStorage(clusters, opts, instrumentOpts)
	if err != nil {
		return nil, nil, err
	}

	stores := []storage.Storage{localStorage}
	remoteEnabled := false
	remoteOpts := config.RemoteOptionsFromConfig(cfg.RPC)
	if remoteOpts.ServeEnabled() {
		logger.Info("rpc serve enabled")
		server, err := startGRPCServer(localStorage, queryContextOptions,
			poolWrapper, remoteOpts, instrumentOpts)
		if err != nil {
			return nil, nil, err
		}

		cleanup = func() error {
			server.GracefulStop()
			return nil
		}
	}

	if remoteOpts.ListenEnabled() {
		remoteStorages, enabled, err := remoteClient(poolWrapper, remoteOpts,
			opts, instrumentOpts)
		if err != nil {
			return nil, nil, err
		}

		if enabled {
			stores = append(stores, remoteStorages...)
			remoteEnabled = enabled
		}
	}

	readFilter := filter.LocalOnly
	writeFilter := filter.LocalOnly
	completeTagsFilter := filter.CompleteTagsLocalOnly
	if remoteEnabled {
		// If remote enabled, allow all for read and complete tags
		// but continue to only send writes locally
		readFilter = filter.AllowAll
		completeTagsFilter = filter.CompleteTagsAllowAll
	}

	switch cfg.Filter.Read {
	case config.FilterLocalOnly:
		readFilter = filter.LocalOnly
	case config.FilterRemoteOnly:
		readFilter = filter.RemoteOnly
	case config.FilterAllowAll:
		readFilter = filter.AllowAll
	case config.FilterAllowNone:
		readFilter = filter.AllowNone
	}

	switch cfg.Filter.Write {
	case config.FilterLocalOnly:
		writeFilter = filter.LocalOnly
	case config.FilterRemoteOnly:
		writeFilter = filter.RemoteOnly
	case config.FilterAllowAll:
		writeFilter = filter.AllowAll
	case config.FilterAllowNone:
		writeFilter = filter.AllowNone
	}

	switch cfg.Filter.CompleteTags {
	case config.FilterLocalOnly:
		completeTagsFilter = filter.CompleteTagsLocalOnly
	case config.FilterRemoteOnly:
		completeTagsFilter = filter.CompleteTagsRemoteOnly
	case config.FilterAllowAll:
		completeTagsFilter = filter.CompleteTagsAllowAll
	case config.FilterAllowNone:
		completeTagsFilter = filter.CompleteTagsAllowNone
	}

	fanoutStorage := fanout.NewStorage(stores, readFilter, writeFilter,
		completeTagsFilter, opts.TagOptions(), opts, instrumentOpts)
	return fanoutStorage, cleanup, nil
}

func remoteZoneStorage(
	zone config.Remote,
	poolWrapper *pools.PoolWrapper,
	opts m3.Options,
	instrumentOpts instrument.Options,
) (storage.Storage, error) {
	if len(zone.Addresses) == 0 {
		// No addresses; skip.
		return nil, nil
	}

	client, err := tsdbremote.NewGRPCClient(zone.Name, zone.Addresses,
		poolWrapper, opts, instrumentOpts)
	if err != nil {
		return nil, err
	}

	remoteOpts := remote.Options{
		Name:          zone.Name,
		ErrorBehavior: zone.ErrorBehavior,
	}

	remoteStorage := remote.NewStorage(client, remoteOpts)
	return remoteStorage, nil
}

func remoteClient(
	poolWrapper *pools.PoolWrapper,
	remoteOpts config.RemoteOptions,
	opts m3.Options,
	instrumentOpts instrument.Options,
) ([]storage.Storage, bool, error) {
	logger := instrumentOpts.Logger()
	remotes := remoteOpts.Remotes()
	remoteStores := make([]storage.Storage, 0, len(remotes))
	for _, zone := range remotes {
		logger.Info(
			"creating RPC client with remotes",
			zap.String("name", zone.Name),
			zap.Strings("addresses", zone.Addresses),
		)

		remote, err := remoteZoneStorage(zone, poolWrapper, opts,
			instrumentOpts)
		if err != nil {
			return nil, false, err
		}

		remoteStores = append(remoteStores, remote)
	}

	return remoteStores, true, nil
}

func startGRPCServer(
	storage m3.Storage,
	queryContextOptions models.QueryContextOptions,
	poolWrapper *pools.PoolWrapper,
	opts config.RemoteOptions,
	instrumentOpts instrument.Options,
) (*grpc.Server, error) {
	logger := instrumentOpts.Logger()

	logger.Info("creating gRPC server")
	server := tsdbremote.NewGRPCServer(storage,
		queryContextOptions, poolWrapper, instrumentOpts)

	if opts.ReflectionEnabled() {
		reflection.Register(server)
	}

	logger.Info("gRPC server reflection configured",
		zap.Bool("enabled", opts.ReflectionEnabled()))

	listener, err := net.Listen("tcp", opts.ServeAddress())
	if err != nil {
		return nil, err
	}
	go func() {
		if err := server.Serve(listener); err != nil {
			logger.Error("error from serving gRPC server", zap.Error(err))
		}
	}()

	return server, nil
}

func startCarbonIngestion(
	ingesterCfg config.CarbonIngesterConfiguration,
	listenerOpts xnet.ListenerOptions,
	iOpts instrument.Options,
	logger *zap.Logger,
	m3dbClusters m3.Clusters,
	clusterNamespacesWatcher m3.ClusterNamespacesWatcher,
	downsamplerAndWriter ingest.DownsamplerAndWriter,
) xserver.Server {
	logger.Info("carbon ingestion enabled, configuring ingester")

	// Setup worker pool.
	var (
		carbonIOpts = iOpts.SetMetricsScope(
			iOpts.MetricsScope().SubScope("ingest-carbon"))
		carbonWorkerPoolOpts xsync.PooledWorkerPoolOptions
		carbonWorkerPoolSize int
	)
	if ingesterCfg.MaxConcurrency > 0 {
		// Use a bounded worker pool if they requested a specific maximum concurrency.
		carbonWorkerPoolOpts = xsync.NewPooledWorkerPoolOptions().
			SetGrowOnDemand(false).
			SetInstrumentOptions(carbonIOpts)
		carbonWorkerPoolSize = ingesterCfg.MaxConcurrency
	} else {
		carbonWorkerPoolOpts = xsync.NewPooledWorkerPoolOptions().
			SetGrowOnDemand(true).
			SetKillWorkerProbability(0.001)
		carbonWorkerPoolSize = defaultCarbonIngesterWorkerPoolSize
	}
	workerPool, err := xsync.NewPooledWorkerPool(carbonWorkerPoolSize, carbonWorkerPoolOpts)
	if err != nil {
		logger.Fatal("unable to create worker pool for carbon ingester", zap.Error(err))
	}
	workerPool.Init()

	if m3dbClusters == nil {
		logger.Fatal("carbon ingestion is only supported when connecting to M3DB clusters directly")
	}

	// Create ingester.
	ingester, err := ingestcarbon.NewIngester(
		downsamplerAndWriter, clusterNamespacesWatcher, ingestcarbon.Options{
			InstrumentOptions: carbonIOpts,
			WorkerPool:        workerPool,
			IngesterConfig:    ingesterCfg,
		})
	if err != nil {
		logger.Fatal("unable to create carbon ingester", zap.Error(err))
	}

	// Start server.
	var (
		serverOpts = xserver.NewOptions().
				SetInstrumentOptions(carbonIOpts).
				SetListenerOptions(listenerOpts)
		carbonListenAddress = ingesterCfg.ListenAddressOrDefault()
		carbonServer        = xserver.NewServer(carbonListenAddress, ingester, serverOpts)
	)
	if strings.TrimSpace(carbonListenAddress) == "" {
		logger.Fatal("no listen address specified for carbon ingester")
	}

	logger.Info("starting carbon ingestion server", zap.String("listenAddress", carbonListenAddress))
	err = carbonServer.ListenAndServe()
	if err != nil {
		logger.Fatal("unable to start carbon ingestion server at listen address",
			zap.String("listenAddress", carbonListenAddress), zap.Error(err))
	}

	logger.Info("started carbon ingestion server", zap.String("listenAddress", carbonListenAddress))

	return carbonServer
}

func newDownsamplerAndWriter(
	storage storage.Storage,
	downsampler downsample.Downsampler,
	workerPoolPolicy xconfig.WorkerPoolPolicy,
	iOpts instrument.Options,
) (ingest.DownsamplerAndWriter, error) {
	// Make sure the downsampler and writer gets its own PooledWorkerPool and that its not shared with any other
	// codepaths because PooledWorkerPools can deadlock if used recursively.
	downAndWriterWorkerPoolOpts, writePoolSize := workerPoolPolicy.Options()
	downAndWriterWorkerPoolOpts = downAndWriterWorkerPoolOpts.SetInstrumentOptions(iOpts.
		SetMetricsScope(iOpts.MetricsScope().SubScope("ingest-writer-worker-pool")))
	downAndWriteWorkerPool, err := xsync.NewPooledWorkerPool(writePoolSize, downAndWriterWorkerPoolOpts)
	if err != nil {
		return nil, err
	}
	downAndWriteWorkerPool.Init()

	return ingest.NewDownsamplerAndWriter(storage, downsampler, downAndWriteWorkerPool, iOpts), nil
}

func newPromQLEngine(
	cfg config.Configuration,
	registry *extprom.Registry,
	instrumentOpts instrument.Options,
) (*prometheuspromql.Engine, error) {
	lookbackDelta, err := cfg.LookbackDurationOrDefault()
	if err != nil {
		return nil, err
	}

	var (
		kitLogger = kitlogzap.NewZapSugarLogger(instrumentOpts.Logger(), zapcore.InfoLevel)
		opts      = prometheuspromql.EngineOpts{
			Logger:        log.With(kitLogger, "component", "prometheus_engine"),
			Reg:           registry,
			MaxSamples:    cfg.Query.Prometheus.MaxSamplesPerQueryOrDefault(),
			Timeout:       cfg.Query.TimeoutOrDefault(),
			LookbackDelta: lookbackDelta,
			NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
				return durationMilliseconds(1 * time.Minute)
			},
		}
	)
	return prometheuspromql.NewEngine(opts), nil
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}
