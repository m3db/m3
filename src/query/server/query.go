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
	memcluster "github.com/m3db/m3/src/cluster/mem"
	handleroptions3 "github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/serve"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	ingestcarbon "github.com/m3db/m3/src/cmd/services/m3coordinator/ingest/carbon"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
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
	"github.com/m3db/m3/src/query/promqlengine"
	tsdbremote "github.com/m3db/m3/src/query/remote"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/fanout"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/promremote"
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

	// CustomHandlerOptions creates custom handler options.
	CustomHandlerOptions CustomHandlerOptionsFn

	// CustomPromQLParseFunction is a custom PromQL parsing function.
	CustomPromQLParseFunction promql.ParseFn

	// ApplyCustomTSDBOptions is a transform that allows for custom tsdb options.
	ApplyCustomTSDBOptions CustomTSDBOptionsFn

	// BackendStorageTransform is a custom backend storage transform.
	BackendStorageTransform BackendStorageTransformFn

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
type CustomTSDBOptionsFn func(m3.Options, instrument.Options) (m3.Options, error)

// CustomHandlerOptionsFn is a factory for options.CustomHandlerOptions.
type CustomHandlerOptionsFn func(instrument.Options) (options.CustomHandlerOptions, error)

// BackendStorageTransformFn is a transformation function for backend storage.
type BackendStorageTransformFn func(
	storage.Storage,
	m3.Options,
	instrument.Options,
) (storage.Storage, error)

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

	interruptOpts := xos.NewInterruptOptions()
	if runOpts.InterruptCh != nil {
		interruptOpts.InterruptCh = runOpts.InterruptCh
	}
	intWatchCancel := xos.WatchForInterrupt(logger, interruptOpts)
	defer intWatchCancel()

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
		clusterNamespacesWatcher = m3.NewClusterNamespacesWatcher()
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
	defer clusterNamespacesWatcher.Close()

	tagOptions, err := config.TagOptionsFromConfig(cfg.TagOptions)
	if err != nil {
		logger.Fatal("could not create tag options", zap.Error(err))
	}

	lookbackDuration, err := cfg.LookbackDurationOrDefault()
	if err != nil {
		logger.Fatal("error validating LookbackDuration", zap.Error(err))
	}
	cfg.LookbackDuration = &lookbackDuration

	promConvertOptions := cfg.Query.Prometheus.ConvertOptionsOrDefault()

	readWorkerPool, writeWorkerPool, err := pools.BuildWorkerPools(
		instrumentOptions,
		cfg.ReadWorkerPool,
		cfg.WriteWorkerPoolOrDefault(),
		scope)
	if err != nil {
		logger.Fatal("could not create worker pools", zap.Error(err))
	}

	var (
		encodingOpts    = encoding.NewOptions()
		m3dbClusters    m3.Clusters
		m3dbPoolWrapper *pools.PoolWrapper
	)

	tsdbOpts := m3.NewOptions(encodingOpts).
		SetTagOptions(tagOptions).
		SetLookbackDuration(lookbackDuration).
		SetConsolidationFunc(consolidators.TakeLast).
		SetReadWorkerPool(readWorkerPool).
		SetWriteWorkerPool(writeWorkerPool).
		SetSeriesConsolidationMatchOptions(matchOptions).
		SetPromConvertOptions(promConvertOptions)

	if runOpts.ApplyCustomTSDBOptions != nil {
		tsdbOpts, err = runOpts.ApplyCustomTSDBOptions(tsdbOpts, instrumentOptions)
		if err != nil {
			logger.Fatal("could not apply ApplyCustomTSDBOptions", zap.Error(err))
		}
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
			pools.BuildIteratorPools(encodingOpts, pools.BuildIteratorPoolsOptions{}))
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
	case "", config.M3DBStorageType:
		// For m3db backend, we need to make connections to the m3db cluster
		// which generates a session and use the storage with the session.
		m3dbClusters, m3dbPoolWrapper, err = initClusters(cfg, runOpts.DBConfig,
			clusterNamespacesWatcher, runOpts.DBClient, encodingOpts,
			instrumentOptions, tsdbOpts.CustomAdminOptions())
		if err != nil {
			logger.Fatal("unable to init clusters", zap.Error(err))
		}

		var cleanup cleanupFn
		backendStorage, cleanup, err = newM3DBStorage(
			cfg, m3dbClusters, m3dbPoolWrapper, queryCtxOpts, tsdbOpts, instrumentOptions,
		)
		if err != nil {
			logger.Fatal("unable to setup m3db backend", zap.Error(err))
		}

		defer cleanup()

		etcdConfig, err := resolveEtcdForM3DB(cfg)
		if err != nil {
			logger.Fatal("unable to resolve etcd config for m3db backend", zap.Error(err))
		}

		logger.Info("configuring downsampler to use with aggregated cluster namespaces",
			zap.Int("numAggregatedClusterNamespaces", len(m3dbClusters.ClusterNamespaces())))

		downsampler, clusterClient, err = newDownsamplerAsync(cfg.Downsample, etcdConfig, backendStorage,
			clusterNamespacesWatcher, tsdbOpts.TagOptions(), clockOpts, instrumentOptions, rwOpts, runOpts,
			interruptOpts,
		)
		if err != nil {
			var interruptErr *xos.InterruptError
			if errors.As(err, &interruptErr) {
				logger.Warn("interrupt received. closing server", zap.Error(err))
				return runResult
			}
			logger.Fatal("unable to setup downsampler for m3db backend", zap.Error(err))
		}
	case config.PromRemoteStorageType:
		opts, err := promremote.NewOptions(cfg.PrometheusRemoteBackend, scope, instrumentOptions.Logger())
		if err != nil {
			logger.Fatal("invalid configuration", zap.Error(err))
		}
		backendStorage, err = promremote.NewStorage(opts)
		if err != nil {
			logger.Fatal("unable to setup prom remote backend", zap.Error(err))
		}
		defer func() {
			if err := backendStorage.Close(); err != nil {
				logger.Error("error when closing storage", zap.Error(err))
			}
		}()

		logger.Info("configuring downsampler to use with aggregated namespaces",
			zap.Int("numAggregatedClusterNamespaces", opts.Namespaces().NumAggregatedClusterNamespaces()))
		err = clusterNamespacesWatcher.Update(opts.Namespaces())
		if err != nil {
			logger.Fatal("unable to update namespaces", zap.Error(err))
		}

		downsampler, clusterClient, err = newDownsamplerAsync(cfg.Downsample, cfg.ClusterManagement.Etcd, backendStorage,
			clusterNamespacesWatcher, tsdbOpts.TagOptions(), clockOpts, instrumentOptions, rwOpts, runOpts,
			interruptOpts,
		)
		if err != nil {
			logger.Fatal("unable to setup downsampler for prom remote backend", zap.Error(err))
		}
	default:
		logger.Fatal("unrecognized backend", zap.String("backend", string(cfg.Backend)))
	}

	if fn := runOpts.BackendStorageTransform; fn != nil {
		backendStorage, err = fn(backendStorage, tsdbOpts, instrumentOptions)
		if err != nil {
			logger.Fatal("could not apply BackendStorageTransform", zap.Error(err))
		}
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
		graphiteStorageOpts = graphite.M3WrappedStorageOptions{
			AggregateNamespacesAllData:                 cfg.Carbon.AggregateNamespacesAllData,
			ShiftTimeStart:                             cfg.Carbon.ShiftTimeStart,
			ShiftTimeEnd:                               cfg.Carbon.ShiftTimeEnd,
			ShiftStepsStart:                            cfg.Carbon.ShiftStepsStart,
			ShiftStepsEnd:                              cfg.Carbon.ShiftStepsEnd,
			ShiftStepsStartWhenAtResolutionBoundary:    cfg.Carbon.ShiftStepsStartWhenAtResolutionBoundary,
			ShiftStepsEndWhenAtResolutionBoundary:      cfg.Carbon.ShiftStepsEndWhenAtResolutionBoundary,
			ShiftStepsEndWhenStartAtResolutionBoundary: cfg.Carbon.ShiftStepsEndWhenStartAtResolutionBoundary,
			ShiftStepsStartWhenEndAtResolutionBoundary: cfg.Carbon.ShiftStepsStartWhenEndAtResolutionBoundary,
			RenderPartialStart:                         cfg.Carbon.RenderPartialStart,
			RenderPartialEnd:                           cfg.Carbon.RenderPartialEnd,
			RenderSeriesAllNaNs:                        cfg.Carbon.RenderSeriesAllNaNs,
			CompileEscapeAllNotOnlyQuotes:              cfg.Carbon.CompileEscapeAllNotOnlyQuotes,
			FindResultsIncludeBothExpandableAndLeaf:    cfg.Carbon.FindResultsIncludeBothExpandableAndLeaf,
		}
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

	defaultPrometheusEngine, err := newPromQLEngine(lookbackDuration, cfg, prometheusEngineRegistry, instrumentOptions)
	if err != nil {
		logger.Fatal("unable to create PromQL engine", zap.Error(err))
	}
	prometheusEngineFn := func(lookbackDuration time.Duration) (*prometheuspromql.Engine, error) {
		// NB: use nil metric registry to avoid duplicate metric registration when creating multiple engines
		return newPromQLEngine(lookbackDuration, cfg, nil, instrumentOptions)
	}

	enginesByLookback, err := createEnginesWithResolutionBasedLookbacks(
		lookbackDuration,
		defaultPrometheusEngine,
		runOpts.Config.Clusters,
		cfg.Middleware.Prometheus.ResolutionMultiplier,
		prometheusEngineFn,
	)
	if err != nil {
		logger.Fatal("failed creating PromgQL engines with resolution based lookback durations", zap.Error(err))
	}
	engineCache := promqlengine.NewCache(enginesByLookback, prometheusEngineFn)

	handlerOptions, err := options.NewHandlerOptions(downsamplerAndWriter,
		tagOptions, engine, engineCache.Get, m3dbClusters, clusterClient, cfg,
		runOpts.DBConfig, fetchOptsBuilder, graphiteFindFetchOptsBuilder, graphiteRenderFetchOptsBuilder,
		queryCtxOpts, instrumentOptions, cpuProfileDuration,
		serviceOptionDefaults, httpd.NewQueryRouter(), httpd.NewQueryRouter(),
		graphiteStorageOpts, tsdbOpts, httpd.NewGraphiteRenderRouter(), httpd.NewGraphiteFindRouter(), lookbackDuration)
	if err != nil {
		logger.Fatal("unable to set up handler options", zap.Error(err))
	}

	var customHandlerOpts options.CustomHandlerOptions
	if runOpts.CustomHandlerOptions != nil {
		customHandlerOpts, err = runOpts.CustomHandlerOptions(instrumentOptions)
		if err != nil {
			logger.Fatal("could not create custom handlers", zap.Error(err))
		}
	}

	if fn := customHandlerOpts.OptionTransformFn; fn != nil {
		handlerOptions = fn(handlerOptions)
	}

	customHandlers := customHandlerOpts.CustomHandlers
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

	// Stop our async watch and now block waiting for the interrupt.
	intWatchCancel()
	select {
	case <-interruptOpts.InterruptedCh:
		logger.Warn("interrupt already received. closing")
	default:
		xos.WaitForInterrupt(logger, interruptOpts)
	}

	return runResult
}

// make connections to the m3db cluster(s) and generate sessions for those clusters along with the storage
func newM3DBStorage(cfg config.Configuration, clusters m3.Clusters, poolWrapper *pools.PoolWrapper,
	queryContextOptions models.QueryContextOptions, tsdbOpts m3.Options,
	instrumentOptions instrument.Options,
) (storage.Storage, cleanupFn, error) {
	fanoutStorage, storageCleanup, err := newStorages(clusters, cfg,
		poolWrapper, queryContextOptions, tsdbOpts, instrumentOptions)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to set up storages")
	}
	logger := instrumentOptions.Logger()
	cleanup := func() error {
		lastErr := storageCleanup()
		// Don't want to quit on the first error since the full cleanup is important
		if lastErr != nil {
			logger.Error("error during storage cleanup", zap.Error(lastErr))
		}

		if err := clusters.Close(); err != nil {
			lastErr = errors.Wrap(err, "unable to close M3DB cluster sessions")
			// Make sure the previous error is at least logged
			logger.Error("error during cluster cleanup", zap.Error(err))
		}

		return lastErr
	}

	return fanoutStorage, cleanup, err
}

func resolveEtcdForM3DB(cfg config.Configuration) (*etcdclient.Configuration, error) {
	etcdConfig := cfg.ClusterManagement.Etcd
	if etcdConfig == nil && len(cfg.Clusters) == 1 &&
		cfg.Clusters[0].Client.EnvironmentConfig != nil {
		syncCfg, err := cfg.Clusters[0].Client.EnvironmentConfig.Services.SyncCluster()
		if err != nil {
			return nil, errors.Wrap(err, "unable to get etcd sync cluster config")
		}
		etcdConfig = syncCfg.Service
	}
	return etcdConfig, nil
}

func newDownsamplerAsync(
	cfg downsample.Configuration, etcdCfg *etcdclient.Configuration, storage storage.Appender,
	clusterNamespacesWatcher m3.ClusterNamespacesWatcher, tagOptions models.TagOptions, clockOpts clock.Options,
	instrumentOptions instrument.Options, rwOpts xio.Options, runOpts RunOptions, interruptOpts xos.InterruptOptions,
) (downsample.Downsampler, clusterclient.Client, error) {
	var (
		clusterClient       clusterclient.Client
		clusterClientWaitCh <-chan struct{}
		err                 error
	)
	if clusterClientCh := runOpts.ClusterClient; clusterClientCh != nil {
		// Only use a cluster client if we are going to receive one, that
		// way passing nil to httpd NewHandler disables the endpoints entirely
		clusterClientDoneCh := make(chan struct{}, 1)
		clusterClientWaitCh = clusterClientDoneCh
		clusterClient = m3dbcluster.NewAsyncClient(func() (clusterclient.Client, error) {
			return <-clusterClientCh, nil
		}, clusterClientDoneCh)
	} else if etcdCfg != nil {
		// We resolved an etcd configuration for cluster management endpoints
		var (
			clusterSvcClientOpts = etcdCfg.NewOptions()
			err                  error
		)
		clusterClient, err = etcdclient.NewConfigServiceClient(clusterSvcClientOpts)
		if err != nil {
			return nil, nil, errors.Wrap(err, "unable to create cluster management etcd client")
		}
	} else if cfg.RemoteAggregator == nil {
		// NB(antanas): M3 Coordinator with in process aggregator can run with in memory cluster client.
		instrumentOptions.Logger().Info("no etcd config and no remote aggregator - will run with in memory cluster client")
		clusterClient = memcluster.New(kv.NewOverrideOptions())
	} else {
		return nil, nil, fmt.Errorf("no configured cluster management config, must set this config for remote aggregator")
	}

	newDownsamplerFn := func() (downsample.Downsampler, error) {
		ds, err := newDownsampler(
			cfg, clusterClient,
			storage, clusterNamespacesWatcher,
			tagOptions, clockOpts, instrumentOptions, rwOpts, runOpts.ApplyCustomRuleStore,
			interruptOpts.InterruptedCh)
		if err != nil {
			return nil, err
		}

		// Notify the downsampler ready channel that
		// the downsampler has now been created and is ready.
		if runOpts.DownsamplerReadyCh != nil {
			runOpts.DownsamplerReadyCh <- struct{}{}
		}

		return ds, nil
	}

	if clusterClientWaitCh != nil {
		// Need to wait before constructing and instead return an async downsampler
		// since the cluster client will return errors until it's initialized itself
		// and will fail constructing the downsampler consequently
		downsampler := downsample.NewAsyncDownsampler(func() (downsample.Downsampler, error) {
			<-clusterClientWaitCh
			return newDownsamplerFn()
		}, nil)
		return downsampler, clusterClient, err
	} else {
		// Otherwise we already have a client and can immediately construct the downsampler
		downsampler, err := newDownsamplerFn()
		return downsampler, clusterClient, err
	}
}

func newDownsampler(
	cfg downsample.Configuration,
	clusterClient clusterclient.Client,
	storage storage.Appender,
	clusterNamespacesWatcher m3.ClusterNamespacesWatcher,
	tagOptions models.TagOptions,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
	rwOpts xio.Options,
	applyCustomRuleStore downsample.CustomRuleStoreFn,
	interruptedCh <-chan struct{},
) (downsample.Downsampler, error) {
	// Namespace the downsampler metrics.
	instrumentOpts = instrumentOpts.SetMetricsScope(
		instrumentOpts.MetricsScope().SubScope("downsampler"))

	var kvStore kv.Store
	var err error

	if applyCustomRuleStore == nil {
		kvStore, err = clusterClient.KV()
		if err != nil {
			return nil, errors.Wrap(err, "unable to create KV store from the "+
				"cluster management config client")
		}
	} else {
		kvStore, err = applyCustomRuleStore(clusterClient, instrumentOpts)
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
		ClusterClient:              clusterClient,
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
		InterruptedCh:              interruptedCh,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create downsampler: %w", err)
	}

	return downsampler, nil
}

func initClusters(
	cfg config.Configuration,
	dbCfg *dbconfig.DBConfiguration,
	clusterNamespacesWatcher m3.ClusterNamespacesWatcher,
	dbClientCh <-chan client.Client,
	encodingOpts encoding.Options,
	instrumentOpts instrument.Options,
	customAdminOptions []client.CustomAdminOption,
) (m3.Clusters, *pools.PoolWrapper, error) {
	instrumentOpts = instrumentOpts.
		SetMetricsScope(instrumentOpts.MetricsScope().SubScope("m3db-client"))

	var (
		logger                = instrumentOpts.Logger()
		clusters              m3.Clusters
		poolWrapper           *pools.PoolWrapper
		staticNamespaceConfig bool
		err                   error
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
			EncodingOptions:    encodingOpts,
		}
		if staticNamespaceConfig {
			clusters, err = cfg.Clusters.NewStaticClusters(instrumentOpts, opts, clusterNamespacesWatcher)
		} else {
			// No namespaces defined in config -- pull namespace data from etcd.
			clusters, err = cfg.Clusters.NewDynamicClusters(instrumentOpts, opts, clusterNamespacesWatcher)
		}
		if err != nil {
			return nil, nil, errors.Wrap(err, "unable to connect to clusters")
		}

		poolWrapper = pools.NewPoolsWrapper(
			pools.BuildIteratorPools(encodingOpts, pools.BuildIteratorPoolsOptions{}))
	} else {
		localCfg := cfg.Local
		if localCfg == nil {
			localCfg = &config.LocalConfiguration{}
		}
		if len(localCfg.Namespaces) > 0 {
			staticNamespaceConfig = true
		}

		if dbClientCh == nil {
			return nil, nil, errors.New("no clusters configured and not running local cluster")
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
				return nil, nil, errors.New("environment config required when dynamically fetching namespaces")
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
			EncodingOptions:    encodingOpts,
		}

		if staticNamespaceConfig {
			clusters, err = clustersCfg.NewStaticClusters(instrumentOpts, cfgOptions, clusterNamespacesWatcher)
		} else {
			clusters, err = clustersCfg.NewDynamicClusters(instrumentOpts, cfgOptions, clusterNamespacesWatcher)
		}
		if err != nil {
			return nil, nil, errors.Wrap(err, "unable to connect to clusters")
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

	return clusters, poolWrapper, nil
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
	lookbackDelta time.Duration,
	cfg config.Configuration,
	registry extprom.Registerer,
	instrumentOpts instrument.Options,
) (*prometheuspromql.Engine, error) {
	if lookbackDelta < 0 {
		return nil, errors.New("lookbackDelta cannot be negative")
	}

	instrumentOpts.Logger().Debug("creating new PromQL engine", zap.Duration("lookbackDelta", lookbackDelta))
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

func createEnginesWithResolutionBasedLookbacks(
	defaultLookback time.Duration,
	defaultEngine *prometheuspromql.Engine,
	clusters m3.ClustersStaticConfiguration,
	resolutionMultiplier int,
	prometheusEngineFn func(time.Duration) (*prometheuspromql.Engine, error),
) (map[time.Duration]*prometheuspromql.Engine, error) {
	enginesByLookback := make(map[time.Duration]*prometheuspromql.Engine)
	enginesByLookback[defaultLookback] = defaultEngine
	if resolutionMultiplier > 0 {
		for _, cluster := range clusters {
			for _, ns := range cluster.Namespaces {
				if res := ns.Resolution; res > 0 {
					resolutionBasedLookback := res * time.Duration(resolutionMultiplier)
					if _, ok := enginesByLookback[resolutionBasedLookback]; !ok {
						eng, err := prometheusEngineFn(resolutionBasedLookback)
						if err != nil {
							return nil, err
						}
						enginesByLookback[resolutionBasedLookback] = eng
					}
				}
			}
		}
	}
	return enginesByLookback, nil
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}
