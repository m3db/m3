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
	"sort"
	"strings"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	ingestcarbon "github.com/m3db/m3/src/cmd/services/m3coordinator/ingest/carbon"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/httpd"
	m3dbcluster "github.com/m3db/m3/src/query/cluster/m3db"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/policy/filter"
	"github.com/m3db/m3/src/query/pools"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/fanout"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/remote"
	"github.com/m3db/m3/src/query/stores/m3db"
	tsdbRemote "github.com/m3db/m3/src/query/tsdb/remote"
	"github.com/m3db/m3/src/x/clock"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/instrument"
	xos "github.com/m3db/m3/src/x/os"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xserver "github.com/m3db/m3/src/x/server"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	serviceName            = "m3query"
	cpuProfileDuration     = 5 * time.Second
	defaultM3DBServiceName = "m3db"
)

var (
	defaultLocalConfiguration = &config.LocalConfiguration{
		Namespaces: []m3.ClusterStaticNamespaceConfiguration{
			{
				Namespace: "default",
				Type:      storage.UnaggregatedMetricsType,
				Retention: 2 * 24 * time.Hour,
			},
		},
	}

	defaultDownsamplerAndWriterWorkerPoolSize = 1024
	defaultCarbonIngesterWorkerPoolSize       = 1024
)

type cleanupFn func() error

// RunOptions provides options for running the server
// with backwards compatibility if only solely adding fields.
type RunOptions struct {
	// ConfigFiles is the array of config files to use. All files of the array get merged together.
	ConfigFiles []string

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

	// ListenerCh is a programmatic channel to receive the server listener
	// on once it has opened.
	ListenerCh chan<- net.Listener
}

// Run runs the server programmatically given a filename for the configuration file.
func Run(runOpts RunOptions) {
	rand.Seed(time.Now().UnixNano())

	var cfg config.Configuration
	if len(runOpts.ConfigFiles) > 0 {
		if err := xconfig.LoadFiles(&cfg, runOpts.ConfigFiles, xconfig.Options{}); err != nil {
			fmt.Fprintf(os.Stderr, "unable to load %s: %v", runOpts.ConfigFiles, err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stdout, "using %s config files: %v", serviceName, runOpts.ConfigFiles)
	} else {
		cfg = runOpts.Config
	}

	logger, err := cfg.Logging.BuildLogger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create logger: %v", err)
		os.Exit(1)
	}

	defer logger.Sync()

	xconfig.WarnOnDeprecation(cfg, logger)

	scope, closer, err := cfg.Metrics.NewRootScope()
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

	instrumentOptions := instrument.NewOptions().
		SetMetricsScope(scope).
		SetLogger(logger).
		SetTracer(tracer)

	opentracing.SetGlobalTracer(tracer)

	// Close metrics scope
	defer func() {
		if e := recover(); e != nil {
			logger.Warn("recovered from panic", zap.String("e", fmt.Sprintf("%v", e)))
		}
		logger.Info("closing metrics scope")
		if err := closer.Close(); err != nil {
			logger.Error("unable to close metrics scope", zap.Error(err))
		}
	}()

	buildInfoOpts := instrumentOptions.SetMetricsScope(
		instrumentOptions.MetricsScope().SubScope("build_info"))
	buildReporter := instrument.NewBuildReporter(buildInfoOpts)
	if err := buildReporter.Start(); err != nil {
		logger.Fatal("could not start build reporter", zap.Error(err))
	}

	defer buildReporter.Stop()

	var (
		backendStorage      storage.Storage
		clusterClient       clusterclient.Client
		downsampler         downsample.Downsampler
		fetchOptsBuilderCfg = cfg.Limits.PerQuery.AsFetchOptionsBuilderOptions()
		fetchOptsBuilder    = handler.NewFetchOptionsBuilder(fetchOptsBuilderCfg)
		queryCtxOpts        = models.QueryContextOptions{
			LimitMaxTimeseries: fetchOptsBuilderCfg.Limit,
		}
	)
	readWorkerPool, writeWorkerPool, err := pools.BuildWorkerPools(
		instrumentOptions,
		cfg.ReadWorkerPool,
		cfg.WriteWorkerPool,
		scope,
	)
	if err != nil {
		logger.Fatal("could not create worker pools", zap.Error(err))
	}

	tagOptions, err := config.TagOptionsFromConfig(cfg.TagOptions)
	if err != nil {
		logger.Fatal("could not create tag options", zap.Error(err))
	}

	lookbackDuration, err := cfg.LookbackDurationOrDefault()
	if err != nil {
		logger.Fatal("error validating LookbackDuration", zap.Error(err))
	}
	cfg.LookbackDuration = &lookbackDuration

	var (
		m3dbClusters    m3.Clusters
		m3dbPoolWrapper *pools.PoolWrapper
	)
	if cfg.Backend == config.GRPCStorageType {
		// For grpc backend, we need to setup only the grpc client and a storage
		// accompanying that client.
		poolWrapper := pools.NewPoolsWrapper(pools.BuildIteratorPools())
		remotes, enabled, err := remoteClient(cfg, tagOptions, poolWrapper,
			readWorkerPool, instrumentOptions)
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
			instrumentOptions)
		logger.Info("setup grpc backend")
	} else {
		// For m3db backend, we need to make connections to the m3db cluster
		// which generates a session and use the storage with the session.
		m3dbClusters, m3dbPoolWrapper, err = initClusters(cfg,
			runOpts.DBClient, instrumentOptions)
		if err != nil {
			logger.Fatal("unable to init clusters", zap.Error(err))
		}

		var cleanup cleanupFn
		backendStorage, clusterClient, downsampler, cleanup, err = newM3DBStorage(
			runOpts, cfg, tagOptions, m3dbClusters, m3dbPoolWrapper,
			readWorkerPool, writeWorkerPool, queryCtxOpts, instrumentOptions)
		if err != nil {
			logger.Fatal("unable to setup m3db backend", zap.Error(err))
		}
		defer cleanup()
	}

	perQueryEnforcer, err := newConfiguredChainedEnforcer(&cfg, instrumentOptions)
	if err != nil {
		logger.Fatal("unable to setup perQueryEnforcer", zap.Error(err))
	}

	engineOpts := executor.NewEngineOptions().
		SetStore(backendStorage).
		SetLookbackDuration(*cfg.LookbackDuration).
		SetGlobalEnforcer(perQueryEnforcer).
		SetInstrumentOptions(instrumentOptions.
			SetMetricsScope(instrumentOptions.MetricsScope().SubScope("engine")))
	engine := executor.NewEngine(engineOpts)
	downsamplerAndWriter, err := newDownsamplerAndWriter(backendStorage, downsampler)
	if err != nil {
		logger.Fatal("unable to create new downsampler and writer", zap.Error(err))
	}

	handler, err := httpd.NewHandler(downsamplerAndWriter, tagOptions, engine,
		m3dbClusters, clusterClient, cfg, runOpts.DBConfig, perQueryEnforcer,
		fetchOptsBuilder, queryCtxOpts, instrumentOptions, cpuProfileDuration,
		[]string{defaultM3DBServiceName})
	if err != nil {
		logger.Fatal("unable to set up handlers", zap.Error(err))
	}

	if err := handler.RegisterRoutes(); err != nil {
		logger.Fatal("unable to register routes", zap.Error(err))
	}

	listenAddress, err := cfg.ListenAddress.Resolve()
	if err != nil {
		logger.Fatal("unable to get listen address", zap.Error(err))
	}

	srv := &http.Server{Addr: listenAddress, Handler: handler.Router()}
	defer func() {
		logger.Info("closing server")
		if err := srv.Shutdown(context.Background()); err != nil {
			logger.Error("error closing server", zap.Error(err))
		}
	}()

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logger.Fatal("unable to listen on listen address",
			zap.String("address", listenAddress),
			zap.Error(err))
	}
	if runOpts.ListenerCh != nil {
		runOpts.ListenerCh <- listener
	}
	go func() {
		logger.Info("starting API server", zap.String("address", listenAddress))
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server serve error",
				zap.String("address", listenAddress),
				zap.Error(err))
		}
	}()

	if cfg.Ingest != nil {
		logger.Info("starting m3msg server",
			zap.String("address", cfg.Ingest.M3Msg.Server.ListenAddress))
		ingester, err := cfg.Ingest.Ingester.NewIngester(backendStorage, instrumentOptions)
		if err != nil {
			logger.Fatal("unable to create ingester", zap.Error(err))
		}

		server, err := cfg.Ingest.M3Msg.NewServer(
			ingester.Ingest,
			instrumentOptions.SetMetricsScope(scope.SubScope("ingest-m3msg")),
		)

		if err != nil {
			logger.Fatal("unable to create m3msg server", zap.Error(err))
		}

		if err := server.ListenAndServe(); err != nil {
			logger.Fatal("unable to listen on ingest server", zap.Error(err))
		}

		logger.Info("started m3msg server ")
		defer server.Close()
	} else {
		logger.Info("no m3msg server configured")
	}

	if cfg.Carbon != nil && cfg.Carbon.Ingester != nil {
		server, ok := startCarbonIngestion(cfg.Carbon, instrumentOptions,
			logger, m3dbClusters, downsamplerAndWriter)
		if ok {
			defer server.Close()
		}
	}

	// Wait for process interrupt.
	xos.WaitForInterrupt(logger, xos.InterruptOptions{
		InterruptCh: runOpts.InterruptCh,
	})
}

// make connections to the m3db cluster(s) and generate sessions for those clusters along with the storage
func newM3DBStorage(
	runOpts RunOptions,
	cfg config.Configuration,
	tagOptions models.TagOptions,
	clusters m3.Clusters,
	poolWrapper *pools.PoolWrapper,
	readWorkerPool xsync.PooledWorkerPool,
	writeWorkerPool xsync.PooledWorkerPool,
	queryContextOptions models.QueryContextOptions,
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
		case cfg.ClusterManagement != nil:
			etcdCfg = &cfg.ClusterManagement.Etcd

		case len(cfg.Clusters) == 1 &&
			cfg.Clusters[0].Client.EnvironmentConfig != nil &&
			cfg.Clusters[0].Client.EnvironmentConfig.Service != nil:
			etcdCfg = cfg.Clusters[0].Client.EnvironmentConfig.Service
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

	fanoutStorage, storageCleanup, err := newStorages(clusters, cfg, tagOptions,
		poolWrapper, readWorkerPool, writeWorkerPool, queryContextOptions,
		instrumentOptions)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "unable to set up storages")
	}

	var (
		namespaces  = clusters.ClusterNamespaces()
		downsampler downsample.Downsampler
	)
	if n := namespaces.NumAggregatedClusterNamespaces(); n > 0 {
		logger.Info("configuring downsampler to use with aggregated cluster namespaces",
			zap.Int("numAggregatedClusterNamespaces", n))
		autoMappingRules, err := newDownsamplerAutoMappingRules(namespaces)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		newDownsamplerFn := func() (downsample.Downsampler, error) {
			return newDownsampler(cfg.Downsample, clusterClient,
				fanoutStorage, autoMappingRules, tagOptions, instrumentOptions)
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
	}

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

	return fanoutStorage, clusterClient, downsampler, cleanup, nil
}

func newDownsampler(
	cfg downsample.Configuration,
	clusterManagementClient clusterclient.Client,
	storage storage.Storage,
	autoMappingRules []downsample.MappingRule,
	tagOptions models.TagOptions,
	instrumentOpts instrument.Options,
) (downsample.Downsampler, error) {
	if clusterManagementClient == nil {
		return nil, fmt.Errorf("no configured cluster management config, " +
			"must set this config for downsampler")
	}

	kvStore, err := clusterManagementClient.KV()
	if err != nil {
		return nil, errors.Wrap(err, "unable to create KV store from the "+
			"cluster management config client")
	}

	tagEncoderOptions := serialize.NewTagEncoderOptions()
	tagDecoderOptions := serialize.NewTagDecoderOptions()
	tagEncoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-encoder-pool")))
	tagDecoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-decoder-pool")))

	downsampler, err := cfg.NewDownsampler(downsample.DownsamplerOptions{
		Storage:          storage,
		ClusterClient:    clusterManagementClient,
		RulesKVStore:     kvStore,
		AutoMappingRules: autoMappingRules,
		ClockOptions:     clock.NewOptions(),
		// TODO: remove after https://github.com/m3db/m3/issues/992 is fixed
		InstrumentOptions:     instrumentOpts.SetMetricsScope(tally.NoopScope),
		TagEncoderOptions:     tagEncoderOptions,
		TagDecoderOptions:     tagDecoderOptions,
		TagEncoderPoolOptions: tagEncoderPoolOptions,
		TagDecoderPoolOptions: tagDecoderPoolOptions,
		TagOptions:            tagOptions,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create downsampler")
	}

	return downsampler, nil
}

func newDownsamplerAutoMappingRules(
	namespaces []m3.ClusterNamespace,
) ([]downsample.MappingRule, error) {
	var autoMappingRules []downsample.MappingRule
	for _, namespace := range namespaces {
		opts := namespace.Options()
		attrs := opts.Attributes()
		if attrs.MetricsType == storage.AggregatedMetricsType {
			downsampleOpts, err := opts.DownsampleOptions()
			if err != nil {
				errFmt := "unable to resolve downsample options for namespace: %v"
				return nil, fmt.Errorf(errFmt, namespace.NamespaceID().String())
			}
			if downsampleOpts.All {
				storagePolicy := policy.NewStoragePolicy(attrs.Resolution,
					xtime.Second, attrs.Retention)
				autoMappingRules = append(autoMappingRules, downsample.MappingRule{
					// NB(r): By default we will apply just keep all last values
					// since coordinator only uses downsampling with Prometheus
					// remote write endpoint.
					// More rich static configuration mapping rules can be added
					// in the future but they are currently not required.
					Aggregations: []aggregation.Type{aggregation.Last},
					Policies:     policy.StoragePolicies{storagePolicy},
				})
			}
		}
	}
	return autoMappingRules, nil
}

func initClusters(
	cfg config.Configuration,
	dbClientCh <-chan client.Client,
	instrumentOpts instrument.Options,
) (m3.Clusters, *pools.PoolWrapper, error) {
	instrumentOpts = instrumentOpts.
		SetMetricsScope(instrumentOpts.MetricsScope().SubScope("m3db-client"))

	var (
		logger      = instrumentOpts.Logger()
		clusters    m3.Clusters
		poolWrapper *pools.PoolWrapper
		err         error
	)
	if len(cfg.Clusters) > 0 {
		clusters, err = cfg.Clusters.NewClusters(instrumentOpts,
			m3.ClustersStaticConfigurationOptions{
				AsyncSessions: true,
			})
		if err != nil {
			return nil, nil, errors.Wrap(err, "unable to connect to clusters")
		}

		poolWrapper = pools.NewPoolsWrapper(pools.BuildIteratorPools())
	} else {
		localCfg := cfg.Local
		if localCfg == nil {
			localCfg = defaultLocalConfiguration
		}

		if dbClientCh == nil {
			return nil, nil, errors.New("no clusters configured and not running local cluster")
		}

		sessionInitChan := make(chan struct{})
		session := m3db.NewAsyncSession(func() (client.Client, error) {
			return <-dbClientCh, nil
		}, sessionInitChan)

		clustersCfg := m3.ClustersStaticConfiguration{
			m3.ClusterStaticConfiguration{
				Namespaces: localCfg.Namespaces,
			},
		}

		clusters, err = clustersCfg.NewClusters(instrumentOpts,
			m3.ClustersStaticConfigurationOptions{
				ProvidedSession: session,
			})
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
	tagOptions models.TagOptions,
	poolWrapper *pools.PoolWrapper,
	readWorkerPool xsync.PooledWorkerPool,
	writeWorkerPool xsync.PooledWorkerPool,
	queryContextOptions models.QueryContextOptions,
	instrumentOpts instrument.Options,
) (storage.Storage, cleanupFn, error) {
	var (
		logger  = instrumentOpts.Logger()
		cleanup = func() error { return nil }
	)
	localStorage, err := m3.NewStorage(clusters, readWorkerPool,
		writeWorkerPool, tagOptions, *cfg.LookbackDuration, instrumentOpts)
	if err != nil {
		return nil, nil, err
	}

	stores := []storage.Storage{localStorage}
	remoteEnabled := false
	if cfg.RPC != nil && cfg.RPC.Enabled {
		logger.Info("rpc enabled")
		server, err := startGRPCServer(localStorage, queryContextOptions,
			poolWrapper, cfg.RPC, instrumentOpts)
		if err != nil {
			return nil, nil, err
		}

		cleanup = func() error {
			server.GracefulStop()
			return nil
		}

		remoteStorages, enabled, err := remoteClient(
			cfg,
			tagOptions,
			poolWrapper,
			readWorkerPool,
			instrumentOpts,
		)
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
		completeTagsFilter, instrumentOpts)
	return fanoutStorage, cleanup, nil
}

func remoteZoneStorage(
	remoteAddresses []string,
	lookbackDuration time.Duration,
	tagOptions models.TagOptions,
	poolWrapper *pools.PoolWrapper,
	readWorkerPool xsync.PooledWorkerPool,
) (storage.Storage, error) {
	if len(remoteAddresses) == 0 {
		// No addresses; skip.
		return nil, nil
	}

	client, err := tsdbRemote.NewGRPCClient(
		remoteAddresses,
		poolWrapper,
		readWorkerPool,
		tagOptions,
		lookbackDuration,
	)
	if err != nil {
		return nil, err
	}

	remoteStorage := remote.NewStorage(client)
	return remoteStorage, nil
}

func remoteClient(
	cfg config.Configuration,
	tagOptions models.TagOptions,
	poolWrapper *pools.PoolWrapper,
	readWorkerPool xsync.PooledWorkerPool,
	instrumentOpts instrument.Options,
) ([]storage.Storage, bool, error) {
	if cfg.RPC == nil {
		return nil, false, nil
	}

	var (
		lookback        = *cfg.LookbackDuration
		rpc             = cfg.RPC
		remotes         = rpc.Remotes
		zoneCount       = len(remotes)
		listenAddresses = rpc.RemoteListenAddresses
		logger          = instrumentOpts.Logger()
	)

	// NB: if no remote zones are provided, fallback to legacy listen addresses
	// for determining remote clients. This legacy approach is capped to a single
	// zone.
	if zoneCount == 0 {
		if len(listenAddresses) == 0 {
			return nil, false, nil
		}

		logger.Info(
			"creating RPC client with remote",
			zap.Strings("addresses", listenAddresses),
		)

		remote, err := remoteZoneStorage(listenAddresses, lookback, tagOptions,
			poolWrapper, readWorkerPool)

		if err != nil {
			return nil, false, err
		}

		return []storage.Storage{remote}, true, nil
	}

	remoteStores := make([]storage.Storage, 0, zoneCount)
	for _, zone := range remotes {
		logger.Info(
			"creating RPC client with remotes",
			zap.String("name", zone.Name),
			zap.Strings("addresses", zone.RemoteListenAddresses),
		)

		remote, err := remoteZoneStorage(zone.RemoteListenAddresses, lookback,
			tagOptions, poolWrapper, readWorkerPool)
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
	cfg *config.RPCConfiguration,
	instrumentOpts instrument.Options,
) (*grpc.Server, error) {
	logger := instrumentOpts.Logger()

	logger.Info("creating gRPC server")
	server := tsdbRemote.NewGRPCServer(storage,
		queryContextOptions, poolWrapper, instrumentOpts)
	if cfg.ReflectionEnabled {
		reflection.Register(server)
	}

	logger.Info("gRPC server reflection configured",
		zap.Bool("enabled", cfg.ReflectionEnabled))

	listener, err := net.Listen("tcp", cfg.ListenAddress)
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
	cfg *config.CarbonConfiguration,
	iOpts instrument.Options,
	logger *zap.Logger,
	m3dbClusters m3.Clusters,
	downsamplerAndWriter ingest.DownsamplerAndWriter,
) (xserver.Server, bool) {
	ingesterCfg := cfg.Ingester
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

	// Validate provided rules.
	var (
		clusterNamespaces = m3dbClusters.ClusterNamespaces()
		rules             = ingestcarbon.CarbonIngesterRules{
			Rules: ingesterCfg.RulesOrDefault(clusterNamespaces),
		}
	)
	for _, rule := range rules.Rules {
		// Sort so we can detect duplicates.
		sort.Slice(rule.Policies, func(i, j int) bool {
			if rule.Policies[i].Resolution == rule.Policies[j].Resolution {
				return rule.Policies[i].Retention < rule.Policies[j].Retention
			}

			return rule.Policies[i].Resolution < rule.Policies[j].Resolution
		})

		var lastPolicy config.CarbonIngesterStoragePolicyConfiguration
		for i, policy := range rule.Policies {
			if i > 0 && policy == lastPolicy {
				logger.Fatal(
					"cannot include the same storage policy multiple times for a single carbon ingestion rule",
					zap.String("pattern", rule.Pattern), zap.Duration("resolution", policy.Resolution), zap.Duration("retention", policy.Retention))
			}

			if i > 0 && !rule.Aggregation.EnabledOrDefault() && policy.Resolution != lastPolicy.Resolution {
				logger.Fatal(
					"cannot include multiple storage policies with different resolutions if aggregation is disabled",
					zap.String("pattern", rule.Pattern), zap.Duration("resolution", policy.Resolution), zap.Duration("retention", policy.Retention))
			}

			_, ok := m3dbClusters.AggregatedClusterNamespace(m3.RetentionResolution{
				Resolution: policy.Resolution,
				Retention:  policy.Retention,
			})

			// Disallow storage policies that don't match any known M3DB clusters.
			if !ok {
				logger.Fatal(
					"cannot enable carbon ingestion without a corresponding aggregated M3DB namespace",
					zap.String("resolution", policy.Resolution.String()), zap.String("retention", policy.Retention.String()))
			}
		}
	}

	if len(rules.Rules) == 0 {
		logger.Warn("no carbon ingestion rules were provided and no aggregated M3DB namespaces exist, carbon metrics will not be ingested")
		return nil, false
	}

	if len(ingesterCfg.Rules) == 0 {
		logger.Info("no carbon ingestion rules were provided, all carbon metrics will be written to all aggregated M3DB namespaces")
	}

	// Create ingester.
	ingester, err := ingestcarbon.NewIngester(
		downsamplerAndWriter, rules, ingestcarbon.Options{
			Debug:             ingesterCfg.Debug,
			InstrumentOptions: carbonIOpts,
			WorkerPool:        workerPool,
		})
	if err != nil {
		logger.Fatal("unable to create carbon ingester", zap.Error(err))
	}

	// Start server.
	var (
		serverOpts          = xserver.NewOptions().SetInstrumentOptions(carbonIOpts)
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

	return carbonServer, true
}

func newDownsamplerAndWriter(storage storage.Storage, downsampler downsample.Downsampler) (ingest.DownsamplerAndWriter, error) {
	// Make sure the downsampler and writer gets its own PooledWorkerPool and that its not shared with any other
	// codepaths because PooledWorkerPools can deadlock if used recursively.
	downAndWriterWorkerPoolOpts := xsync.NewPooledWorkerPoolOptions().
		SetGrowOnDemand(true).
		SetKillWorkerProbability(0.001)
	downAndWriteWorkerPool, err := xsync.NewPooledWorkerPool(
		defaultDownsamplerAndWriterWorkerPoolSize, downAndWriterWorkerPoolOpts)
	if err != nil {
		return nil, err
	}
	downAndWriteWorkerPool.Init()

	return ingest.NewDownsamplerAndWriter(storage, downsampler, downAndWriteWorkerPool), nil
}
