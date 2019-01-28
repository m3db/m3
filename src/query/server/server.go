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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest/carbon"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/policy"
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
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/clock"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xserver "github.com/m3db/m3x/server"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"

	"github.com/pkg/errors"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	defaultLocalConfiguration = &config.LocalConfiguration{
		Namespace: "default",
		Retention: 2 * 24 * time.Hour,
	}

	defaultCarbonIngesterWorkerPoolSize = 1024
)

type cleanupFn func() error

// RunOptions provides options for running the server
// with backwards compatibility if only solely adding fields.
type RunOptions struct {
	// ConfigFile is the config file to use.
	ConfigFile string

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
}

// Run runs the server programmatically given a filename for the configuration file.
func Run(runOpts RunOptions) {
	rand.Seed(time.Now().UnixNano())

	var cfg config.Configuration
	if runOpts.ConfigFile != "" {
		if err := xconfig.LoadFile(&cfg, runOpts.ConfigFile, xconfig.Options{}); err != nil {
			fmt.Fprintf(os.Stderr, "unable to load %s: %v", runOpts.ConfigFile, err)
			os.Exit(1)
		}
	} else {
		cfg = runOpts.Config
	}

	logging.InitWithCores(nil)
	ctx := context.Background()
	logger := logging.WithContext(ctx)
	defer logger.Sync()

	scope, closer, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatal("could not connect to metrics", zap.Any("error", err))
	}
	instrumentOptions := instrument.NewOptions().
		SetMetricsScope(scope).
		SetZapLogger(logger)

	// Close metrics scope
	defer func() {
		logger.Info("closing metrics scope")
		if err := closer.Close(); err != nil {
			logger.Error("unable to close metrics scope", zap.Error(err))
		}
	}()

	var (
		backendStorage storage.Storage
		clusterClient  clusterclient.Client
		downsampler    downsample.Downsampler
		enabled        bool
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

	var (
		m3dbClusters    m3.Clusters
		m3dbPoolWrapper *pools.PoolWrapper
	)
	// For grpc backend, we need to setup only the grpc client and a storage accompanying that client.
	// For m3db backend, we need to make connections to the m3db cluster which generates a session and use the storage with the session.
	if cfg.Backend == config.GRPCStorageType {
		poolWrapper := pools.NewPoolsWrapper(pools.BuildIteratorPools())
		backendStorage, enabled, err = remoteClient(
			cfg,
			tagOptions,
			poolWrapper,
			readWorkerPool,
		)
		if err != nil {
			logger.Fatal("unable to setup grpc backend", zap.Error(err))
		}
		if !enabled {
			logger.Fatal("need remote clients for grpc backend")
		}

		logger.Info("setup grpc backend")
	} else {
		m3dbClusters, m3dbPoolWrapper, err = initClusters(cfg, runOpts.DBClient, logger)
		if err != nil {
			logger.Fatal("unable to init clusters", zap.Error(err))
		}

		var cleanup cleanupFn
		backendStorage, clusterClient, downsampler, cleanup, err = newM3DBStorage(
			runOpts,
			cfg,
			tagOptions,
			logger,
			m3dbClusters,
			m3dbPoolWrapper,
			instrumentOptions,
			readWorkerPool,
			writeWorkerPool,
		)
		if err != nil {
			logger.Fatal("unable to setup m3db backend", zap.Error(err))
		}
		defer cleanup()
	}

	engine := executor.NewEngine(backendStorage, scope.SubScope("engine"))

	promDownsamplerAndWriter := ingest.NewDownsamplerAndWriter(backendStorage, downsampler)
	handler, err := httpd.NewHandler(promDownsamplerAndWriter, tagOptions, engine,
		m3dbClusters, clusterClient, cfg, runOpts.DBConfig, scope)
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
		if err := srv.Shutdown(ctx); err != nil {
			logger.Error("error closing server", zap.Error(err))
		}
	}()

	go func() {
		logger.Info("starting server", zap.String("address", listenAddress))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server error while listening",
				zap.String("address", listenAddress), zap.Error(err))
		}
	}()

	if cfg.Ingest != nil {
		logger.Info("starting m3msg server")
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

	carbonIngestConfig := cfg.Carbon.Ingestion
	if carbonIngestConfig.EnabledOrDefault() {
		logger.Info("carbon ingestion enabled")

		var (
			carbonIOpts = instrumentOptions.SetMetricsScope(
				instrumentOptions.MetricsScope().SubScope("ingest-carbon"))
			carbonWorkerPoolOpts xsync.PooledWorkerPoolOptions
			carbonWorkerPoolSize int
		)
		if carbonIngestConfig.MaxConcurrency > 0 {
			// Use a bounded worker pool if they requested a specific maximum concurrency.
			carbonWorkerPoolOpts = xsync.NewPooledWorkerPoolOptions().
				SetGrowOnDemand(false).
				SetInstrumentOptions(carbonIOpts)
			carbonWorkerPoolSize = carbonIngestConfig.MaxConcurrency
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

		// Create a new downsampler and writer because we don't want the carbon ingester to write
		// any data unaggregated, so we pass nil for storage.Storage.
		carbonIngestDownsamplerAndWriter := ingest.NewDownsamplerAndWriter(nil, downsampler)
		ingester, err := ingestcarbon.NewIngester(carbonIngestDownsamplerAndWriter, ingestcarbon.Options{
			InstrumentOptions: carbonIOpts,
			WorkerPool:        workerPool,
			Timeout:           carbonIngestConfig.TimeoutOrDefault(),
		})
		if err != nil {
			logger.Fatal("unable to create carbon ingester", zap.Error(err))
		}

		var (
			serverOpts          = xserver.NewOptions().SetInstrumentOptions(carbonIOpts)
			carbonListenAddress = carbonIngestConfig.ListenAddressOrDefault()
			carbonServer        = xserver.NewServer(carbonListenAddress, ingester, serverOpts)
		)

		logger.Info("starting carbon ingestion server", zap.String("listenAddress", carbonListenAddress))
		err = carbonServer.ListenAndServe()
		if err != nil {
			logger.Fatal("unable to start carbon ingestion server at listen address",
				zap.String("listenAddress", carbonListenAddress), zap.Error(err))
		}
		logger.Info("started carbon ingestion server", zap.String("listenAddress", carbonListenAddress))
	}

	var interruptCh <-chan error = make(chan error)
	if runOpts.InterruptCh != nil {
		interruptCh = runOpts.InterruptCh
	}

	var interruptErr error
	if runOpts.DBConfig != nil {
		interruptErr = <-interruptCh
	} else {
		// Only use this if running standalone, as otherwise it will
		// obfuscate signal channel for the db
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		select {
		case sig := <-sigChan:
			interruptErr = fmt.Errorf("%v", sig)
		case interruptErr = <-interruptCh:
		}
	}

	logger.Info("interrupt", zap.String("cause", interruptErr.Error()))
}

// make connections to the m3db cluster(s) and generate sessions for those clusters along with the storage
func newM3DBStorage(
	runOpts RunOptions,
	cfg config.Configuration,
	tagOptions models.TagOptions,
	logger *zap.Logger,
	clusters m3.Clusters,
	poolWrapper *pools.PoolWrapper,
	instrumentOptions instrument.Options,
	readWorkerPool xsync.PooledWorkerPool,
	writeWorkerPool xsync.PooledWorkerPool,
) (storage.Storage, clusterclient.Client, downsample.Downsampler, cleanupFn, error) {
	var (
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

	fanoutStorage, storageCleanup, err := newStorages(
		logger,
		clusters,
		cfg,
		tagOptions,
		poolWrapper,
		readWorkerPool,
		writeWorkerPool,
	)
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
	logger *zap.Logger,
) (m3.Clusters, *pools.PoolWrapper, error) {
	var (
		clusters    m3.Clusters
		poolWrapper *pools.PoolWrapper
		err         error
	)

	if len(cfg.Clusters) > 0 {
		opts := m3.ClustersStaticConfigurationOptions{
			AsyncSessions: true,
		}
		clusters, err = cfg.Clusters.NewClusters(opts)
		if err != nil {
			return nil, nil, errors.Wrap(err, "unable to connect to clusters")
		}
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

		clusters, err = m3.NewClusters(m3.UnaggregatedClusterNamespaceDefinition{
			NamespaceID: ident.StringID(localCfg.Namespace),
			Session:     session,
			Retention:   localCfg.Retention,
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
	logger *zap.Logger,
	clusters m3.Clusters,
	cfg config.Configuration,
	tagOptions models.TagOptions,
	poolWrapper *pools.PoolWrapper,
	readWorkerPool xsync.PooledWorkerPool,
	writeWorkerPool xsync.PooledWorkerPool,
) (storage.Storage, cleanupFn, error) {
	cleanup := func() error { return nil }

	localStorage := m3.NewStorage(
		clusters,
		readWorkerPool,
		writeWorkerPool,
		tagOptions,
	)
	stores := []storage.Storage{localStorage}
	remoteEnabled := false
	if cfg.RPC != nil && cfg.RPC.Enabled {
		logger.Info("rpc enabled")
		server, err := startGrpcServer(logger, localStorage, poolWrapper, cfg.RPC)
		if err != nil {
			return nil, nil, err
		}

		cleanup = func() error {
			server.GracefulStop()
			return nil
		}

		remoteStorage, enabled, err := remoteClient(
			cfg,
			tagOptions,
			poolWrapper,
			readWorkerPool,
		)
		if err != nil {
			return nil, nil, err
		}

		if enabled {
			stores = append(stores, remoteStorage)
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

	fanoutStorage := fanout.NewStorage(stores, readFilter, writeFilter, completeTagsFilter)
	return fanoutStorage, cleanup, nil
}

func remoteClient(
	cfg config.Configuration,
	tagOptions models.TagOptions,
	poolWrapper *pools.PoolWrapper,
	readWorkerPool xsync.PooledWorkerPool,
) (storage.Storage, bool, error) {
	if cfg.RPC == nil {
		return nil, false, nil
	}

	if remotes := cfg.RPC.RemoteListenAddresses; len(remotes) > 0 {
		client, err := tsdbRemote.NewGRPCClient(
			remotes,
			poolWrapper,
			readWorkerPool,
			tagOptions,
		)
		if err != nil {
			return nil, false, err
		}

		remoteStorage := remote.NewStorage(client)
		return remoteStorage, true, nil
	}

	return nil, false, nil
}

func startGrpcServer(
	logger *zap.Logger,
	storage m3.Storage,
	poolWrapper *pools.PoolWrapper,
	cfg *config.RPCConfiguration,
) (*grpc.Server, error) {
	logger.Info("creating gRPC server")
	server := tsdbRemote.CreateNewGrpcServer(storage, poolWrapper)
	waitForStart := make(chan struct{})
	var startErr error
	go func() {
		logger.Info("starting gRPC server on port", zap.String("rpc", cfg.ListenAddress))
		err := tsdbRemote.StartNewGrpcServer(server, cfg.ListenAddress, waitForStart)
		// TODO: consider removing logger.Fatal here and pass back error through a channel
		if err != nil {
			startErr = errors.Wrap(err, "unable to start gRPC server")
		}
	}()
	<-waitForStart
	return server, startErr
}
