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

	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/config"
	dbconfig "github.com/m3db/m3db/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3db/src/coordinator/api/v1/httpd"
	m3dbcluster "github.com/m3db/m3db/src/coordinator/cluster/m3db"
	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/policy/filter"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/coordinator/storage/fanout"
	"github.com/m3db/m3db/src/coordinator/storage/local"
	"github.com/m3db/m3db/src/coordinator/storage/remote"
	"github.com/m3db/m3db/src/coordinator/stores/m3db"
	tsdbRemote "github.com/m3db/m3db/src/coordinator/tsdb/remote"
	"github.com/m3db/m3db/src/coordinator/util/logging"
	"github.com/m3db/m3db/src/dbnode/client"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	defaultWorkerPoolCount = 4096
	defaultWorkerPoolSize  = 20
)

var (
	defaultLocalConfiguration = &config.LocalConfiguration{
		Namespace: "default",
		Retention: 2 * 24 * time.Hour,
	}
)

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

	scope, _, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatal("could not connect to metrics", zap.Any("error", err))
	}

	var clusterClientCh <-chan clusterclient.Client
	if runOpts.ClusterClient != nil {
		clusterClientCh = runOpts.ClusterClient
	}

	if clusterClientCh == nil && cfg.ClusterManagement != nil {
		// We resolved an etcd configuration for cluster management endpoints
		etcdCfg := cfg.ClusterManagement.Etcd
		clusterSvcClientOpts := etcdCfg.NewOptions()
		clusterClient, err := etcd.NewConfigServiceClient(clusterSvcClientOpts)
		if err != nil {
			logger.Fatal("unable to create cluster management etcd client", zap.Any("error", err))
		}

		clusterClientSendableCh := make(chan clusterclient.Client, 1)
		clusterClientSendableCh <- clusterClient
		clusterClientCh = clusterClientSendableCh
	}

	var clusters local.Clusters
	if len(cfg.Clusters) > 0 {
		clusters, err = cfg.Clusters.NewClusters()
		if err != nil {
			logger.Fatal("unable to connect to clusters", zap.Any("error", err))
		}
	} else {
		localCfg := cfg.Local
		if localCfg == nil {
			localCfg = defaultLocalConfiguration
		}
		dbClientCh := runOpts.DBClient
		if localCfg == nil || dbClientCh == nil {
			logger.Fatal("not running local embedded")
		}
		session := m3db.NewAsyncSession(func() (client.Client, error) {
			return <-dbClientCh, nil
		}, nil)
		clusters, err = local.NewClusters(local.UnaggregatedClusterNamespaceDefinition{
			NamespaceID: ident.StringID(localCfg.Namespace),
			Session:     session,
			Retention:   localCfg.Retention,
		}, nil)
		if err != nil {
			logger.Fatal("unable to connect to clusters", zap.Any("error", err))
		}
	}

	workerPoolCount := cfg.DecompressWorkerPoolCount
	if workerPoolCount == 0 {
		workerPoolCount = defaultWorkerPoolCount
	}

	workerPoolSize := cfg.DecompressWorkerPoolSize
	if workerPoolSize == 0 {
		workerPoolSize = defaultWorkerPoolSize
	}

	instrumentOptions := instrument.NewOptions().
		SetZapLogger(logger).
		SetMetricsScope(scope.SubScope("series-decompression-pool"))

	poolOptions := pool.NewObjectPoolOptions().
		SetSize(workerPoolCount).
		SetInstrumentOptions(instrumentOptions)

	objectPool := pool.NewObjectPool(poolOptions)
	objectPool.Init(func() interface{} {
		workerPool := xsync.NewWorkerPool(workerPoolSize)
		workerPool.Init()
		return workerPool
	})

	fanoutStorage, storageCleanup := setupStorages(logger, clusters, cfg, objectPool)
	defer storageCleanup()

	var clusterClient clusterclient.Client
	if clusterClientCh != nil {
		// Only use a cluster client if we are going to receive one, that
		// way passing nil to httpd NewHandler disables the endpoints entirely
		clusterClient = m3dbcluster.NewAsyncClient(func() (clusterclient.Client, error) {
			return <-clusterClientCh, nil
		}, nil)
	}

	handler, err := httpd.NewHandler(fanoutStorage, executor.NewEngine(fanoutStorage),
		clusterClient, cfg, runOpts.DBConfig, scope)
	if err != nil {
		logger.Fatal("unable to set up handlers", zap.Any("error", err))
	}
	handler.RegisterRoutes()

	logger.Info("starting server", zap.String("address", cfg.ListenAddress))
	go func() {
		if err := http.ListenAndServe(cfg.ListenAddress, handler.Router); err != nil {
			logger.Fatal("unable to serve on listen address",
				zap.Any("address", cfg.ListenAddress), zap.Any("error", err))
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	if err := clusters.Close(); err != nil {
		logger.Fatal("unable to close M3DB cluster sessions", zap.Any("error", err))
	}
}

func setupStorages(logger *zap.Logger, clusters local.Clusters, cfg config.Configuration, workerPool pool.ObjectPool) (storage.Storage, func()) {
	cleanup := func() {}

	localStorage := local.NewStorage(clusters, workerPool)
	stores := []storage.Storage{localStorage}
	remoteEnabled := false
	if cfg.RPC != nil && cfg.RPC.Enabled {
		logger.Info("rpc enabled")
		server := startGrpcServer(logger, localStorage, cfg.RPC)
		cleanup = func() {
			server.GracefulStop()
		}

		if remotes := cfg.RPC.RemoteListenAddresses; len(remotes) > 0 {
			client, err := tsdbRemote.NewGrpcClient(remotes)
			if err != nil {
				logger.Fatal("unable to start remote clients for addresses", zap.Any("error", err))
			}

			stores = append(stores, remote.NewStorage(client))
			remoteEnabled = true
		}
	}

	readFilter := filter.LocalOnly
	if remoteEnabled {
		readFilter = filter.AllowAll
	}

	fanoutStorage := fanout.NewStorage(stores, readFilter, filter.LocalOnly)
	return fanoutStorage, cleanup
}

func startGrpcServer(logger *zap.Logger, storage storage.Storage, cfg *config.RPCConfiguration) *grpc.Server {
	logger.Info("creating gRPC server")
	server := tsdbRemote.CreateNewGrpcServer(storage)
	waitForStart := make(chan struct{})
	go func() {
		logger.Info("starting gRPC server on port", zap.Any("rpc", cfg.ListenAddress))
		err := tsdbRemote.StartNewGrpcServer(server, cfg.ListenAddress, waitForStart)
		if err != nil {
			logger.Fatal("unable to start gRPC server", zap.Any("error", err))
		}
	}()
	<-waitForStart
	return server
}
