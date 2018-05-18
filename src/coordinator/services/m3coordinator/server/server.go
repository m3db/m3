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
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/httpd"
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

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	namespace  = "metrics"
	resolution = time.Minute
)

// RunOptions provides options for running the server
// with backwards compatibility if only solely adding fields.
type RunOptions struct {
	// ConfigFile is the config file to use.
	ConfigFile string

	// Config is an alternate way to provide configuration and will be used
	// instead of parsing ConfigFile if ConfigFile is not specified.
	Config config.Configuration

	// DBClient is the M3DB client to use instead of instantiating a new one
	// from client config.
	DBClient client.Client

	// ClusterClient is the M3DB cluster client to use instead of instantiating
	// one from the client config.
	ClusterClient clusterclient.Client
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

	var clusterClient clusterclient.Client
	if runOpts.ClusterClient != nil {
		clusterClient = runOpts.ClusterClient
	}

	var dbClient client.Client
	if runOpts.DBClient != nil {
		dbClient = runOpts.DBClient
	}

	if dbClient == nil {
		// If not provided create cluster client and DB client
		clientCfg := cfg.DBClient
		if clientCfg == nil {
			logger.Fatal("missing coordinator m3db client configuration")
		}

		var err error
		if clientCfg.EnvironmentConfig.Service != nil {
			clusterSvcClientOpts := clientCfg.EnvironmentConfig.Service.NewOptions()
			clusterClient, err = etcd.NewConfigServiceClient(clusterSvcClientOpts)
			if err != nil {
				logger.Fatal("unable to create etcd client", zap.Any("error", err))
			}
		}

		dbClient, err = clientCfg.NewClient(client.ConfigurationParameters{})
		if err != nil {
			logger.Fatal("unable to create m3db client", zap.Any("error", err))
		}
	}

	session := m3db.NewAsyncSession(dbClient, nil)

	fanoutStorage, storageCleanup := setupStorages(logger, session, cfg)
	defer storageCleanup()

	handler, err := httpd.NewHandler(fanoutStorage, executor.NewEngine(fanoutStorage),
		clusterClient, cfg)
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
	if err := session.Close(); err != nil {
		logger.Fatal("unable to close m3db client session", zap.Any("error", err))
	}
}

func setupStorages(logger *zap.Logger, session client.Session, cfg config.Configuration) (storage.Storage, func()) {
	cleanup := func() {}
	localStorage := local.NewStorage(session, namespace, resolution)
	stores := []storage.Storage{localStorage}
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
		}
	}
	fanoutStorage := fanout.NewStorage(stores, filter.LocalOnly, filter.LocalOnly)
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
