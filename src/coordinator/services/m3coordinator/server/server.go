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
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/policy/filter"
	"github.com/m3db/m3db/src/coordinator/services/m3coordinator/config"
	"github.com/m3db/m3db/src/coordinator/services/m3coordinator/httpd"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/coordinator/storage/fanout"
	"github.com/m3db/m3db/src/coordinator/storage/local"
	"github.com/m3db/m3db/src/coordinator/storage/remote"
	"github.com/m3db/m3db/src/coordinator/stores/m3db"
	tsdbRemote "github.com/m3db/m3db/src/coordinator/tsdb/remote"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3db/client"
	xconfig "github.com/m3db/m3x/config"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	namespace      = "metrics"
	resolution     = time.Minute
	configLoadOpts = xconfig.Options{
		DisableUnmarshalStrict: false,
		DisableValidate:        false,
	}
)

// RunOptions provides options for running the server
// with backwards compatibility if only solely adding fields.
type RunOptions struct {
	ConfigFile           string
	ListenAddress        string
	RPCEnabled           bool
	RPCAddress           string
	Remotes              []string
	MaxConcurrentQueries int
	QueryTimeout         time.Duration

	// DBClient is the M3DB client to use instead of instantiating a new one from config
	DBClient client.Client
}

// Run runs the server programmatically given a filename for the configuration file.
func Run(runOpts RunOptions) {
	rand.Seed(time.Now().UnixNano())

	logging.InitWithCores(nil)
	ctx := context.Background()
	logger := logging.WithContext(ctx)
	defer logger.Sync()

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, runOpts.ConfigFile, configLoadOpts); err != nil {
		logger.Fatal("unable to load", zap.String("configFile", runOpts.ConfigFile), zap.Any("error", err))
	}

	m3dbClientOpts := cfg.M3DBClientCfg

	var (
		clusterClient m3clusterClient.Client
		err           error
	)
	if m3dbClientOpts.EnvironmentConfig.Service != nil {
		clusterSvcClientOpts := m3dbClientOpts.EnvironmentConfig.Service.NewOptions()
		clusterClient, err = etcd.NewConfigServiceClient(clusterSvcClientOpts)
		if err != nil {
			logger.Fatal("unable to create etcd client", zap.Any("error", err))
		}
	}

	dbClient := runOpts.DBClient
	if dbClient == nil {
		dbClient, err = m3dbClientOpts.NewClient(client.ConfigurationParameters{})
		if err != nil {
			logger.Fatal("unable to create m3db client", zap.Any("error", err))
		}
	}

	session := m3db.NewAsyncSession(dbClient, nil)

	fanoutStorage, storageCleanup := setupStorages(logger, session, runOpts)
	defer storageCleanup()

	handler, err := httpd.NewHandler(fanoutStorage, executor.NewEngine(fanoutStorage), clusterClient, cfg)
	if err != nil {
		logger.Fatal("unable to set up handlers", zap.Any("error", err))
	}
	handler.RegisterRoutes()

	logger.Info("starting server", zap.String("address", runOpts.ListenAddress))
	go http.ListenAndServe(runOpts.ListenAddress, handler.Router)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	if err := session.Close(); err != nil {
		logger.Fatal("unable to close m3db client session", zap.Any("error", err))
	}
}

func startGrpcServer(logger *zap.Logger, storage storage.Storage, flags RunOptions) *grpc.Server {
	logger.Info("creating gRPC server")
	server := tsdbRemote.CreateNewGrpcServer(storage)
	waitForStart := make(chan struct{})
	go func() {
		logger.Info("starting gRPC server on port", zap.Any("rpc", flags.RPCAddress))
		err := tsdbRemote.StartNewGrpcServer(server, flags.RPCAddress, waitForStart)
		if err != nil {
			logger.Fatal("unable to start gRPC server", zap.Any("error", err))
		}
	}()
	<-waitForStart
	return server
}

func setupStorages(logger *zap.Logger, session client.Session, flags RunOptions) (storage.Storage, func()) {
	cleanup := func() {}
	localStorage := local.NewStorage(session, namespace, resolution)
	stores := []storage.Storage{localStorage}
	if flags.RPCEnabled {
		logger.Info("rpc enabled")
		server := startGrpcServer(logger, localStorage, flags)
		cleanup = func() {
			server.GracefulStop()
		}
		if len(flags.Remotes) > 0 {
			client, err := tsdbRemote.NewGrpcClient(flags.Remotes)
			if err != nil {
				logger.Fatal("unable to start remote clients for addresses", zap.Any("error", err))
			}
			stores = append(stores, remote.NewStorage(client))
		}
	}
	fanoutStorage := fanout.NewStorage(stores, filter.LocalOnly, filter.LocalOnly)
	return fanoutStorage, cleanup
}
