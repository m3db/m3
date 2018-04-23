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

package main

import (
	"context"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/m3db/m3coordinator/executor"
	"github.com/m3db/m3coordinator/policy/filter"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/services/m3coordinator/config"
	"github.com/m3db/m3coordinator/services/m3coordinator/httpd"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/storage/fanout"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/storage/remote"
	tsdbRemote "github.com/m3db/m3coordinator/tsdb/remote"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3metrics/policy"
	xconfig "github.com/m3db/m3x/config"
	xtime "github.com/m3db/m3x/time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	namespace = "metrics"
)

type m3config struct {
	configFile           string
	listenAddress        string
	rpcEnabled           bool
	rpcAddress           string
	remotes              []string
	maxConcurrentQueries int
	queryTimeout         time.Duration
}

func main() {
	rand.Seed(time.Now().UnixNano())

	logging.InitWithCores(nil)
	ctx := context.Background()
	logger := logging.WithContext(ctx)
	defer logger.Sync()

	flags := parseFlags(logger)

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, flags.configFile); err != nil {
		logger.Fatal("unable to load", zap.String("configFile", flags.configFile), zap.Any("error", err))
	}

	m3dbClientOpts := cfg.M3DBClientCfg
	m3dbClient, err := m3dbClientOpts.NewClient(client.ConfigurationParameters{})
	if err != nil {
		logger.Fatal("unable to create m3db client", zap.Any("error", err))
	}

	session, err := m3dbClient.NewSession()
	if err != nil {
		logger.Fatal("unable to create m3db client session", zap.Any("error", err))
	}

	fanoutStorage, storageCleanup := setupStorages(logger, session, flags)
	defer storageCleanup()

	var clusterClient m3clusterClient.Client
	if m3dbClientOpts.EnvironmentConfig.Service != nil {
		clusterSvcClientOpts := m3dbClientOpts.EnvironmentConfig.Service.NewOptions()
		clusterClient, err = etcd.NewConfigServiceClient(clusterSvcClientOpts)
		if err != nil {
			logger.Fatal("unable to create etcd client", zap.Any("error", err))
		}
	}

	handler, err := httpd.NewHandler(fanoutStorage, executor.NewEngine(fanoutStorage), clusterClient)
	if err != nil {
		logger.Fatal("unable to set up handlers", zap.Any("error", err))
	}
	handler.RegisterRoutes()

	logger.Info("starting server", zap.String("address", flags.listenAddress))
	go http.ListenAndServe(flags.listenAddress, handler.Router)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	if err := session.Close(); err != nil {
		logger.Fatal("unable to close m3db client session", zap.Any("error", err))
	}
}

func parseFlags(logger *zap.Logger) *m3config {
	cfg := m3config{}
	a := kingpin.New(filepath.Base(os.Args[0]), "M3Coordinator")

	a.Version("1.0")

	a.HelpFlag.Short('h')

	a.Flag("config.file", "M3Coordinator configuration file path.").
		Default("coordinator.yml").StringVar(&cfg.configFile)

	a.Flag("query.port", "Address to listen on.").
		Default("0.0.0.0:7201").StringVar(&cfg.listenAddress)

	a.Flag("query.timeout", "Maximum time a query may take before being aborted.").
		Default("2m").DurationVar(&cfg.queryTimeout)

	a.Flag("query.max-concurrency", "Maximum number of queries executed concurrently.").
		Default("20").IntVar(&cfg.maxConcurrentQueries)

	a.Flag("rpc.enabled", "True enables remote clients.").
		Default("false").BoolVar(&cfg.rpcEnabled)

	a.Flag("rpc.port", "Address which the remote gRPC server will listen on for outbound connections.").
		Default("0.0.0.0:7288").StringVar(&cfg.rpcAddress)

	a.Flag("rpc.remotes", "Address which the remote gRPC server will listen on for outbound connections.").
		Default("[]").StringsVar(&cfg.remotes)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		logger.Error("unable to parse command line arguments", zap.Any("error", err))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	return &cfg
}

func startGrpcServer(logger *zap.Logger, storage storage.Storage, flags *m3config) *grpc.Server {
	logger.Info("creating gRPC server")
	server := tsdbRemote.CreateNewGrpcServer(storage)
	waitForStart := make(chan struct{})
	go func() {
		logger.Info("starting gRPC server on port", zap.Any("rpc", flags.rpcAddress))
		err := tsdbRemote.StartNewGrpcServer(server, flags.rpcAddress, waitForStart)
		if err != nil {
			logger.Fatal("unable to start gRPC server", zap.Any("error", err))
		}
	}()
	<-waitForStart
	return server
}

// Setup all the storages
func setupStorages(logger *zap.Logger, session client.Session, flags *m3config) (storage.Storage, func()) {
	cleanup := func() {}
	localStorage := local.NewStorage(session, namespace, resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	stores := []storage.Storage{localStorage}
	if flags.rpcEnabled {
		logger.Info("rpc enabled")
		server := startGrpcServer(logger, localStorage, flags)
		cleanup = func() {
			server.GracefulStop()
		}
		if len(flags.remotes) > 0 {
			client, err := tsdbRemote.NewGrpcClient(flags.remotes)
			if err != nil {
				logger.Fatal("unable to start remote clients for addresses", zap.Any("error", err))
			}
			stores = append(stores, remote.NewStorage(client))
		}
	}
	fanoutStorage := fanout.NewStorage(stores, filter.LocalOnly, filter.LocalOnly)
	return fanoutStorage, cleanup
}
