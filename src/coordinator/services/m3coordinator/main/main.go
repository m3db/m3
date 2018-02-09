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

	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/services/m3coordinator/config"
	"github.com/m3db/m3coordinator/services/m3coordinator/httpd"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/tsdb/remote"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3metrics/policy"
	xconfig "github.com/m3db/m3x/config"
	xtime "github.com/m3db/m3x/time"

	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	namespace = "metrics"
)

type m3config struct {
	configFile           string
	listenAddress        string
	rpcAddress           string
	maxConcurrentQueries int
	queryTimeout         time.Duration
}

func main() {
	rand.Seed(time.Now().UnixNano())

	logging.InitWithCores(nil)
	ctx := context.TODO()
	logger := logging.WithContext(ctx)
	defer logger.Sync()

	flags := parseFlags(logger)

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, flags.configFile); err != nil {
		logger.Fatal("Unable to load", zap.String("configFile", flags.configFile), zap.Any("error", err))
	}

	m3dbClientOpts := cfg.M3DBClientCfg
	m3dbClient, err := m3dbClientOpts.NewClient(client.ConfigurationParameters{})
	if err != nil {
		logger.Fatal("Unable to create m3db client", zap.Any("error", err))
	}

	session, err := m3dbClient.NewSession()
	if err != nil {

		logger.Fatal("Unable to create m3db client session", zap.Any("error", err))
	}

	storage := local.NewStorage(session, namespace, resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	handler, err := httpd.NewHandler(storage)
	if err != nil {
		logger.Fatal("Unable to set up handlers", zap.Any("error", err))
	}
	handler.RegisterRoutes()

	logger.Info("Creating gRPC server")
	server := remote.CreateNewGrpcServer(storage)

	go func() {
		logger.Info("Starting gRPC server")
		err = remote.StartNewGrpcServer(server, flags.rpcAddress)
		if err != nil {
			logger.Fatal("Unable to start gRPC server", zap.Any("error", err))
		}
	}()

	logger.Info("Starting server", zap.String("address", flags.listenAddress))
	go http.ListenAndServe(flags.listenAddress, handler.Router)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	if err := session.Close(); err != nil {
		logger.Fatal("Unable to close m3db client session", zap.Any("error", err))
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

	a.Flag("rpc.port", "Address which the remote gRPC server will listen on for outbound connections.").
		Default("0.0.0.0:7288").StringVar(&cfg.rpcAddress)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		logger.Error("Error parsing commandline arguments", zap.Any("error", err))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	return &cfg
}
