// Copyright (c) 2016 Uber Technologies, Inc.
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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	m3aggregator "github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/serve"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/etcd"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

const (
	gracefulShutdownTimeout = 15 * time.Second
)

var (
	configFile = flag.String("f", "", "configuration file")
)

func main() {
	flag.Parse()

	if len(*configFile) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// Set globals for etcd related packages.
	etcd.SetGlobals()

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, *configFile, xconfig.Options{}); err != nil {
		fmt.Printf("error loading config file: %v\n", err)
		os.Exit(1)
	}

	// Create logger and metrics scope.
	logger, err := cfg.Logging.BuildLogger()
	if err != nil {
		fmt.Printf("error creating logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	xconfig.WarnOnDeprecation(cfg, logger)

	scope, closer, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatal("error creating metrics root scope", zap.Error(err))
	}
	defer closer.Close()
	instrumentOpts := instrument.NewOptions().
		SetLogger(logger).
		SetMetricsScope(scope).
		SetMetricsSamplingRate(cfg.Metrics.SampleRate()).
		SetReportInterval(cfg.Metrics.ReportInterval())

	// Create the raw TCP server options.
	rawTCPAddr := cfg.RawTCP.ListenAddress
	rawTCPServerScope := scope.SubScope("rawtcp-server").Tagged(map[string]string{"server": "rawtcp"})
	iOpts := instrumentOpts.SetMetricsScope(rawTCPServerScope)
	rawTCPServerOpts := cfg.RawTCP.NewServerOptions(iOpts)

	// Create the http server options.
	httpAddr := cfg.HTTP.ListenAddress
	httpServerOpts := cfg.HTTP.NewServerOptions()

	// Create the kv client.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("kv-client"))
	client, err := cfg.KVClient.NewKVClient(iOpts)
	if err != nil {
		logger.Fatal("error creating the kv client", zap.Error(err))
	}

	// Create the runtime options manager.
	runtimeOptsManager := cfg.RuntimeOptions.NewRuntimeOptionsManager()

	// Create the aggregator.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("aggregator"))
	aggregatorOpts, err := cfg.Aggregator.NewAggregatorOptions(rawTCPAddr, client, runtimeOptsManager, iOpts)
	if err != nil {
		logger.Fatal("error creating aggregator options", zap.Error(err))
	}
	aggregator := m3aggregator.NewAggregator(aggregatorOpts)
	if err := aggregator.Open(); err != nil {
		logger.Fatal("error opening the aggregator", zap.Error(err))
	}

	// Watch runtime option changes after aggregator is open.
	placementManager := aggregatorOpts.PlacementManager()
	cfg.RuntimeOptions.WatchRuntimeOptionChanges(client, runtimeOptsManager, placementManager, logger)

	doneCh := make(chan struct{})
	closedCh := make(chan struct{})
	go func() {
		if err := serve.Serve(
			rawTCPAddr,
			rawTCPServerOpts,
			httpAddr,
			httpServerOpts,
			aggregator,
			doneCh,
			instrumentOpts,
		); err != nil {
			logger.Fatal("could not start serving traffic", zap.Error(err))
		}
		logger.Debug("server closed")
		close(closedCh)
	}()

	// Handle interrupts.
	logger.Warn("interrupt", zap.Any("signal", interrupt()))

	close(doneCh)

	select {
	case <-closedCh:
		logger.Info("server closed clean")
	case <-time.After(gracefulShutdownTimeout):
		logger.Info("server closed due to timeout", zap.Duration("timeout", gracefulShutdownTimeout))
	}
}

func interrupt() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
