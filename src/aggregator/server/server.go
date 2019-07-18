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

package server

import (
	"fmt"
	"os"
	"time"

	m3aggregator "github.com/m3db/m3/src/aggregator/aggregator"
	httpserver "github.com/m3db/m3/src/aggregator/server/http"
	rawtcpserver "github.com/m3db/m3/src/aggregator/server/rawtcp"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/instrument"
	xos "github.com/m3db/m3/src/x/os"

	"go.uber.org/zap"
)

const (
	serverGracefulCloseTimeout = 10 * time.Second
)

// RunOptions provides options for running the server
// with backwards compatibility if only solely adding fields.
type RunOptions struct {
	// ConfigFile is the YAML configuration file to use to run the server.
	ConfigFile string

	// Config is an alternate way to provide configuration and will be used
	// instead of parsing ConfigFile if ConfigFile is not specified.
	Config config.Configuration

	// InterruptCh is a programmatic interrupt channel to supply to
	// interrupt and shutdown the server.
	InterruptCh <-chan error
}

// Run starts serving RPC traffic.
func Run(runOpts RunOptions) {
	var cfg config.Configuration
	if runOpts.ConfigFile != "" {
		var rootCfg config.Configuration
		if err := xconfig.LoadFile(&rootCfg, runOpts.ConfigFile, xconfig.Options{}); err != nil {
			fmt.Fprintf(os.Stderr, "unable to load %s: %v", runOpts.ConfigFile, err)
			os.Exit(1)
		}
		cfg = rootCfg
	} else {
		cfg = runOpts.Config
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

	defer aggregator.Close()

	rawTCPServer := rawtcpserver.NewServer(rawTCPAddr, aggregator, rawTCPServerOpts)
	if err := rawTCPServer.ListenAndServe(); err != nil {
		logger.Fatal("could not start raw TCP server", zap.String("rawTCPAddr", rawTCPAddr), zap.Error(err))
	}
	defer rawTCPServer.Close()
	logger.Info("raw TCP server: listening", zap.String("rawTCPAddr", rawTCPAddr))

	httpServer := httpserver.NewServer(httpAddr, aggregator, httpServerOpts)
	if err := httpServer.ListenAndServe(); err != nil {
		logger.Fatal("could not start http server", zap.String("httpAddr", httpAddr), zap.Error(err))
	}
	defer httpServer.Close()
	logger.Info("http server: listening", zap.String("httpAddr", httpAddr))

	// Wait for process interrupt.
	xos.WaitForInterrupt(logger, xos.InterruptOptions{
		InterruptCh: runOpts.InterruptCh,
	})
}
