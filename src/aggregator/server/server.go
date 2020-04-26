// Copyright (c) 2020 Uber Technologies, Inc.
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
	"os/signal"
	"syscall"
	"time"

	m3aggregator "github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/server/http"
	"github.com/m3db/m3/src/aggregator/server/m3msg"
	"github.com/m3db/m3/src/aggregator/server/rawtcp"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/serve"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

const (
	gracefulShutdownTimeout = 15 * time.Second
)

// RunOptions are the server options for running the aggregator server.
type RunOptions struct {
	Config config.Configuration
}

// Run runs the aggregator server.
func Run(opts RunOptions) {
	cfg := opts.Config

	// Create logger and metrics scope.
	logger, err := cfg.Logging.BuildLogger()
	if err != nil {
		// NB(r): Use fmt.Fprintf(os.Stderr, ...) to avoid etcd.SetGlobals()
		// sending stdlib "log" to black hole. Don't remove unless with good reason.
		fmt.Fprintf(os.Stderr, "error creating logger: %v\n", err)
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
		SetTimerOptions(instrument.TimerOptions{StandardSampleRate: cfg.Metrics.SampleRate()}).
		SetReportInterval(cfg.Metrics.ReportInterval())

	buildReporter := instrument.NewBuildReporter(instrumentOpts)
	if err := buildReporter.Start(); err != nil {
		logger.Fatal("could not start build reporter", zap.Error(err))
	}

	defer buildReporter.Stop()

	var (
		m3msgAddr        string
		m3msgServerOpts  m3msg.Options
		rawTCPAddr       string
		rawTCPServerOpts rawtcp.Options
		httpAddr         string
		httpServerOpts   http.Options
	)
	if cfg.M3Msg != nil {
		// Create the M3Msg server options.
		m3msgAddr = cfg.M3Msg.Server.ListenAddress
		m3msgInsrumentOpts := instrumentOpts.
			SetMetricsScope(scope.
				SubScope("m3msg-server").
				Tagged(map[string]string{"server": "m3msg"}))
		m3msgServerOpts, err = cfg.M3Msg.NewServerOptions(m3msgInsrumentOpts)
		if err != nil {
			logger.Fatal("could not create m3msg server options", zap.Error(err))
		}
	}

	if cfg.RawTCP != nil {
		// Create the raw TCP server options.
		rawTCPAddr = cfg.RawTCP.ListenAddress
		rawTCPInstrumentOpts := instrumentOpts.
			SetMetricsScope(scope.
				SubScope("rawtcp-server").
				Tagged(map[string]string{"server": "rawtcp"}))
		rawTCPServerOpts = cfg.RawTCP.NewServerOptions(rawTCPInstrumentOpts)
	}

	if cfg.HTTP != nil {
		// Create the http server options.
		httpAddr = cfg.HTTP.ListenAddress
		httpServerOpts = cfg.HTTP.NewServerOptions()
	}

	// Create the kv client.
	client, err := cfg.KVClient.NewKVClient(instrumentOpts.
		SetMetricsScope(scope.SubScope("kv-client")))
	if err != nil {
		logger.Fatal("error creating the kv client", zap.Error(err))
	}

	// Create the runtime options manager.
	runtimeOptsManager := cfg.RuntimeOptions.NewRuntimeOptionsManager()

	// Create the aggregator.
	aggregatorOpts, err := cfg.Aggregator.NewAggregatorOptions(rawTCPAddr,
		client, runtimeOptsManager,
		instrumentOpts.SetMetricsScope(scope.SubScope("aggregator")))
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
			m3msgAddr,
			m3msgServerOpts,
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
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)

	logger.Warn("interrupt", zap.Any("signal", fmt.Errorf("%s", <-sigC)))

	if s := cfg.Aggregator.ShutdownWaitTimeout; s != 0 {
		logger.Info("waiting intentional shutdown period", zap.Duration("waitTimeout", s))
		select {
		case sig := <-sigC:
			logger.Info("second signal received, skipping shutdown wait", zap.String("signal", sig.String()))
		case <-time.After(cfg.Aggregator.ShutdownWaitTimeout):
			logger.Info("shutdown period elapsed")
		}
	}

	close(doneCh)

	select {
	case <-closedCh:
		logger.Info("server closed clean")
	case <-time.After(gracefulShutdownTimeout):
		logger.Info("server closed due to timeout", zap.Duration("timeout", gracefulShutdownTimeout))
	}
}
