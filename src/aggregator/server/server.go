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
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	m3aggregator "github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/serve"
	"github.com/m3db/m3/src/x/clock"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/instrument"
	xos "github.com/m3db/m3/src/x/os"

	"go.uber.org/zap"
)

const (
	gracefulShutdownTimeout = 15 * time.Second
)

// RunOptions are the server options for running the aggregator server.
type RunOptions struct {
	// Config is the aggregator configuration.
	Config config.Configuration

	// AdminOptions are additional options to apply to the aggregator server.
	AdminOptions []AdminOption

	// CustomBuildTags are additional tags to be added to the instrument build
	// reporter.
	CustomBuildTags map[string]string

	// InterruptCh is a programmatic interrupt channel to supply to
	// interrupt and shutdown the server.
	InterruptCh <-chan error

	// ShutdownCh is an optional channel to supply if interested in receiving
	// a notification that the server has shutdown.
	ShutdownCh chan<- struct{}
}

// AdminOption is an additional option to apply to the aggregator server.
type AdminOption func(opts serve.Options) (serve.Options, error)

// Run runs the aggregator server.
func Run(opts RunOptions) {
	cfg := opts.Config

	// Create logger and metrics scope.
	logger, err := cfg.Logging.BuildLogger()
	if err != nil {
		log.Fatalf("error creating logger: %v", err)
	}

	// NB(nate): Register shutdown notification defer function first so that
	// it's the last defer to fire before terminating. This allows other defer methods
	// that clean up resources to execute first.
	if opts.ShutdownCh != nil {
		defer func() {
			select {
			case opts.ShutdownCh <- struct{}{}:
				break
			default:
				logger.Warn("could not send shutdown notification as channel was full")
			}
		}()
	}

	defer logger.Sync()

	cfg.Debug.SetRuntimeValues(logger)

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
		SetReportInterval(cfg.Metrics.ReportInterval()).
		SetCustomBuildTags(opts.CustomBuildTags)

	buildReporter := instrument.NewBuildReporter(instrumentOpts)
	if err := buildReporter.Start(); err != nil {
		logger.Fatal("could not start build reporter", zap.Error(err))
	}

	defer buildReporter.Stop()

	serverOptions := serve.NewOptions(instrumentOpts)
	if cfg.M3Msg != nil {
		// Create the M3Msg server options.
		m3msgInsrumentOpts := instrumentOpts.
			SetMetricsScope(scope.
				SubScope("m3msg-server").
				Tagged(map[string]string{"server": "m3msg"}))
		m3msgServerOpts, err := cfg.M3Msg.NewServerOptions(m3msgInsrumentOpts)
		if err != nil {
			logger.Fatal("could not create m3msg server options", zap.Error(err))
		}

		serverOptions = serverOptions.
			SetM3MsgAddr(cfg.M3Msg.Server.ListenAddress).
			SetM3MsgServerOpts(m3msgServerOpts)
	}

	if cfg.RawTCP != nil {
		// Create the raw TCP server options.
		rawTCPInstrumentOpts := instrumentOpts.
			SetMetricsScope(scope.
				SubScope("rawtcp-server").
				Tagged(map[string]string{"server": "rawtcp"}))

		serverOptions = serverOptions.
			SetRawTCPAddr(cfg.RawTCP.ListenAddress).
			SetRawTCPServerOpts(cfg.RawTCP.NewServerOptions(rawTCPInstrumentOpts))
	}

	if cfg.HTTP != nil {
		// Create the http server options.
		serverOptions = serverOptions.
			SetHTTPAddr(cfg.HTTP.ListenAddress).
			SetHTTPServerOpts(cfg.HTTP.NewServerOptions())
	}

	for i, transform := range opts.AdminOptions {
		if opts, err := transform(serverOptions); err != nil {
			logger.Fatal("could not apply transform",
				zap.Int("index", i), zap.Error(err))
		} else {
			serverOptions = opts
		}
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
	aggregatorOpts, err := cfg.Aggregator.NewAggregatorOptions(
		serverOptions.RawTCPAddr(),
		client, serverOptions, runtimeOptsManager, clock.NewOptions(),
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
			aggregator,
			doneCh,
			serverOptions,
		); err != nil {
			logger.Fatal("could not start serving traffic", zap.Error(err))
		}
		logger.Debug("server closed")
		close(closedCh)
	}()

	// Handle interrupts.
	xos.WaitForInterrupt(logger, xos.InterruptOptions{
		InterruptCh: opts.InterruptCh,
	})

	if s := cfg.Aggregator.ShutdownWaitTimeout; s != 0 {
		sigC := make(chan os.Signal, 1)
		signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)

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
