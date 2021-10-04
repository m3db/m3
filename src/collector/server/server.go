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
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/m3db/m3/src/aggregator/server"
	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/serve"
	"github.com/m3db/m3/src/cmd/services/m3collector/config"
	"github.com/m3db/m3/src/collector/api/v1/httpd"
	"github.com/m3db/m3/src/collector/reporter"
	"github.com/m3db/m3/src/collector/reporter/m3aggregator"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"

	"go.uber.org/zap"
)

// RunOptions provides options for running the server
// with backwards compatibility if only solely adding fields.
type RunOptions struct {
	// Config will be used to configure the application.
	Config config.Configuration

	// InterruptCh is a programmatic interrupt channel to supply to
	// interrupt and shutdown the server.
	InterruptCh <-chan error

	// AggregatorServerOptions are server options for aggregator.
	AggregatorServerOptions []server.AdminOption
}

// Run runs the server programmatically given a filename for the configuration file.
func Run(runOpts RunOptions) {
	cfg := runOpts.Config

	ctx := context.Background()
	logger, err := cfg.Logging.Build()
	if err != nil {
		// NB(r): Use fmt.Fprintf(os.Stderr, ...) to avoid etcd.SetGlobals()
		// sending stdlib "log" to black hole. Don't remove unless with good reason.
		fmt.Fprintf(os.Stderr, "unable to create logger: %v", err)
		os.Exit(1)
	}
	defer logger.Sync()

	xconfig.WarnOnDeprecation(cfg, logger)

	logger.Info("creating metrics scope")
	scope, closer, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatal("could not connect to metrics", zap.Error(err))
	}
	defer closer.Close()

	instrumentOpts := instrument.NewOptions().
		SetMetricsScope(scope).
		SetLogger(logger)

	logger.Info("creating etcd client")
	clusterClient, err := cfg.Etcd.NewClient(instrumentOpts)
	if err != nil {
		logger.Fatal("could not create etcd client", zap.Error(err))
	}

	serveOptions := serve.NewOptions(instrumentOpts)
	for i, transform := range runOpts.AggregatorServerOptions {
		if opts, err := transform(serveOptions); err != nil {
			logger.Fatal("could not apply transform",
				zap.Int("index", i), zap.Error(err))
		} else {
			serveOptions = opts
		}
	}

	rwOpts := serveOptions.RWOptions()
	logger.Info("creating reporter")
	reporter, err := newReporter(cfg.Reporter, clusterClient, instrumentOpts, rwOpts)
	if err != nil {
		logger.Fatal("could not create reporter", zap.Error(err))
	}

	tagEncoderOptions := serialize.NewTagEncoderOptions().
		SetInstrumentOptions(instrumentOpts)
	tagDecoderOptions := serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{})
	tagEncoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-encoder-pool")))
	tagDecoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-decoder-pool")))
	tagEncoderPool := serialize.NewTagEncoderPool(tagEncoderOptions,
		tagEncoderPoolOptions)
	tagEncoderPool.Init()
	tagDecoderPool := serialize.NewTagDecoderPool(tagDecoderOptions,
		tagDecoderPoolOptions)
	tagDecoderPool.Init()

	logger.Info("creating http handlers and registering routes")
	handler, err := httpd.NewHandler(reporter, tagEncoderPool,
		tagDecoderPool, instrumentOpts)
	if err != nil {
		logger.Fatal("unable to set up handlers", zap.Error(err))
	}

	if err := handler.RegisterRoutes(); err != nil {
		logger.Fatal("unable to register routes", zap.Error(err))
	}

	srv := &http.Server{Addr: cfg.ListenAddress, Handler: handler.Router()}
	defer func() {
		logger.Info("closing server")
		if err := srv.Shutdown(ctx); err != nil {
			logger.Error("error closing server", zap.Error(err))
		}
	}()

	go func() {
		logger.Info("starting server", zap.String("address", cfg.ListenAddress))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server error while listening",
				zap.String("address", cfg.ListenAddress), zap.Error(err))
		}
	}()

	var interruptCh <-chan error = make(chan error)
	if runOpts.InterruptCh != nil {
		interruptCh = runOpts.InterruptCh
	}

	var interruptErr error
	if runOpts.InterruptCh != nil {
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

func newReporter(
	cfg config.ReporterConfiguration,
	clusterClient clusterclient.Client,
	instrumentOpts instrument.Options,
	rwOpts xio.Options,
) (reporter.Reporter, error) {
	scope := instrumentOpts.MetricsScope()
	logger := instrumentOpts.Logger()
	clockOpts := cfg.Clock.NewOptions()

	logger.Info("creating metrics matcher cache")
	cache := cfg.Cache.NewCache(clockOpts,
		instrumentOpts.SetMetricsScope(scope.SubScope("cache")))

	logger.Info("creating metrics matcher")
	matcher, err := cfg.Matcher.NewMatcher(cache, clusterClient, clockOpts,
		instrumentOpts.SetMetricsScope(scope.SubScope("matcher")))
	if err != nil {
		return nil, fmt.Errorf("unable to create matcher: %v", err)
	}

	logger.Info("creating aggregator client")
	aggClient, err := cfg.Client.NewClient(clusterClient, clockOpts,
		instrumentOpts.SetMetricsScope(scope.SubScope("backend")), rwOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create agg tier client: %v", err)
	}

	logger.Info("connecting to aggregator cluster")
	if err := aggClient.Init(); err != nil {
		return nil, fmt.Errorf("unable to initialize agg tier client: %v", err)
	}

	logger.Info("creating aggregator reporter")
	reporterOpts := m3aggregator.NewReporterOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts)
	return m3aggregator.NewReporter(matcher, aggClient, reporterOpts), nil
}
