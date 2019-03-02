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

	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metric/id"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cmd/services/m3collector/config"
	"github.com/m3db/m3/src/collector/api/v1/httpd"
	"github.com/m3db/m3/src/collector/reporter"
	"github.com/m3db/m3/src/collector/reporter/m3aggregator"
	"github.com/m3db/m3/src/x/serialize"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"

	"go.uber.org/zap"
)

// RunOptions provides options for running the server
// with backwards compatibility if only solely adding fields.
type RunOptions struct {
	// ConfigFile is the config file to use.
	ConfigFile string

	// Config is an alternate way to provide configuration and will be used
	// instead of parsing ConfigFile if ConfigFile is not specified.
	Config config.Configuration

	// InterruptCh is a programmatic interrupt channel to supply to
	// interrupt and shutdown the server.
	InterruptCh <-chan error
}

// Run runs the server programmatically given a filename for the configuration file.
func Run(runOpts RunOptions) {
	var cfg config.Configuration
	if runOpts.ConfigFile != "" {
		if err := xconfig.LoadFile(&cfg, runOpts.ConfigFile, xconfig.Options{}); err != nil {
			fmt.Fprintf(os.Stderr, "unable to load %s: %v", runOpts.ConfigFile, err)
			os.Exit(1)
		}
	} else {
		cfg = runOpts.Config
	}

	ctx := context.Background()
	logger, err := cfg.Logging.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create logger: %v", err)
		os.Exit(1)
	}

	defer logger.Sync()

	logger.Info("creating metrics scope")
	scope, closer, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatal("could not connect to metrics", zap.Error(err))
	}
	defer closer.Close()

	instrumentOpts := instrument.NewOptions().
		SetMetricsScope(scope).
		SetZapLogger(logger)

	logger.Info("creating etcd client")
	clusterClient, err := cfg.Etcd.NewClient(instrumentOpts)
	if err != nil {
		logger.Fatal("could not create etcd client", zap.Error(err))
	}

	pools := newAggregatorPool(instrumentOpts)

	logger.Info("creating reporter")
	reporter, err := newReporter(cfg.Reporter, pools, clusterClient, instrumentOpts)
	if err != nil {
		logger.Fatal("could not create reporter", zap.Error(err))
	}

	logger.Info("creating http handlers and registering routes")
	handler, err := httpd.NewHandler(reporter, pools.tagEncoderPool,
		pools.tagDecoderPool, instrumentOpts)
	if err != nil {
		logger.Fatal("unable to set up handlers", zap.Error(err))
	}

	if err := handler.RegisterRoutes(); err != nil {
		logger.Fatal("unable to register routes", zap.Error(err))
	}

	listenAddress, err := cfg.ListenAddress.Resolve()
	if err != nil {
		logger.Fatal("unable to resolve listen address", zap.Error(err))
	}

	srv := &http.Server{Addr: listenAddress, Handler: handler.Router()}
	defer func() {
		logger.Info("closing server")
		if err := srv.Shutdown(ctx); err != nil {
			logger.Error("error closing server", zap.Error(err))
		}
	}()

	go func() {
		logger.Info("starting server", zap.String("address", listenAddress))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server error while listening",
				zap.String("address", listenAddress), zap.Error(err))
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
	pools aggPools,
	clusterClient clusterclient.Client,
	instrumentOpts instrument.Options,
) (reporter.Reporter, error) {
	scope := instrumentOpts.MetricsScope()
	logger := instrumentOpts.ZapLogger()
	clockOpts := cfg.Clock.NewOptions()

	matchOpts, err := cfg.Matcher.NewOptions(clusterClient, clockOpts, instrumentOpts)
	if err != nil {
		return nil, err
	}

	nameTag := []byte(cfg.Matcher.NameTagKey)

	sortedTagIteratorFn := func(tagPairs []byte) id.SortedTagIterator {
		it := pools.encodedTagsIteratorPool.Get()
		it.Reset(tagPairs)
		return it
	}

	tagsFilterOpts := filters.TagsFilterOptions{
		NameTagKey: nameTag,
		NameAndTagsFn: func(id []byte) ([]byte, []byte, error) {
			name, err := resolveEncodedTagsNameTag(id, pools.encodedTagsIteratorPool, nameTag)
			if err != nil {
				return nil, nil, err
			}
			// ID is always the encoded tags for IDs in the downsampler
			tags := id
			return name, tags, nil
		},
		SortedTagIteratorFn: sortedTagIteratorFn,
	}

	isRollupIDFn := func(name []byte, tags []byte) bool {
		return isRollupID(tags, pools.encodedTagsIteratorPool)
	}

	newRollupIDProviderPool := newRollupIDProviderPool(pools.tagEncoderPool,
		pools.tagEncoderPoolOptions)
	newRollupIDProviderPool.Init()

	newRollupIDFn := func(name []byte, tagPairs []id.TagPair) []byte {
		tagPairs = append(tagPairs, id.TagPair{Name: []byte(nameTag), Value: name})
		rollupIDProvider := newRollupIDProviderPool.Get()
		id, err := rollupIDProvider.provide(tagPairs)
		if err != nil {
			panic(err) // Encoding should never fail
		}
		rollupIDProvider.finalize()
		return id
	}

	matchOpts = matchOpts.SetRuleSetOptions(matchOpts.RuleSetOptions().
		SetTagsFilterOptions(tagsFilterOpts).
		SetNewRollupIDFn(newRollupIDFn).
		SetIsRollupIDFn(isRollupIDFn))
	matchOpts.SetInstrumentOptions(instrumentOpts.SetMetricsScope(scope.SubScope("matcher")))

	logger.Info("creating metrics matcher cache")
	cache := cfg.Cache.NewCache(clockOpts,
		instrumentOpts.SetMetricsScope(scope.SubScope("cache")))

	logger.Info("creating metrics matcher")
	matcher, err := matcher.NewMatcher(cache, matchOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create matcher: %v", err)
	}

	logger.Info("creating aggregator client")
	aggClient, err := cfg.Client.NewClient(clusterClient, clockOpts,
		instrumentOpts.SetMetricsScope(scope.SubScope("backend")))
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

type aggPools struct {
	tagEncoderPool          serialize.TagEncoderPool
	tagEncoderPoolOptions   pool.ObjectPoolOptions
	tagDecoderPool          serialize.TagDecoderPool
	tagDecoderPoolOptions   pool.ObjectPoolOptions
	encodedTagsIteratorPool *encodedTagsIteratorPool
}

func newAggregatorPool(instrumentOpts instrument.Options) aggPools {
	tagEncoderOptions := serialize.NewTagEncoderOptions()
	tagDecoderOptions := serialize.NewTagDecoderOptions()
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

	encodedTagsIteratorPool := newEncodedTagsIteratorPool(tagDecoderPool,
		tagDecoderPoolOptions)
	encodedTagsIteratorPool.Init()
	return aggPools{
		tagDecoderPool:          tagDecoderPool,
		tagDecoderPoolOptions:   tagDecoderPoolOptions,
		tagEncoderPool:          tagEncoderPool,
		tagEncoderPoolOptions:   tagEncoderPoolOptions,
		encodedTagsIteratorPool: encodedTagsIteratorPool,
	}
}
