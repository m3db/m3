// Copyright (c) 2017 Uber Technologies, Inc.
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

package integration

import (
	"time"

	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/collector/integration/server"
	aggreporter "github.com/m3db/m3/src/collector/reporter/m3aggregator"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	defaultServerStateChangeTimeout = 5 * time.Second
)

type testOptions interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) testOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetServerAddr sets the server listening address.
	SetServerAddr(value string) testOptions

	// ServerAddr returns the server listening address.
	ServerAddr() string

	// SetServerOptions sets the server options.
	SetServerOptions(value server.Options) testOptions

	// ServerOptions returns the server options.
	ServerOptions() server.Options

	// SetServerStateChangeTimeout sets the server state change timeout.
	SetServerStateChangeTimeout(value time.Duration) testOptions

	// ServerStateChangeTimeout returns the server state change timeout.
	ServerStateChangeTimeout() time.Duration

	// SetKVStore sets the key value store.
	SetKVStore(value kv.Store) testOptions

	// KVStore returns the key value store.
	KVStore() kv.Store

	// SetCacheOptions sets the cache options.
	SetCacheOptions(value cache.Options) testOptions

	// CacheOptions returns the cache options.
	CacheOptions() cache.Options

	// SetMatcherOptions sets the matcher options.
	SetMatcherOptions(value matcher.Options) testOptions

	// MatcherOptions returns the matcher options.
	MatcherOptions() matcher.Options

	// SetAggregatorClientOptions sets the aggregator client options.
	SetAggregatorClientOptions(value aggclient.Options) testOptions

	// AggregatorClientOptions returns the aggregator client options.
	AggregatorClientOptions() aggclient.Options

	// SetAggregatorReporterOptions sets the aggregator reporter options.
	SetAggregatorReporterOptions(value aggreporter.ReporterOptions) testOptions

	// AggregatorReporterOptions returns the reporter options.
	AggregatorReporterOptions() aggreporter.ReporterOptions
}

type options struct {
	instrumentOpts           instrument.Options
	serverAddr               string
	serverOpts               server.Options
	serverStateChangeTimeout time.Duration
	store                    kv.Store
	cacheOpts                cache.Options
	matcherOpts              matcher.Options
	aggClientOpts            aggclient.Options
	aggReporterOpts          aggreporter.ReporterOptions
}

func newTestOptions() testOptions {
	return &options{
		instrumentOpts:           instrument.NewOptions(),
		serverOpts:               server.NewOptions(),
		serverStateChangeTimeout: defaultServerStateChangeTimeout,
		store:                    mem.NewStore(),
		cacheOpts:                cache.NewOptions(),
		matcherOpts:              matcher.NewOptions(),
		aggClientOpts: aggclient.NewOptions().
			SetMaxBatchSize(65536).
			SetFlushWorkerCount(4),
		aggReporterOpts: aggreporter.NewReporterOptions(),
	}
}

func (o *options) SetInstrumentOptions(value instrument.Options) testOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetServerAddr(value string) testOptions {
	opts := *o
	opts.serverAddr = value
	return &opts
}

func (o *options) ServerAddr() string {
	return o.serverAddr
}

func (o *options) SetServerOptions(value server.Options) testOptions {
	opts := *o
	opts.serverOpts = value
	return &opts
}

func (o *options) ServerOptions() server.Options {
	return o.serverOpts
}

func (o *options) SetServerStateChangeTimeout(value time.Duration) testOptions {
	opts := *o
	opts.serverStateChangeTimeout = value
	return &opts
}

func (o *options) ServerStateChangeTimeout() time.Duration {
	return o.serverStateChangeTimeout
}

func (o *options) SetKVStore(value kv.Store) testOptions {
	opts := *o
	opts.store = value
	return &opts
}

func (o *options) KVStore() kv.Store {
	return o.store
}

func (o *options) SetCacheOptions(value cache.Options) testOptions {
	opts := *o
	opts.cacheOpts = value
	return &opts
}

func (o *options) CacheOptions() cache.Options {
	return o.cacheOpts
}

func (o *options) SetMatcherOptions(value matcher.Options) testOptions {
	opts := *o
	opts.matcherOpts = value
	return &opts
}

func (o *options) MatcherOptions() matcher.Options {
	return o.matcherOpts
}

func (o *options) SetAggregatorClientOptions(value aggclient.Options) testOptions {
	opts := *o
	opts.aggClientOpts = value
	return &opts
}

func (o *options) AggregatorClientOptions() aggclient.Options {
	return o.aggClientOpts
}

func (o *options) SetAggregatorReporterOptions(value aggreporter.ReporterOptions) testOptions {
	opts := *o
	opts.aggReporterOpts = value
	return &opts
}

func (o *options) AggregatorReporterOptions() aggreporter.ReporterOptions {
	return o.aggReporterOpts
}
