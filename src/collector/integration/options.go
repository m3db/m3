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

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	msgpackbackend "github.com/m3db/m3collector/backend/msgpack"
	msgpackserver "github.com/m3db/m3collector/integration/msgpack"
	"github.com/m3db/m3collector/reporter"
	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3metrics/matcher/cache"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultClientConnectTimeout     = time.Second
	defaultServerStateChangeTimeout = 5 * time.Second
)

type testOptions interface {
	// SetClientConnectTimeout sets the client-side connect timeout.
	SetClientConnectTimeout(value time.Duration) testOptions

	// ClientConnectTimeout returns the client-side connect timeout.
	ClientConnectTimeout() time.Duration

	// SetServerStateChangeTimeout sets the server state change timeout.
	SetServerStateChangeTimeout(value time.Duration) testOptions

	// ServerStateChangeTimeout returns the server state change timeout.
	ServerStateChangeTimeout() time.Duration

	// SetKVStore sets the key value store.
	SetKVStore(value kv.Store) testOptions

	// KVStore returns the key value store.
	KVStore() kv.Store

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) testOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetCacheOptions sets the cache options.
	SetCacheOptions(value cache.Options) testOptions

	// CacheOptions returns the cache options.
	CacheOptions() cache.Options

	// SetMatcherOptions sets the matcher options.
	SetMatcherOptions(value matcher.Options) testOptions

	// MatcherOptions returns the matcher options.
	MatcherOptions() matcher.Options

	// SetBackendOptions sets the backend options.
	SetBackendOptions(value msgpackbackend.ServerOptions) testOptions

	// BackendOptions returns the backend options.
	BackendOptions() msgpackbackend.ServerOptions

	// SetReporterOptions sets the reporter options.
	SetReporterOptions(value reporter.Options) testOptions

	// ReporterOptions returns the reporter options.
	ReporterOptions() reporter.Options

	// SetServerAddr sets the server listening address.
	SetServerAddr(value string) testOptions

	// ServerAddr returns the server listening address.
	ServerAddr() string

	// SetServerOptions sets the server options.
	SetServerOptions(value msgpackserver.Options) testOptions

	// ServerOptions returns the server options.
	ServerOptions() msgpackserver.Options
}

type options struct {
	clientConnectTimeout     time.Duration
	serverStateChangeTimeout time.Duration
	store                    kv.Store
	instrumentOpts           instrument.Options
	cacheOpts                cache.Options
	matcherOpts              matcher.Options
	backendOpts              msgpackbackend.ServerOptions
	reporterOpts             reporter.Options
	serverAddr               string
	serverOpts               msgpackserver.Options
}

func newTestOptions() testOptions {
	return &options{
		clientConnectTimeout:     defaultClientConnectTimeout,
		serverStateChangeTimeout: defaultServerStateChangeTimeout,
		store:          mem.NewStore(),
		instrumentOpts: instrument.NewOptions(),
		cacheOpts:      cache.NewOptions(),
		matcherOpts:    matcher.NewOptions(),
		backendOpts:    msgpackbackend.NewServerOptions(),
		reporterOpts:   reporter.NewOptions(),
		serverOpts:     msgpackserver.NewOptions(),
	}
}

func (o *options) SetClientConnectTimeout(value time.Duration) testOptions {
	opts := *o
	opts.clientConnectTimeout = value
	return &opts
}

func (o *options) ClientConnectTimeout() time.Duration {
	return o.clientConnectTimeout
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

func (o *options) SetInstrumentOptions(value instrument.Options) testOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
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

func (o *options) SetBackendOptions(value msgpackbackend.ServerOptions) testOptions {
	opts := *o
	opts.backendOpts = value
	return &opts
}

func (o *options) BackendOptions() msgpackbackend.ServerOptions {
	return o.backendOpts
}

func (o *options) SetReporterOptions(value reporter.Options) testOptions {
	opts := *o
	opts.reporterOpts = value
	return &opts
}

func (o *options) ReporterOptions() reporter.Options {
	return o.reporterOpts
}

func (o *options) SetServerAddr(value string) testOptions {
	opts := *o
	opts.serverAddr = value
	return &opts
}

func (o *options) ServerAddr() string {
	return o.serverAddr
}

func (o *options) SetServerOptions(value msgpackserver.Options) testOptions {
	opts := *o
	opts.serverOpts = value
	return &opts
}

func (o *options) ServerOptions() msgpackserver.Options {
	return o.serverOpts
}
