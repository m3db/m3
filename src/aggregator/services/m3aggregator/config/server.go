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

package config

import (
	"time"

	"github.com/m3db/m3aggregator/server/http"
	"github.com/m3db/m3aggregator/server/msgpack"
	"github.com/m3db/m3metrics/policy"
	msgpackp "github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
)

// MsgpackServerConfiguration contains msgpack server configuration.
type MsgpackServerConfiguration struct {
	// Msgpack server listening address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// Retry mechanism configuration.
	Retry xretry.Configuration `yaml:"retry"`

	// Iterator configuration.
	Iterator unaggregatedIteratorConfiguration `yaml:"iterator"`
}

// NewMsgpackServerOptions create a new set of msgpack server options.
func (c *MsgpackServerConfiguration) NewMsgpackServerOptions(
	instrumentOpts instrument.Options,
) msgpack.Options {
	opts := msgpack.NewOptions().SetInstrumentOptions(instrumentOpts)

	// Set retrier.
	retrier := c.Retry.NewRetrier(instrumentOpts.MetricsScope())
	opts = opts.SetRetrier(retrier)

	// Set unaggregated iterator pool.
	iteratorPool := c.Iterator.NewUnaggregatedIteratorPool(instrumentOpts)
	opts = opts.SetIteratorPool(iteratorPool)

	return opts
}

// unaggregatedIteratorConfiguration contains configuration for unaggregated iterator.
type unaggregatedIteratorConfiguration struct {
	// Whether to ignore encoded data streams whose version is higher than the current known version.
	IgnoreHigherVersion bool `yaml:"ignoreHigherVersion"`

	// Pool of float slices.
	FloatsPool pool.BucketizedPoolConfiguration `yaml:"floatsPool"`

	// Pool of policies.
	PoliciesPool pool.BucketizedPoolConfiguration `yaml:"policiesPool"`

	// Pool of unaggregated iterators.
	IteratorPool pool.ObjectPoolConfiguration `yaml:"iteratorPool"`
}

func (c *unaggregatedIteratorConfiguration) NewUnaggregatedIteratorPool(
	instrumentOpts instrument.Options,
) msgpackp.UnaggregatedIteratorPool {
	scope := instrumentOpts.MetricsScope()
	opts := msgpackp.NewUnaggregatedIteratorOptions().SetIgnoreHigherVersion(c.IgnoreHigherVersion)

	// NB(xichen): intentionally not using the same floats pool used for computing
	// timer quantiles to accommodate different usage patterns and reduce contention.
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("floats-pool"))
	floatsPoolOpts := c.FloatsPool.NewObjectPoolOptions(iOpts)
	floatsPool := pool.NewFloatsPool(c.FloatsPool.NewBuckets(), floatsPoolOpts)
	opts = opts.SetFloatsPool(floatsPool)
	floatsPool.Init()

	// Set policies pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("policies-pool"))
	policiesPoolOpts := c.PoliciesPool.NewObjectPoolOptions(iOpts)
	policiesPool := policy.NewPoliciesPool(c.FloatsPool.NewBuckets(), policiesPoolOpts)
	opts = opts.SetPoliciesPool(policiesPool)
	policiesPool.Init()

	// Set iterator pool.
	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("unaggreagted-iterator-pool"))
	iteratorPoolOpts := c.IteratorPool.NewObjectPoolOptions(iOpts)
	iteratorPool := msgpackp.NewUnaggregatedIteratorPool(iteratorPoolOpts)
	opts = opts.SetIteratorPool(iteratorPool)
	iteratorPool.Init(func() msgpackp.UnaggregatedIterator { return msgpackp.NewUnaggregatedIterator(nil, opts) })

	return iteratorPool
}

// HTTPServerConfiguration contains http server configuration.
type HTTPServerConfiguration struct {
	// HTTP server listending address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// HTTP server debug listening address.
	DebugListenAddress string `yaml:"debugListenAddress"`

	// HTTP server read timeout.
	ReadTimeout time.Duration `yaml:"readTimeout"`

	// HTTP server write timeout.
	WriteTimeout time.Duration `yaml:"writeTimeout"`
}

// NewHTTPServerOptions create a new set of http server options.
func (c *HTTPServerConfiguration) NewHTTPServerOptions(
	instrumentOpts instrument.Options,
) http.Options {
	opts := http.NewOptions()
	if c.ReadTimeout != 0 {
		opts = opts.SetReadTimeout(c.ReadTimeout)
	}
	if c.WriteTimeout != 0 {
		opts = opts.SetWriteTimeout(c.WriteTimeout)
	}
	return opts
}
