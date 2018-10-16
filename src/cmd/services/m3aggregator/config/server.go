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

	"github.com/m3db/m3/src/aggregator/server/http"
	"github.com/m3db/m3/src/aggregator/server/rawtcp"
	"github.com/m3db/m3/src/metrics/encoding/msgpack"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
	xserver "github.com/m3db/m3x/server"
)

// RawTCPServerConfiguration contains raw TCP server configuration.
type RawTCPServerConfiguration struct {
	// Raw TCP server listening address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// Error log limit per second.
	ErrorLogLimitPerSecond *int64 `yaml:"errorLogLimitPerSecond"`

	// Whether keep alives are enabled on connections.
	KeepAliveEnabled *bool `yaml:"keepAliveEnabled"`

	// KeepAlive period.
	KeepAlivePeriod *time.Duration `yaml:"keepAlivePeriod"`

	// Retry mechanism configuration.
	Retry retry.Configuration `yaml:"retry"`

	// Read buffer size.
	ReadBufferSize *int `yaml:"readBufferSize"`

	// Msgpack iterator configuration.
	MsgpackIterator msgpackUnaggregatedIteratorConfiguration `yaml:"msgpackIterator"`

	// Protobuf iterator configuration.
	ProtobufIterator protobufUnaggregatedIteratorConfiguration `yaml:"protobufIterator"`
}

// NewServerOptions create a new set of raw TCP server options.
func (c *RawTCPServerConfiguration) NewServerOptions(
	instrumentOpts instrument.Options,
) rawtcp.Options {
	opts := rawtcp.NewOptions().SetInstrumentOptions(instrumentOpts)

	// Set server options.
	serverOpts := xserver.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRetryOptions(c.Retry.NewOptions(instrumentOpts.MetricsScope()))
	if c.KeepAliveEnabled != nil {
		serverOpts = serverOpts.SetTCPConnectionKeepAlive(*c.KeepAliveEnabled)
	}
	if c.KeepAlivePeriod != nil {
		serverOpts = serverOpts.SetTCPConnectionKeepAlivePeriod(*c.KeepAlivePeriod)
	}
	opts = opts.SetServerOptions(serverOpts)

	// Set msgpack iterator options.
	msgpackItOpts := c.MsgpackIterator.NewOptions(instrumentOpts)
	opts = opts.SetMsgpackUnaggregatedIteratorOptions(msgpackItOpts)

	// Set protobuf iterator options.
	protobufItOpts := c.ProtobufIterator.NewOptions(instrumentOpts)
	opts = opts.SetProtobufUnaggregatedIteratorOptions(protobufItOpts)

	if c.ReadBufferSize != nil {
		opts = opts.SetReadBufferSize(*c.ReadBufferSize)
	}
	if c.ErrorLogLimitPerSecond != nil {
		opts = opts.SetErrorLogLimitPerSecond(*c.ErrorLogLimitPerSecond)
	}
	return opts
}

// msgpackUnaggregatedIteratorConfiguration contains configuration for msgpack unaggregated iterator.
type msgpackUnaggregatedIteratorConfiguration struct {
	// Whether to ignore encoded data streams whose version is higher than the current known version.
	IgnoreHigherVersion *bool `yaml:"ignoreHigherVersion"`

	// Reader buffer size.
	ReaderBufferSize *int `yaml:"readerBufferSize"`

	// Whether a float slice is considered a "large" slice and therefore resort to
	// the large floats pool for allocating that slice.
	LargeFloatsSize *int `yaml:"largeFloatsSize"`

	// Pool of large float slices.
	LargeFloatsPool pool.BucketizedPoolConfiguration `yaml:"largeFloatsPool"`
}

func (c *msgpackUnaggregatedIteratorConfiguration) NewOptions(
	instrumentOpts instrument.Options,
) msgpack.UnaggregatedIteratorOptions {
	scope := instrumentOpts.MetricsScope()
	opts := msgpack.NewUnaggregatedIteratorOptions()
	if c.IgnoreHigherVersion != nil {
		opts = opts.SetIgnoreHigherVersion(*c.IgnoreHigherVersion)
	}
	if c.ReaderBufferSize != nil {
		opts = opts.SetReaderBufferSize(*c.ReaderBufferSize)
	}
	if c.LargeFloatsSize != nil {
		opts = opts.SetLargeFloatsSize(*c.LargeFloatsSize)
	}

	// NB(xichen): intentionally not using the same floats pool used for computing
	// timer quantiles to accommodate different usage patterns and reduce contention.
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("large-floats-pool"))
	largeFloatsPoolOpts := c.LargeFloatsPool.NewObjectPoolOptions(iOpts)
	largeFloatsPool := pool.NewFloatsPool(c.LargeFloatsPool.NewBuckets(), largeFloatsPoolOpts)
	opts = opts.SetLargeFloatsPool(largeFloatsPool)
	largeFloatsPool.Init()

	return opts
}

// protobufUnaggregatedIteratorConfiguration contains configuration for protobuf unaggregated iterator.
type protobufUnaggregatedIteratorConfiguration struct {
	// Initial buffer size.
	InitBufferSize *int `yaml:"initBufferSize"`

	// Maximum message size.
	MaxMessageSize *int `yaml:"maxMessageSize"`

	// Bytes pool.
	BytesPool pool.BucketizedPoolConfiguration `yaml:"bytesPool"`
}

func (c *protobufUnaggregatedIteratorConfiguration) NewOptions(
	instrumentOpts instrument.Options,
) protobuf.UnaggregatedOptions {
	opts := protobuf.NewUnaggregatedOptions()
	if c.InitBufferSize != nil {
		opts = opts.SetInitBufferSize(*c.InitBufferSize)
	}
	if c.MaxMessageSize != nil {
		opts = opts.SetMaxMessageSize(*c.MaxMessageSize)
	}

	// Set bytes pool.
	scope := instrumentOpts.MetricsScope()
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("bytes-pool"))
	objectPoolOpts := c.BytesPool.NewObjectPoolOptions(iOpts)
	buckets := c.BytesPool.NewBuckets()
	bytesPool := pool.NewBytesPool(buckets, objectPoolOpts)
	opts = opts.SetBytesPool(bytesPool)
	bytesPool.Init()

	return opts
}

// HTTPServerConfiguration contains http server configuration.
type HTTPServerConfiguration struct {
	// HTTP server listening address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// HTTP server read timeout.
	ReadTimeout time.Duration `yaml:"readTimeout"`

	// HTTP server write timeout.
	WriteTimeout time.Duration `yaml:"writeTimeout"`
}

// NewServerOptions create a new set of http server options.
func (c *HTTPServerConfiguration) NewServerOptions() http.Options {
	opts := http.NewOptions()
	if c.ReadTimeout != 0 {
		opts = opts.SetReadTimeout(c.ReadTimeout)
	}
	if c.WriteTimeout != 0 {
		opts = opts.SetWriteTimeout(c.WriteTimeout)
	}
	return opts
}
