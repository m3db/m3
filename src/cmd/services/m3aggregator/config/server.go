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
	"github.com/m3db/m3/src/aggregator/server/m3msg"
	"github.com/m3db/m3/src/aggregator/server/rawtcp"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/retry"
	xserver "github.com/m3db/m3/src/x/server"
)

// M3MsgServerConfiguration contains M3Msg server configuration.
type M3MsgServerConfiguration struct {
	// Server is the server configuration.
	Server xserver.Configuration `yaml:"server"`

	// Consumer is the M3Msg consumer configuration.
	Consumer consumer.Configuration `yaml:"consumer"`
}

// NewServerOptions creates a new set of M3Msg server options.
func (c *M3MsgServerConfiguration) NewServerOptions(
	instrumentOpts instrument.Options,
) (m3msg.Options, error) {
	opts := m3msg.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetServerOptions(c.Server.NewOptions(instrumentOpts)).
		SetConsumerOptions(c.Consumer.NewOptions(instrumentOpts))
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return opts, nil
}

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

	// ProtobufIterator configuration is deprecated.
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
	if c.ReadBufferSize != nil {
		opts = opts.SetReadBufferSize(*c.ReadBufferSize)
	}
	if c.ErrorLogLimitPerSecond != nil {
		opts = opts.SetErrorLogLimitPerSecond(*c.ErrorLogLimitPerSecond)
	}
	return opts
}

// protobufUnaggregatedIteratorConfiguration is deprecated.
type protobufUnaggregatedIteratorConfiguration struct {
	// Initial buffer size.
	InitBufferSize *int `yaml:"initBufferSize"`

	// Maximum message size.
	MaxMessageSize *int `yaml:"maxMessageSize"`

	// Bytes pool.
	BytesPool pool.BucketizedPoolConfiguration `yaml:"bytesPool"`
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
