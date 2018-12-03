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

package m3msg

import (
	"github.com/m3db/m3/src/metrics/encoding/msgpack"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/server"
)

// Configuration configs the m3msg server.
type Configuration struct {
	// Server configs the server.
	Server server.Configuration `yaml:"server"`

	// Handler configs the handler.
	Handler handlerConfiguration `yaml:"handler"`

	// Consumer configs the consumer.
	Consumer consumer.Configuration `yaml:"consumer"`
}

// NewServer creates a new server.
func (c Configuration) NewServer(
	writeFn WriteFn,
	iOpts instrument.Options,
) (server.Server, error) {
	scope := iOpts.MetricsScope().Tagged(map[string]string{"server": "m3msg"})
	hOpts := c.Handler.NewOptions(
		writeFn,
		iOpts.SetMetricsScope(scope.Tagged(map[string]string{
			"handler": "msgpack",
		})),
	)
	msgpackHandler, err := newHandler(hOpts)
	if err != nil {
		return nil, err
	}
	return c.Server.NewServer(
		consumer.NewConsumerHandler(
			msgpackHandler.Handle,
			c.Consumer.NewOptions(
				iOpts.SetMetricsScope(scope.Tagged(map[string]string{
					"component": "consumer",
				})),
			),
		),
		iOpts.SetMetricsScope(scope),
	), nil
}

type handlerConfiguration struct {
	// Msgpack configs the msgpack iterator.
	Msgpack MsgpackIteratorConfiguration `yaml:"msgpack"`
}

// NewOptions creates handler options.
func (c handlerConfiguration) NewOptions(
	writeFn WriteFn,
	iOpts instrument.Options,
) Options {
	return Options{
		WriteFn:                   writeFn,
		InstrumentOptions:         iOpts,
		AggregatedIteratorOptions: c.Msgpack.NewOptions(),
	}
}

// MsgpackIteratorConfiguration configs the msgpack iterator.
type MsgpackIteratorConfiguration struct {
	// Whether to ignore encoded data streams whose version is higher than the current known version.
	IgnoreHigherVersion *bool `yaml:"ignoreHigherVersion"`

	// Reader buffer size.
	ReaderBufferSize *int `yaml:"readerBufferSize"`
}

// NewOptions creates a new msgpack aggregated iterator options.
func (c MsgpackIteratorConfiguration) NewOptions() msgpack.AggregatedIteratorOptions {
	opts := msgpack.NewAggregatedIteratorOptions()
	if c.IgnoreHigherVersion != nil {
		opts = opts.SetIgnoreHigherVersion(*c.IgnoreHigherVersion)
	}
	if c.ReaderBufferSize != nil {
		opts = opts.SetReaderBufferSize(*c.ReaderBufferSize)
	}
	return opts
}
