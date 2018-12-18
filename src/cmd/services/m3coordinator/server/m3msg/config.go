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
	"github.com/m3db/m3x/pool"
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
	cOpts := c.Consumer.NewOptions(
		iOpts.SetMetricsScope(scope.Tagged(map[string]string{
			"component": "consumer",
		})),
	)
	h, err := c.Handler.newHandler(writeFn, cOpts, iOpts.SetMetricsScope(scope))
	if err != nil {
		return nil, err
	}
	return c.Server.NewServer(
		h,
		iOpts.SetMetricsScope(scope),
	), nil
}

type handlerConfiguration struct {
	// Msgpack configs the msgpack iterator.
	Msgpack MsgpackIteratorConfiguration `yaml:"msgpack"`

	// ProtobufDecoderPool configs the protobuf decoder pool.
	ProtobufDecoderPool pool.ObjectPoolConfiguration `yaml:"protobufDecoderPool"`

	// ProtobufEnabled configs whether protobuf decoding is enabled, if not,
	// msgpack decoding will be applied by default.
	ProtobufEnabled bool `yaml:"protobufEnabled"`
}

func (c handlerConfiguration) newHandler(
	writeFn WriteFn,
	cOpts consumer.Options,
	iOpts instrument.Options,
) (server.Handler, error) {
	if c.ProtobufEnabled {
		h := newProtobufHandler(Options{
			WriteFn: writeFn,
			InstrumentOptions: iOpts.SetMetricsScope(
				iOpts.MetricsScope().Tagged(map[string]string{
					"handler": "protobuf",
				}),
			),
			ProtobufDecoderPoolOptions: c.ProtobufDecoderPool.NewObjectPoolOptions(iOpts),
		})
		return consumer.NewMessageHandler(h.message, cOpts), nil
	}

	h, err := newMsgpackHandler(Options{
		WriteFn: writeFn,
		InstrumentOptions: iOpts.SetMetricsScope(
			iOpts.MetricsScope().Tagged(map[string]string{
				"handler": "msgpack",
			}),
		),
		AggregatedIteratorOptions: c.Msgpack.NewOptions(),
	})
	if err != nil {
		return nil, err
	}
	return consumer.NewConsumerHandler(
		h.Handle,
		cOpts,
	), nil
}

// NewOptions creates handler options.
func (c handlerConfiguration) NewOptions(
	writeFn WriteFn,
	iOpts instrument.Options,
) Options {
	return Options{
		WriteFn:                    writeFn,
		InstrumentOptions:          iOpts,
		AggregatedIteratorOptions:  c.Msgpack.NewOptions(),
		ProtobufDecoderPoolOptions: c.ProtobufDecoderPool.NewObjectPoolOptions(iOpts),
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
