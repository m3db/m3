package m3msg

import (
	"github.com/m3db/m3metrics/encoding/msgpack"
	"github.com/m3db/m3msg/consumer"
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
		consumer.NewHandler(
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
