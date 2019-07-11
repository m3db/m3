package m3msg

import (
	coordinatorM3msg "github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/x/instrument"
)

// Options provides a set of options for the m3msg server
type Options interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetM3msgConfiguration sets the m3msg configuration used by the server.
	SetM3msgConfiguration(value *coordinatorM3msg.Configuration) Options

	// M3msgConfiguration returns the m3msg configuration used by the server.
	M3msgConfiguration() *coordinatorM3msg.Configuration
}

type options struct {
	instrumentOpts     instrument.Options
	m3msgConfiguration *coordinatorM3msg.Configuration
}

// NewServerOptions creates a new set of m3msg server options.
func NewServerOptions(
	iOpts instrument.Options,
	m3msgConfiguration *coordinatorM3msg.Configuration,
) (string, Options) {
	var addr string
	if m3msgConfiguration != nil {
		addr = m3msgConfiguration.Server.ListenAddress
	}
	return addr, &options{
		instrumentOpts:     iOpts,
		m3msgConfiguration: m3msgConfiguration,
	}
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetM3msgConfiguration(value *coordinatorM3msg.Configuration) Options {
	opts := *o
	opts.m3msgConfiguration = value
	return &opts
}

func (o *options) M3msgConfiguration() *coordinatorM3msg.Configuration {
	return o.m3msgConfiguration
}
