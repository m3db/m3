// Copyright (c) 2020 Uber Technologies, Inc.
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
	"errors"

	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3/src/x/instrument"
	xserver "github.com/m3db/m3/src/x/server"
)

var (
	errNoInstrumentOptions = errors.New("no instrument options")
	errNoServerOptions     = errors.New("no server options")
	errNoConsumerOptions   = errors.New("no consumer options")
)

// Options is a set of M3Msg options.
type Options interface {
	// Validate validates the options.
	Validate() error

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetServerOptions sets the server options.
	SetServerOptions(value xserver.Options) Options

	// ServerOptions returns the server options.
	ServerOptions() xserver.Options

	// SetConsumerOptions sets the consumer options.
	SetConsumerOptions(value consumer.Options) Options

	// ConsumerOptions returns the consumer options.
	ConsumerOptions() consumer.Options
}

type options struct {
	instrumentOpts instrument.Options
	serverOpts     xserver.Options
	consumerOpts   consumer.Options
}

// NewOptions returns a set of M3Msg options.
func NewOptions() Options {
	return &options{}
}

func (o *options) Validate() error {
	if o.instrumentOpts == nil {
		return errNoInstrumentOptions
	}
	if o.serverOpts == nil {
		return errNoServerOptions
	}
	if o.consumerOpts == nil {
		return errNoConsumerOptions
	}
	return nil
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetServerOptions(value xserver.Options) Options {
	opts := *o
	opts.serverOpts = value
	return &opts
}

func (o *options) ServerOptions() xserver.Options {
	return o.serverOpts
}

func (o *options) SetConsumerOptions(value consumer.Options) Options {
	opts := *o
	opts.consumerOpts = value
	return &opts
}

func (o *options) ConsumerOptions() consumer.Options {
	return o.consumerOpts
}
