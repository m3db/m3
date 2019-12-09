// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/x/instrument"
)

var (
	errServerAddressEmpty = errors.New("m3msg server address is empty")
)

// Options provides a set of options for the m3msg server
type Options interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetM3msgConfiguration sets the m3msg configuration used by the server.
	SetM3MsgConfiguration(value *m3msg.Configuration) Options

	// M3msgConfiguration returns the m3msg configuration used by the server.
	M3MsgConfiguration() *m3msg.Configuration

	// Validate validates the m3msg server options and returns an error in case
	Validate() error
}

type options struct {
	instrumentOpts     instrument.Options
	m3msgConfiguration *m3msg.Configuration
}

// NewServerOptions creates a new set of m3msg server options.
func NewServerOptions(
	iOpts instrument.Options,
	m3msgConfiguration *m3msg.Configuration,
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

func (o *options) SetM3MsgConfiguration(value *m3msg.Configuration) Options {
	opts := *o
	opts.m3msgConfiguration = value
	return &opts
}

func (o *options) M3MsgConfiguration() *m3msg.Configuration {
	return o.m3msgConfiguration
}

func (o *options) Validate() error {
	if o.M3MsgConfiguration().Server.ListenAddress == "" {
		return errServerAddressEmpty
	}
	return nil
}
