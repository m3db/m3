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

package msgpack

import (
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/instrument"
	xserver "github.com/m3db/m3x/server"
)

// HandleFn handles a metric alongside its policies list.
type HandleFn func(unaggregated.MetricUnion, policy.PoliciesList) error

// Options provide a set of options for the msgpack server.
type Options interface {
	// SetHandleFn sets the handle fn.
	SetHandleFn(value HandleFn) Options

	// HandleFn returns the handle fn.
	HandleFn() HandleFn

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetServerOptions sets the server options.
	SetServerOptions(value xserver.Options) Options

	// ServerOptions returns the server options.
	ServerOptions() xserver.Options
}

type options struct {
	handleFn       HandleFn
	instrumentOpts instrument.Options
	serverOpts     xserver.Options
}

// NewOptions create a new set of options.
func NewOptions() Options {
	return &options{
		instrumentOpts: instrument.NewOptions(),
		serverOpts:     xserver.NewOptions(),
	}
}

func (o *options) SetHandleFn(value HandleFn) Options {
	opts := *o
	opts.handleFn = value
	return &opts
}

func (o *options) HandleFn() HandleFn {
	return o.handleFn
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
