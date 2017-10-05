// Copyright (c) 2016 Uber Technologies, Inc.
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
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/server"
)

const (
	defaultErrorLogSamplingRate = 1.0
)

// Options provide a set of server options.
type Options interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetErrorLogSamplingRate sets the error log sampling rate.
	SetErrorLogSamplingRate(value float64) Options

	// ErrorLogSamplingRate returns the error log sampling rate.
	ErrorLogSamplingRate() float64

	// SetServerOptions sets the server options.
	SetServerOptions(value server.Options) Options

	// ServerOptiosn returns the server options.
	ServerOptions() server.Options

	// SetIteratorPool sets the iterator pool.
	SetIteratorPool(value msgpack.UnaggregatedIteratorPool) Options

	// IteratorPool returns the iterator pool.
	IteratorPool() msgpack.UnaggregatedIteratorPool
}

type options struct {
	clockOpts          clock.Options
	instrumentOpts     instrument.Options
	errLogSamplingRate float64
	serverOpts         server.Options
	iteratorPool       msgpack.UnaggregatedIteratorPool
}

// NewOptions creates a new set of server options.
func NewOptions() Options {
	iteratorPool := msgpack.NewUnaggregatedIteratorPool(nil)
	opts := msgpack.NewUnaggregatedIteratorOptions().SetIteratorPool(iteratorPool)
	iteratorPool.Init(func() msgpack.UnaggregatedIterator {
		return msgpack.NewUnaggregatedIterator(nil, opts)
	})

	return &options{
		clockOpts:          clock.NewOptions(),
		instrumentOpts:     instrument.NewOptions(),
		errLogSamplingRate: defaultErrorLogSamplingRate,
		serverOpts:         server.NewOptions(),
		iteratorPool:       iteratorPool,
	}
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetErrorLogSamplingRate(value float64) Options {
	opts := *o
	opts.errLogSamplingRate = value
	return &opts
}

func (o *options) ErrorLogSamplingRate() float64 {
	return o.errLogSamplingRate
}

func (o *options) SetServerOptions(value server.Options) Options {
	opts := *o
	opts.serverOpts = value
	return &opts
}

func (o *options) ServerOptions() server.Options {
	return o.serverOpts
}

func (o *options) SetIteratorPool(value msgpack.UnaggregatedIteratorPool) Options {
	opts := *o
	opts.iteratorPool = value
	return &opts
}

func (o *options) IteratorPool() msgpack.UnaggregatedIteratorPool {
	return o.iteratorPool
}
