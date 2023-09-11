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

package writer

import (
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

// Options provide a set of options for the writer.
type Options interface {
	// SetClockOptions is deprecated
	SetClockOptions(value clock.Options) Options

	// ClockOptions is deprecated
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetBytesPool sets the bytes pool.
	SetBytesPool(value pool.BytesPool) Options

	// BytesPool returns the bytes pool.
	BytesPool() pool.BytesPool

	// SetEncodingTimeSamplingRate is deprecated
	SetEncodingTimeSamplingRate(value float64) Options

	// EncodingTimeSamplingRate is deprecated
	EncodingTimeSamplingRate() float64
}

type options struct {
	clockOpts                clock.Options
	instrumentOpts           instrument.Options
	bytesPool                pool.BytesPool
	encodingTimeSamplingRate float64
}

// NewOptions provide a set of writer options.
func NewOptions() Options {
	return &options{
		clockOpts:      clock.NewOptions(),
		instrumentOpts: instrument.NewOptions(),
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

func (o *options) SetBytesPool(value pool.BytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) BytesPool() pool.BytesPool {
	return o.bytesPool
}

func (o *options) SetEncodingTimeSamplingRate(value float64) Options {
	return o
}

func (o *options) EncodingTimeSamplingRate() float64 {
	return 0
}
