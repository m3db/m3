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

package aggregator

import (
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

const (
	defaultFlushTimesKeyFormat = "/shardset/%d/flush"
)

// FlushTimesManagerOptions provide a set of options for flush times manager.
type FlushTimesManagerOptions interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) FlushTimesManagerOptions

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) FlushTimesManagerOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetFlushTimesKeyFmt sets the flush times key format.
	SetFlushTimesKeyFmt(value string) FlushTimesManagerOptions

	// FlushTimesKeyFmt returns the flush times key format.
	FlushTimesKeyFmt() string

	// SetFlushTimesStore sets the flush times store.
	SetFlushTimesStore(value kv.Store) FlushTimesManagerOptions

	// FlushTimesStore returns the flush times store.
	FlushTimesStore() kv.Store

	// SetFlushTimesPersistRetrier sets the retrier for persisting flush times.
	SetFlushTimesPersistRetrier(value retry.Retrier) FlushTimesManagerOptions

	// FlushTimesPersistRetrier returns the retrier for persisting flush times.
	FlushTimesPersistRetrier() retry.Retrier
}

type flushTimesManagerOptions struct {
	clockOpts                clock.Options
	instrumentOpts           instrument.Options
	flushTimesKeyFmt         string
	flushTimesStore          kv.Store
	flushTimesPersistRetrier retry.Retrier
}

// NewFlushTimesManagerOptions create a new set of flush times manager options.
func NewFlushTimesManagerOptions() FlushTimesManagerOptions {
	return &flushTimesManagerOptions{
		clockOpts:                clock.NewOptions(),
		instrumentOpts:           instrument.NewOptions(),
		flushTimesKeyFmt:         defaultFlushTimesKeyFormat,
		flushTimesPersistRetrier: retry.NewRetrier(retry.NewOptions()),
	}
}

func (o *flushTimesManagerOptions) SetClockOptions(value clock.Options) FlushTimesManagerOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *flushTimesManagerOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *flushTimesManagerOptions) SetInstrumentOptions(value instrument.Options) FlushTimesManagerOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *flushTimesManagerOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *flushTimesManagerOptions) SetFlushTimesKeyFmt(value string) FlushTimesManagerOptions {
	opts := *o
	opts.flushTimesKeyFmt = value
	return &opts
}

func (o *flushTimesManagerOptions) FlushTimesKeyFmt() string {
	return o.flushTimesKeyFmt
}

func (o *flushTimesManagerOptions) SetFlushTimesStore(value kv.Store) FlushTimesManagerOptions {
	opts := *o
	opts.flushTimesStore = value
	return &opts
}

func (o *flushTimesManagerOptions) FlushTimesStore() kv.Store {
	return o.flushTimesStore
}

func (o *flushTimesManagerOptions) SetFlushTimesPersistRetrier(value retry.Retrier) FlushTimesManagerOptions {
	opts := *o
	opts.flushTimesPersistRetrier = value
	return &opts
}

func (o *flushTimesManagerOptions) FlushTimesPersistRetrier() retry.Retrier {
	return o.flushTimesPersistRetrier
}
