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

package index

import (
	"errors"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/x/xpool"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultCheckedBytesPoolInitialSize = 128
)

var (
	errOptionsIdentifierPoolUnspecified = errors.New("identifier pool is unset")
	errOptionsWrapperPoolUnspecified    = errors.New("checkedbyteswrapper pool is unset")
)

type opts struct {
	clockOpts      clock.Options
	instrumentOpts instrument.Options
	memOpts        mem.Options
	idPool         ident.Pool
	wrapperPool    xpool.CheckedBytesWrapperPool
}

// NewOptions returns a new index.Options object with default properties.
func NewOptions() Options {
	idPool := ident.NewPool(nil, nil)
	wrapperPool := xpool.NewCheckedBytesWrapperPool(defaultCheckedBytesPoolInitialSize)
	wrapperPool.Init()
	return &opts{
		clockOpts:      clock.NewOptions(),
		instrumentOpts: instrument.NewOptions(),
		memOpts:        mem.NewOptions(),
		idPool:         idPool,
		wrapperPool:    wrapperPool,
	}

}

func (o *opts) Validate() error {
	if o.idPool == nil {
		return errOptionsIdentifierPoolUnspecified
	}
	if o.wrapperPool == nil {
		return errOptionsWrapperPoolUnspecified
	}
	return nil
}

func (o *opts) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *opts) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *opts) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	memOpts := opts.MemSegmentOptions().SetInstrumentOptions(value)
	opts.instrumentOpts = value
	opts.memOpts = memOpts
	return &opts
}

func (o *opts) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *opts) SetMemSegmentOptions(value mem.Options) Options {
	opts := *o
	opts.memOpts = value
	return &opts
}

func (o *opts) MemSegmentOptions() mem.Options {
	return o.memOpts
}

func (o *opts) SetIdentifierPool(value ident.Pool) Options {
	opts := *o
	opts.idPool = value
	return &opts
}

func (o *opts) IdentifierPool() ident.Pool {
	return o.idPool
}

func (o *opts) SetCheckedBytesWrapperPool(value xpool.CheckedBytesWrapperPool) Options {
	opts := *o
	opts.wrapperPool = value
	return &opts
}

func (o *opts) CheckedBytesWrapperPool() xpool.CheckedBytesWrapperPool {
	return o.wrapperPool
}
