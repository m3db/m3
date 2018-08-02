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

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

const (
	// defaultIndexInsertMode sets the default indexing mode to synchronous.
	defaultIndexInsertMode = InsertSync
)

var (
	errOptionsIdentifierPoolUnspecified = errors.New("identifier pool is unset")
	errOptionsBytesPoolUnspecified      = errors.New("checkedbytes pool is unset")
	errOptionsResultsPoolUnspecified    = errors.New("results pool is unset")
	errIDGenerationDisabled             = errors.New("id generation is disabled")
)

type opts struct {
	insertMode     InsertMode
	clockOpts      clock.Options
	instrumentOpts instrument.Options
	memOpts        mem.Options
	fstOpts        fst.Options
	idPool         ident.Pool
	bytesPool      pool.CheckedBytesPool
	resultsPool    ResultsPool
	compactOpts    CompactionOptions
}

var undefinedUUIDFn = func() ([]byte, error) { return nil, errIDGenerationDisabled }

// NewOptions returns a new index.Options object with default properties.
func NewOptions() Options {
	resultsPool := NewResultsPool(pool.NewObjectPoolOptions())
	bytesPool := pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	idPool := ident.NewPool(bytesPool, ident.PoolOptions{})
	// TODO(prateek): transitively update dependent fst/mem opts when any of the shared knobs are set
	memOpts := mem.NewOptions().SetNewUUIDFn(undefinedUUIDFn)
	fstOpts := fst.NewOptions().
		SetPostingsListPool(memOpts.PostingsListPool()).
		SetBytesPool(bytesPool.BytesPool())
	opts := &opts{
		insertMode:     defaultIndexInsertMode,
		clockOpts:      clock.NewOptions(),
		instrumentOpts: instrument.NewOptions(),
		bytesPool:      bytesPool,
		memOpts:        memOpts,
		fstOpts:        fstOpts,
		idPool:         idPool,
		resultsPool:    resultsPool,
		compactOpts:    NewCompactionOptions(),
	}
	resultsPool.Init(func() Results { return NewResults(opts) })
	return opts
}

func (o *opts) Validate() error {
	if o.idPool == nil {
		return errOptionsIdentifierPoolUnspecified
	}
	if o.bytesPool == nil {
		return errOptionsBytesPoolUnspecified
	}
	if o.resultsPool == nil {
		return errOptionsResultsPoolUnspecified
	}
	return nil
}

func (o *opts) SetInsertMode(value InsertMode) Options {
	opts := *o
	opts.insertMode = value
	return &opts
}

func (o *opts) InsertMode() InsertMode {
	return o.insertMode
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

func (o *opts) SetFSTSegmentOptions(value fst.Options) Options {
	opts := *o
	opts.fstOpts = value
	return &opts
}

func (o *opts) FSTSegmentOptions() fst.Options {
	return o.fstOpts
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

func (o *opts) SetCheckedBytesPool(value pool.CheckedBytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *opts) CheckedBytesPool() pool.CheckedBytesPool {
	return o.bytesPool
}

func (o *opts) SetResultsPool(value ResultsPool) Options {
	opts := *o
	opts.resultsPool = value
	return &opts
}

func (o *opts) ResultsPool() ResultsPool {
	return o.resultsPool
}

func (o *opts) SetCompactionOptions(value CompactionOptions) Options {
	opts := *o
	opts.compactOpts = value
	return &opts
}

func (o *opts) CompactionOptions() CompactionOptions {
	return o.compactOpts
}
