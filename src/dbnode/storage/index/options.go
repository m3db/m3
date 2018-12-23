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
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

const (
	// defaultIndexInsertMode sets the default indexing mode to synchronous.
	defaultIndexInsertMode = InsertSync

	// documentArrayPool size in general: 256*256*sizeof(doc.Document)
	// = 256 * 256 * 16
	// = 1mb (but with Go's heap probably 2mb)
	// TODO(r): Make this configurable in a followup change.
	documentArrayPoolSize        = 256
	documentArrayPoolCapacity    = 256
	documentArrayPoolMaxCapacity = 256 // Do not allow grows, since we know the size
)

var (
	errOptionsIdentifierPoolUnspecified = errors.New("identifier pool is unset")
	errOptionsBytesPoolUnspecified      = errors.New("checkedbytes pool is unset")
	errOptionsResultsPoolUnspecified    = errors.New("results pool is unset")
	errIDGenerationDisabled             = errors.New("id generation is disabled")

	defaultForegroundCompactionOpts compaction.PlannerOptions
	defaultBackgroundCompactionOpts compaction.PlannerOptions
)

func init() {
	// Foreground compaction opts are the same as background compaction
	// but with only a single level, 1/4 the size of the first level of
	// the background compaction.
	defaultForegroundCompactionOpts = compaction.DefaultOptions
	defaultForegroundCompactionOpts.Levels = []compaction.Level{
		{
			MinSizeInclusive: 0,
			MaxSizeExclusive: 1 << 16,
		},
	}

	defaultBackgroundCompactionOpts = compaction.DefaultOptions
	defaultBackgroundCompactionOpts.Levels = []compaction.Level{
		{
			MinSizeInclusive: 0,
			MaxSizeExclusive: 1 << 18,
		},
		{
			MinSizeInclusive: 1 << 18,
			MaxSizeExclusive: 1 << 20,
		},
		{
			MinSizeInclusive: 1 << 20,
			MaxSizeExclusive: 1 << 22,
		},
	}
}

type opts struct {
	insertMode                      InsertMode
	clockOpts                       clock.Options
	instrumentOpts                  instrument.Options
	builderOpts                     builder.Options
	memOpts                         mem.Options
	fstOpts                         fst.Options
	idPool                          ident.Pool
	bytesPool                       pool.CheckedBytesPool
	resultsPool                     ResultsPool
	docArrayPool                    doc.DocumentArrayPool
	foregroundCompactionPlannerOpts compaction.PlannerOptions
	backgroundCompactionPlannerOpts compaction.PlannerOptions
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

	docArrayPool := doc.NewDocumentArrayPool(doc.DocumentArrayPoolOpts{
		Options: pool.NewObjectPoolOptions().
			SetSize(documentArrayPoolSize),
		Capacity:    documentArrayPoolCapacity,
		MaxCapacity: documentArrayPoolMaxCapacity,
	})
	docArrayPool.Init()

	instrumentOpts := instrument.NewOptions()
	opts := &opts{
		insertMode:                      defaultIndexInsertMode,
		clockOpts:                       clock.NewOptions(),
		instrumentOpts:                  instrumentOpts,
		builderOpts:                     builder.NewOptions().SetNewUUIDFn(undefinedUUIDFn),
		memOpts:                         mem.NewOptions().SetNewUUIDFn(undefinedUUIDFn),
		fstOpts:                         fst.NewOptions().SetInstrumentOptions(instrumentOpts),
		bytesPool:                       bytesPool,
		idPool:                          idPool,
		resultsPool:                     resultsPool,
		docArrayPool:                    docArrayPool,
		foregroundCompactionPlannerOpts: defaultForegroundCompactionOpts,
		backgroundCompactionPlannerOpts: defaultBackgroundCompactionOpts,
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
	fstOpts := opts.FSTSegmentOptions().SetInstrumentOptions(value)
	opts.instrumentOpts = value
	opts.memOpts = memOpts
	opts.fstOpts = fstOpts
	return &opts
}

func (o *opts) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *opts) SetSegmentBuilderOptions(value builder.Options) Options {
	opts := *o
	opts.builderOpts = value
	return &opts
}

func (o *opts) SegmentBuilderOptions() builder.Options {
	return o.builderOpts
}

func (o *opts) SetMemSegmentOptions(value mem.Options) Options {
	opts := *o
	opts.memOpts = value
	return &opts
}

func (o *opts) MemSegmentOptions() mem.Options {
	return o.memOpts
}

func (o *opts) SetFSTSegmentOptions(value fst.Options) Options {
	opts := *o
	opts.fstOpts = value
	return &opts
}

func (o *opts) FSTSegmentOptions() fst.Options {
	return o.fstOpts
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

func (o *opts) SetDocumentArrayPool(value doc.DocumentArrayPool) Options {
	opts := *o
	opts.docArrayPool = value
	return &opts
}

func (o *opts) DocumentArrayPool() doc.DocumentArrayPool {
	return o.docArrayPool
}

func (o *opts) SetForegroundCompactionPlannerOptions(value compaction.PlannerOptions) Options {
	opts := *o
	opts.foregroundCompactionPlannerOpts = value
	return &opts
}

func (o *opts) ForegroundCompactionPlannerOptions() compaction.PlannerOptions {
	return o.foregroundCompactionPlannerOpts
}

func (o *opts) SetBackgroundCompactionPlannerOptions(value compaction.PlannerOptions) Options {
	opts := *o
	opts.backgroundCompactionPlannerOpts = value
	return &opts
}

func (o *opts) BackgroundCompactionPlannerOptions() compaction.PlannerOptions {
	return o.backgroundCompactionPlannerOpts
}
