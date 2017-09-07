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

package storage

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/counter"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

const (
	// defaultMaxFlushRetries is the default number of retries when flush fails
	defaultMaxFlushRetries = 3

	// defaultBytesPoolBucketCapacity is the default bytes buffer capacity for the default bytes pool bucket
	defaultBytesPoolBucketCapacity = 256

	// defaultBytesPoolBucketCount is the default count of elements for the default bytes pool bucket
	defaultBytesPoolBucketCount = 4096

	// defaultRepairEnabled enables repair by default
	defaultRepairEnabled = true

	// defaultErrorWindowForLoad is the default error window for evaluating server load
	defaultErrorWindowForLoad = 10 * time.Second

	// defaultErrorThresholdForLoad is the default error threshold for considering server overloaded
	defaultErrorThresholdForLoad = 1000

	// defaultTickInterval is the default tick interval
	defaultTickInterval = 2 * time.Minute
)

var (
	// defaultBootstrapProcess is the default bootstrap for the database
	defaultBootstrapProcess = bootstrap.NewNoOpProcess()

	// defaultPoolOptions are the pool options used by default
	defaultPoolOptions pool.ObjectPoolOptions

	timeZero time.Time
)

var (
	errNamespaceInitializerNotSet = errors.New("namespace registry initializer not set")
	errRepairOptionsNotSet        = errors.New("repair enabled but repair options are not set")
	errTickIntervalNegative       = errors.New("tick interval must be a positive duration")
)

// NewSeriesOptionsFromOptions creates a new set of database series options from provided options.
func NewSeriesOptionsFromOptions(opts Options, ropts retention.Options) series.Options {
	if ropts == nil {
		ropts = retention.NewOptions()
	}

	return opts.SeriesOptions().
		SetClockOptions(opts.ClockOptions()).
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetRetentionOptions(ropts).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions()).
		SetContextPool(opts.ContextPool()).
		SetEncoderPool(opts.EncoderPool()).
		SetMultiReaderIteratorPool(opts.MultiReaderIteratorPool()).
		SetIdentifierPool(opts.IdentifierPool())
}

type options struct {
	clockOpts                      clock.Options
	instrumentOpts                 instrument.Options
	nsRegistryInitializer          namespace.Initializer
	blockOpts                      block.Options
	commitLogOpts                  commitlog.Options
	runtimeOptsMgr                 runtime.OptionsManager
	errCounterOpts                 xcounter.Options
	errWindowForLoad               time.Duration
	errThresholdForLoad            int64
	repairEnabled                  bool
	repairOpts                     repair.Options
	newEncoderFn                   encoding.NewEncoderFn
	newDecoderFn                   encoding.NewDecoderFn
	bootstrapProcess               bootstrap.Process
	persistManager                 persist.Manager
	tickInterval                   time.Duration
	maxFlushRetries                int
	blockRetrieverManager          block.DatabaseBlockRetrieverManager
	poolOpts                       pool.ObjectPoolOptions
	contextPool                    context.Pool
	seriesOpts                     series.Options
	seriesPool                     series.DatabaseSeriesPool
	bytesPool                      pool.CheckedBytesPool
	encoderPool                    encoding.EncoderPool
	segmentReaderPool              xio.SegmentReaderPool
	readerIteratorPool             encoding.ReaderIteratorPool
	multiReaderIteratorPool        encoding.MultiReaderIteratorPool
	identifierPool                 ts.IdentifierPool
	fetchBlockMetadataResultsPool  block.FetchBlockMetadataResultsPool
	fetchBlocksMetadataResultsPool block.FetchBlocksMetadataResultsPool
}

// NewOptions creates a new set of storage options with defaults
func NewOptions() Options {
	return newOptions(defaultPoolOptions)
}

func newOptions(poolOpts pool.ObjectPoolOptions) Options {
	bytesPool := pool.NewCheckedBytesPool(nil, poolOpts, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, poolOpts)
	})
	bytesPool.Init()
	seriesOpts := series.NewOptions()
	o := &options{
		clockOpts:                      clock.NewOptions(),
		instrumentOpts:                 instrument.NewOptions(),
		blockOpts:                      block.NewOptions(),
		commitLogOpts:                  commitlog.NewOptions(),
		runtimeOptsMgr:                 runtime.NewOptionsManager(runtime.NewOptions()),
		errCounterOpts:                 xcounter.NewOptions(),
		errWindowForLoad:               defaultErrorWindowForLoad,
		errThresholdForLoad:            defaultErrorThresholdForLoad,
		repairEnabled:                  defaultRepairEnabled,
		repairOpts:                     repair.NewOptions(),
		bootstrapProcess:               defaultBootstrapProcess,
		persistManager:                 fs.NewPersistManager(fs.NewOptions()),
		tickInterval:                   defaultTickInterval,
		maxFlushRetries:                defaultMaxFlushRetries,
		poolOpts:                       poolOpts,
		contextPool:                    context.NewPool(poolOpts, poolOpts),
		seriesOpts:                     seriesOpts,
		seriesPool:                     series.NewDatabaseSeriesPool(poolOpts),
		bytesPool:                      bytesPool,
		encoderPool:                    encoding.NewEncoderPool(poolOpts),
		segmentReaderPool:              xio.NewSegmentReaderPool(poolOpts),
		readerIteratorPool:             encoding.NewReaderIteratorPool(poolOpts),
		multiReaderIteratorPool:        encoding.NewMultiReaderIteratorPool(poolOpts),
		identifierPool:                 ts.NewIdentifierPool(bytesPool, poolOpts),
		fetchBlockMetadataResultsPool:  block.NewFetchBlockMetadataResultsPool(poolOpts, 0),
		fetchBlocksMetadataResultsPool: block.NewFetchBlocksMetadataResultsPool(poolOpts, 0),
	}
	return o.SetEncodingM3TSZPooled()
}

func (o *options) Validate() error {
	if o.tickInterval <= 0 {
		return errTickIntervalNegative
	}

	// validate namespace registry
	init := o.NamespaceInitializer()
	if init == nil {
		return errNamespaceInitializerNotSet
	}

	// validate commit log options
	clOpts := o.CommitLogOptions()
	if err := clOpts.Validate(); err != nil {
		return fmt.Errorf("unable to validate commit log options: %v", err)
	}

	// validate repair options
	if o.RepairEnabled() {
		rOpts := o.RepairOptions()
		if rOpts == nil {
			return errRepairOptionsNotSet
		}
		if err := rOpts.Validate(); err != nil {
			return fmt.Errorf("unable to validate repair options, err: %v", err)
		}
	}

	return nil
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	opts.commitLogOpts = opts.commitLogOpts.SetClockOptions(value)
	opts.seriesOpts = NewSeriesOptionsFromOptions(&opts, nil)
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	opts.commitLogOpts = opts.commitLogOpts.SetInstrumentOptions(value)
	opts.seriesOpts = NewSeriesOptionsFromOptions(&opts, nil)
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetNamespaceInitializer(value namespace.Initializer) Options {
	opts := *o
	opts.nsRegistryInitializer = value
	return &opts
}

func (o *options) NamespaceInitializer() namespace.Initializer {
	return o.nsRegistryInitializer
}

func (o *options) SetDatabaseBlockOptions(value block.Options) Options {
	opts := *o
	opts.blockOpts = value
	opts.seriesOpts = NewSeriesOptionsFromOptions(&opts, nil)
	return &opts
}

func (o *options) DatabaseBlockOptions() block.Options {
	return o.blockOpts
}

func (o *options) SetCommitLogOptions(value commitlog.Options) Options {
	opts := *o
	opts.commitLogOpts = value
	return &opts
}

func (o *options) CommitLogOptions() commitlog.Options {
	return o.commitLogOpts
}

func (o *options) SetRuntimeOptionsManager(value runtime.OptionsManager) Options {
	opts := *o
	opts.runtimeOptsMgr = value
	return &opts
}

func (o *options) RuntimeOptionsManager() runtime.OptionsManager {
	return o.runtimeOptsMgr
}

func (o *options) SetErrorCounterOptions(value xcounter.Options) Options {
	opts := *o
	opts.errCounterOpts = value
	return &opts
}

func (o *options) ErrorCounterOptions() xcounter.Options {
	return o.errCounterOpts
}

func (o *options) SetErrorWindowForLoad(value time.Duration) Options {
	opts := *o
	opts.errWindowForLoad = value
	return &opts
}

func (o *options) ErrorWindowForLoad() time.Duration {
	return o.errWindowForLoad
}

func (o *options) SetErrorThresholdForLoad(value int64) Options {
	opts := *o
	opts.errThresholdForLoad = value
	return &opts
}

func (o *options) ErrorThresholdForLoad() int64 {
	return o.errThresholdForLoad
}

func (o *options) SetRepairEnabled(b bool) Options {
	opts := *o
	opts.repairEnabled = b
	return &opts
}

func (o *options) RepairEnabled() bool {
	return o.repairEnabled
}

func (o *options) SetRepairOptions(value repair.Options) Options {
	opts := *o
	opts.repairOpts = value
	return &opts
}

func (o *options) RepairOptions() repair.Options {
	return o.repairOpts
}

func (o *options) SetEncodingM3TSZPooled() Options {
	opts := *o

	buckets := []pool.Bucket{{
		Capacity: defaultBytesPoolBucketCapacity,
		Count:    defaultBytesPoolBucketCount,
	}}
	newBackingBytesPool := func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, opts.poolOpts)
	}
	bytesPool := pool.NewCheckedBytesPool(buckets, o.poolOpts, newBackingBytesPool)
	bytesPool.Init()
	opts.bytesPool = bytesPool

	// initialize context pool
	opts.contextPool = context.NewPool(opts.poolOpts, opts.poolOpts)

	encoderPool := encoding.NewEncoderPool(opts.poolOpts)
	readerIteratorPool := encoding.NewReaderIteratorPool(opts.poolOpts)

	// initialize segment reader pool
	segmentReaderPool := xio.NewSegmentReaderPool(opts.poolOpts)
	segmentReaderPool.Init()
	opts.segmentReaderPool = segmentReaderPool

	encodingOpts := encoding.NewOptions().
		SetBytesPool(bytesPool).
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(readerIteratorPool).
		SetSegmentReaderPool(segmentReaderPool)

	// initialize encoder pool
	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(timeZero, nil, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})
	opts.encoderPool = encoderPool

	// initialize single reader iterator pool
	readerIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})
	opts.readerIteratorPool = readerIteratorPool

	// initialize multi reader iterator pool
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(opts.poolOpts)
	multiReaderIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})
	opts.multiReaderIteratorPool = multiReaderIteratorPool

	opts.blockOpts = opts.blockOpts.
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(readerIteratorPool).
		SetMultiReaderIteratorPool(multiReaderIteratorPool).
		SetBytesPool(bytesPool)

	opts.seriesOpts = NewSeriesOptionsFromOptions(&opts, nil)
	return &opts
}

func (o *options) SetNewEncoderFn(value encoding.NewEncoderFn) Options {
	opts := *o
	opts.newEncoderFn = value
	return &opts
}

func (o *options) NewEncoderFn() encoding.NewEncoderFn {
	return o.newEncoderFn
}

func (o *options) SetNewDecoderFn(value encoding.NewDecoderFn) Options {
	opts := *o
	opts.newDecoderFn = value
	return &opts
}

func (o *options) NewDecoderFn() encoding.NewDecoderFn {
	return o.newDecoderFn
}

func (o *options) SetBootstrapProcess(value bootstrap.Process) Options {
	opts := *o
	opts.bootstrapProcess = value
	return &opts
}

func (o *options) BootstrapProcess() bootstrap.Process {
	return o.bootstrapProcess
}

func (o *options) SetPersistManager(value persist.Manager) Options {
	opts := *o
	opts.persistManager = value
	return &opts
}

func (o *options) PersistManager() persist.Manager {
	return o.persistManager
}

func (o *options) SetTickInterval(value time.Duration) Options {
	opts := *o
	opts.tickInterval = value
	return &opts
}

func (o *options) TickInterval() time.Duration {
	return o.tickInterval
}

func (o *options) SetMaxFlushRetries(value int) Options {
	opts := *o
	opts.maxFlushRetries = value
	return &opts
}

func (o *options) MaxFlushRetries() int {
	return o.maxFlushRetries
}

func (o *options) SetDatabaseBlockRetrieverManager(value block.DatabaseBlockRetrieverManager) Options {
	opts := *o
	opts.blockRetrieverManager = value
	return &opts
}

func (o *options) DatabaseBlockRetrieverManager() block.DatabaseBlockRetrieverManager {
	return o.blockRetrieverManager
}

func (o *options) SetContextPool(value context.Pool) Options {
	opts := *o
	opts.contextPool = value
	return &opts
}

func (o *options) ContextPool() context.Pool {
	return o.contextPool
}

func (o *options) SetSeriesOptions(value series.Options) Options {
	opts := *o
	opts.seriesOpts = value
	return &opts
}

func (o *options) SeriesOptions() series.Options {
	return o.seriesOpts
}

func (o *options) SetDatabaseSeriesPool(value series.DatabaseSeriesPool) Options {
	opts := *o
	opts.seriesPool = value
	return &opts
}

func (o *options) DatabaseSeriesPool() series.DatabaseSeriesPool {
	return o.seriesPool
}

func (o *options) SetBytesPool(value pool.CheckedBytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) BytesPool() pool.CheckedBytesPool {
	return o.bytesPool
}

func (o *options) SetEncoderPool(value encoding.EncoderPool) Options {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *options) EncoderPool() encoding.EncoderPool {
	return o.encoderPool
}

func (o *options) SetSegmentReaderPool(value xio.SegmentReaderPool) Options {
	opts := *o
	opts.segmentReaderPool = value
	return &opts
}

func (o *options) SegmentReaderPool() xio.SegmentReaderPool {
	return o.segmentReaderPool
}

func (o *options) SetReaderIteratorPool(value encoding.ReaderIteratorPool) Options {
	opts := *o
	opts.readerIteratorPool = value
	return &opts
}

func (o *options) ReaderIteratorPool() encoding.ReaderIteratorPool {
	return o.readerIteratorPool
}

func (o *options) SetMultiReaderIteratorPool(value encoding.MultiReaderIteratorPool) Options {
	opts := *o
	opts.multiReaderIteratorPool = value
	return &opts
}

func (o *options) MultiReaderIteratorPool() encoding.MultiReaderIteratorPool {
	return o.multiReaderIteratorPool
}

func (o *options) SetIdentifierPool(value ts.IdentifierPool) Options {
	opts := *o
	opts.identifierPool = value
	return &opts
}

func (o *options) IdentifierPool() ts.IdentifierPool {
	return o.identifierPool
}

func (o *options) SetFetchBlockMetadataResultsPool(value block.FetchBlockMetadataResultsPool) Options {
	opts := *o
	opts.fetchBlockMetadataResultsPool = value
	return &opts
}

func (o *options) FetchBlockMetadataResultsPool() block.FetchBlockMetadataResultsPool {
	return o.fetchBlockMetadataResultsPool
}

func (o *options) SetFetchBlocksMetadataResultsPool(value block.FetchBlocksMetadataResultsPool) Options {
	opts := *o
	opts.fetchBlocksMetadataResultsPool = value
	return &opts
}

func (o *options) FetchBlocksMetadataResultsPool() block.FetchBlocksMetadataResultsPool {
	return o.fetchBlocksMetadataResultsPool
}
