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
	"math"
	"runtime"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/storage/repair"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/mmap"
	"github.com/m3db/m3/src/x/pool"
	xsync "github.com/m3db/m3/src/x/sync"
)

const (
	// defaultBytesPoolBucketCapacity is the default bytes buffer capacity for the default bytes pool bucket.
	defaultBytesPoolBucketCapacity = 256

	// defaultBytesPoolBucketCount is the default count of elements for the default bytes pool bucket.
	defaultBytesPoolBucketCount = 4096

	// defaultRepairEnabled enables repair by default.
	defaultRepairEnabled = true

	// defaultErrorWindowForLoad is the default error window for evaluating server load.
	defaultErrorWindowForLoad = 10 * time.Second

	// defaultErrorThresholdForLoad is the default error threshold for considering server overloaded.
	defaultErrorThresholdForLoad = 1000

	// defaultIndexingEnabled disables indexing by default.
	defaultIndexingEnabled = false

	// defaultNumLoadedBytesLimit is the default limit (2GiB) for the number of outstanding loaded bytes that
	// the memory tracker will allow.
	defaultNumLoadedBytesLimit = 2 << 30

	defaultMediatorTickInterval = 5 * time.Second

	// defaultWideBatchSize is the default batch size for wide queries.
	defaultWideBatchSize = 1024
)

var (
	// defaultBootstrapProcessProvider is the default bootstrap provider for the database.
	defaultBootstrapProcessProvider = bootstrap.NewNoOpProcessProvider()

	// defaultPoolOptions are the pool options used by default.
	defaultPoolOptions pool.ObjectPoolOptions

	timeZero time.Time
)

var (
	errNamespaceInitializerNotSet = errors.New("namespace registry initializer not set")
	errRepairOptionsNotSet        = errors.New("repair enabled but repair options are not set")
	errIndexOptionsNotSet         = errors.New("index enabled but index options are not set")
	errPersistManagerNotSet       = errors.New("persist manager is not set")
	errIndexClaimsManagerNotSet   = errors.New("index claims manager is not set")
	errBlockLeaserNotSet          = errors.New("block leaser is not set")
	errOnColdFlushNotSet          = errors.New("on cold flush is not set, requires at least a no-op implementation")
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
		SetCachePolicy(opts.SeriesCachePolicy()).
		SetContextPool(opts.ContextPool()).
		SetEncoderPool(opts.EncoderPool()).
		SetMultiReaderIteratorPool(opts.MultiReaderIteratorPool()).
		SetIdentifierPool(opts.IdentifierPool()).
		SetBufferBucketPool(opts.BufferBucketPool()).
		SetBufferBucketVersionsPool(opts.BufferBucketVersionsPool()).
		SetRuntimeOptionsManager(opts.RuntimeOptionsManager())
}

type options struct {
	clockOpts                       clock.Options
	instrumentOpts                  instrument.Options
	nsRegistryInitializer           namespace.Initializer
	blockOpts                       block.Options
	commitLogOpts                   commitlog.Options
	runtimeOptsMgr                  m3dbruntime.OptionsManager
	errWindowForLoad                time.Duration
	errThresholdForLoad             int64
	indexingEnabled                 bool
	repairEnabled                   bool
	truncateType                    series.TruncateType
	transformOptions                series.WriteTransformOptions
	indexOpts                       index.Options
	repairOpts                      repair.Options
	newEncoderFn                    encoding.NewEncoderFn
	newDecoderFn                    encoding.NewDecoderFn
	bootstrapProcessProvider        bootstrap.ProcessProvider
	persistManager                  persist.Manager
	indexClaimsManager              fs.IndexClaimsManager
	blockRetrieverManager           block.DatabaseBlockRetrieverManager
	poolOpts                        pool.ObjectPoolOptions
	contextPool                     context.Pool
	seriesCachePolicy               series.CachePolicy
	seriesOpts                      series.Options
	seriesPool                      series.DatabaseSeriesPool
	bytesPool                       pool.CheckedBytesPool
	encoderPool                     encoding.EncoderPool
	segmentReaderPool               xio.SegmentReaderPool
	readerIteratorPool              encoding.ReaderIteratorPool
	multiReaderIteratorPool         encoding.MultiReaderIteratorPool
	identifierPool                  ident.Pool
	fetchBlockMetadataResultsPool   block.FetchBlockMetadataResultsPool
	fetchBlocksMetadataResultsPool  block.FetchBlocksMetadataResultsPool
	queryIDsWorkerPool              xsync.WorkerPool
	writeBatchPool                  *writes.WriteBatchPool
	bufferBucketPool                *series.BufferBucketPool
	bufferBucketVersionsPool        *series.BufferBucketVersionsPool
	retrieveRequestPool             fs.RetrieveRequestPool
	checkedBytesWrapperPool         xpool.CheckedBytesWrapperPool
	schemaReg                       namespace.SchemaRegistry
	blockLeaseManager               block.LeaseManager
	onColdFlush                     OnColdFlush
	forceColdWritesEnabled          bool
	sourceLoggerBuilder             limits.SourceLoggerBuilder
	iterationOptions                index.IterationOptions
	memoryTracker                   MemoryTracker
	mmapReporter                    mmap.Reporter
	doNotIndexWithFieldsMap         map[string]string
	namespaceRuntimeOptsMgrRegistry namespace.RuntimeOptionsManagerRegistry
	mediatorTickInterval            time.Duration
	adminClient                     client.AdminClient
	wideBatchSize                   int
	newBackgroundProcessFns         []NewBackgroundProcessFn
	namespaceHooks                  NamespaceHooks
	tileAggregator                  TileAggregator
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

	// Default to using half of the available cores for querying IDs
	queryIDsWorkerPool := xsync.NewWorkerPool(int(math.Ceil(float64(runtime.NumCPU()) / 2)))
	queryIDsWorkerPool.Init()

	writeBatchPool := writes.NewWriteBatchPool(poolOpts, nil, nil)
	writeBatchPool.Init()

	segmentReaderPool := xio.NewSegmentReaderPool(poolOpts)
	segmentReaderPool.Init()

	retrieveRequestPool := fs.NewRetrieveRequestPool(segmentReaderPool, poolOpts)
	retrieveRequestPool.Init()

	bytesWrapperPool := xpool.NewCheckedBytesWrapperPool(poolOpts)
	bytesWrapperPool.Init()

	o := &options{
		clockOpts:                clock.NewOptions(),
		instrumentOpts:           instrument.NewOptions(),
		blockOpts:                block.NewOptions(),
		commitLogOpts:            commitlog.NewOptions(),
		runtimeOptsMgr:           m3dbruntime.NewOptionsManager(),
		errWindowForLoad:         defaultErrorWindowForLoad,
		errThresholdForLoad:      defaultErrorThresholdForLoad,
		indexingEnabled:          defaultIndexingEnabled,
		indexOpts:                index.NewOptions(),
		repairEnabled:            defaultRepairEnabled,
		repairOpts:               repair.NewOptions(),
		bootstrapProcessProvider: defaultBootstrapProcessProvider,
		poolOpts:                 poolOpts,
		contextPool: context.NewPool(context.NewOptions().
			SetContextPoolOptions(poolOpts).
			SetFinalizerPoolOptions(poolOpts)),
		seriesCachePolicy:       series.DefaultCachePolicy,
		seriesOpts:              seriesOpts,
		seriesPool:              series.NewDatabaseSeriesPool(poolOpts),
		bytesPool:               bytesPool,
		encoderPool:             encoding.NewEncoderPool(poolOpts),
		segmentReaderPool:       segmentReaderPool,
		readerIteratorPool:      encoding.NewReaderIteratorPool(poolOpts),
		multiReaderIteratorPool: encoding.NewMultiReaderIteratorPool(poolOpts),
		identifierPool: ident.NewPool(bytesPool, ident.PoolOptions{
			IDPoolOptions:           poolOpts,
			TagsPoolOptions:         poolOpts,
			TagsIteratorPoolOptions: poolOpts,
		}),
		fetchBlockMetadataResultsPool:   block.NewFetchBlockMetadataResultsPool(poolOpts, 0),
		fetchBlocksMetadataResultsPool:  block.NewFetchBlocksMetadataResultsPool(poolOpts, 0),
		queryIDsWorkerPool:              queryIDsWorkerPool,
		writeBatchPool:                  writeBatchPool,
		bufferBucketVersionsPool:        series.NewBufferBucketVersionsPool(poolOpts),
		bufferBucketPool:                series.NewBufferBucketPool(poolOpts),
		retrieveRequestPool:             retrieveRequestPool,
		checkedBytesWrapperPool:         bytesWrapperPool,
		schemaReg:                       namespace.NewSchemaRegistry(false, nil),
		onColdFlush:                     &noOpColdFlush{},
		memoryTracker:                   NewMemoryTracker(NewMemoryTrackerOptions(defaultNumLoadedBytesLimit)),
		namespaceRuntimeOptsMgrRegistry: namespace.NewRuntimeOptionsManagerRegistry(),
		mediatorTickInterval:            defaultMediatorTickInterval,
		wideBatchSize:                   defaultWideBatchSize,
		namespaceHooks:                  &noopNamespaceHooks{},
		tileAggregator:                  &noopTileAggregator{},
	}
	return o.SetEncodingM3TSZPooled()
}

func (o *options) Validate() error {
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

	// validate indexing options
	iOpts := o.IndexOptions()
	if iOpts == nil {
		return errIndexOptionsNotSet
	}
	if err := iOpts.Validate(); err != nil {
		return fmt.Errorf("unable to validate index options, err: %v", err)
	}

	// validate that persist manager is present, if not return
	// error if error occurred during default creation otherwise
	// it was set to nil by a caller
	if o.persistManager == nil {
		return errPersistManagerNotSet
	}

	// validate that index claims manager is present
	if o.indexClaimsManager == nil {
		return errIndexClaimsManagerNotSet
	}

	// validate series cache policy
	if err := series.ValidateCachePolicy(o.seriesCachePolicy); err != nil {
		return err
	}

	if o.blockLeaseManager == nil {
		return errBlockLeaserNotSet
	}

	if o.onColdFlush == nil {
		return errOnColdFlushNotSet
	}

	return nil
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	opts.commitLogOpts = opts.commitLogOpts.SetClockOptions(value)
	opts.indexOpts = opts.indexOpts.SetClockOptions(value)
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
	opts.indexOpts = opts.indexOpts.SetInstrumentOptions(value)
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

func (o *options) SetRuntimeOptionsManager(value m3dbruntime.OptionsManager) Options {
	opts := *o
	opts.runtimeOptsMgr = value
	return &opts
}

func (o *options) RuntimeOptionsManager() m3dbruntime.OptionsManager {
	return o.runtimeOptsMgr
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

func (o *options) SetIndexOptions(value index.Options) Options {
	opts := *o
	opts.indexOpts = value
	return &opts
}

func (o *options) IndexOptions() index.Options {
	return o.indexOpts
}

func (o *options) SetRepairEnabled(b bool) Options {
	opts := *o
	opts.repairEnabled = b
	return &opts
}

func (o *options) RepairEnabled() bool {
	return o.repairEnabled
}

func (o *options) SetTruncateType(value series.TruncateType) Options {
	opts := *o
	opts.truncateType = value
	return &opts
}

func (o *options) TruncateType() series.TruncateType {
	return o.truncateType
}

func (o *options) SetWriteTransformOptions(
	value series.WriteTransformOptions,
) Options {
	opts := *o
	opts.transformOptions = value
	return &opts
}

func (o *options) WriteTransformOptions() series.WriteTransformOptions {
	return o.transformOptions
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
	opts.contextPool = context.NewPool(context.NewOptions().
		SetContextPoolOptions(opts.poolOpts).
		SetFinalizerPoolOptions(opts.poolOpts))

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
	readerIteratorPool.Init(func(r io.Reader, descr namespace.SchemaDescr) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})
	opts.readerIteratorPool = readerIteratorPool

	// initialize multi reader iterator pool
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(opts.poolOpts)
	multiReaderIteratorPool.Init(func(r io.Reader, descr namespace.SchemaDescr) encoding.ReaderIterator {
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

func (o *options) SetBootstrapProcessProvider(value bootstrap.ProcessProvider) Options {
	opts := *o
	opts.bootstrapProcessProvider = value
	return &opts
}

func (o *options) BootstrapProcessProvider() bootstrap.ProcessProvider {
	return o.bootstrapProcessProvider
}

func (o *options) SetPersistManager(value persist.Manager) Options {
	opts := *o
	opts.persistManager = value
	return &opts
}

func (o *options) PersistManager() persist.Manager {
	return o.persistManager
}

func (o *options) SetIndexClaimsManager(value fs.IndexClaimsManager) Options {
	opts := *o
	opts.indexClaimsManager = value
	return &opts
}

func (o *options) IndexClaimsManager() fs.IndexClaimsManager {
	return o.indexClaimsManager
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

func (o *options) SetSeriesCachePolicy(value series.CachePolicy) Options {
	opts := *o
	opts.seriesCachePolicy = value
	return &opts
}

func (o *options) SeriesCachePolicy() series.CachePolicy {
	return o.seriesCachePolicy
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

func (o *options) SetIdentifierPool(value ident.Pool) Options {
	opts := *o
	opts.indexOpts = opts.indexOpts.SetIdentifierPool(value)
	opts.identifierPool = value
	return &opts
}

func (o *options) IdentifierPool() ident.Pool {
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

func (o *options) SetQueryIDsWorkerPool(value xsync.WorkerPool) Options {
	opts := *o
	opts.queryIDsWorkerPool = value
	return &opts
}

func (o *options) QueryIDsWorkerPool() xsync.WorkerPool {
	return o.queryIDsWorkerPool
}

func (o *options) SetWriteBatchPool(value *writes.WriteBatchPool) Options {
	opts := *o
	opts.writeBatchPool = value
	return &opts
}

func (o *options) WriteBatchPool() *writes.WriteBatchPool {
	return o.writeBatchPool
}

func (o *options) SetBufferBucketPool(value *series.BufferBucketPool) Options {
	opts := *o
	opts.bufferBucketPool = value
	return &opts
}

func (o *options) BufferBucketPool() *series.BufferBucketPool {
	return o.bufferBucketPool
}

func (o *options) SetBufferBucketVersionsPool(value *series.BufferBucketVersionsPool) Options {
	opts := *o
	opts.bufferBucketVersionsPool = value
	return &opts
}

func (o *options) BufferBucketVersionsPool() *series.BufferBucketVersionsPool {
	return o.bufferBucketVersionsPool
}

func (o *options) SetRetrieveRequestPool(value fs.RetrieveRequestPool) Options {
	opts := *o
	opts.retrieveRequestPool = value
	return &opts
}

func (o *options) RetrieveRequestPool() fs.RetrieveRequestPool {
	return o.retrieveRequestPool
}

func (o *options) SetCheckedBytesWrapperPool(value xpool.CheckedBytesWrapperPool) Options {
	opts := *o
	opts.checkedBytesWrapperPool = value
	return &opts
}

func (o *options) CheckedBytesWrapperPool() xpool.CheckedBytesWrapperPool {
	return o.checkedBytesWrapperPool
}

func (o *options) SetSchemaRegistry(registry namespace.SchemaRegistry) Options {
	opts := *o
	opts.schemaReg = registry
	return &opts
}

func (o *options) SchemaRegistry() namespace.SchemaRegistry {
	return o.schemaReg
}

func (o *options) SetBlockLeaseManager(leaseMgr block.LeaseManager) Options {
	opts := *o
	opts.blockLeaseManager = leaseMgr
	return &opts
}

func (o *options) BlockLeaseManager() block.LeaseManager {
	return o.blockLeaseManager
}

func (o *options) SetOnColdFlush(value OnColdFlush) Options {
	opts := *o
	opts.onColdFlush = value
	return &opts
}

func (o *options) OnColdFlush() OnColdFlush {
	return o.onColdFlush
}

func (o *options) SetForceColdWritesEnabled(value bool) Options {
	opts := *o
	opts.forceColdWritesEnabled = value
	return &opts
}

func (o *options) ForceColdWritesEnabled() bool {
	return o.forceColdWritesEnabled
}

func (o *options) SetSourceLoggerBuilder(value limits.SourceLoggerBuilder) Options {
	opts := *o
	opts.sourceLoggerBuilder = value
	return &opts
}

func (o *options) SourceLoggerBuilder() limits.SourceLoggerBuilder {
	return o.sourceLoggerBuilder
}

func (o *options) SetIterationOptions(value index.IterationOptions) Options {
	opts := *o
	opts.iterationOptions = value
	return &opts
}

func (o *options) IterationOptions() index.IterationOptions {
	return o.iterationOptions
}

func (o *options) SetMemoryTracker(memTracker MemoryTracker) Options {
	opts := *o
	opts.memoryTracker = memTracker
	return &opts
}

func (o *options) MemoryTracker() MemoryTracker {
	return o.memoryTracker
}

func (o *options) SetMmapReporter(mmapReporter mmap.Reporter) Options {
	opts := *o
	opts.mmapReporter = mmapReporter
	return &opts
}

func (o *options) MmapReporter() mmap.Reporter {
	return o.mmapReporter
}

func (o *options) SetDoNotIndexWithFieldsMap(value map[string]string) Options {
	opts := *o
	opts.doNotIndexWithFieldsMap = value
	return &opts
}

func (o *options) DoNotIndexWithFieldsMap() map[string]string {
	return o.doNotIndexWithFieldsMap
}

func (o *options) SetNamespaceRuntimeOptionsManagerRegistry(
	value namespace.RuntimeOptionsManagerRegistry,
) Options {
	opts := *o
	opts.namespaceRuntimeOptsMgrRegistry = value
	return &opts
}

func (o *options) NamespaceRuntimeOptionsManagerRegistry() namespace.RuntimeOptionsManagerRegistry {
	return o.namespaceRuntimeOptsMgrRegistry
}

func (o *options) SetMediatorTickInterval(value time.Duration) Options {
	opts := *o
	opts.mediatorTickInterval = value
	return &opts
}

func (o *options) MediatorTickInterval() time.Duration {
	return o.mediatorTickInterval
}

func (o *options) SetAdminClient(value client.AdminClient) Options {
	opts := *o
	opts.adminClient = value
	return &opts
}

func (o *options) AdminClient() client.AdminClient {
	return o.adminClient
}

func (o *options) SetWideBatchSize(value int) Options {
	opts := *o
	opts.wideBatchSize = value
	return &opts
}

func (o *options) WideBatchSize() int {
	return o.wideBatchSize
}

func (o *options) SetBackgroundProcessFns(fns []NewBackgroundProcessFn) Options {
	opts := *o
	opts.newBackgroundProcessFns = fns
	return &opts
}

func (o *options) BackgroundProcessFns() []NewBackgroundProcessFn {
	return o.newBackgroundProcessFns
}

func (o *options) SetNamespaceHooks(value NamespaceHooks) Options {
	opts := *o
	opts.namespaceHooks = value
	return &opts
}

func (o *options) NamespaceHooks() NamespaceHooks {
	return o.namespaceHooks
}

func (o *options) SetTileAggregator(value TileAggregator) Options {
	opts := *o
	opts.tileAggregator = value

	return &opts
}

func (o *options) TileAggregator() TileAggregator {
	return o.tileAggregator
}

type noOpColdFlush struct{}

func (n *noOpColdFlush) ColdFlushNamespace(Namespace) (OnColdFlushNamespace, error) {
	return &persist.NoOpColdFlushNamespace{}, nil
}

type noopNamespaceHooks struct{}

func (h *noopNamespaceHooks) OnCreatedNamespace(Namespace, GetNamespaceFn) error {
	return nil
}

type noopTileAggregator struct{}

func (a *noopTileAggregator) AggregateTiles(
	ctx context.Context,
	sourceNs, targetNs Namespace,
	shardID uint32,
	blockReaders []fs.DataFileSetReader,
	writer fs.StreamingWriter,
	onFlushSeries persist.OnFlushSeries,
	opts AggregateTilesOptions,
) (int64, error) {
	return 0, nil
}
