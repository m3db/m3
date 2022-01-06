// Copyright (c) 2020 Uber Technologies, Inc.
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
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	goruntime "runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	m3dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/storage/limits/permits"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/m3ninx/x"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xopentracing "github.com/m3db/m3/src/x/opentracing"
	xresource "github.com/m3db/m3/src/x/resource"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/bitset"
	"github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	errDbIndexAlreadyClosed               = errors.New("database index has already been closed")
	errDbIndexUnableToWriteClosed         = errors.New("unable to write to database index, already closed")
	errDbIndexUnableToQueryClosed         = errors.New("unable to query database index, already closed")
	errDbIndexUnableToFlushClosed         = errors.New("unable to flush database index, already closed")
	errDbIndexUnableToCleanupClosed       = errors.New("unable to cleanup database index, already closed")
	errDbIndexTerminatingTickCancellation = errors.New("terminating tick early due to cancellation")
	errDbIndexIsBootstrapping             = errors.New("index is already bootstrapping")
	errDbIndexDoNotIndexSeries            = errors.New("series matched do not index fields")
)

const (
	defaultFlushReadDataBlocksBatchSize = int64(4096)
	nsIndexReportStatsInterval          = 10 * time.Second

	defaultFlushDocsBatchSize = 8192
)

var allQuery = idx.NewAllQuery()

// nolint: maligned
type nsIndex struct {
	state nsIndexState

	// all the vars below this line are not modified past the ctor
	// and don't require a lock when being accessed.
	nowFn                 clock.NowFn
	blockSize             time.Duration
	retentionPeriod       time.Duration
	futureRetentionPeriod time.Duration
	bufferPast            time.Duration
	bufferFuture          time.Duration
	coldWritesEnabled     bool

	namespaceRuntimeOptsMgr namespace.RuntimeOptionsManager
	indexFilesetsBeforeFn   indexFilesetsBeforeFn
	deleteFilesFn           deleteFilesFn
	readIndexInfoFilesFn    readIndexInfoFilesFn

	newBlockFn            index.NewBlockFn
	logger                *zap.Logger
	opts                  Options
	nsMetadata            namespace.Metadata
	runtimeOptsListener   xresource.SimpleCloser
	runtimeNsOptsListener xresource.SimpleCloser

	resultsPool          index.QueryResultsPool
	aggregateResultsPool index.AggregateResultsPool

	permitsManager permits.Manager

	// queriesWg tracks outstanding queries to ensure
	// we wait for all queries to complete before actually closing
	// blocks and other cleanup tasks on index close
	queriesWg sync.WaitGroup

	metrics nsIndexMetrics

	// forwardIndexDice determines if an incoming index write should be dual
	// written to the next block.
	forwardIndexDice forwardIndexDice

	doNotIndexWithFields []doc.Field
	shardSet             sharding.ShardSet

	activeBlock index.Block
}

type nsIndexState struct {
	sync.RWMutex // NB: guards all variables in this struct

	closed         bool
	closeCh        chan struct{}
	bootstrapState BootstrapState

	runtimeOpts nsIndexRuntimeOptions

	insertQueue namespaceIndexInsertQueue

	// NB: `latestBlock` v `blocksByTime`: blocksByTime contains all the blocks known to `nsIndex`.
	// `latestBlock` refers to the block with greatest StartTime within blocksByTime. We do this
	// to skip accessing the map blocksByTime in the vast majority of write/query requests. It's
	// lazily updated, so it can point to an older element until a Tick()/write rotates it.
	blocksByTime map[xtime.UnixNano]index.Block
	latestBlock  index.Block

	// NB: `blockStartsDescOrder` contains the keys from the map `blocksByTime` in reverse
	// chronological order. This is used at query time to enforce determinism about results
	// returned.
	// NB(r): Reference to this slice can be safely taken for iteration purposes
	// for Query(..) since it is rebuilt each time and immutable once built.
	blocksDescOrderImmutable []blockAndBlockStart

	// shardsFilterID is set every time the shards change to correctly
	// only return IDs that this node owns.
	shardsFilterID func(ident.ID) bool

	// shardFilteredForID is set every time the shards change to correctly
	// only return IDs that this node owns, and the shard responsible for that ID.
	shardFilteredForID func(id ident.ID) (uint32, bool)

	shardsAssigned map[uint32]struct{}
}

type blockAndBlockStart struct {
	block      index.Block
	blockStart xtime.UnixNano
}

// NB: nsIndexRuntimeOptions does not contain its own mutex as some of the variables
// are needed for each index write which already at least acquires read lock from
// nsIndex mutex, so to keep the lock acquisitions to a minimum these are protected
// under the same nsIndex mutex.
type nsIndexRuntimeOptions struct {
	insertMode          index.InsertMode
	maxQuerySeriesLimit int64
	maxQueryDocsLimit   int64
}

// NB(prateek): the returned filesets are strictly before the given time, i.e. they
// live in the period (-infinity, exclusiveTime).
type indexFilesetsBeforeFn func(dir string,
	nsID ident.ID,
	exclusiveTime xtime.UnixNano,
) ([]string, error)

type readIndexInfoFilesFn func(opts fs.ReadIndexInfoFilesOptions) []fs.ReadIndexInfoFileResult

type newNamespaceIndexOpts struct {
	md                      namespace.Metadata
	namespaceRuntimeOptsMgr namespace.RuntimeOptionsManager
	shardSet                sharding.ShardSet
	opts                    Options
	newIndexQueueFn         newNamespaceIndexInsertQueueFn
	newBlockFn              index.NewBlockFn
}

// execBlockQueryFn executes a query against the given block whilst tracking state.
type execBlockQueryFn func(
	ctx context.Context,
	block index.Block,
	permit permits.Permit,
	iter index.ResultIterator,
	opts index.QueryOptions,
	state *asyncQueryExecState,
	results index.BaseResults,
	logFields []opentracinglog.Field,
)

// newBlockIterFn returns a new ResultIterator for the query.
type newBlockIterFn func(
	ctx context.Context,
	block index.Block,
	query index.Query,
	results index.BaseResults,
) (index.ResultIterator, error)

// asyncQueryExecState tracks the async execution errors for a query.
type asyncQueryExecState struct {
	sync.RWMutex
	multiErr  xerrors.MultiError
	waitCount atomic.Uint64
}

func (s *asyncQueryExecState) hasErr() bool {
	s.RLock()
	defer s.RUnlock()
	return s.multiErr.NumErrors() > 0
}

func (s *asyncQueryExecState) addErr(err error) {
	s.Lock()
	s.multiErr = s.multiErr.Add(err)
	s.Unlock()
}

func (s *asyncQueryExecState) incWaited(i int) {
	s.waitCount.Add(uint64(i))
}

func (s *asyncQueryExecState) waited() int {
	return int(s.waitCount.Load())
}

// newNamespaceIndex returns a new namespaceIndex for the provided namespace.
func newNamespaceIndex(
	nsMD namespace.Metadata,
	namespaceRuntimeOptsMgr namespace.RuntimeOptionsManager,
	shardSet sharding.ShardSet,
	opts Options,
) (NamespaceIndex, error) {
	return newNamespaceIndexWithOptions(newNamespaceIndexOpts{
		md:                      nsMD,
		namespaceRuntimeOptsMgr: namespaceRuntimeOptsMgr,
		shardSet:                shardSet,
		opts:                    opts,
		newIndexQueueFn:         newNamespaceIndexInsertQueue,
		newBlockFn:              index.NewBlock,
	})
}

// newNamespaceIndexWithInsertQueueFn is a ctor used in tests to override the insert queue.
func newNamespaceIndexWithInsertQueueFn(
	nsMD namespace.Metadata,
	namespaceRuntimeOptsMgr namespace.RuntimeOptionsManager,
	shardSet sharding.ShardSet,
	newIndexQueueFn newNamespaceIndexInsertQueueFn,
	opts Options,
) (NamespaceIndex, error) {
	return newNamespaceIndexWithOptions(newNamespaceIndexOpts{
		md:                      nsMD,
		namespaceRuntimeOptsMgr: namespaceRuntimeOptsMgr,
		shardSet:                shardSet,
		opts:                    opts,
		newIndexQueueFn:         newIndexQueueFn,
		newBlockFn:              index.NewBlock,
	})
}

// newNamespaceIndexWithNewBlockFn is a ctor used in tests to inject blocks.
func newNamespaceIndexWithNewBlockFn(
	nsMD namespace.Metadata,
	namespaceRuntimeOptsMgr namespace.RuntimeOptionsManager,
	shardSet sharding.ShardSet,
	newBlockFn index.NewBlockFn,
	opts Options,
) (NamespaceIndex, error) {
	return newNamespaceIndexWithOptions(newNamespaceIndexOpts{
		md:                      nsMD,
		namespaceRuntimeOptsMgr: namespaceRuntimeOptsMgr,
		shardSet:                shardSet,
		opts:                    opts,
		newIndexQueueFn:         newNamespaceIndexInsertQueue,
		newBlockFn:              newBlockFn,
	})
}

// newNamespaceIndexWithOptions returns a new namespaceIndex with the provided configuration options.
func newNamespaceIndexWithOptions(
	newIndexOpts newNamespaceIndexOpts,
) (NamespaceIndex, error) {
	var (
		nsMD            = newIndexOpts.md
		shardSet        = newIndexOpts.shardSet
		indexOpts       = newIndexOpts.opts.IndexOptions()
		instrumentOpts  = newIndexOpts.opts.InstrumentOptions()
		newIndexQueueFn = newIndexOpts.newIndexQueueFn
		newBlockFn      = newIndexOpts.newBlockFn
		coreFn          = newIndexOpts.opts.CoreFn()
		runtimeOptsMgr  = newIndexOpts.opts.RuntimeOptionsManager()
	)
	if err := indexOpts.Validate(); err != nil {
		return nil, err
	}

	scope := instrumentOpts.MetricsScope().
		SubScope("dbindex").
		Tagged(map[string]string{
			"namespace": nsMD.ID().String(),
		})
	instrumentOpts = instrumentOpts.SetMetricsScope(scope)
	indexOpts = indexOpts.SetInstrumentOptions(instrumentOpts)

	nowFn := indexOpts.ClockOptions().NowFn()
	logger := indexOpts.InstrumentOptions().Logger()

	var doNotIndexWithFields []doc.Field
	if m := newIndexOpts.opts.DoNotIndexWithFieldsMap(); m != nil && len(m) != 0 {
		for k, v := range m {
			doNotIndexWithFields = append(doNotIndexWithFields, doc.Field{
				Name:  []byte(k),
				Value: []byte(v),
			})
		}
	}

	idx := &nsIndex{
		state: nsIndexState{
			closeCh: make(chan struct{}),
			runtimeOpts: nsIndexRuntimeOptions{
				insertMode: indexOpts.InsertMode(), // FOLLOWUP(prateek): wire to allow this to be tweaked at runtime
			},
			blocksByTime:   make(map[xtime.UnixNano]index.Block),
			shardsAssigned: make(map[uint32]struct{}),
		},

		nowFn:                 nowFn,
		blockSize:             nsMD.Options().IndexOptions().BlockSize(),
		retentionPeriod:       nsMD.Options().RetentionOptions().RetentionPeriod(),
		futureRetentionPeriod: nsMD.Options().RetentionOptions().FutureRetentionPeriod(),
		bufferPast:            nsMD.Options().RetentionOptions().BufferPast(),
		bufferFuture:          nsMD.Options().RetentionOptions().BufferFuture(),
		coldWritesEnabled:     nsMD.Options().ColdWritesEnabled(),

		namespaceRuntimeOptsMgr: newIndexOpts.namespaceRuntimeOptsMgr,
		indexFilesetsBeforeFn:   fs.IndexFileSetsBefore,
		readIndexInfoFilesFn:    fs.ReadIndexInfoFiles,
		deleteFilesFn:           fs.DeleteFiles,

		newBlockFn: newBlockFn,
		opts:       newIndexOpts.opts,
		logger:     logger,
		nsMetadata: nsMD,

		resultsPool:          indexOpts.QueryResultsPool(),
		aggregateResultsPool: indexOpts.AggregateResultsPool(),

		permitsManager: newIndexOpts.opts.PermitsOptions().IndexQueryPermitsManager(),
		metrics:        newNamespaceIndexMetrics(indexOpts, instrumentOpts),

		doNotIndexWithFields: doNotIndexWithFields,
		shardSet:             shardSet,
	}

	activeBlock, err := idx.newBlockFn(xtime.UnixNano(0), idx.nsMetadata,
		index.BlockOptions{ActiveBlock: true}, idx.namespaceRuntimeOptsMgr,
		idx.opts.IndexOptions())
	if err != nil {
		return nil, idx.unableToAllocBlockInvariantError(err)
	}

	idx.activeBlock = activeBlock

	// Assign shard set upfront.
	idx.AssignShardSet(shardSet)

	idx.runtimeOptsListener = runtimeOptsMgr.RegisterListener(idx)
	idx.runtimeNsOptsListener = idx.namespaceRuntimeOptsMgr.RegisterListener(idx)

	// set up forward index dice.
	dice, err := newForwardIndexDice(newIndexOpts.opts)
	if err != nil {
		return nil, err
	}

	if dice.enabled {
		logger.Info("namespace forward indexing configured",
			zap.Stringer("namespace", nsMD.ID()),
			zap.Bool("enabled", dice.enabled),
			zap.Duration("threshold", dice.forwardIndexThreshold),
			zap.Float64("rate", dice.forwardIndexDice.Rate()))
	} else {
		idxOpts := newIndexOpts.opts.IndexOptions()
		logger.Info("namespace forward indexing not enabled",
			zap.Stringer("namespace", nsMD.ID()),
			zap.Bool("enabled", false),
			zap.Float64("threshold", idxOpts.ForwardIndexThreshold()),
			zap.Float64("probability", idxOpts.ForwardIndexProbability()))
	}

	idx.forwardIndexDice = dice

	// allocate indexing queue and start it up.
	queue := newIndexQueueFn(idx.writeBatches, nsMD, nowFn, coreFn, scope)
	if err := queue.Start(); err != nil {
		return nil, err
	}
	idx.state.insertQueue = queue

	// allocate the current block to ensure we're able to index as soon as we return
	currentBlock := xtime.ToUnixNano(nowFn()).Truncate(idx.blockSize)
	idx.state.RLock()
	_, err = idx.ensureBlockPresentWithRLock(currentBlock)
	idx.state.RUnlock()
	if err != nil {
		return nil, err
	}

	// Report stats
	go idx.reportStatsUntilClosed()

	return idx, nil
}

func (i *nsIndex) SetRuntimeOptions(runtime.Options) {
}

func (i *nsIndex) SetNamespaceRuntimeOptions(opts namespace.RuntimeOptions) {
	// We don't like to log from every single index segment that has
	// settings updated so we log the changes here.
	i.logger.Info("set namespace runtime index options",
		zap.Stringer("namespace", i.nsMetadata.ID()),
		zap.Any("writeIndexingPerCPUConcurrency", opts.WriteIndexingPerCPUConcurrency()),
		zap.Any("flushIndexingPerCPUConcurrency", opts.FlushIndexingPerCPUConcurrency()))
}

func (i *nsIndex) reportStatsUntilClosed() {
	ticker := time.NewTicker(nsIndexReportStatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := i.reportStats()
			if err != nil {
				i.logger.Warn("could not report index stats", zap.Error(err))
			}
		case <-i.state.closeCh:
			return
		}
	}
}

type nsIndexCompactionLevelStats struct {
	numSegments  int64
	numTotalDocs int64
}

func (i *nsIndex) reportStats() error {
	i.state.RLock()
	defer i.state.RUnlock()

	foregroundLevels := i.metrics.blockMetrics.ForegroundSegments.Levels
	foregroundLevelStats := make([]nsIndexCompactionLevelStats, len(foregroundLevels))

	backgroundLevels := i.metrics.blockMetrics.BackgroundSegments.Levels
	backgroundLevelStats := make([]nsIndexCompactionLevelStats, len(backgroundLevels))

	flushedLevels := i.metrics.blockMetrics.FlushedSegments.Levels
	flushedLevelStats := make([]nsIndexCompactionLevelStats, len(flushedLevels))

	minIndexConcurrency := 0
	maxIndexConcurrency := 0
	sumIndexConcurrency := 0
	numIndexingStats := 0
	reporter := index.NewBlockStatsReporter(
		func(s index.BlockSegmentStats) {
			var (
				levels     []nsIndexBlocksSegmentsLevelMetrics
				levelStats []nsIndexCompactionLevelStats
			)
			switch s.Type {
			case index.ActiveForegroundSegment:
				levels = foregroundLevels
				levelStats = foregroundLevelStats
			case index.ActiveBackgroundSegment:
				levels = backgroundLevels
				levelStats = backgroundLevelStats
			case index.FlushedSegment:
				levels = flushedLevels
				levelStats = flushedLevelStats
			}

			for i, l := range levels {
				contained := s.Size >= l.MinSizeInclusive && s.Size < l.MaxSizeExclusive
				if !contained {
					continue
				}

				l.SegmentsAge.Record(s.Age)
				levelStats[i].numSegments++
				levelStats[i].numTotalDocs += s.Size

				break
			}
		},
		func(s index.BlockIndexingStats) {
			first := numIndexingStats == 0
			numIndexingStats++

			if first {
				minIndexConcurrency = s.IndexConcurrency
				maxIndexConcurrency = s.IndexConcurrency
				sumIndexConcurrency = s.IndexConcurrency
				return
			}

			if v := s.IndexConcurrency; v < minIndexConcurrency {
				minIndexConcurrency = v
			}
			if v := s.IndexConcurrency; v > maxIndexConcurrency {
				maxIndexConcurrency = v
			}
			sumIndexConcurrency += s.IndexConcurrency
		})

	// iterate known blocks in a defined order of time (newest first)
	// for debug log ordering
	for _, b := range i.state.blocksDescOrderImmutable {
		err := b.block.Stats(reporter)
		if err == index.ErrUnableReportStatsBlockClosed {
			// Closed blocks are temporarily in the list still
			continue
		}
		if err != nil {
			return err
		}
	}
	// Active block should always be open.
	if err := i.activeBlock.Stats(reporter); err != nil {
		return err
	}

	// Update level stats.
	for _, elem := range []struct {
		levels     []nsIndexBlocksSegmentsLevelMetrics
		levelStats []nsIndexCompactionLevelStats
	}{
		{foregroundLevels, foregroundLevelStats},
		{backgroundLevels, backgroundLevelStats},
	} {
		for i, v := range elem.levelStats {
			elem.levels[i].NumSegments.Update(float64(v.numSegments))
			elem.levels[i].NumTotalDocs.Update(float64(v.numTotalDocs))
		}
	}

	// Update the indexing stats.
	i.metrics.indexingConcurrencyMin.Update(float64(minIndexConcurrency))
	i.metrics.indexingConcurrencyMax.Update(float64(maxIndexConcurrency))
	avgIndexConcurrency := float64(sumIndexConcurrency) / float64(numIndexingStats)
	i.metrics.indexingConcurrencyAvg.Update(avgIndexConcurrency)

	return nil
}

func (i *nsIndex) BlockStartForWriteTime(writeTime xtime.UnixNano) xtime.UnixNano {
	return writeTime.Truncate(i.blockSize)
}

func (i *nsIndex) BlockForBlockStart(blockStart xtime.UnixNano) (index.Block, error) {
	result, err := i.ensureBlockPresent(blockStart)
	if err != nil {
		return nil, err
	}
	return result.block, nil
}

// NB(prateek): including the call chains leading to this point:
//
// - For new entry (previously unseen in the shard):
//     shard.WriteTagged()
//       => shard.insertSeriesAsyncBatched()
//       => shardInsertQueue.Insert()
//       => shard.writeBatch()
//       => index.WriteBatch()
//       => indexQueue.Insert()
//       => index.writeBatch()
//
// - For entry which exists in the shard, but needs indexing (either past
//   the TTL or the last indexing hasn't happened/failed):
//      shard.WriteTagged()
//        => shard.insertSeriesForIndexingAsyncBatched()
//        => shardInsertQueue.Insert()
//        => shard.writeBatch()
//        => index.Write()
//        => indexQueue.Insert()
//      	=> index.writeBatch()

func (i *nsIndex) WriteBatch(
	batch *index.WriteBatch,
) error {
	// Filter anything with a pending index out before acquiring lock.
	batch.MarkUnmarkedIfAlreadyIndexedSuccessAndFinalize()
	if !batch.PendingAny() {
		return nil
	}

	i.state.RLock()
	if !i.isOpenWithRLock() {
		i.state.RUnlock()
		i.metrics.insertAfterClose.Inc(1)
		err := errDbIndexUnableToWriteClosed
		batch.MarkUnmarkedEntriesError(err)
		return err
	}

	// NB(prateek): retrieving insertMode here while we have the RLock.
	insertMode := i.state.runtimeOpts.insertMode
	wg, err := i.state.insertQueue.InsertBatch(batch)

	// release the lock because we don't need it past this point.
	i.state.RUnlock()

	// if we're unable to index, we still have to finalize the reference we hold.
	if err != nil {
		batch.MarkUnmarkedEntriesError(err)
		return err
	}
	// once the write has been queued in the indexInsertQueue, it assumes
	// responsibility for calling the resource hooks.

	// wait/terminate depending on if we are indexing synchronously or not.
	if insertMode != index.InsertAsync {
		wg.Wait()

		// Re-sort the batch by initial enqueue order
		if numErrs := batch.NumErrs(); numErrs > 0 {
			// Restore the sort order from when enqueued for the caller.
			batch.SortByEnqueued()
			return fmt.Errorf("check batch: %d insert errors", numErrs)
		}
	}

	return nil
}

func (i *nsIndex) WritePending(
	pending []writes.PendingIndexInsert,
) error {
	// Filter anything with a pending index out before acquiring lock.
	incoming := pending
	pending = pending[:0]
	for j := range incoming {
		t := i.BlockStartForWriteTime(incoming[j].Entry.Timestamp)
		if incoming[j].Entry.OnIndexSeries.IfAlreadyIndexedMarkIndexSuccessAndFinalize(t) {
			continue
		}
		// Continue to add this element.
		pending = append(pending, incoming[j])
	}
	if len(pending) == 0 {
		return nil
	}

	i.state.RLock()
	if !i.isOpenWithRLock() {
		i.state.RUnlock()
		i.metrics.insertAfterClose.Inc(1)
		return errDbIndexUnableToWriteClosed
	}
	_, err := i.state.insertQueue.InsertPending(pending)
	// release the lock because we don't need it past this point.
	i.state.RUnlock()

	return err
}

// WriteBatches is called by the indexInsertQueue.
func (i *nsIndex) writeBatches(
	batch *index.WriteBatch,
) {
	// NB(prateek): we use a read lock to guard against mutation of the
	// indexBlocks, mutations within the underlying blocks are guarded
	// by primitives internal to it.
	i.state.RLock()
	if !i.isOpenWithRLock() {
		i.state.RUnlock()
		// NB(prateek): deliberately skip calling any of the `OnIndexFinalize` methods
		// on the provided inserts to terminate quicker during shutdown.
		return
	}
	var (
		now                        = xtime.ToUnixNano(i.nowFn())
		blockSize                  = i.blockSize
		futureLimit                = now.Add(1 * i.bufferFuture)
		pastLimit                  = now.Add(-1 * i.bufferPast)
		earliestBlockStartToRetain = retention.FlushTimeStartForRetentionPeriod(i.retentionPeriod, i.blockSize, now)
		batchOptions               = batch.Options()
		forwardIndexDice           = i.forwardIndexDice
		forwardIndexEnabled        = forwardIndexDice.enabled
		total                      int
		notSkipped                 int
		forwardIndexHits           int
		forwardIndexMiss           int

		forwardIndexBatch *index.WriteBatch
	)
	// NB(r): Release lock early to avoid writing batches impacting ticking
	// speed, etc.
	// Sometimes foreground compaction can take a long time during heavy inserts.
	// Each lookup to ensureBlockPresent checks that index is still open, etc.
	i.state.RUnlock()

	if forwardIndexEnabled {
		// NB(arnikola): Don't initialize forward index batch if forward indexing
		// is not enabled.
		forwardIndexBatch = index.NewWriteBatch(batchOptions)
	}

	// Ensure timestamp is not too old/new based on retention policies and that
	// doc is valid. Add potential forward writes to the forwardWriteBatch.
	batch.ForEach(
		func(idx int, entry index.WriteBatchEntry,
			d doc.Metadata, _ index.WriteBatchEntryResult) {
			total++

			if len(i.doNotIndexWithFields) != 0 {
				// This feature rarely used, do not optimize and just do n*m checks.
				drop := true
				for _, matchField := range i.doNotIndexWithFields {
					matchedField := false
					for _, actualField := range d.Fields {
						if bytes.Equal(actualField.Name, matchField.Name) {
							matchedField = bytes.Equal(actualField.Value, matchField.Value)
							break
						}
					}
					if !matchedField {
						drop = false
						break
					}
				}
				if drop {
					batch.MarkUnmarkedEntryError(errDbIndexDoNotIndexSeries, idx)
					return
				}
			}

			ts := entry.Timestamp
			// NB(bodu): Always check first to see if the write is within retention.
			if !ts.After(earliestBlockStartToRetain) {
				batch.MarkUnmarkedEntryError(m3dberrors.ErrTooPast, idx)
				return
			}

			if !futureLimit.After(ts) {
				batch.MarkUnmarkedEntryError(m3dberrors.ErrTooFuture, idx)
				return
			}

			if ts.Before(pastLimit) && !i.coldWritesEnabled {
				// NB(bodu): We only mark entries as too far in the past if
				// cold writes are not enabled.
				batch.MarkUnmarkedEntryError(m3dberrors.ErrTooPast, idx)
				return
			}

			if forwardIndexEnabled {
				if forwardIndexDice.roll(ts) {
					forwardIndexHits++
					forwardEntryTimestamp := ts.Truncate(blockSize).Add(blockSize)
					if entry.OnIndexSeries.NeedsIndexUpdate(forwardEntryTimestamp) {
						forwardIndexEntry := entry
						forwardIndexEntry.Timestamp = forwardEntryTimestamp
						t := i.BlockStartForWriteTime(forwardEntryTimestamp)
						forwardIndexEntry.OnIndexSeries.OnIndexPrepare(t)
						forwardIndexBatch.Append(forwardIndexEntry, d)
					}
				} else {
					forwardIndexMiss++
				}
			}

			notSkipped++
		})

	if forwardIndexEnabled && forwardIndexBatch.Len() > 0 {
		i.metrics.forwardIndexCounter.Inc(int64(forwardIndexBatch.Len()))
		batch.AppendAll(forwardIndexBatch)
	}

	// Sort the inserts by which block they're applicable for, and do the inserts
	// for each block, making sure to not try to insert any entries already marked
	// with a result.
	batch.ForEachUnmarkedBatchByBlockStart(i.writeBatchForBlockStart)

	// Track index insertions.
	// Note: attemptTotal should = attemptSkip + attemptWrite.
	i.metrics.asyncInsertAttemptTotal.Inc(int64(total))
	i.metrics.asyncInsertAttemptSkip.Inc(int64(total - notSkipped))
	i.metrics.forwardIndexHits.Inc(int64(forwardIndexHits))
	i.metrics.forwardIndexMisses.Inc(int64(forwardIndexMiss))
}

func (i *nsIndex) writeBatchForBlockStart(
	blockStart xtime.UnixNano, batch *index.WriteBatch,
) {
	// NB(r): Capture pending entries so we can emit the latencies
	pending := batch.PendingEntries()
	numPending := len(pending)

	// Track attempted write.
	// Note: attemptTotal should = attemptSkip + attemptWrite.
	i.metrics.asyncInsertAttemptWrite.Inc(int64(numPending))

	// i.e. we have the block and the inserts, perform the writes.
	result, err := i.activeBlock.WriteBatch(batch)

	// Record the end to end indexing latency.
	now := i.nowFn()
	for idx := range pending {
		took := now.Sub(pending[idx].EnqueuedAt)
		i.metrics.insertEndToEndLatency.Record(took)
	}

	// NB: we don't need to do anything to the OnIndexSeries refs in `inserts` at this point,
	// the index.Block WriteBatch assumes responsibility for calling the appropriate methods.
	if n := result.NumSuccess; n > 0 {
		i.metrics.asyncInsertSuccess.Inc(n)
	}

	// Record mutable segments count foreground/background if latest block.
	if stats := result.MutableSegmentsStats; !stats.Empty() {
		i.metrics.latestBlockNumSegmentsForeground.Update(float64(stats.Foreground.NumSegments))
		i.metrics.latestBlockNumDocsForeground.Update(float64(stats.Foreground.NumDocs))
		i.metrics.latestBlockNumSegmentsBackground.Update(float64(stats.Background.NumSegments))
		i.metrics.latestBlockNumDocsBackground.Update(float64(stats.Background.NumDocs))
	}

	// Allow for duplicate write errors since due to re-indexing races
	// we may try to re-index a series more than once.
	if err := i.sanitizeAllowDuplicatesWriteError(err); err != nil {
		numErrors := numPending - int(result.NumSuccess)
		if partialError, ok := err.(*m3ninxindex.BatchPartialError); ok {
			// If it was a batch partial error we know exactly how many failed
			// after filtering out for duplicate ID errors.
			numErrors = len(partialError.Errs())
		}
		i.metrics.asyncInsertErrors.Inc(int64(numErrors))
		i.logger.Error("error writing to index block", zap.Error(err))
	}
}

// Bootstrap bootstraps the index with the provide blocks.
func (i *nsIndex) Bootstrap(
	bootstrapResults result.IndexResults,
) error {
	i.state.Lock()
	if i.state.bootstrapState == Bootstrapping {
		i.state.Unlock()
		return errDbIndexIsBootstrapping
	}
	i.state.bootstrapState = Bootstrapping
	i.state.Unlock()

	i.state.RLock()
	defer func() {
		i.state.RUnlock()
		i.state.Lock()
		i.state.bootstrapState = Bootstrapped
		i.state.Unlock()
	}()

	var multiErr xerrors.MultiError
	for blockStart, blockResults := range bootstrapResults {
		blockResult, err := i.ensureBlockPresentWithRLock(blockStart)
		if err != nil { // should never happen
			multiErr = multiErr.Add(i.unableToAllocBlockInvariantError(err))
			continue
		}
		if err := blockResult.block.AddResults(blockResults); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (i *nsIndex) Bootstrapped() bool {
	i.state.RLock()
	result := i.state.bootstrapState == Bootstrapped
	i.state.RUnlock()
	return result
}

func (i *nsIndex) Tick(
	c context.Cancellable,
	startTime xtime.UnixNano,
) (namespaceIndexTickResult, error) {
	var result namespaceIndexTickResult

	// First collect blocks and acquire lock to remove those that need removing
	// but then release lock so can Tick and do other expensive tasks
	// such as notify of sealed blocks.
	tickingBlocks, multiErr := i.tickingBlocks(startTime)

	result.NumBlocks = int64(tickingBlocks.totalBlocks)
	for _, block := range tickingBlocks.tickingBlocks {
		if c.IsCancelled() {
			multiErr = multiErr.Add(errDbIndexTerminatingTickCancellation)
			return result, multiErr.FinalError()
		}

		blockTickResult, tickErr := block.Tick(c)
		multiErr = multiErr.Add(tickErr)
		result.NumSegments += blockTickResult.NumSegments
		result.NumSegmentsBootstrapped += blockTickResult.NumSegmentsBootstrapped
		result.NumSegmentsMutable += blockTickResult.NumSegmentsMutable
		result.NumTotalDocs += blockTickResult.NumDocs
		result.FreeMmap += blockTickResult.FreeMmap
	}

	blockTickResult, tickErr := tickingBlocks.activeBlock.Tick(c)
	multiErr = multiErr.Add(tickErr)
	result.NumSegments += blockTickResult.NumSegments
	result.NumSegmentsBootstrapped += blockTickResult.NumSegmentsBootstrapped
	result.NumSegmentsMutable += blockTickResult.NumSegmentsMutable
	result.NumTotalDocs += blockTickResult.NumDocs
	result.FreeMmap += blockTickResult.FreeMmap

	i.metrics.tick.Inc(1)

	return result, multiErr.FinalError()
}

type tickingBlocksResult struct {
	totalBlocks   int
	activeBlock   index.Block
	tickingBlocks []index.Block
}

func (i *nsIndex) tickingBlocks(
	startTime xtime.UnixNano,
) (tickingBlocksResult, xerrors.MultiError) {
	multiErr := xerrors.NewMultiError()
	earliestBlockStartToRetain := retention.FlushTimeStartForRetentionPeriod(
		i.retentionPeriod, i.blockSize, startTime)

	i.state.Lock()
	activeBlock := i.activeBlock
	tickingBlocks := make([]index.Block, 0, len(i.state.blocksByTime))
	defer func() {
		i.updateBlockStartsWithLock()
		i.state.Unlock()
	}()

	for blockStart, block := range i.state.blocksByTime {
		// Drop any blocks past the retention period.
		if blockStart.Before(earliestBlockStartToRetain) {
			multiErr = multiErr.Add(block.Close())
			delete(i.state.blocksByTime, blockStart)
			continue
		}

		// Tick any blocks we're going to retain, but don't tick inline here
		// we'll do this out of the block.
		tickingBlocks = append(tickingBlocks, block)

		// Seal any blocks that are sealable while holding lock (seal is fast).
		if !blockStart.After(i.lastSealableBlockStart(startTime)) && !block.IsSealed() {
			multiErr = multiErr.Add(block.Seal())
		}
	}

	return tickingBlocksResult{
		totalBlocks:   len(i.state.blocksByTime),
		activeBlock:   activeBlock,
		tickingBlocks: tickingBlocks,
	}, multiErr
}

func (i *nsIndex) WarmFlush(
	flush persist.IndexFlush,
	shards []databaseShard,
) error {
	if len(shards) == 0 {
		// No-op if no shards currently owned.
		return nil
	}

	flushable, err := i.flushableBlocks(shards, series.WarmWrite)
	if err != nil {
		return err
	}

	// Determine the current flush indexing concurrency.
	namespaceRuntimeOpts := i.namespaceRuntimeOptsMgr.Get()
	perCPUFraction := namespaceRuntimeOpts.FlushIndexingPerCPUConcurrencyOrDefault()
	cpus := math.Ceil(perCPUFraction * float64(goruntime.GOMAXPROCS(0)))
	concurrency := int(math.Max(1, cpus))

	builderOpts := i.opts.IndexOptions().SegmentBuilderOptions().
		SetConcurrency(concurrency)

	builder, err := builder.NewBuilderFromDocuments(builderOpts)
	if err != nil {
		return err
	}
	defer builder.Close()

	// Emit concurrency, then reset gauge to zero to show time
	// active during flushing broken down per namespace.
	i.metrics.flushIndexingConcurrency.Update(float64(concurrency))
	defer i.metrics.flushIndexingConcurrency.Update(0)

	var evicted int
	for _, block := range flushable {
		immutableSegments, err := i.flushBlock(flush, block, shards, builder)
		if err != nil {
			return err
		}
		// Make a result that covers the entire time ranges for the
		// block for each shard
		fulfilled := result.NewShardTimeRangesFromRange(block.StartTime(), block.EndTime(),
			dbShards(shards).IDs()...)

		// Add the results to the block.
		persistedSegments := make([]result.Segment, 0, len(immutableSegments))
		for _, elem := range immutableSegments {
			persistedSegment := result.NewSegment(elem, true)
			persistedSegments = append(persistedSegments, persistedSegment)
		}
		blockResult := result.NewIndexBlock(persistedSegments, fulfilled)
		results := result.NewIndexBlockByVolumeType(block.StartTime())
		results.SetBlock(idxpersist.DefaultIndexVolumeType, blockResult)
		if err := block.AddResults(results); err != nil {
			return err
		}

		evicted++

		// It's now safe to remove the mutable segments as anything the block
		// held is covered by the owned shards we just read
		if err := block.EvictMutableSegments(); err != nil {
			// deliberately choosing to not mark this as an error as we have successfully
			// flushed any mutable data.
			i.logger.Warn("encountered error while evicting mutable segments for index block",
				zap.Error(err),
				zap.Time("blockStart", block.StartTime().ToTime()),
			)
		}

		for _, t := range i.blockStartsFromIndexBlockStart(block.StartTime()) {
			for _, s := range shards {
				s.MarkWarmIndexFlushStateSuccessOrError(t, err)
			}
		}
	}
	i.metrics.blocksEvictedMutableSegments.Inc(int64(evicted))
	return nil
}

func (i *nsIndex) ColdFlush(shards []databaseShard) (OnColdFlushDone, error) {
	if len(shards) == 0 {
		// No-op if no shards currently owned.
		return func() error { return nil }, nil
	}

	flushable, err := i.flushableBlocks(shards, series.ColdWrite)
	if err != nil {
		return nil, err
	}
	// We only rotate cold mutable segments in phase I of cold flushing.
	for _, block := range flushable {
		if err := block.RotateColdMutableSegments(); err != nil {
			return nil, err
		}
	}
	// We can't immediately evict cold mutable segments so we return a callback to do so
	// when cold flush finishes.
	return func() error {
		multiErr := xerrors.NewMultiError()
		for _, block := range flushable {
			multiErr = multiErr.Add(block.EvictColdMutableSegments())
		}
		return multiErr.FinalError()
	}, nil
}

// WarmFlushBlockStarts returns all index blockStarts which have been flushed to disk.
func (i *nsIndex) WarmFlushBlockStarts() []xtime.UnixNano {
	flushed := make([]xtime.UnixNano, 0)
	infoFiles := i.readInfoFilesAsMap()

	for blockStart := range infoFiles {
		if i.hasIndexWarmFlushedToDisk(infoFiles, blockStart) {
			flushed = append(flushed, blockStart)
		}
	}
	return flushed
}

// BackgroundCompact background compacts eligible segments.
func (i *nsIndex) BackgroundCompact() {
	if i.activeBlock != nil {
		i.activeBlock.BackgroundCompact()
	}
	for _, b := range i.state.blocksByTime {
		b.BackgroundCompact()
	}
}

func (i *nsIndex) readInfoFilesAsMap() map[xtime.UnixNano][]fs.ReadIndexInfoFileResult {
	fsOpts := i.opts.CommitLogOptions().FilesystemOptions()
	infoFiles := i.readIndexInfoFilesFn(fs.ReadIndexInfoFilesOptions{
		FilePathPrefix:   fsOpts.FilePathPrefix(),
		Namespace:        i.nsMetadata.ID(),
		ReaderBufferSize: fsOpts.InfoReaderBufferSize(),
	})
	result := make(map[xtime.UnixNano][]fs.ReadIndexInfoFileResult)
	for _, infoFile := range infoFiles {
		t := xtime.UnixNano(infoFile.Info.BlockStart)
		files := result[t]
		result[t] = append(files, infoFile)
	}
	return result
}

func (i *nsIndex) flushableBlocks(
	shards []databaseShard,
	flushType series.WriteType,
) ([]index.Block, error) {
	i.state.RLock()
	defer i.state.RUnlock()
	if !i.isOpenWithRLock() {
		return nil, errDbIndexUnableToFlushClosed
	}
	// NB(bodu): We read index info files once here to avoid re-reading all of them
	// for each block.
	infoFiles := i.readInfoFilesAsMap()
	flushable := make([]index.Block, 0, len(i.state.blocksByTime))

	now := xtime.ToUnixNano(i.nowFn())
	earliestBlockStartToRetain := retention.FlushTimeStartForRetentionPeriod(i.retentionPeriod, i.blockSize, now)
	currentBlockStart := now.Truncate(i.blockSize)
	// Check for flushable blocks by iterating through all block starts w/in retention.
	for blockStart := earliestBlockStartToRetain; blockStart.Before(currentBlockStart); blockStart = blockStart.Add(i.blockSize) {
		blockResult, err := i.ensureBlockPresentWithRLock(blockStart)
		if err != nil {
			return nil, err
		}

		canFlush, err := i.canFlushBlockWithRLock(infoFiles, blockStart,
			blockResult.block, shards, flushType)
		if err != nil {
			return nil, err
		}
		if !canFlush {
			continue
		}

		flushable = append(flushable, blockResult.block)
	}
	return flushable, nil
}

func (i *nsIndex) canFlushBlockWithRLock(
	infoFiles map[xtime.UnixNano][]fs.ReadIndexInfoFileResult,
	blockStart xtime.UnixNano,
	block index.Block,
	shards []databaseShard,
	flushType series.WriteType,
) (bool, error) {
	switch flushType {
	case series.WarmWrite:
		// NB(bodu): We should always attempt to warm flush sealed blocks to disk if
		// there doesn't already exist data on disk. We're checking this instead of
		// `block.NeedsMutableSegmentsEvicted()` since bootstrap writes for cold block starts
		// get marked as warm writes if there doesn't already exist data on disk and need to
		// properly go through the warm flush lifecycle.
		if !block.IsSealed() || i.hasIndexWarmFlushedToDisk(infoFiles, blockStart) {
			return false, nil
		}
	case series.ColdWrite:
		if !block.NeedsColdMutableSegmentsEvicted() {
			return false, nil
		}
	}

	// Check all data files exist for the shards we own
	for _, shard := range shards {
		if !shard.IsBootstrapped() {
			i.logger.
				With(zap.Uint32("shard", shard.ID())).
				Debug("skipping index cold flush due to shard not bootstrapped yet")
			continue
		}

		for _, t := range i.blockStartsFromIndexBlockStart(blockStart) {
			flushState, err := shard.FlushState(t)
			if err != nil {
				return false, err
			}

			// Skip if the data flushing failed. Data flushing precedes index flushing.
			if flushState.WarmStatus.DataFlushed != fileOpSuccess {
				return false, nil
			}
		}
	}

	return true, nil
}

// blockStartsFromIndexBlockStart returns the possibly many blocksStarts that exist within
// a given index block (since index block size >= data block size)
func (i *nsIndex) blockStartsFromIndexBlockStart(blockStart xtime.UnixNano) []xtime.UnixNano {
	start := blockStart
	end := blockStart.Add(i.blockSize)
	dataBlockSize := i.nsMetadata.Options().RetentionOptions().BlockSize()
	blockStarts := make([]xtime.UnixNano, 0)
	for t := start; t.Before(end); t = t.Add(dataBlockSize) {
		blockStarts = append(blockStarts, t)
	}
	return blockStarts
}

func (i *nsIndex) hasIndexWarmFlushedToDisk(
	infoFiles map[xtime.UnixNano][]fs.ReadIndexInfoFileResult,
	blockStart xtime.UnixNano,
) bool {
	// NB(bodu): We consider the block to have been warm flushed if there are any
	// filesets on disk. This is consistent with the "has warm flushed" check in the db shard.
	// Shard block starts are marked as having warm flushed if an info file is successfully read from disk.
	f, ok := infoFiles[blockStart]
	if !ok {
		return false
	}

	for _, fileInfo := range f {
		indexVolumeType := idxpersist.DefaultIndexVolumeType
		if fileInfo.Info.IndexVolumeType != nil {
			indexVolumeType = idxpersist.IndexVolumeType(fileInfo.Info.IndexVolumeType.Value)
		}
		match := fileInfo.ID.BlockStart == blockStart && indexVolumeType == idxpersist.DefaultIndexVolumeType
		if match {
			return true
		}
	}
	return false
}

func (i *nsIndex) flushBlock(
	flush persist.IndexFlush,
	indexBlock index.Block,
	shards []databaseShard,
	builder segment.DocumentsBuilder,
) ([]segment.Segment, error) {
	allShards := make(map[uint32]struct{})
	for _, shard := range shards {
		// Populate all shards
		allShards[shard.ID()] = struct{}{}
	}

	volumeIndex, err := i.opts.IndexClaimsManager().ClaimNextIndexFileSetVolumeIndex(
		i.nsMetadata,
		indexBlock.StartTime(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to claim next index volume index: %w", err)
	}

	preparedPersist, err := flush.PrepareIndex(persist.IndexPrepareOptions{
		NamespaceMetadata: i.nsMetadata,
		BlockStart:        indexBlock.StartTime(),
		FileSetType:       persist.FileSetFlushType,
		Shards:            allShards,
		// NB(bodu): By default, we always write to the "default" index volume type.
		IndexVolumeType: idxpersist.DefaultIndexVolumeType,
		VolumeIndex:     volumeIndex,
	})
	if err != nil {
		return nil, err
	}

	var closed bool
	defer func() {
		if !closed {
			segments, _ := preparedPersist.Close()
			// NB(r): Safe to for over a nil array so disregard error here.
			for _, segment := range segments {
				segment.Close()
			}
		}
	}()

	// Flush a single block segment.
	if err := i.flushBlockSegment(preparedPersist, indexBlock, shards, builder); err != nil {
		return nil, err
	}

	closed = true

	// Now return the immutable segments
	return preparedPersist.Close()
}

func (i *nsIndex) flushBlockSegment(
	preparedPersist persist.PreparedIndexPersist,
	indexBlock index.Block,
	shards []databaseShard,
	builder segment.DocumentsBuilder,
) error {
	// Reset the builder
	builder.Reset()

	var (
		batch     = m3ninxindex.Batch{AllowPartialUpdates: true}
		batchSize = defaultFlushDocsBatchSize
	)
	ctx := i.opts.ContextPool().Get()
	defer ctx.Close()

	for _, shard := range shards {
		var (
			first     = true
			pageToken PageToken
		)
		for first || pageToken != nil {
			first = false

			var (
				opts = block.FetchBlocksMetadataOptions{
					// NB(bodu): There is a lag between when data gets flushed
					// to disk and when it gets removed from memory during the next
					// Tick. In this case, the same series can exist both on disk
					// and in memory at the same time resulting in dupe series IDs.
					// Only read data from disk when flushing index segments.
					OnlyDisk: true,
				}
				limit   = defaultFlushReadDataBlocksBatchSize
				results block.FetchBlocksMetadataResults
				err     error
			)
			ctx.Reset()
			results, pageToken, err = shard.FetchBlocksMetadataV2(ctx,
				indexBlock.StartTime(), indexBlock.EndTime(),
				limit, pageToken, opts)
			if err != nil {
				return err
			}

			// Reset docs batch before use.
			batch.Docs = batch.Docs[:0]
			for _, result := range results.Results() {
				doc, exists, err := shard.DocRef(result.ID)
				if err != nil {
					return err
				}
				if !exists {
					doc, err = convert.FromSeriesIDAndTagIter(result.ID, result.Tags)
					if err != nil {
						return err
					}
					i.metrics.flushDocsNew.Inc(1)
				} else {
					i.metrics.flushDocsCached.Inc(1)
				}

				batch.Docs = append(batch.Docs, doc)
				if len(batch.Docs) < batchSize {
					continue
				}

				err = i.sanitizeAllowDuplicatesWriteError(builder.InsertBatch(batch))
				if err != nil {
					return err
				}

				// Reset docs after insertions.
				batch.Docs = batch.Docs[:0]
			}

			// Add last batch if remaining.
			if len(batch.Docs) > 0 {
				err := i.sanitizeAllowDuplicatesWriteError(builder.InsertBatch(batch))
				if err != nil {
					return err
				}
			}

			results.Close()

			// Use BlockingCloseReset so that we can reuse the context without
			// it going back to the pool.
			ctx.BlockingCloseReset()
		}
	}

	// Finally flush this segment
	return preparedPersist.Persist(builder)
}

func (i *nsIndex) sanitizeAllowDuplicatesWriteError(err error) error {
	if err == nil {
		return nil
	}

	// NB: dropping duplicate id error messages from logs as they're expected when we see
	// repeated inserts. as long as a block has an ID, it's not an error so we don't need
	// to pollute the logs with these messages.
	if partialError, ok := err.(*m3ninxindex.BatchPartialError); ok {
		err = partialError.FilterDuplicateIDErrors()
	}

	return err
}

func (i *nsIndex) AssignShardSet(shardSet sharding.ShardSet) {
	// NB(r): Allocate the filter function once, it can be used outside
	// of locks as it depends on no internal state.
	set := bitset.NewBitSet(uint(shardSet.Max()))
	assigned := make(map[uint32]struct{})
	for _, shardID := range shardSet.AllIDs() {
		set.Set(uint(shardID))
		assigned[shardID] = struct{}{}
	}

	i.state.Lock()
	i.state.shardsFilterID = func(id ident.ID) bool {
		// NB(r): Use a bitset for fast lookups.
		return set.Test(uint(shardSet.Lookup(id)))
	}

	i.state.shardFilteredForID = func(id ident.ID) (uint32, bool) {
		shard := shardSet.Lookup(id)
		return shard, set.Test(uint(shard))
	}

	i.state.shardsAssigned = assigned
	i.state.Unlock()
}

func (i *nsIndex) shardsFilterID() func(id ident.ID) bool {
	i.state.RLock()
	v := i.state.shardsFilterID
	i.state.RUnlock()
	return v
}

func (i *nsIndex) shardForID() func(id ident.ID) (uint32, bool) {
	i.state.RLock()
	v := i.state.shardFilteredForID
	i.state.RUnlock()
	return v
}

func (i *nsIndex) Query(
	ctx context.Context,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResult, error) {
	var logFields []opentracinglog.Field
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.NSIdxQuery)
	defer sp.Finish()
	if sampled {
		// Only allocate metadata such as query string if sampling trace.
		logFields = []opentracinglog.Field{
			opentracinglog.String("query", query.String()),
			opentracinglog.String("namespace", i.nsMetadata.ID().String()),
			opentracinglog.Int("seriesLimit", opts.SeriesLimit),
			opentracinglog.Int("docsLimit", opts.DocsLimit),
			xopentracing.Time("queryStart", opts.StartInclusive.ToTime()),
			xopentracing.Time("queryEnd", opts.EndExclusive.ToTime()),
		}
		sp.LogFields(logFields...)
	}

	// Get results and set the namespace ID and size limit.
	results := i.resultsPool.Get()
	results.Reset(i.nsMetadata.ID(), index.QueryResultsOptions{
		SizeLimit: opts.SeriesLimit,
		FilterID:  i.shardsFilterID(),
	})
	ctx.RegisterFinalizer(results)
	queryRes, err := i.query(ctx, query, results, opts, i.execBlockQueryFn,
		i.newBlockQueryIterFn, logFields)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		return index.QueryResult{}, err
	}

	return index.QueryResult{
		Results:    results,
		Exhaustive: queryRes.exhaustive,
		Waited:     queryRes.waited,
	}, nil
}

func (i *nsIndex) AggregateQuery(
	ctx context.Context,
	query index.Query,
	opts index.AggregationOptions,
) (index.AggregateQueryResult, error) {
	id := i.nsMetadata.ID()
	logFields := []opentracinglog.Field{
		opentracinglog.String("query", query.String()),
		opentracinglog.String("namespace", id.String()),
		opentracinglog.Int("seriesLimit", opts.SeriesLimit),
		opentracinglog.Int("docsLimit", opts.DocsLimit),
		xopentracing.Time("queryStart", opts.StartInclusive.ToTime()),
		xopentracing.Time("queryEnd", opts.EndExclusive.ToTime()),
	}

	ctx, sp := ctx.StartTraceSpan(tracepoint.NSIdxAggregateQuery)
	sp.LogFields(logFields...)
	defer sp.Finish()

	metrics := index.NewAggregateUsageMetrics(id, i.opts.InstrumentOptions())
	// Get results and set the filters, namespace ID and size limit.
	results := i.aggregateResultsPool.Get()
	aopts := index.AggregateResultsOptions{
		SizeLimit:             opts.SeriesLimit,
		DocsLimit:             opts.DocsLimit,
		FieldFilter:           opts.FieldFilter,
		Type:                  opts.Type,
		AggregateUsageMetrics: metrics,
	}
	ctx.RegisterFinalizer(results)
	// use appropriate fn to query underlying blocks.
	// use block.Aggregate() for querying and set the query if required.
	fn := i.execBlockAggregateQueryFn
	isAllQuery := query.Equal(allQuery)
	if !isAllQuery {
		if field, isFieldQuery := idx.FieldQuery(query.Query); isFieldQuery {
			aopts.FieldFilter = aopts.FieldFilter.AddIfMissing(field)
		} else {
			// Need to actually restrict whether we should return a term or not
			// based on running the actual query to resolve a postings list and
			// then seeing if that intersects the aggregated term postings list
			// at all.
			aopts.RestrictByQuery = &query
		}
	}
	aopts.FieldFilter = aopts.FieldFilter.SortAndDedupe()
	results.Reset(id, aopts)
	queryRes, err := i.query(ctx, query, results, opts.QueryOptions, fn,
		i.newBlockAggregatorIterFn, logFields)
	if err != nil {
		return index.AggregateQueryResult{}, err
	}
	return index.AggregateQueryResult{
		Results:    results,
		Exhaustive: queryRes.exhaustive,
		Waited:     queryRes.waited,
	}, nil
}

type queryResult struct {
	exhaustive bool
	waited     int
}

func (i *nsIndex) query(
	ctx context.Context,
	query index.Query,
	results index.BaseResults,
	opts index.QueryOptions,
	execBlockFn execBlockQueryFn,
	newBlockIterFn newBlockIterFn,
	logFields []opentracinglog.Field,
) (queryResult, error) {
	ctx, sp, sampled := ctx.StartSampledTraceSpan(tracepoint.NSIdxQueryHelper)
	sp.LogFields(logFields...)
	defer sp.Finish()
	if sampled {
		// Only log fields if sampled.
		sp.LogFields(logFields...)
	}

	queryRes, err := i.queryWithSpan(ctx, query, results, opts, execBlockFn,
		newBlockIterFn, sp, logFields)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))

		if queryRes.exhaustive {
			i.metrics.queryExhaustiveInternalError.Inc(1)
		} else {
			i.metrics.queryNonExhaustiveInternalError.Inc(1)
		}
		return queryRes, err
	}

	if queryRes.exhaustive {
		i.metrics.queryExhaustiveSuccess.Inc(1)
		return queryRes, nil
	}

	// If require exhaustive but not, return error.
	if opts.RequireExhaustive {
		seriesCount := results.Size()
		docsCount := results.TotalDocsCount()
		if opts.SeriesLimitExceeded(seriesCount) {
			i.metrics.queryNonExhaustiveSeriesLimitError.Inc(1)
		} else if opts.DocsLimitExceeded(docsCount) {
			i.metrics.queryNonExhaustiveDocsLimitError.Inc(1)
		} else {
			i.metrics.queryNonExhaustiveLimitError.Inc(1)
		}

		// NB(r): Make sure error is not retried and returns as bad request.
		return queryRes, xerrors.NewInvalidParamsError(limits.NewQueryLimitExceededError(fmt.Sprintf(
			"query exceeded limit: require_exhaustive=%v, series_limit=%d, series_matched=%d, docs_limit=%d, docs_matched=%d",
			opts.RequireExhaustive,
			opts.SeriesLimit,
			seriesCount,
			opts.DocsLimit,
			docsCount,
		)))
	}

	// Otherwise non-exhaustive but not required to be.
	i.metrics.queryNonExhaustiveSuccess.Inc(1)
	return queryRes, nil
}

// blockIter is a composite type to hold various state about a block while iterating over the results.
type blockIter struct {
	iter           index.ResultIterator
	iterCloser     io.Closer
	block          index.Block
	waitTime       time.Duration
	processingTime time.Duration
}

func (i *nsIndex) queryWithSpan(
	ctx context.Context,
	query index.Query,
	results index.BaseResults,
	opts index.QueryOptions,
	execBlockFn execBlockQueryFn,
	newBlockIterFn newBlockIterFn,
	span opentracing.Span,
	logFields []opentracinglog.Field,
) (queryResult, error) {
	i.state.RLock()
	if !i.isOpenWithRLock() {
		i.state.RUnlock()
		return queryResult{}, errDbIndexUnableToQueryClosed
	}

	// Track this as an inflight query that needs to finish
	// when the index is closed.
	i.queriesWg.Add(1)
	defer i.queriesWg.Done()

	// Enact overrides for query options
	opts = i.overriddenOptsForQueryWithRLock(opts)

	// Retrieve blocks to query, then we can release lock.
	// NB(r): Important not to block ticking, and other tasks by
	// holding the RLock during a query.
	qryRange := xtime.NewRanges(xtime.Range{
		Start: opts.StartInclusive,
		End:   opts.EndExclusive,
	})
	// NB(r): Safe to take ref to i.state.blocksDescOrderImmutable since it's
	// immutable and we only create an iterator over it.
	blocks := newBlocksIterStackAlloc(i.activeBlock, i.state.blocksDescOrderImmutable, qryRange)

	// Can now release the lock and execute the query without holding the lock.
	i.state.RUnlock()

	var (
		// State contains concurrent mutable state for async execution below.
		state = &asyncQueryExecState{}
		wg    sync.WaitGroup
	)
	perms, err := i.permitsManager.NewPermits(ctx)
	if err != nil {
		return queryResult{}, err
	}
	defer perms.Close()

	var blockIters []*blockIter
	for b, ok := blocks.Next(); ok; b, ok = b.Next() {
		block := b.Current()
		iter, err := newBlockIterFn(ctx, block, query, results)
		if err != nil {
			return queryResult{}, err
		}
		blockIters = append(blockIters, &blockIter{
			iter:       iter,
			iterCloser: x.NewSafeCloser(iter),
			block:      block,
		})
	}

	defer func() {
		for _, iter := range blockIters {
			// safe to call Close multiple times, so it's fine to eagerly close in the loop below and here.
			_ = iter.iterCloser.Close()
		}
	}()

	// queryCanceled returns true if the query has been canceled and the current iteration should terminate.
	queryCanceled := func() bool {
		return opts.LimitsExceeded(results.Size(), results.TotalDocsCount()) || state.hasErr()
	}
	// waitForPermit waits for a permit. returns non-nil if the permit was acquired and the wait time.
	waitForPermit := func() (permits.Permit, time.Duration) {
		// make sure the query hasn't been canceled before waiting for a permit.
		if queryCanceled() {
			return nil, 0
		}

		startWait := time.Now()
		acquireResult, err := perms.Acquire(ctx)
		waitTime := time.Since(startWait)
		var success bool
		defer func() {
			// Note: ALWAYS release if we do not successfully return back
			// the permit and we checked one out.
			if !success && acquireResult.Permit != nil {
				perms.Release(acquireResult.Permit)
			}
		}()
		if acquireResult.Waited {
			// Potentially break an error if require no wait set.
			if err == nil && opts.RequireNoWait {
				// Fail iteration if request requires no waiting occurs.
				err = permits.ErrOperationWaitedOnRequireNoWait
			}
			state.incWaited(1)
		}
		if err != nil {
			state.addErr(err)
			return nil, waitTime
		}

		// make sure the query hasn't been canceled while waiting for a permit.
		if queryCanceled() {
			return nil, waitTime
		}

		success = true
		return acquireResult.Permit, waitTime
	}

	// We're looping through all the blocks that we need to query and kicking
	// off parallel queries which are bounded by the permits maximum
	// concurrency. It's possible at this point that we've completed querying one or more blocks and already exhausted
	// the maximum number of results that we're allowed to return. If thats the case, there is no value in kicking off
	// more parallel queries, so we break out of the loop.
	for _, blockIter := range blockIters {
		// Capture for async query execution below.
		blockIter := blockIter

		// acquire a permit before kicking off the goroutine to process the iterator. this limits the number of
		// concurrent goroutines to # of permits + large queries that needed multiple iterations to finish.
		permit, waitTime := waitForPermit()
		blockIter.waitTime += waitTime
		if permit == nil {
			break
		}

		wg.Add(1)
		// kick off a go routine to process the entire iterator.
		go func() {
			defer wg.Done()
			first := true
			for !blockIter.iter.Done() {
				// if this is not the first iteration of the iterator, need to acquire another permit.
				if !first {
					permit, waitTime = waitForPermit()
					blockIter.waitTime += waitTime
					if permit == nil {
						break
					}
				}
				blockLogFields := append(logFields, xopentracing.Duration("permitWaitTime", waitTime))
				first = false
				startProcessing := time.Now()
				execBlockFn(ctx, blockIter.block, permit, blockIter.iter, opts, state, results, blockLogFields)
				processingTime := time.Since(startProcessing)
				blockIter.processingTime += processingTime
				permit.Use(int64(processingTime))
				perms.Release(permit)
			}
			if first {
				// this should never happen since a new iter cannot be Done, but just to be safe.
				perms.Release(permit)
			}

			// close the iterator since it's no longer needed. it's safe to call Close multiple times, here and in the
			// defer when the function returns.
			if err := blockIter.iterCloser.Close(); err != nil {
				state.addErr(err)
			}
		}()
	}

	// wait for all workers to finish. if the caller cancels the call, the workers will be interrupted and eventually
	// finish.
	wg.Wait()

	i.metrics.loadedDocsPerQuery.RecordValue(float64(results.TotalDocsCount()))

	exhaustive := opts.Exhaustive(results.Size(), results.TotalDocsCount())
	// ok to read state without lock since all parallel queries are done.
	multiErr := state.multiErr
	err = multiErr.FinalError()

	return queryResult{
		exhaustive: exhaustive,
		waited:     state.waited(),
	}, err
}

func (i *nsIndex) newBlockQueryIterFn(
	ctx context.Context,
	block index.Block,
	query index.Query,
	_ index.BaseResults,
) (index.ResultIterator, error) {
	return block.QueryIter(ctx, query)
}

//nolint: dupl
func (i *nsIndex) execBlockQueryFn(
	ctx context.Context,
	block index.Block,
	permit permits.Permit,
	iter index.ResultIterator,
	opts index.QueryOptions,
	state *asyncQueryExecState,
	results index.BaseResults,
	logFields []opentracinglog.Field,
) {
	logFields = append(logFields,
		xopentracing.Time("blockStart", block.StartTime().ToTime()),
		xopentracing.Time("blockEnd", block.EndTime().ToTime()),
	)

	ctx, sp := ctx.StartTraceSpan(tracepoint.NSIdxBlockQuery)
	sp.LogFields(logFields...)
	defer sp.Finish()

	docResults, ok := results.(index.DocumentResults)
	if !ok { // should never happen
		state.addErr(fmt.Errorf("unknown results type [%T] received during query", results))
		return
	}
	queryIter, ok := iter.(index.QueryIterator)
	if !ok { // should never happen
		state.addErr(fmt.Errorf("unknown results type [%T] received during query", iter))
		return
	}

	deadline := time.Now().Add(time.Duration(permit.AllowedQuota()))
	err := block.QueryWithIter(ctx, opts, queryIter, docResults, deadline, logFields)
	if err == index.ErrUnableToQueryBlockClosed {
		// NB(r): Because we query this block outside of the results lock, it's
		// possible this block may get closed if it slides out of retention, in
		// that case those results are no longer considered valid and outside of
		// retention regardless, so this is a non-issue.
		err = nil
	}

	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		state.addErr(err)
	}
}

func (i *nsIndex) newBlockAggregatorIterFn(
	ctx context.Context,
	block index.Block,
	_ index.Query,
	results index.BaseResults,
) (index.ResultIterator, error) {
	aggResults, ok := results.(index.AggregateResults)
	if !ok { // should never happen
		return nil, fmt.Errorf("unknown results type [%T] received during aggregation", results)
	}
	return block.AggregateIter(ctx, aggResults.AggregateResultsOptions())
}

func (i *nsIndex) execBlockAggregateQueryFn(
	ctx context.Context,
	block index.Block,
	permit permits.Permit,
	iter index.ResultIterator,
	opts index.QueryOptions,
	state *asyncQueryExecState,
	results index.BaseResults,
	logFields []opentracinglog.Field,
) {
	logFields = append(logFields,
		xopentracing.Time("blockStart", block.StartTime().ToTime()),
		xopentracing.Time("blockEnd", block.EndTime().ToTime()),
	)

	ctx, sp := ctx.StartTraceSpan(tracepoint.NSIdxBlockAggregateQuery)
	sp.LogFields(logFields...)
	defer sp.Finish()

	aggResults, ok := results.(index.AggregateResults)
	if !ok { // should never happen
		state.addErr(fmt.Errorf("unknown results type [%T] received during aggregation", results))
		return
	}
	aggIter, ok := iter.(index.AggregateIterator)
	if !ok { // should never happen
		state.addErr(fmt.Errorf("unknown results type [%T] received during query", iter))
		return
	}

	deadline := time.Now().Add(time.Duration(permit.AllowedQuota()))
	err := block.AggregateWithIter(ctx, aggIter, opts, aggResults, deadline, logFields)
	if err == index.ErrUnableToQueryBlockClosed {
		// NB(r): Because we query this block outside of the results lock, it's
		// possible this block may get closed if it slides out of retention, in
		// that case those results are no longer considered valid and outside of
		// retention regardless, so this is a non-issue.
		err = nil
	}

	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		state.addErr(err)
	}
}

func (i *nsIndex) overriddenOptsForQueryWithRLock(
	opts index.QueryOptions,
) index.QueryOptions {
	// Override query response limits if needed.
	if i.state.runtimeOpts.maxQuerySeriesLimit > 0 && (opts.SeriesLimit == 0 ||
		int64(opts.SeriesLimit) > i.state.runtimeOpts.maxQuerySeriesLimit) {
		i.logger.Debug("overriding query response series limit",
			zap.Int("requested", opts.SeriesLimit),
			zap.Int64("maxAllowed", i.state.runtimeOpts.maxQuerySeriesLimit)) // FOLLOWUP(prateek): log query too once it's serializable.
		opts.SeriesLimit = int(i.state.runtimeOpts.maxQuerySeriesLimit)
	}
	if i.state.runtimeOpts.maxQueryDocsLimit > 0 && (opts.DocsLimit == 0 ||
		int64(opts.DocsLimit) > i.state.runtimeOpts.maxQueryDocsLimit) {
		i.logger.Debug("overriding query response docs limit",
			zap.Int("requested", opts.DocsLimit),
			zap.Int64("maxAllowed", i.state.runtimeOpts.maxQueryDocsLimit)) // FOLLOWUP(prateek): log query too once it's serializable.
		opts.DocsLimit = int(i.state.runtimeOpts.maxQueryDocsLimit)
	}
	return opts
}

type blockPresentResult struct {
	block  index.Block
	latest bool
}

func (i *nsIndex) ensureBlockPresent(blockStart xtime.UnixNano) (blockPresentResult, error) {
	i.state.RLock()
	defer i.state.RUnlock()
	if !i.isOpenWithRLock() {
		return blockPresentResult{}, errDbIndexUnableToWriteClosed
	}
	return i.ensureBlockPresentWithRLock(blockStart)
}

func (i *nsIndex) isLatestBlockWithRLock(blockStart xtime.UnixNano) bool {
	return i.state.latestBlock != nil && i.state.latestBlock.StartTime().Equal(blockStart)
}

// ensureBlockPresentWithRLock guarantees an index.Block exists for the specified
// blockStart, allocating one if it does not. It returns the desired block, or
// error if it's unable to do so.
func (i *nsIndex) ensureBlockPresentWithRLock(blockStart xtime.UnixNano) (blockPresentResult, error) {
	// check if the current latest block matches the required block, this
	// is the usual path and can short circuit the rest of the logic in this
	// function in most cases.
	if i.isLatestBlockWithRLock(blockStart) {
		return blockPresentResult{
			block:  i.state.latestBlock,
			latest: true,
		}, nil
	}

	// check if exists in the map (this can happen if the latestBlock has not
	// been rotated yet).
	if block, ok := i.state.blocksByTime[blockStart]; ok {
		return blockPresentResult{block: block}, nil
	}

	// i.e. block start does not exist, so we have to alloc.
	// we release the RLock (the function is called with this lock), and acquire
	// the write lock to do the extra allocation.
	i.state.RUnlock()
	i.state.Lock()

	// need to guarantee all exit paths from the function leave with the RLock
	// so we release the write lock and re-acquire a read lock.
	defer func() {
		i.state.Unlock()
		i.state.RLock()
	}()

	// re-check if exists in the map (another routine did the alloc)
	if block, ok := i.state.blocksByTime[blockStart]; ok {
		return blockPresentResult{
			block:  block,
			latest: i.isLatestBlockWithRLock(blockStart),
		}, nil
	}

	// ok now we know for sure we have to alloc
	block, err := i.newBlockFn(blockStart, i.nsMetadata,
		index.BlockOptions{}, i.namespaceRuntimeOptsMgr, i.opts.IndexOptions())
	if err != nil { // unable to allocate the block, should never happen.
		return blockPresentResult{}, i.unableToAllocBlockInvariantError(err)
	}

	// NB(bodu): Use same time barrier as `Tick` to make sealing of cold index blocks consistent.
	// We need to seal cold blocks write away for cold writes.
	if !blockStart.After(i.lastSealableBlockStart(xtime.ToUnixNano(i.nowFn()))) {
		if err := block.Seal(); err != nil {
			return blockPresentResult{}, err
		}
	}

	// add to tracked blocks map
	i.state.blocksByTime[blockStart] = block

	// update ordered blockStarts slice, and latestBlock
	i.updateBlockStartsWithLock()

	return blockPresentResult{
		block:  block,
		latest: i.isLatestBlockWithRLock(blockStart),
	}, nil
}

func (i *nsIndex) lastSealableBlockStart(t xtime.UnixNano) xtime.UnixNano {
	return retention.FlushTimeEndForBlockSize(i.blockSize, t.Add(-i.bufferPast))
}

func (i *nsIndex) updateBlockStartsWithLock() {
	// update ordered blockStarts slice
	var (
		latestBlockStart xtime.UnixNano
		latestBlock      index.Block
	)

	blocks := make([]blockAndBlockStart, 0, len(i.state.blocksByTime)+1)
	for ts, block := range i.state.blocksByTime {
		if ts >= latestBlockStart {
			latestBlockStart = ts
			latestBlock = block
		}
		blocks = append(blocks, blockAndBlockStart{
			block:      block,
			blockStart: ts,
		})
	}

	// order in desc order (i.e. reverse chronological)
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].blockStart > blocks[j].blockStart
	})

	// NB(r): Important not to modify this once set since we take reference
	// to this slice with an RLock, release with RUnlock and then loop over it
	// during query time so it must not be altered and stay immutable.
	// This is done to avoid allocating a copy of the slice at query time for
	// each query.
	i.state.blocksDescOrderImmutable = blocks

	// rotate latestBlock
	i.state.latestBlock = latestBlock
}

func (i *nsIndex) isOpenWithRLock() bool {
	return !i.state.closed
}

func (i *nsIndex) CleanupExpiredFileSets(t xtime.UnixNano) error {
	// we only expire data on drive that we don't hold a reference to, and is
	// past the expiration period. the earliest data we have to retain is given
	// by the following computation:
	//  Min(FIRST_EXPIRED_BLOCK, EARLIEST_RETAINED_BLOCK)
	i.state.RLock()
	defer i.state.RUnlock()
	if i.state.closed {
		return errDbIndexUnableToCleanupClosed
	}

	// earliest block to retain based on retention period
	earliestBlockStartToRetain := retention.FlushTimeStartForRetentionPeriod(i.retentionPeriod, i.blockSize, t)

	// now we loop through the blocks we hold, to ensure we don't delete any data for them.
	for t := range i.state.blocksByTime {
		if t.Before(earliestBlockStartToRetain) {
			earliestBlockStartToRetain = t
		}
	}

	// know the earliest block to retain, find all blocks earlier than it
	var (
		pathPrefix = i.opts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
		nsID       = i.nsMetadata.ID()
	)
	filesets, err := i.indexFilesetsBeforeFn(pathPrefix, nsID, earliestBlockStartToRetain)
	if err != nil {
		return err
	}

	// and delete them
	return i.deleteFilesFn(filesets)
}

func (i *nsIndex) CleanupCorruptedFileSets() error {
	/*
	   Corrupted index filesets can be safely cleaned up if its not
	   the latest volume index per index volume type/block start combo.

	   We are guaranteed not to be actively writing to an index fileset once
	   we're already writing to later volume indices.
	*/
	fsOpts := i.opts.CommitLogOptions().FilesystemOptions()
	infoFiles := i.readIndexInfoFilesFn(fs.ReadIndexInfoFilesOptions{
		FilePathPrefix:   fsOpts.FilePathPrefix(),
		Namespace:        i.nsMetadata.ID(),
		ReaderBufferSize: fsOpts.InfoReaderBufferSize(),
		IncludeCorrupted: true,
	})

	if len(infoFiles) == 0 {
		return nil
	}

	var (
		toDelete []string
		begin    = 0 // marks the beginning of a subslice that contains filesets with same block starts
	)
	// It's expected that info files are ordered by block start and volume index
	for j := range infoFiles {
		if infoFiles[begin].ID.BlockStart.Before(infoFiles[j].ID.BlockStart) {
			files, err := i.getCorruptedVolumesForDeletion(infoFiles[begin:j])
			if err != nil {
				return err
			}
			toDelete = append(toDelete, files...)
			begin = j
		} else if infoFiles[begin].ID.BlockStart.After(infoFiles[j].ID.BlockStart) {
			errorMessage := "filesets are expected to be ordered by block start"
			instrument.EmitAndLogInvariantViolation(i.opts.InstrumentOptions(), func(l *zap.Logger) {
				l.Error(errorMessage)
			})
			return instrument.InvariantErrorf(errorMessage)
		}
	}

	// Process the volumes in the last block, which are not covered by the loop.
	files, err := i.getCorruptedVolumesForDeletion(infoFiles[begin:])
	if err != nil {
		return err
	}
	toDelete = append(toDelete, files...)

	return i.deleteFilesFn(toDelete)
}

func (i *nsIndex) getCorruptedVolumesForDeletion(filesets []fs.ReadIndexInfoFileResult) ([]string, error) {
	if len(filesets) <= 1 {
		return nil, nil
	}

	// Check for invariants.
	for j := 1; j < len(filesets); j++ {
		if !filesets[j-1].ID.BlockStart.Equal(filesets[j].ID.BlockStart) {
			errorMessage := "all the filesets passed to this function should have the same block start"
			instrument.EmitAndLogInvariantViolation(i.opts.InstrumentOptions(), func(l *zap.Logger) {
				l.Error(errorMessage)
			})
			return nil, instrument.InvariantErrorf(errorMessage)
		} else if filesets[j-1].ID.VolumeIndex >= filesets[j].ID.VolumeIndex {
			errorMessage := "filesets should be ordered by volume index in increasing order"
			instrument.EmitAndLogInvariantViolation(i.opts.InstrumentOptions(), func(l *zap.Logger) {
				l.Error(errorMessage)
			})
			return nil, instrument.InvariantErrorf(errorMessage)
		}
	}

	toDelete := make([]string, 0)
	hasMoreRecentVolumeOfType := make(map[idxpersist.IndexVolumeType]struct{})
	// Iterate filesets in reverse order to process higher volume indexes first.
	for j := len(filesets) - 1; j >= 0; j-- {
		f := filesets[j]

		// NB: If the fileset info fields contains inconsistent information (e.g. block start inside
		// info file doesn't match the block start extracted from the filename), it means that info file
		// is missing or corrupted. Thus we cannot trust the information of this fileset
		// and we cannot be sure what's the actual volume type of it. However, a part of corrupted
		// fileset cleanup logic depends on knowing the volume type.
		//
		// Such fileset is deleted, except when it is the most recent volume in the block.
		//
		// The most recent volume is excluded because it is more likely to be actively written to.
		// If info file writes are not atomic, due to timing readers might observe the file
		// to be corrupted, even though at that moment the file is being written/re-written.
		if f.Corrupted && !f.ID.BlockStart.Equal(xtime.UnixNano(f.Info.BlockStart)) {
			if j != len(filesets)-1 {
				toDelete = append(toDelete, f.AbsoluteFilePaths...)
			}
			continue
		}

		volType := idxpersist.DefaultIndexVolumeType
		if f.Info.IndexVolumeType != nil {
			volType = idxpersist.IndexVolumeType(f.Info.IndexVolumeType.Value)
		}
		// Delete corrupted filesets if there are more recent volumes with the same volume type.
		if _, ok := hasMoreRecentVolumeOfType[volType]; !ok {
			hasMoreRecentVolumeOfType[volType] = struct{}{}
		} else if f.Corrupted {
			toDelete = append(toDelete, f.AbsoluteFilePaths...)
		}
	}
	return toDelete, nil
}

func (i *nsIndex) CleanupDuplicateFileSets(activeShards []uint32) error {
	fsOpts := i.opts.CommitLogOptions().FilesystemOptions()
	infoFiles := i.readIndexInfoFilesFn(fs.ReadIndexInfoFilesOptions{
		FilePathPrefix:   fsOpts.FilePathPrefix(),
		Namespace:        i.nsMetadata.ID(),
		ReaderBufferSize: fsOpts.InfoReaderBufferSize(),
	})

	segmentsOrderByVolumeIndexByVolumeTypeAndBlockStart := make(map[xtime.UnixNano]map[idxpersist.IndexVolumeType][]fs.Segments)
	for _, file := range infoFiles {
		seg := fs.NewSegments(file.Info, file.ID.VolumeIndex, file.AbsoluteFilePaths)
		blockStart := seg.BlockStart()
		segmentsOrderByVolumeIndexByVolumeType, ok := segmentsOrderByVolumeIndexByVolumeTypeAndBlockStart[blockStart]
		if !ok {
			segmentsOrderByVolumeIndexByVolumeType = make(map[idxpersist.IndexVolumeType][]fs.Segments)
			segmentsOrderByVolumeIndexByVolumeTypeAndBlockStart[blockStart] = segmentsOrderByVolumeIndexByVolumeType
		}

		volumeType := seg.VolumeType()
		if _, ok := segmentsOrderByVolumeIndexByVolumeType[volumeType]; !ok {
			segmentsOrderByVolumeIndexByVolumeType[volumeType] = make([]fs.Segments, 0)
		}
		segmentsOrderByVolumeIndexByVolumeType[volumeType] = append(segmentsOrderByVolumeIndexByVolumeType[volumeType], seg)
	}

	// Ensure that segments are sorted by volume index.
	for _, segmentsOrderByVolumeIndexByVolumeType := range segmentsOrderByVolumeIndexByVolumeTypeAndBlockStart {
		for _, segs := range segmentsOrderByVolumeIndexByVolumeType {
			sort.SliceStable(segs, func(i, j int) bool {
				return segs[i].VolumeIndex() < segs[j].VolumeIndex()
			})
		}
	}

	multiErr := xerrors.NewMultiError()
	// Check for dupes and remove.
	filesToDelete := make([]string, 0)
	for _, segmentsOrderByVolumeIndexByVolumeType := range segmentsOrderByVolumeIndexByVolumeTypeAndBlockStart {
		for _, segmentsOrderByVolumeIndex := range segmentsOrderByVolumeIndexByVolumeType {
			segmentsToKeep := make([]fs.Segments, 0)
			for _, seg := range segmentsOrderByVolumeIndex {
				for len(segmentsToKeep) > 0 {
					idx := len(segmentsToKeep) - 1
					if previous := segmentsToKeep[idx]; seg.ShardTimeRanges().IsSuperset(
						previous.ShardTimeRanges().FilterShards(activeShards)) {
						filesToDelete = append(filesToDelete, previous.AbsoluteFilePaths()...)
						segmentsToKeep = segmentsToKeep[:idx]
					} else {
						break
					}
				}
				segmentsToKeep = append(segmentsToKeep, seg)
			}
		}
	}
	multiErr = multiErr.Add(i.deleteFilesFn(filesToDelete))
	return multiErr.FinalError()
}

func (i *nsIndex) DebugMemorySegments(opts DebugMemorySegmentsOptions) error {
	i.state.RLock()
	defer i.state.RUnlock()
	if i.state.closed {
		return errDbIndexAlreadyClosed
	}

	ctx := context.NewBackground()
	defer ctx.Close()

	// Create a new set of file system options to output to new directory.
	fsOpts := i.opts.CommitLogOptions().
		FilesystemOptions().
		SetFilePathPrefix(opts.OutputDirectory)

	for _, block := range i.state.blocksByTime {
		segmentsData, err := block.MemorySegmentsData(ctx)
		if err != nil {
			return err
		}

		for numSegment, segmentData := range segmentsData {
			indexWriter, err := fs.NewIndexWriter(fsOpts)
			if err != nil {
				return err
			}

			fileSetID := fs.FileSetFileIdentifier{
				FileSetContentType: persist.FileSetIndexContentType,
				Namespace:          i.nsMetadata.ID(),
				BlockStart:         block.StartTime(),
				VolumeIndex:        numSegment,
			}
			openOpts := fs.IndexWriterOpenOptions{
				Identifier:      fileSetID,
				BlockSize:       i.blockSize,
				FileSetType:     persist.FileSetFlushType,
				Shards:          i.state.shardsAssigned,
				IndexVolumeType: idxpersist.DefaultIndexVolumeType,
			}
			if err := indexWriter.Open(openOpts); err != nil {
				return err
			}

			segWriter, err := idxpersist.NewFSTSegmentDataFileSetWriter(segmentData)
			if err != nil {
				return err
			}

			if err := indexWriter.WriteSegmentFileSet(segWriter); err != nil {
				return err
			}

			if err := indexWriter.Close(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (i *nsIndex) Close() error {
	i.state.Lock()
	if !i.isOpenWithRLock() {
		i.state.Unlock()
		return errDbIndexAlreadyClosed
	}

	i.state.closed = true
	close(i.state.closeCh)

	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(i.state.insertQueue.Stop())

	blocks := make([]index.Block, 0, len(i.state.blocksByTime)+1)
	for _, block := range i.state.blocksByTime {
		blocks = append(blocks, block)
	}
	blocks = append(blocks, i.activeBlock)

	i.activeBlock = nil
	i.state.latestBlock = nil
	i.state.blocksByTime = nil
	i.state.blocksDescOrderImmutable = nil

	if i.runtimeOptsListener != nil {
		i.runtimeOptsListener.Close()
		i.runtimeOptsListener = nil
	}

	if i.runtimeNsOptsListener != nil {
		i.runtimeNsOptsListener.Close()
		i.runtimeNsOptsListener = nil
	}

	// Can now unlock after collecting blocks to close and setting closed state.
	i.state.Unlock()

	// Wait for inflight queries to finish before closing blocks, do this
	// outside of lock in case an inflight query needs to acquire a read lock
	// to finish but can't acquire it because close was holding the lock waiting
	// for queries to drain first.
	i.queriesWg.Wait()

	for _, block := range blocks {
		multiErr = multiErr.Add(block.Close())
	}

	return multiErr.FinalError()
}

func (i *nsIndex) unableToAllocBlockInvariantError(err error) error {
	ierr := fmt.Errorf("index unable to allocate block: %v", err)
	instrument.EmitAndLogInvariantViolation(i.opts.InstrumentOptions(), func(l *zap.Logger) {
		l.Error(ierr.Error())
	})
	return ierr
}

type nsIndexMetrics struct {
	tick tally.Counter

	asyncInsertAttemptTotal tally.Counter
	asyncInsertAttemptSkip  tally.Counter
	asyncInsertAttemptWrite tally.Counter

	asyncInsertSuccess               tally.Counter
	asyncInsertErrors                tally.Counter
	insertAfterClose                 tally.Counter
	queryAfterClose                  tally.Counter
	forwardIndexHits                 tally.Counter
	forwardIndexMisses               tally.Counter
	forwardIndexCounter              tally.Counter
	insertEndToEndLatency            tally.Timer
	blocksEvictedMutableSegments     tally.Counter
	blockMetrics                     nsIndexBlocksMetrics
	indexingConcurrencyMin           tally.Gauge
	indexingConcurrencyMax           tally.Gauge
	indexingConcurrencyAvg           tally.Gauge
	flushIndexingConcurrency         tally.Gauge
	flushDocsNew                     tally.Counter
	flushDocsCached                  tally.Counter
	latestBlockNumSegmentsForeground tally.Gauge
	latestBlockNumDocsForeground     tally.Gauge
	latestBlockNumSegmentsBackground tally.Gauge
	latestBlockNumDocsBackground     tally.Gauge

	loadedDocsPerQuery                 tally.Histogram
	queryExhaustiveSuccess             tally.Counter
	queryExhaustiveInternalError       tally.Counter
	queryNonExhaustiveSuccess          tally.Counter
	queryNonExhaustiveInternalError    tally.Counter
	queryNonExhaustiveLimitError       tally.Counter
	queryNonExhaustiveSeriesLimitError tally.Counter
	queryNonExhaustiveDocsLimitError   tally.Counter
}

func newNamespaceIndexMetrics(
	opts index.Options,
	iopts instrument.Options,
) nsIndexMetrics {
	const (
		indexAttemptName         = "index-attempt"
		forwardIndexName         = "forward-index"
		indexingConcurrency      = "indexing-concurrency"
		flushIndexingConcurrency = "flush-indexing-concurrency"
	)
	scope := iopts.MetricsScope()
	blocksScope := scope.SubScope("blocks")
	m := nsIndexMetrics{
		tick: scope.Counter("index-tick"),
		asyncInsertAttemptTotal: scope.Tagged(map[string]string{
			"stage": "process",
		}).Counter(indexAttemptName),
		asyncInsertAttemptSkip: scope.Tagged(map[string]string{
			"stage": "skip",
		}).Counter(indexAttemptName),
		asyncInsertAttemptWrite: scope.Tagged(map[string]string{
			"stage": "write",
		}).Counter(indexAttemptName),
		asyncInsertSuccess: scope.Counter("index-success"),
		asyncInsertErrors: scope.Tagged(map[string]string{
			"error_type": "async-insert",
		}).Counter("index-error"),
		insertAfterClose: scope.Tagged(map[string]string{
			"error_type": "insert-closed",
		}).Counter("insert-after-close"),
		queryAfterClose: scope.Tagged(map[string]string{
			"error_type": "query-closed",
		}).Counter("query-after-error"),
		forwardIndexHits: scope.Tagged(map[string]string{
			"status": "hit",
		}).Counter(forwardIndexName),
		forwardIndexMisses: scope.Tagged(map[string]string{
			"status": "miss",
		}).Counter(forwardIndexName),
		forwardIndexCounter: scope.Tagged(map[string]string{
			"status": "count",
		}).Counter(forwardIndexName),
		insertEndToEndLatency: instrument.NewTimer(scope,
			"insert-end-to-end-latency", iopts.TimerOptions()),
		blocksEvictedMutableSegments: scope.Counter("blocks-evicted-mutable-segments"),
		blockMetrics:                 newNamespaceIndexBlocksMetrics(opts, blocksScope),
		indexingConcurrencyMin: scope.Tagged(map[string]string{
			"stat": "min",
		}).Gauge(indexingConcurrency),
		indexingConcurrencyMax: scope.Tagged(map[string]string{
			"stat": "max",
		}).Gauge(indexingConcurrency),
		indexingConcurrencyAvg: scope.Tagged(map[string]string{
			"stat": "avg",
		}).Gauge(indexingConcurrency),
		flushIndexingConcurrency: scope.Gauge(flushIndexingConcurrency),
		flushDocsNew: scope.Tagged(map[string]string{
			"status": "new",
		}).Counter("flush-docs"),
		flushDocsCached: scope.Tagged(map[string]string{
			"status": "cached",
		}).Counter("flush-docs"),
		latestBlockNumSegmentsForeground: scope.Tagged(map[string]string{
			"segment_type": "foreground",
		}).Gauge("latest-block-num-segments"),
		latestBlockNumDocsForeground: scope.Tagged(map[string]string{
			"segment_type": "foreground",
		}).Gauge("latest-block-num-docs"),
		latestBlockNumSegmentsBackground: scope.Tagged(map[string]string{
			"segment_type": "background",
		}).Gauge("latest-block-num-segments"),
		latestBlockNumDocsBackground: scope.Tagged(map[string]string{
			"segment_type": "background",
		}).Gauge("latest-block-num-docs"),
		loadedDocsPerQuery: scope.Histogram(
			"loaded-docs-per-query",
			tally.MustMakeExponentialValueBuckets(10, 2, 16),
		),
		queryExhaustiveSuccess: scope.Tagged(map[string]string{
			"exhaustive": "true",
			"result":     "success",
		}).Counter("query"),
		queryExhaustiveInternalError: scope.Tagged(map[string]string{
			"exhaustive": "true",
			"result":     "error_internal",
		}).Counter("query"),
		queryNonExhaustiveSuccess: scope.Tagged(map[string]string{
			"exhaustive": "false",
			"result":     "success",
		}).Counter("query"),
		queryNonExhaustiveInternalError: scope.Tagged(map[string]string{
			"exhaustive": "false",
			"result":     "error_internal",
		}).Counter("query"),
		queryNonExhaustiveLimitError: scope.Tagged(map[string]string{
			"exhaustive": "false",
			"result":     "error_require_exhaustive",
		}).Counter("query"),
		queryNonExhaustiveSeriesLimitError: scope.Tagged(map[string]string{
			"exhaustive": "false",
			"result":     "error_series_require_exhaustive",
		}).Counter("query"),
		queryNonExhaustiveDocsLimitError: scope.Tagged(map[string]string{
			"exhaustive": "false",
			"result":     "error_docs_require_exhaustive",
		}).Counter("query"),
	}

	// Initialize gauges that should default to zero before
	// returning results so that they are exported with an
	// explicit zero value at process startup.
	m.flushIndexingConcurrency.Update(0)

	return m
}

type nsIndexBlocksMetrics struct {
	ForegroundSegments nsIndexBlocksSegmentsMetrics
	BackgroundSegments nsIndexBlocksSegmentsMetrics
	FlushedSegments    nsIndexBlocksSegmentsMetrics
}

func newNamespaceIndexBlocksMetrics(
	opts index.Options,
	scope tally.Scope,
) nsIndexBlocksMetrics {
	return nsIndexBlocksMetrics{
		ForegroundSegments: newNamespaceIndexBlocksSegmentsMetrics(
			opts.ForegroundCompactionPlannerOptions(),
			scope.Tagged(map[string]string{
				"segment-type": "foreground",
			})),
		BackgroundSegments: newNamespaceIndexBlocksSegmentsMetrics(
			opts.BackgroundCompactionPlannerOptions(),
			scope.Tagged(map[string]string{
				"segment-type": "background",
			})),
		FlushedSegments: newNamespaceIndexBlocksSegmentsMetrics(
			opts.BackgroundCompactionPlannerOptions(),
			scope.Tagged(map[string]string{
				"segment-type": "flushed",
			})),
	}
}

type nsIndexBlocksSegmentsMetrics struct {
	Levels []nsIndexBlocksSegmentsLevelMetrics
}

type nsIndexBlocksSegmentsLevelMetrics struct {
	MinSizeInclusive int64
	MaxSizeExclusive int64
	NumSegments      tally.Gauge
	NumTotalDocs     tally.Gauge
	SegmentsAge      tally.Timer
}

func newNamespaceIndexBlocksSegmentsMetrics(
	compactionOpts compaction.PlannerOptions,
	scope tally.Scope,
) nsIndexBlocksSegmentsMetrics {
	segmentLevelsScope := scope.SubScope("segment-levels")
	levels := make([]nsIndexBlocksSegmentsLevelMetrics, 0, len(compactionOpts.Levels))
	for _, level := range compactionOpts.Levels {
		subScope := segmentLevelsScope.Tagged(map[string]string{
			"level-min-size": strconv.Itoa(int(level.MinSizeInclusive)),
			"level-max-size": strconv.Itoa(int(level.MaxSizeExclusive)),
		})
		levels = append(levels, nsIndexBlocksSegmentsLevelMetrics{
			MinSizeInclusive: level.MinSizeInclusive,
			MaxSizeExclusive: level.MaxSizeExclusive,
			NumSegments:      subScope.Gauge("num-segments"),
			NumTotalDocs:     subScope.Gauge("num-total-docs"),
			SegmentsAge:      subScope.Timer("segments-age"),
		})
	}

	return nsIndexBlocksSegmentsMetrics{
		Levels: levels,
	}
}

type dbShards []databaseShard

func (shards dbShards) IDs() []uint32 {
	ids := make([]uint32, 0, len(shards))
	for _, s := range shards {
		ids = append(ids, s.ID())
	}
	return ids
}

// blocksIterStackAlloc is a stack allocated block iterator, ensuring no
// allocations per query.
type blocksIterStackAlloc struct {
	activeBlock index.Block
	blocks      []blockAndBlockStart
	queryRanges xtime.Ranges
	idx         int
}

func newBlocksIterStackAlloc(
	activeBlock index.Block,
	blocks []blockAndBlockStart,
	queryRanges xtime.Ranges,
) blocksIterStackAlloc {
	return blocksIterStackAlloc{
		activeBlock: activeBlock,
		blocks:      blocks,
		queryRanges: queryRanges,
		idx:         -2,
	}
}

func (i blocksIterStackAlloc) Next() (blocksIterStackAlloc, bool) {
	iter := i

	for {
		iter.idx++
		if iter.idx == -1 {
			// This will return the active block.
			return iter, true
		}

		// No more ranges to query, perform this second so that
		// the in memory block always returns results.
		if i.queryRanges.IsEmpty() {
			return iter, false
		}

		if iter.idx >= len(i.blocks) {
			return iter, false
		}

		block := i.blocks[iter.idx].block

		// Ensure the block has data requested by the query.
		blockRange := xtime.Range{
			Start: block.StartTime(),
			End:   block.EndTime(),
		}
		if !i.queryRanges.Overlaps(blockRange) {
			continue
		}

		// Remove this range from the query range.
		i.queryRanges.RemoveRange(blockRange)

		return iter, true
	}
}

func (i blocksIterStackAlloc) Current() index.Block {
	if i.idx == -1 {
		return i.activeBlock
	}
	return i.blocks[i.idx].block
}
