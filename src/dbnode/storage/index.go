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
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xopentracing "github.com/m3db/m3/src/x/opentracing"
	xresource "github.com/m3db/m3/src/x/resource"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/bitset"
	"github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
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

var (
	allQuery = idx.NewAllQuery()
)

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

	// NB(r): Use a pooled goroutine worker once pooled goroutine workers
	// support timeouts for query workers pool.
	queryWorkersPool xsync.WorkerPool

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
	blockStartsDescOrder []xtime.UnixNano

	// shardsFilterID is set every time the shards change to correctly
	// only return IDs that this node owns.
	shardsFilterID func(ident.ID) bool

	// shardFilteredForID is set every time the shards change to correctly
	// only return IDs that this node owns, and the shard responsible for that ID.
	shardFilteredForID func(id ident.ID) (uint32, bool)

	shardsAssigned map[uint32]struct{}
}

// NB: nsIndexRuntimeOptions does not contain its own mutex as some of the variables
// are needed for each index write which already at least acquires read lock from
// nsIndex mutex, so to keep the lock acquisitions to a minimum these are protected
// under the same nsIndex mutex.
type nsIndexRuntimeOptions struct {
	insertMode          index.InsertMode
	maxQuerySeriesLimit int64
	maxQueryDocsLimit   int64
	defaultQueryTimeout time.Duration
}

// NB(prateek): the returned filesets are strictly before the given time, i.e. they
// live in the period (-infinity, exclusiveTime).
type indexFilesetsBeforeFn func(dir string,
	nsID ident.ID,
	exclusiveTime time.Time,
) ([]string, error)

type readIndexInfoFilesFn func(filePathPrefix string,
	namespace ident.ID,
	readerBufferSize int,
) []fs.ReadIndexInfoFileResult

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
	cancellable *xresource.CancellableLifetime,
	block index.Block,
	query index.Query,
	opts index.QueryOptions,
	state *asyncQueryExecState,
	results index.BaseResults,
	logFields []opentracinglog.Field,
)

// asyncQueryExecState tracks the async execution errors and results for a query.
type asyncQueryExecState struct {
	sync.Mutex
	multiErr   xerrors.MultiError
	exhaustive bool
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

		queryWorkersPool: newIndexOpts.opts.QueryIDsWorkerPool(),
		metrics:          newNamespaceIndexMetrics(indexOpts, instrumentOpts),

		doNotIndexWithFields: doNotIndexWithFields,
		shardSet:             shardSet,
	}

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
	queue := newIndexQueueFn(idx.writeBatches, nsMD, nowFn, scope)
	if err := queue.Start(); err != nil {
		return nil, err
	}
	idx.state.insertQueue = queue

	// allocate the current block to ensure we're able to index as soon as we return
	currentBlock := nowFn().Truncate(idx.blockSize)
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

func (i *nsIndex) SetRuntimeOptions(value runtime.Options) {
	i.state.Lock()
	i.state.runtimeOpts.defaultQueryTimeout = value.IndexDefaultQueryTimeout()
	i.state.Unlock()
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
	for _, start := range i.state.blockStartsDescOrder {
		block, ok := i.state.blocksByTime[start]
		if !ok {
			return i.missingBlockInvariantError(start)
		}

		err := block.Stats(reporter)
		if err == index.ErrUnableReportStatsBlockClosed {
			// Closed blocks are temporarily in the list still
			continue
		}
		if err != nil {
			return err
		}
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

func (i *nsIndex) BlockStartForWriteTime(writeTime time.Time) xtime.UnixNano {
	return xtime.ToUnixNano(writeTime.Truncate(i.blockSize))
}

func (i *nsIndex) BlockForBlockStart(blockStart time.Time) (index.Block, error) {
	return i.ensureBlockPresent(blockStart)
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
		now                        = i.nowFn()
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
					xNanoTimestamp := xtime.ToUnixNano(forwardEntryTimestamp)
					if entry.OnIndexSeries.NeedsIndexUpdate(xNanoTimestamp) {
						forwardIndexEntry := entry
						forwardIndexEntry.Timestamp = forwardEntryTimestamp
						forwardIndexEntry.OnIndexSeries.OnIndexPrepare()
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
	blockStart time.Time, batch *index.WriteBatch,
) {
	// NB(r): Capture pending entries so we can emit the latencies
	pending := batch.PendingEntries()
	numPending := len(pending)

	// NB(r): Notice we acquire each lock only to take a reference to the
	// block we release it so we don't block the tick, etc when we insert
	// batches since writing batches can take significant time when foreground
	// compaction occurs.
	block, err := i.ensureBlockPresent(blockStart)
	if err != nil {
		batch.MarkUnmarkedEntriesError(err)
		i.logger.Error("unable to write to index, dropping inserts",
			zap.Time("blockStart", blockStart),
			zap.Int("numWrites", batch.Len()),
			zap.Error(err),
		)
		i.metrics.asyncInsertErrors.Inc(int64(numPending))
		return
	}

	// Track attempted write.
	// Note: attemptTotal should = attemptSkip + attemptWrite.
	i.metrics.asyncInsertAttemptWrite.Inc(int64(numPending))

	// i.e. we have the block and the inserts, perform the writes.
	result, err := block.WriteBatch(batch)

	// record the end to end indexing latency
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
		block, err := i.ensureBlockPresentWithRLock(blockStart.ToTime())
		if err != nil { // should never happen
			multiErr = multiErr.Add(i.unableToAllocBlockInvariantError(err))
			continue
		}
		if err := block.AddResults(blockResults); err != nil {
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

func (i *nsIndex) Tick(c context.Cancellable, startTime time.Time) (namespaceIndexTickResult, error) {
	var (
		result                     = namespaceIndexTickResult{}
		earliestBlockStartToRetain = retention.FlushTimeStartForRetentionPeriod(i.retentionPeriod, i.blockSize, startTime)
	)

	i.state.Lock()
	defer func() {
		i.updateBlockStartsWithLock()
		i.state.Unlock()
	}()

	result.NumBlocks = int64(len(i.state.blocksByTime))

	var multiErr xerrors.MultiError
	for blockStart, block := range i.state.blocksByTime {
		if c.IsCancelled() {
			multiErr = multiErr.Add(errDbIndexTerminatingTickCancellation)
			return result, multiErr.FinalError()
		}

		// drop any blocks past the retention period
		if blockStart.ToTime().Before(earliestBlockStartToRetain) {
			multiErr = multiErr.Add(block.Close())
			delete(i.state.blocksByTime, blockStart)
			result.NumBlocksEvicted++
			result.NumBlocks--
			continue
		}

		// tick any blocks we're going to retain
		blockTickResult, tickErr := block.Tick(c)
		multiErr = multiErr.Add(tickErr)
		result.NumSegments += blockTickResult.NumSegments
		result.NumSegmentsBootstrapped += blockTickResult.NumSegmentsBootstrapped
		result.NumSegmentsMutable += blockTickResult.NumSegmentsMutable
		result.NumTotalDocs += blockTickResult.NumDocs
		result.FreeMmap += blockTickResult.FreeMmap

		// seal any blocks that are sealable
		if !blockStart.ToTime().After(i.lastSealableBlockStart(startTime)) && !block.IsSealed() {
			multiErr = multiErr.Add(block.Seal())
			result.NumBlocksSealed++
		}
	}

	return result, multiErr.FinalError()
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
	cpus := math.Ceil(perCPUFraction * float64(goruntime.NumCPU()))
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
				zap.Time("blockStart", block.StartTime()),
			)
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
		block.RotateColdMutableSegments()
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
	fsOpts := i.opts.CommitLogOptions().FilesystemOptions()
	infoFiles := i.readIndexInfoFilesFn(
		fsOpts.FilePathPrefix(),
		i.nsMetadata.ID(),
		fsOpts.InfoReaderBufferSize(),
	)
	flushable := make([]index.Block, 0, len(i.state.blocksByTime))

	now := i.nowFn()
	earliestBlockStartToRetain := retention.FlushTimeStartForRetentionPeriod(i.retentionPeriod, i.blockSize, now)
	currentBlockStart := now.Truncate(i.blockSize)
	// Check for flushable blocks by iterating through all block starts w/in retention.
	for blockStart := earliestBlockStartToRetain; blockStart.Before(currentBlockStart); blockStart = blockStart.Add(i.blockSize) {
		block, err := i.ensureBlockPresentWithRLock(blockStart)
		if err != nil {
			return nil, err
		}

		canFlush, err := i.canFlushBlockWithRLock(infoFiles, now, blockStart,
			block, shards, flushType)
		if err != nil {
			return nil, err
		}
		if !canFlush {
			continue
		}

		flushable = append(flushable, block)
	}
	return flushable, nil
}

func (i *nsIndex) canFlushBlockWithRLock(
	infoFiles []fs.ReadIndexInfoFileResult,
	startTime time.Time,
	blockStart time.Time,
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
		start := blockStart
		end := blockStart.Add(i.blockSize)
		dataBlockSize := i.nsMetadata.Options().RetentionOptions().BlockSize()
		for t := start; t.Before(end); t = t.Add(dataBlockSize) {
			flushState, err := shard.FlushState(t)
			if err != nil {
				return false, err
			}
			if flushState.WarmStatus != fileOpSuccess {
				return false, nil
			}
		}
	}

	return true, nil
}

func (i *nsIndex) hasIndexWarmFlushedToDisk(
	infoFiles []fs.ReadIndexInfoFileResult,
	blockStart time.Time,
) bool {
	var hasIndexWarmFlushedToDisk bool
	// NB(bodu): We consider the block to have been warm flushed if there are any
	// filesets on disk. This is consistent with the "has warm flushed" check in the db shard.
	// Shard block starts are marked as having warm flushed if an info file is successfully read from disk.
	for _, f := range infoFiles {
		indexVolumeType := idxpersist.DefaultIndexVolumeType
		if f.Info.IndexVolumeType != nil {
			indexVolumeType = idxpersist.IndexVolumeType(f.Info.IndexVolumeType.Value)
		}
		if f.ID.BlockStart == blockStart && indexVolumeType == idxpersist.DefaultIndexVolumeType {
			hasIndexWarmFlushedToDisk = true
		}
	}
	return hasIndexWarmFlushedToDisk
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
	logFields := []opentracinglog.Field{
		opentracinglog.String("query", query.String()),
		opentracinglog.String("namespace", i.nsMetadata.ID().String()),
		opentracinglog.Int("seriesLimit", opts.SeriesLimit),
		opentracinglog.Int("docsLimit", opts.DocsLimit),
		xopentracing.Time("queryStart", opts.StartInclusive),
		xopentracing.Time("queryEnd", opts.EndExclusive),
	}

	ctx, sp := ctx.StartTraceSpan(tracepoint.NSIdxQuery)
	sp.LogFields(logFields...)
	defer sp.Finish()

	// Get results and set the namespace ID and size limit.
	results := i.resultsPool.Get()
	results.Reset(i.nsMetadata.ID(), index.QueryResultsOptions{
		SizeLimit: opts.SeriesLimit,
		FilterID:  i.shardsFilterID(),
	})
	ctx.RegisterFinalizer(results)
	exhaustive, err := i.query(ctx, query, results, opts, i.execBlockQueryFn, logFields)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		return index.QueryResult{}, err
	}
	return index.QueryResult{
		Results:    results,
		Exhaustive: exhaustive,
	}, nil
}

func (i *nsIndex) WideQuery(
	ctx context.Context,
	query index.Query,
	collector chan *ident.IDBatch,
	opts index.WideQueryOptions,
) error {
	logFields := []opentracinglog.Field{
		opentracinglog.String("wideQuery", query.String()),
		opentracinglog.String("namespace", i.nsMetadata.ID().String()),
		opentracinglog.Int("batchSize", opts.BatchSize),
		xopentracing.Time("queryStart", opts.StartInclusive),
		xopentracing.Time("queryEnd", opts.EndExclusive),
	}

	ctx, sp := ctx.StartTraceSpan(tracepoint.NSIdxWideQuery)
	sp.LogFields(logFields...)
	defer sp.Finish()

	results := index.NewWideQueryResults(
		i.nsMetadata.ID(),
		i.opts.IdentifierPool(),
		i.shardForID(),
		collector,
		opts,
	)

	// NB: result should be finalized here, regardless of outcome
	// to prevent deadlocking while waiting on channel close.
	defer results.Finalize()
	queryOpts := opts.ToQueryOptions()

	_, err := i.query(ctx, query, results, queryOpts, i.execBlockWideQueryFn, logFields)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		return err
	}

	return nil
}

func (i *nsIndex) AggregateQuery(
	ctx context.Context,
	query index.Query,
	opts index.AggregationOptions,
) (index.AggregateQueryResult, error) {
	logFields := []opentracinglog.Field{
		opentracinglog.String("query", query.String()),
		opentracinglog.String("namespace", i.nsMetadata.ID().String()),
		opentracinglog.Int("seriesLimit", opts.SeriesLimit),
		opentracinglog.Int("docsLimit", opts.DocsLimit),
		xopentracing.Time("queryStart", opts.StartInclusive),
		xopentracing.Time("queryEnd", opts.EndExclusive),
	}

	ctx, sp := ctx.StartTraceSpan(tracepoint.NSIdxAggregateQuery)
	sp.LogFields(logFields...)
	defer sp.Finish()

	// Get results and set the filters, namespace ID and size limit.
	results := i.aggregateResultsPool.Get()
	aopts := index.AggregateResultsOptions{
		SizeLimit:   opts.SeriesLimit,
		FieldFilter: opts.FieldFilter,
		Type:        opts.Type,
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
	results.Reset(i.nsMetadata.ID(), aopts)
	exhaustive, err := i.query(ctx, query, results, opts.QueryOptions, fn, logFields)
	if err != nil {
		return index.AggregateQueryResult{}, err
	}
	return index.AggregateQueryResult{
		Results:    results,
		Exhaustive: exhaustive,
	}, nil
}

func (i *nsIndex) query(
	ctx context.Context,
	query index.Query,
	results index.BaseResults,
	opts index.QueryOptions,
	execBlockFn execBlockQueryFn,
	logFields []opentracinglog.Field,
) (bool, error) {
	ctx, sp := ctx.StartTraceSpan(tracepoint.NSIdxQueryHelper)
	sp.LogFields(logFields...)
	defer sp.Finish()

	exhaustive, err := i.queryWithSpan(ctx, query, results, opts, execBlockFn, sp, logFields)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))

		if exhaustive {
			i.metrics.queryExhaustiveInternalError.Inc(1)
		} else {
			i.metrics.queryNonExhaustiveInternalError.Inc(1)
		}
		return exhaustive, err
	}

	if exhaustive {
		i.metrics.queryExhaustiveSuccess.Inc(1)
		return exhaustive, nil
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
		return exhaustive, xerrors.NewInvalidParamsError(fmt.Errorf(
			"query exceeded limit: require_exhaustive=%v, series_limit=%d, series_matched=%d, docs_limit=%d, docs_matched=%d",
			opts.RequireExhaustive,
			opts.SeriesLimit,
			seriesCount,
			opts.DocsLimit,
			docsCount,
		))
	}

	// Otherwise non-exhaustive but not required to be.
	i.metrics.queryNonExhaustiveSuccess.Inc(1)
	return exhaustive, nil
}

func (i *nsIndex) queryWithSpan(
	ctx context.Context,
	query index.Query,
	results index.BaseResults,
	opts index.QueryOptions,
	execBlockFn execBlockQueryFn,
	span opentracing.Span,
	logFields []opentracinglog.Field,
) (bool, error) {
	// Capture start before needing to acquire lock.
	start := i.nowFn()

	i.state.RLock()
	if !i.isOpenWithRLock() {
		i.state.RUnlock()
		return false, errDbIndexUnableToQueryClosed
	}

	// Track this as an inflight query that needs to finish
	// when the index is closed.
	i.queriesWg.Add(1)
	defer i.queriesWg.Done()

	// Enact overrides for query options
	opts = i.overriddenOptsForQueryWithRLock(opts)
	timeout := i.timeoutForQueryWithRLock(ctx)

	// Retrieve blocks to query, then we can release lock
	// NB(r): Important not to block ticking, and other tasks by
	// holding the RLock during a query.
	blocks, err := i.blocksForQueryWithRLock(xtime.NewRanges(xtime.Range{
		Start: opts.StartInclusive,
		End:   opts.EndExclusive,
	}))

	// Can now release the lock and execute the query without holding the lock.
	i.state.RUnlock()

	if err != nil {
		return false, err
	}

	var (
		// State contains concurrent mutable state for async execution below.
		state = asyncQueryExecState{
			exhaustive: true,
		}
		deadline = start.Add(timeout)
		wg       sync.WaitGroup
	)

	// Create a cancellable lifetime and cancel it at end of this method so that
	// no child async task modifies the result after this method returns.
	cancellable := xresource.NewCancellableLifetime()
	defer cancellable.Cancel()

	for _, block := range blocks {
		// Capture block for async query execution below.
		block := block

		// We're looping through all the blocks that we need to query and kicking
		// off parallel queries which are bounded by the queryWorkersPool's maximum
		// concurrency. This means that it's possible at this point that we've
		// completed querying one or more blocks and already exhausted the maximum
		// number of results that we're allowed to return. If thats the case, there
		// is no value in kicking off more parallel queries, so we break out of
		// the loop.
		seriesCount := results.Size()
		docsCount := results.TotalDocsCount()
		alreadyExceededLimit := opts.SeriesLimitExceeded(seriesCount) || opts.DocsLimitExceeded(docsCount)
		if alreadyExceededLimit {
			state.Lock()
			state.exhaustive = false
			state.Unlock()
			// Break out if already not exhaustive.
			break
		}

		if applyTimeout := timeout > 0; !applyTimeout {
			// No timeout, just wait blockingly for a worker.
			wg.Add(1)
			i.queryWorkersPool.Go(func() {
				execBlockFn(ctx, cancellable, block, query, opts, &state, results, logFields)
				wg.Done()
			})
			continue
		}

		// Need to apply timeout to the blocking wait call for a worker.
		var timedOut bool
		if timeLeft := deadline.Sub(i.nowFn()); timeLeft > 0 {
			wg.Add(1)
			timedOut := !i.queryWorkersPool.GoWithTimeout(func() {
				execBlockFn(ctx, cancellable, block, query, opts, &state, results, logFields)
				wg.Done()
			}, timeLeft)

			if timedOut {
				// Did not launch task, need to ensure don't wait for it.
				wg.Done()
			}
		} else {
			timedOut = true
		}

		if timedOut {
			// Exceeded our deadline waiting for this block's query to start.
			return false, fmt.Errorf("index query timed out: %s", timeout.String())
		}
	}

	// Wait for queries to finish.
	if !(timeout > 0) {
		// No timeout, just blockingly wait.
		wg.Wait()
	} else {
		// Need to abort early if timeout hit.
		timeLeft := deadline.Sub(i.nowFn())
		if timeLeft <= 0 {
			return false, fmt.Errorf("index query timed out: %s", timeout.String())
		}

		var (
			ticker  = time.NewTicker(timeLeft)
			doneCh  = make(chan struct{})
			aborted bool
		)
		go func() {
			wg.Wait()
			close(doneCh)
		}()
		select {
		case <-ticker.C:
			aborted = true
		case <-doneCh:
		}

		// Make sure to always free the timer/ticker so they don't sit around.
		ticker.Stop()

		if aborted {
			return false, fmt.Errorf("index query timed out: %s", timeout.String())
		}
	}

	i.metrics.loadedDocsPerQuery.RecordValue(float64(results.TotalDocsCount()))

	state.Lock()
	// Take reference to vars to return while locked.
	exhaustive := state.exhaustive
	err = state.multiErr.FinalError()
	state.Unlock()

	if err != nil {
		return false, err
	}
	return exhaustive, nil
}

func (i *nsIndex) execBlockQueryFn(
	ctx context.Context,
	cancellable *xresource.CancellableLifetime,
	block index.Block,
	query index.Query,
	opts index.QueryOptions,
	state *asyncQueryExecState,
	results index.BaseResults,
	logFields []opentracinglog.Field,
) {
	logFields = append(logFields,
		xopentracing.Time("blockStart", block.StartTime()),
		xopentracing.Time("blockEnd", block.EndTime()),
	)

	ctx, sp := ctx.StartTraceSpan(tracepoint.NSIdxBlockQuery)
	sp.LogFields(logFields...)
	defer sp.Finish()

	blockExhaustive, err := block.Query(ctx, cancellable, query, opts, results, logFields)
	if err == index.ErrUnableToQueryBlockClosed {
		// NB(r): Because we query this block outside of the results lock, it's
		// possible this block may get closed if it slides out of retention, in
		// that case those results are no longer considered valid and outside of
		// retention regardless, so this is a non-issue.
		err = nil
	}

	state.Lock()
	defer state.Unlock()

	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		state.multiErr = state.multiErr.Add(err)
	}
	state.exhaustive = state.exhaustive && blockExhaustive
}

func (i *nsIndex) execBlockWideQueryFn(
	ctx context.Context,
	cancellable *xresource.CancellableLifetime,
	block index.Block,
	query index.Query,
	opts index.QueryOptions,
	state *asyncQueryExecState,
	results index.BaseResults,
	logFields []opentracinglog.Field,
) {
	logFields = append(logFields,
		xopentracing.Time("blockStart", block.StartTime()),
		xopentracing.Time("blockEnd", block.EndTime()),
	)

	ctx, sp := ctx.StartTraceSpan(tracepoint.NSIdxBlockQuery)
	sp.LogFields(logFields...)
	defer sp.Finish()

	_, err := block.Query(ctx, cancellable, query, opts, results, logFields)
	if err == index.ErrUnableToQueryBlockClosed {
		// NB(r): Because we query this block outside of the results lock, it's
		// possible this block may get closed if it slides out of retention, in
		// that case those results are no longer considered valid and outside of
		// retention regardless, so this is a non-issue.
		err = nil
	} else if err == index.ErrWideQueryResultsExhausted {
		// NB: this error indicates a wide query short-circuit, so it is expected
		// after the queried shard set is exhausted.
		err = nil
	}

	state.Lock()
	defer state.Unlock()

	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		state.multiErr = state.multiErr.Add(err)
	}

	// NB: wide queries are always exhaustive.
	state.exhaustive = true
}

func (i *nsIndex) execBlockAggregateQueryFn(
	ctx context.Context,
	cancellable *xresource.CancellableLifetime,
	block index.Block,
	query index.Query,
	opts index.QueryOptions,
	state *asyncQueryExecState,
	results index.BaseResults,
	logFields []opentracinglog.Field,
) {
	logFields = append(logFields,
		xopentracing.Time("blockStart", block.StartTime()),
		xopentracing.Time("blockEnd", block.EndTime()),
	)

	ctx, sp := ctx.StartTraceSpan(tracepoint.NSIdxBlockAggregateQuery)
	sp.LogFields(logFields...)
	defer sp.Finish()

	aggResults, ok := results.(index.AggregateResults)
	if !ok { // should never happen
		state.Lock()
		err := fmt.Errorf("unknown results type [%T] received during aggregation", results)
		state.multiErr = state.multiErr.Add(err)
		state.Unlock()
		return
	}

	blockExhaustive, err := block.Aggregate(ctx, cancellable, opts, aggResults, logFields)
	if err == index.ErrUnableToQueryBlockClosed {
		// NB(r): Because we query this block outside of the results lock, it's
		// possible this block may get closed if it slides out of retention, in
		// that case those results are no longer considered valid and outside of
		// retention regardless, so this is a non-issue.
		err = nil
	}

	state.Lock()
	defer state.Unlock()
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
		state.multiErr = state.multiErr.Add(err)
	}
	state.exhaustive = state.exhaustive && blockExhaustive
}

func (i *nsIndex) timeoutForQueryWithRLock(
	ctx context.Context,
) time.Duration {
	// TODO(r): Allow individual queries to specify timeouts using
	// deadlines passed by the context.
	return i.state.runtimeOpts.defaultQueryTimeout
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

func (i *nsIndex) blocksForQueryWithRLock(queryRange xtime.Ranges) ([]index.Block, error) {
	// Chunk the query request into bounds based on applicable blocks and
	// execute the requests to each of them; and merge results.
	blocks := make([]index.Block, 0, len(i.state.blockStartsDescOrder))

	// Iterate known blocks in a defined order of time (newest first) to enforce
	// some determinism about the results returned.
	for _, start := range i.state.blockStartsDescOrder {
		// Terminate if queryRange doesn't need any more data
		if queryRange.IsEmpty() {
			break
		}

		block, ok := i.state.blocksByTime[start]
		if !ok {
			// This is an invariant, should never occur if state tracking is correct.
			return nil, i.missingBlockInvariantError(start)
		}

		// Ensure the block has data requested by the query.
		blockRange := xtime.Range{Start: block.StartTime(), End: block.EndTime()}
		if !queryRange.Overlaps(blockRange) {
			continue
		}

		// Remove this range from the query range.
		queryRange.RemoveRange(blockRange)

		blocks = append(blocks, block)
	}

	return blocks, nil
}

func (i *nsIndex) ensureBlockPresent(blockStart time.Time) (index.Block, error) {
	i.state.RLock()
	defer i.state.RUnlock()
	if !i.isOpenWithRLock() {
		return nil, errDbIndexUnableToWriteClosed
	}
	return i.ensureBlockPresentWithRLock(blockStart)
}

// ensureBlockPresentWithRLock guarantees an index.Block exists for the specified
// blockStart, allocating one if it does not. It returns the desired block, or
// error if it's unable to do so.
func (i *nsIndex) ensureBlockPresentWithRLock(blockStart time.Time) (index.Block, error) {
	// check if the current latest block matches the required block, this
	// is the usual path and can short circuit the rest of the logic in this
	// function in most cases.
	if i.state.latestBlock != nil && i.state.latestBlock.StartTime().Equal(blockStart) {
		return i.state.latestBlock, nil
	}

	// check if exists in the map (this can happen if the latestBlock has not
	// been rotated yet).
	blockStartNanos := xtime.ToUnixNano(blockStart)
	if block, ok := i.state.blocksByTime[blockStartNanos]; ok {
		return block, nil
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
	if block, ok := i.state.blocksByTime[blockStartNanos]; ok {
		return block, nil
	}

	// ok now we know for sure we have to alloc
	block, err := i.newBlockFn(blockStart, i.nsMetadata,
		index.BlockOptions{}, i.namespaceRuntimeOptsMgr, i.opts.IndexOptions())
	if err != nil { // unable to allocate the block, should never happen.
		return nil, i.unableToAllocBlockInvariantError(err)
	}

	// NB(bodu): Use same time barrier as `Tick` to make sealing of cold index blocks consistent.
	// We need to seal cold blocks write away for cold writes.
	if !blockStart.After(i.lastSealableBlockStart(i.nowFn())) {
		if err := block.Seal(); err != nil {
			return nil, err
		}
	}

	// add to tracked blocks map
	i.state.blocksByTime[blockStartNanos] = block

	// update ordered blockStarts slice, and latestBlock
	i.updateBlockStartsWithLock()
	return block, nil
}

func (i *nsIndex) lastSealableBlockStart(t time.Time) time.Time {
	return retention.FlushTimeEndForBlockSize(i.blockSize, t.Add(-i.bufferPast))
}

func (i *nsIndex) updateBlockStartsWithLock() {
	// update ordered blockStarts slice
	var (
		latestBlockStart xtime.UnixNano
		latestBlock      index.Block
	)

	blockStarts := make([]xtime.UnixNano, 0, len(i.state.blocksByTime))
	for ts, block := range i.state.blocksByTime {
		if ts >= latestBlockStart {
			latestBlock = block
		}
		blockStarts = append(blockStarts, ts)
	}

	// order in desc order (i.e. reverse chronological)
	sort.Slice(blockStarts, func(i, j int) bool {
		return blockStarts[i] > blockStarts[j]
	})
	i.state.blockStartsDescOrder = blockStarts

	// rotate latestBlock
	i.state.latestBlock = latestBlock
}

func (i *nsIndex) isOpenWithRLock() bool {
	return !i.state.closed
}

func (i *nsIndex) CleanupExpiredFileSets(t time.Time) error {
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
		if t.ToTime().Before(earliestBlockStartToRetain) {
			earliestBlockStartToRetain = t.ToTime()
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

func (i *nsIndex) CleanupDuplicateFileSets() error {
	fsOpts := i.opts.CommitLogOptions().FilesystemOptions()
	infoFiles := i.readIndexInfoFilesFn(
		fsOpts.FilePathPrefix(),
		i.nsMetadata.ID(),
		fsOpts.InfoReaderBufferSize(),
	)

	segmentsOrderByVolumeIndexByVolumeTypeAndBlockStart := make(map[xtime.UnixNano]map[idxpersist.IndexVolumeType][]fs.Segments)
	for _, file := range infoFiles {
		seg := fs.NewSegments(file.Info, file.ID.VolumeIndex, file.AbsoluteFilePaths)
		blockStart := xtime.ToUnixNano(seg.BlockStart())
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

	// Ensure that segments are soroted by volume index.
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
			shardTimeRangesCovered := result.NewShardTimeRanges()
			currSegments := make([]fs.Segments, 0)
			for _, seg := range segmentsOrderByVolumeIndex {
				if seg.ShardTimeRanges().IsSuperset(shardTimeRangesCovered) {
					// Mark dupe segments for deletion.
					for _, currSeg := range currSegments {
						filesToDelete = append(filesToDelete, currSeg.AbsoluteFilePaths()...)
					}
					currSegments = []fs.Segments{seg}
					shardTimeRangesCovered = seg.ShardTimeRanges().Copy()
					continue
				}
				currSegments = append(currSegments, seg)
				shardTimeRangesCovered.AddRanges(seg.ShardTimeRanges())
			}
		}
	}
	multiErr = multiErr.Add(i.deleteFilesFn(filesToDelete))
	return multiErr.FinalError()
}

func (i *nsIndex) DebugMemorySegments(opts DebugMemorySegmentsOptions) error {
	i.state.RLock()
	defer i.state.RLock()
	if i.state.closed {
		return errDbIndexAlreadyClosed
	}

	ctx := context.NewContext()
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

	blocks := make([]index.Block, 0, len(i.state.blocksByTime))
	for _, block := range i.state.blocksByTime {
		blocks = append(blocks, block)
	}

	i.state.latestBlock = nil
	i.state.blocksByTime = nil
	i.state.blockStartsDescOrder = nil

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

func (i *nsIndex) missingBlockInvariantError(t xtime.UnixNano) error {
	err := fmt.Errorf("index query did not find block %d despite seeing it in slice", t)
	instrument.EmitAndLogInvariantViolation(i.opts.InstrumentOptions(), func(l *zap.Logger) {
		l.Error(err.Error())
	})
	return err
}

func (i *nsIndex) unableToAllocBlockInvariantError(err error) error {
	ierr := fmt.Errorf("index unable to allocate block: %v", err)
	instrument.EmitAndLogInvariantViolation(i.opts.InstrumentOptions(), func(l *zap.Logger) {
		l.Error(ierr.Error())
	})
	return ierr
}

type nsIndexMetrics struct {
	asyncInsertAttemptTotal tally.Counter
	asyncInsertAttemptSkip  tally.Counter
	asyncInsertAttemptWrite tally.Counter

	asyncInsertSuccess           tally.Counter
	asyncInsertErrors            tally.Counter
	insertAfterClose             tally.Counter
	queryAfterClose              tally.Counter
	forwardIndexHits             tally.Counter
	forwardIndexMisses           tally.Counter
	forwardIndexCounter          tally.Counter
	insertEndToEndLatency        tally.Timer
	blocksEvictedMutableSegments tally.Counter
	blockMetrics                 nsIndexBlocksMetrics
	indexingConcurrencyMin       tally.Gauge
	indexingConcurrencyMax       tally.Gauge
	indexingConcurrencyAvg       tally.Gauge
	flushIndexingConcurrency     tally.Gauge
	flushDocsNew                 tally.Counter
	flushDocsCached              tally.Counter

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
