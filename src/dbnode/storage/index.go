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

package storage

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	m3dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/m3ninx/doc"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/m3ninx/postings"
	xclose "github.com/m3db/m3x/close"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	errDbIndexAlreadyClosed               = errors.New("database index has already been closed")
	errDbIndexUnableToWriteClosed         = errors.New("unable to write to database index, already closed")
	errDbIndexUnableToQueryClosed         = errors.New("unable to query database index, already closed")
	errDbIndexUnableToFlushClosed         = errors.New("unable to flush database index, already closed")
	errDbIndexUnableToCleanupClosed       = errors.New("unable to cleanup database index, already closed")
	errDbIndexTerminatingTickCancellation = errors.New("terminating tick early due to cancellation")
	errDbIndexIsBootstrapping             = errors.New("index is already bootstrapping")
)

const (
	defaultFlushReadDataBlocksBatchSize = int64(4096)
	nsIndexReportStatsInterval          = 10 * time.Second
)

// nolint: maligned
type nsIndex struct {
	state nsIndexState

	// all the vars below this line are not modified past the ctor
	// and don't require a lock when being accessed.
	nowFn           clock.NowFn
	blockSize       time.Duration
	retentionPeriod time.Duration
	bufferPast      time.Duration
	bufferFuture    time.Duration

	indexFilesetsBeforeFn indexFilesetsBeforeFn
	deleteFilesFn         deleteFilesFn

	newBlockFn          newBlockFn
	logger              xlog.Logger
	opts                Options
	nsMetadata          namespace.Metadata
	runtimeOptsListener xclose.SimpleCloser

	resultsPool index.ResultsPool
	// NB(r): Use a pooled goroutine worker once pooled goroutine workers
	// support timeouts for query workers pool.
	queryWorkersPool xsync.WorkerPool

	// queriesWg tracks outstanding queries to ensure
	// we wait for all queries to complete before actually closing
	// blocks and other cleanup tasks on index close
	queriesWg sync.WaitGroup

	metrics nsIndexMetrics
}

type nsIndexState struct {
	sync.RWMutex // NB: guards all variables in this struct

	closed         bool
	closeCh        chan struct{}
	bootstrapState BootstrapState
	runtimeOpts    nsIndexRuntimeOptions

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
}

// NB: nsIndexRuntimeOptions does not contain its own mutex as some of the variables
// are needed for each index write which already at least acquires read lock from
// nsIndex mutex, so to keep the lock acquisitions to a minimum these are protected
// under the same nsIndex mutex.
type nsIndexRuntimeOptions struct {
	insertMode            index.InsertMode
	maxQueryLimit         int64
	flushBlockNumSegments uint
	defaultQueryTimeout   time.Duration
}

type newBlockFn func(time.Time, namespace.Metadata, index.Options) (index.Block, error)

// NB(prateek): the returned filesets are strictly before the given time, i.e. they
// live in the period (-infinity, exclusiveTime).
type indexFilesetsBeforeFn func(dir string,
	nsID ident.ID,
	exclusiveTime time.Time,
) ([]string, error)

type newNamespaceIndexOpts struct {
	md              namespace.Metadata
	opts            Options
	newIndexQueueFn newNamespaceIndexInsertQueueFn
	newBlockFn      newBlockFn
}

// newNamespaceIndex returns a new namespaceIndex for the provided namespace.
func newNamespaceIndex(
	nsMD namespace.Metadata,
	opts Options,
) (namespaceIndex, error) {
	return newNamespaceIndexWithOptions(newNamespaceIndexOpts{
		md:              nsMD,
		opts:            opts,
		newIndexQueueFn: newNamespaceIndexInsertQueue,
		newBlockFn:      index.NewBlock,
	})
}

// newNamespaceIndexWithInsertQueueFn is a ctor used in tests to override the insert queue.
func newNamespaceIndexWithInsertQueueFn(
	nsMD namespace.Metadata,
	newIndexQueueFn newNamespaceIndexInsertQueueFn,
	opts Options,
) (namespaceIndex, error) {
	return newNamespaceIndexWithOptions(newNamespaceIndexOpts{
		md:              nsMD,
		opts:            opts,
		newIndexQueueFn: newIndexQueueFn,
		newBlockFn:      index.NewBlock,
	})
}

// newNamespaceIndexWithNewBlockFn is a ctor used in tests to inject blocks.
func newNamespaceIndexWithNewBlockFn(
	nsMD namespace.Metadata,
	newBlockFn newBlockFn,
	opts Options,
) (namespaceIndex, error) {
	return newNamespaceIndexWithOptions(newNamespaceIndexOpts{
		md:              nsMD,
		opts:            opts,
		newIndexQueueFn: newNamespaceIndexInsertQueue,
		newBlockFn:      newBlockFn,
	})
}

// newNamespaceIndexWithOptions returns a new namespaceIndex with the provided configuration options.
func newNamespaceIndexWithOptions(
	newIndexOpts newNamespaceIndexOpts,
) (namespaceIndex, error) {
	var (
		nsMD            = newIndexOpts.md
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
	idx := &nsIndex{
		state: nsIndexState{
			closeCh: make(chan struct{}),
			runtimeOpts: nsIndexRuntimeOptions{
				insertMode:            indexOpts.InsertMode(), // FOLLOWUP(prateek): wire to allow this to be tweaked at runtime
				flushBlockNumSegments: runtime.DefaultFlushIndexBlockNumSegments,
			},
			blocksByTime: make(map[xtime.UnixNano]index.Block),
		},

		nowFn:           nowFn,
		blockSize:       nsMD.Options().IndexOptions().BlockSize(),
		retentionPeriod: nsMD.Options().RetentionOptions().RetentionPeriod(),
		bufferPast:      nsMD.Options().RetentionOptions().BufferPast(),
		bufferFuture:    nsMD.Options().RetentionOptions().BufferFuture(),

		indexFilesetsBeforeFn: fs.IndexFileSetsBefore,
		deleteFilesFn:         fs.DeleteFiles,

		newBlockFn:       newBlockFn,
		opts:             newIndexOpts.opts,
		logger:           indexOpts.InstrumentOptions().Logger(),
		nsMetadata:       nsMD,
		resultsPool:      indexOpts.ResultsPool(),
		queryWorkersPool: newIndexOpts.opts.QueryIDsWorkerPool(),

		metrics: newNamespaceIndexMetrics(indexOpts, instrumentOpts),
	}
	if runtimeOptsMgr != nil {
		idx.runtimeOptsListener = runtimeOptsMgr.RegisterListener(idx)
	}

	// allocate indexing queue and start it up.
	queue := newIndexQueueFn(idx.writeBatches, nowFn, scope)
	if err := queue.Start(); err != nil {
		return nil, err
	}
	idx.state.insertQueue = queue

	// allocate the current block to ensure we're able to index as soon as we return
	currentBlock := nowFn().Truncate(idx.blockSize)
	idx.state.RLock()
	_, err := idx.ensureBlockPresentWithRLock(currentBlock)
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
	i.state.runtimeOpts.flushBlockNumSegments = value.FlushIndexBlockNumSegments()
	i.state.Unlock()
}

func (i *nsIndex) reportStatsUntilClosed() {
	ticker := time.NewTicker(nsIndexReportStatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := i.reportStats()
			if err != nil {
				i.logger.Warnf("could not report index stats: %v", err)
			}
		case <-i.state.closeCh:
			return
		}
	}
}

func (i *nsIndex) reportStats() error {
	i.state.RLock()
	defer i.state.RUnlock()

	levels := i.metrics.BlockMetrics.ActiveSegmentLevelsMetrics
	levelValues := make([]struct {
		numSegments  int64
		numTotalDocs int64
	}, len(levels))

	// iterate known blocks in a defined order of time (newest first)
	// for debug log ordering
	for _, start := range i.state.blockStartsDescOrder {
		block, ok := i.state.blocksByTime[start]
		if !ok {
			return i.missingBlockInvariantError(start)
		}

		err := block.Stats(
			index.BlockStatsReporterFn(func(s index.BlockSegmentStats) {
				for i, l := range levels {
					contained := s.Size >= l.MinSizeInclusive && s.Size < l.MaxSizeExclusive
					if !contained {
						continue
					}

					levelValues[i].numSegments++
					levelValues[i].numTotalDocs += s.Size
					l.SegmentsAge.Record(s.Age)
					break
				}
			}))
		if err != nil {
			return err
		}
	}

	for i, v := range levelValues {
		levels[i].NumSegments.Update(float64(v.numSegments))
		levels[i].NumTotalDocs.Update(float64(v.numTotalDocs))
	}

	return nil
}

func (i *nsIndex) BlockStartForWriteTime(writeTime time.Time) xtime.UnixNano {
	return xtime.ToUnixNano(writeTime.Truncate(i.blockSize))
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
		i.metrics.InsertAfterClose.Inc(1)
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
			// Restore the sort order from whene enqueued for the caller
			batch.SortByEnqueued()
			return fmt.Errorf("check batch: %d insert errors", numErrs)
		}
	}

	return nil
}

// WriteBatches is called by the indexInsertQueue.
func (i *nsIndex) writeBatches(
	batches []*index.WriteBatch,
) {
	// NB(prateek): we use a read lock to guard against mutation of the
	// indexBlocks, mutations within the underlying blocks are guarded
	// by primitives internal to it.
	i.state.RLock()
	defer i.state.RUnlock()
	if !i.isOpenWithRLock() {
		// NB(prateek): deliberately skip calling any of the `OnIndexFinalize` methods
		// on the provided inserts to terminate quicker during shutdown.
		return
	}

	now := i.nowFn()
	futureLimit := now.Add(1 * i.bufferFuture)
	pastLimit := now.Add(-1 * i.bufferPast)
	writeBatchFn := i.writeBatchForBlockStartWithRLock
	for _, batch := range batches {
		// Ensure timestamp is not too old/new based on retention policies and that
		// doc is valid.
		batch.ForEach(func(idx int, entry index.WriteBatchEntry,
			d doc.Document, _ index.WriteBatchEntryResult) {
			if !futureLimit.After(entry.Timestamp) {
				batch.MarkUnmarkedEntryError(m3dberrors.ErrTooFuture, idx)
				return
			}

			if !entry.Timestamp.After(pastLimit) {
				batch.MarkUnmarkedEntryError(m3dberrors.ErrTooPast, idx)
				return
			}
		})

		// Sort the inserts by which block they're applicable for, and do the inserts
		// for each block, making sure to not try to insert any entries already marked
		// with a result.
		batch.ForEachUnmarkedBatchByBlockStart(writeBatchFn)
	}
}

func (i *nsIndex) writeBatchForBlockStartWithRLock(
	blockStart time.Time, batch *index.WriteBatch,
) {
	// ensure we have an index block for the specified blockStart.
	block, err := i.ensureBlockPresentWithRLock(blockStart)
	if err != nil {
		batch.MarkUnmarkedEntriesError(err)
		i.logger.WithFields(
			xlog.NewField("blockStart", blockStart),
			xlog.NewField("numWrites", batch.Len()),
			xlog.NewField("err", err.Error()),
		).Error("unable to write to index, dropping inserts.")
		i.metrics.AsyncInsertErrors.Inc(int64(batch.Len()))
		return
	}

	// NB(r): Capture pending entries so we can emit the latencies
	pending := batch.PendingEntries()

	// i.e. we have the block and the inserts, perform the writes.
	result, err := block.WriteBatch(batch)

	// record the end to end indexing latency
	now := i.nowFn()
	for idx := range pending {
		took := now.Sub(pending[idx].EnqueuedAt)
		i.metrics.InsertEndToEndLatency.Record(took)
	}

	// NB: we don't need to do anything to the OnIndexSeries refs in `inserts` at this point,
	// the index.Block WriteBatch assumes responsibility for calling the appropriate methods.
	if numErr := result.NumError; numErr != 0 {
		i.metrics.AsyncInsertErrors.Inc(numErr)
	}

	if err != nil {
		// NB: dropping duplicate id error messages from logs as they're expected when we see
		// repeated inserts. as long as a block has an ID, it's not an error so we don't need
		// to pollute the logs with these messages.
		if partialError, ok := err.(*m3ninxindex.BatchPartialError); ok {
			err = partialError.FilterDuplicateIDErrors()
		}
	}
	if err != nil {
		i.logger.Errorf("error writing to index block: %v", err)
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

func (i *nsIndex) Tick(c context.Cancellable, tickStart time.Time) (namespaceIndexTickResult, error) {
	var (
		result                     = namespaceIndexTickResult{}
		earliestBlockStartToRetain = retention.FlushTimeStartForRetentionPeriod(i.retentionPeriod, i.blockSize, tickStart)
		lastSealableBlockStart     = retention.FlushTimeEndForBlockSize(i.blockSize, tickStart.Add(-i.bufferPast))
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
		blockTickResult, tickErr := block.Tick(c, tickStart)
		multiErr = multiErr.Add(tickErr)
		result.NumSegments += blockTickResult.NumSegments
		result.NumTotalDocs += blockTickResult.NumDocs

		// seal any blocks that are sealable
		if !blockStart.ToTime().After(lastSealableBlockStart) && !block.IsSealed() {
			multiErr = multiErr.Add(block.Seal())
			result.NumBlocksSealed++
		}
	}

	return result, multiErr.FinalError()
}

func (i *nsIndex) Flush(
	flush persist.IndexFlush,
	shards []databaseShard,
) error {
	flushable, err := i.flushableBlocks(shards)
	if err != nil {
		return err
	}

	var evictResults index.EvictMutableSegmentResults
	for _, block := range flushable {
		immutableSegments, err := i.flushBlock(flush, block, shards)
		if err != nil {
			return err
		}
		// Make a result that covers the entire time ranges for the
		// block for each shard
		fulfilled := result.NewShardTimeRanges(block.StartTime(), block.EndTime(),
			dbShards(shards).IDs()...)
		// Add the results to the block
		results := result.NewIndexBlock(block.StartTime(), immutableSegments,
			fulfilled)
		if err := block.AddResults(results); err != nil {
			return err
		}
		// It's now safe to remove the mutable segments as anything the block
		// held is covered by the owned shards we just read
		evictResult, err := block.EvictMutableSegments()
		evictResults.Add(evictResult)
		if err != nil {
			// deliberately choosing to not mark this as an error as we have successfully
			// flushed any mutable data.
			i.logger.WithFields(
				xlog.NewField("err", err.Error()),
				xlog.NewField("blockStart", block.StartTime()),
			).Warnf("encountered error while evicting mutable segments for index block")
		}
	}
	i.metrics.FlushEvictedMutableSegments.Inc(evictResults.NumMutableSegments)
	return nil
}

func (i *nsIndex) flushableBlocks(
	shards []databaseShard,
) ([]index.Block, error) {
	i.state.RLock()
	defer i.state.RUnlock()
	if !i.isOpenWithRLock() {
		return nil, errDbIndexUnableToFlushClosed
	}
	flushable := make([]index.Block, 0, len(i.state.blocksByTime))
	for _, block := range i.state.blocksByTime {
		if !i.canFlushBlock(block, shards) {
			continue
		}
		flushable = append(flushable, block)
	}
	return flushable, nil
}

func (i *nsIndex) canFlushBlock(
	block index.Block,
	shards []databaseShard,
) bool {
	// Check the block needs flushing because it is sealed and has
	// any mutable segments that need to be evicted from memory
	if !block.IsSealed() || !block.NeedsMutableSegmentsEvicted() {
		return false
	}

	// Check all data files exist for the shards we own
	for _, shard := range shards {
		start := block.StartTime()
		dataBlockSize := i.nsMetadata.Options().RetentionOptions().BlockSize()
		for t := start; t.Before(block.EndTime()); t = t.Add(dataBlockSize) {
			if shard.FlushState(t).Status != fileOpSuccess {
				return false
			}
		}
	}

	return true
}

func (i *nsIndex) flushBlock(
	flush persist.IndexFlush,
	indexBlock index.Block,
	shards []databaseShard,
) ([]segment.Segment, error) {
	i.state.RLock()
	numSegments := i.state.runtimeOpts.flushBlockNumSegments
	i.state.RUnlock()

	allShards := make(map[uint32]struct{})
	segmentShards := make([][]databaseShard, numSegments)
	for i, shard := range shards {
		// Populate all shards
		allShards[shard.ID()] = struct{}{}

		// Populate segment shards
		idx := i % int(numSegments)
		segmentShards[idx] = append(segmentShards[idx], shard)
	}

	preparedPersist, err := flush.PrepareIndex(persist.IndexPrepareOptions{
		NamespaceMetadata: i.nsMetadata,
		BlockStart:        indexBlock.StartTime(),
		FileSetType:       persist.FileSetFlushType,
		Shards:            allShards,
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

	for _, shards := range segmentShards {
		if len(shards) == 0 {
			// This can happen if fewer shards than num segments we'd like
			continue
		}

		// Flush a single block segment
		err := i.flushBlockSegment(preparedPersist, indexBlock, shards)
		if err != nil {
			return nil, err
		}
	}

	closed = true

	// Now return the immutable segments
	return preparedPersist.Close()
}

func (i *nsIndex) flushBlockSegment(
	preparedPersist persist.PreparedIndexPersist,
	indexBlock index.Block,
	shards []databaseShard,
) error {
	// FOLLOWUP(prateek): use this to track segments when we have multiple segments in a Block.
	postingsOffset := postings.ID(0)
	seg, err := mem.NewSegment(postingsOffset, i.opts.IndexOptions().MemSegmentOptions())
	if err != nil {
		return err
	}
	defer seg.Close()

	ctx := context.NewContext()
	for _, shard := range shards {
		var (
			first     = true
			pageToken PageToken
		)
		for first || pageToken != nil {
			first = false

			var (
				opts    = block.FetchBlocksMetadataOptions{}
				limit   = defaultFlushReadDataBlocksBatchSize
				results block.FetchBlocksMetadataResults
			)
			ctx.Reset()
			results, pageToken, err = shard.FetchBlocksMetadataV2(ctx,
				indexBlock.StartTime(), indexBlock.EndTime(),
				limit, pageToken, opts)
			if err != nil {
				return err
			}

			for _, result := range results.Results() {
				id := result.ID.Bytes()
				exists, err := seg.ContainsID(id)
				if err != nil {
					return err
				}
				if exists {
					continue
				}

				doc, err := convert.FromMetricIter(result.ID, result.Tags)
				if err != nil {
					return err
				}

				if _, err := seg.Insert(doc); err != nil {
					return err
				}
			}

			results.Close()
			ctx.BlockingClose()
		}
	}

	if _, err := seg.Seal(); err != nil {
		return err
	}

	// Finally flush this segment
	return preparedPersist.Persist(seg)
}

func (i *nsIndex) Query(
	ctx context.Context,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResults, error) {
	// Capture start before needing to acquire lock.
	start := i.nowFn()

	i.state.RLock()
	if !i.isOpenWithRLock() {
		i.state.RUnlock()
		return index.QueryResults{}, errDbIndexUnableToQueryClosed
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
		return index.QueryResults{}, err
	}

	var (
		deadline = start.Add(timeout)
		wg       sync.WaitGroup

		// Results contains all concurrent mutable state below.
		results = struct {
			sync.Mutex
			multiErr   xerrors.MultiError
			merged     index.Results
			exhaustive bool
			returned   bool
		}{
			merged:     nil,
			exhaustive: true,
			returned:   false,
		}
	)
	defer func() {
		// Ensure that during early error returns we let any aborted
		// goroutines know not to try to modify/edit the result any longer.
		results.returned = true
	}()

	execBlockQuery := func(block index.Block) {
		blockResults := i.resultsPool.Get()
		blockResults.Reset(i.nsMetadata.ID())

		blockExhaustive, err := block.Query(query, opts, blockResults)
		if err != index.ErrUnableToQueryBlockClosed {
			// NB(r): Because we query this block outside of the results lock, it's
			// possible this block may get closed if it slides out of retention, in
			// that case those results are no longer considered valid and outside of
			// retention regardless, so this is a non-issue.
			err = nil
		}

		var mergedResult bool
		results.Lock()
		defer func() {
			results.Unlock()
			if mergedResult {
				// Only finalize this result if we merged it into another.
				blockResults.Finalize()
			}
		}()

		if results.returned {
			// If already returned then we early cancelled, don't add any
			// further results or errors since caller already has a result.
			return
		}

		if err != nil {
			results.multiErr = results.multiErr.Add(err)
			return
		}

		if results.merged == nil {
			// Return results to pool at end of request.
			ctx.RegisterFinalizer(blockResults)
			// No merged results yet, use this as the first to merge into.
			results.merged = blockResults
		} else {
			// Append the block results.
			mergedResult = true
			size := results.merged.Size()
			for _, entry := range blockResults.Map().Iter() {
				// Break early if reached limit.
				if opts.Limit > 0 && size >= opts.Limit {
					blockExhaustive = false
					break
				}

				// Append to merged results.
				id, tags := entry.Key(), entry.Value()
				_, size, err = results.merged.AddIDAndTags(id, tags)
				if err != nil {
					results.multiErr = results.multiErr.Add(err)
					return
				}
			}
		}

		// If block had more data but we stopped early, need to notify caller.
		if blockExhaustive {
			return
		}
		results.exhaustive = false
	}

	for _, block := range blocks {
		// Capture block for async query execution below.
		block := block

		// Terminate early if we know we don't need any more results.
		results.Lock()
		mergedSize := 0
		if results.merged != nil {
			mergedSize = results.merged.Size()
		}
		alreadyNotExhaustive := opts.Limit > 0 && mergedSize >= opts.Limit
		if alreadyNotExhaustive {
			results.exhaustive = false
		}
		results.Unlock()

		if alreadyNotExhaustive {
			// Break out if already exhaustive.
			break
		}

		if applyTimeout := timeout > 0; !applyTimeout {
			// No timeout, just wait blockingly for a worker.
			wg.Add(1)
			i.queryWorkersPool.Go(func() {
				execBlockQuery(block)
				wg.Done()
			})
			continue
		}

		// Need to apply timeout to the blocking wait call for a worker.
		var timedOut bool
		if timeLeft := deadline.Sub(i.nowFn()); timeLeft > 0 {
			wg.Add(1)
			timedOut := !i.queryWorkersPool.GoWithTimeout(func() {
				execBlockQuery(block)
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
			return index.QueryResults{}, fmt.Errorf("index query timed out: %s", timeout.String())
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
			return index.QueryResults{}, fmt.Errorf("index query timed out: %s", timeout.String())
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
			return index.QueryResults{}, fmt.Errorf("index query timed out: %s", timeout.String())
		}
	}

	results.Lock()
	// Signal not to add any further results since we've returned already.
	results.returned = true
	// Take reference to vars to return while locked, need to allow defer
	// lock/unlock cleanup to not deadlock with this locked code block.
	exhaustive := results.exhaustive
	mergedResults := results.merged
	errResults := results.multiErr.FinalError()
	results.Unlock()

	if errResults != nil {
		return index.QueryResults{}, err
	}

	// If no blocks queried, return an empty result
	if mergedResults == nil {
		mergedResults = i.resultsPool.Get()
		mergedResults.Reset(i.nsMetadata.ID())
	}

	return index.QueryResults{
		Exhaustive: exhaustive,
		Results:    mergedResults,
	}, nil
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
	// Override query response limit if needed.
	if i.state.runtimeOpts.maxQueryLimit > 0 && (opts.Limit == 0 ||
		int64(opts.Limit) > i.state.runtimeOpts.maxQueryLimit) {
		i.logger.Debugf("overriding query response limit, requested: %d, max-allowed: %d",
			opts.Limit, i.state.runtimeOpts.maxQueryLimit) // FOLLOWUP(prateek): log query too once it's serializable.
		opts.Limit = int(i.state.runtimeOpts.maxQueryLimit)
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
		queryRange = queryRange.RemoveRange(blockRange)

		blocks = append(blocks, block)
	}

	return blocks, nil
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
	block, err := i.newBlockFn(blockStart, i.nsMetadata, i.opts.IndexOptions())
	if err != nil { // unable to allocate the block, should never happen.
		return nil, i.unableToAllocBlockInvariantError(err)
	}

	// add to tracked blocks map
	i.state.blocksByTime[blockStartNanos] = block

	// update ordered blockStarts slice, and latestBlock
	i.updateBlockStartsWithLock()
	return block, nil
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
	instrument.EmitAndLogInvariantViolation(i.opts.InstrumentOptions(), func(l xlog.Logger) {
		l.Errorf(err.Error())
	})
	return err
}

func (i *nsIndex) unableToAllocBlockInvariantError(err error) error {
	ierr := fmt.Errorf("index unable to allocate block: %v", err)
	instrument.EmitAndLogInvariantViolation(i.opts.InstrumentOptions(), func(l xlog.Logger) {
		l.Errorf(ierr.Error())
	})
	return ierr
}

type nsIndexMetrics struct {
	AsyncInsertErrors           tally.Counter
	InsertAfterClose            tally.Counter
	QueryAfterClose             tally.Counter
	InsertEndToEndLatency       tally.Timer
	FlushEvictedMutableSegments tally.Counter
	BlockMetrics                nsIndexBlocksMetrics
}

func newNamespaceIndexMetrics(
	opts index.Options,
	iopts instrument.Options,
) nsIndexMetrics {
	scope := iopts.MetricsScope()
	blocksScope := scope.SubScope("blocks")
	return nsIndexMetrics{
		AsyncInsertErrors: scope.Tagged(map[string]string{
			"error_type": "async-insert",
		}).Counter("index-error"),
		InsertAfterClose: scope.Tagged(map[string]string{
			"error_type": "insert-closed",
		}).Counter("insert-after-close"),
		QueryAfterClose: scope.Tagged(map[string]string{
			"error_type": "query-closed",
		}).Counter("query-after-error"),
		InsertEndToEndLatency: instrument.MustCreateSampledTimer(
			scope.Timer("insert-end-to-end-latency"),
			iopts.MetricsSamplingRate()),
		FlushEvictedMutableSegments: scope.Counter("mutable-segment-evicted"),
		BlockMetrics:                newNamespaceIndexBlocksMetrics(opts, blocksScope),
	}
}

type nsIndexBlocksMetrics struct {
	ActiveSegmentLevelsMetrics []nsIndexBlocksSegmentLevelMetrics
}

type nsIndexBlocksSegmentLevelMetrics struct {
	MinSizeInclusive int64
	MaxSizeExclusive int64
	NumSegments      tally.Gauge
	NumTotalDocs     tally.Gauge
	SegmentsAge      tally.Timer
}

func newNamespaceIndexBlocksMetrics(
	opts index.Options,
	scope tally.Scope,
) nsIndexBlocksMetrics {
	segmentLevelsScope := scope.SubScope("segment-levels")

	compactionLevels := opts.CompactionPlannerOptions().Levels
	levels := make([]nsIndexBlocksSegmentLevelMetrics, 0, len(compactionLevels))
	for _, level := range compactionLevels {
		subScope := segmentLevelsScope.Tagged(map[string]string{
			"level_min_size": strconv.Itoa(int(level.MinSizeInclusive)),
			"level_max_size": strconv.Itoa(int(level.MaxSizeExclusive)),
		})
		levels = append(levels, nsIndexBlocksSegmentLevelMetrics{
			MinSizeInclusive: level.MinSizeInclusive,
			MaxSizeExclusive: level.MaxSizeExclusive,
			NumSegments:      subScope.Gauge("num-segments"),
			NumTotalDocs:     subScope.Gauge("num-total-docs"),
			SegmentsAge:      subScope.Timer("segments-age"),
		})
	}

	return nsIndexBlocksMetrics{
		ActiveSegmentLevelsMetrics: levels,
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
