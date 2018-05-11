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
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	errDbIndexAlreadyClosed               = errors.New("database index has already been closed")
	errDbIndexUnableToWriteClosed         = errors.New("unable to write to database index, already closed")
	errDbIndexUnableToQueryClosed         = errors.New("unable to query database index, already closed")
	errDbIndexTerminatingTickCancellation = errors.New("terminating tick early due to cancellation")
	errDbIndexIsBootstrapping             = errors.New("index is already bootstrapping")
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

	newBlockFn newBlockFn
	logger     xlog.Logger
	opts       index.Options
	metrics    nsIndexMetrics
	nsMetadata namespace.Metadata
}

type nsIndexState struct {
	sync.RWMutex // NB: guards all variables in this struct

	closed         bool
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
	insertMode    index.InsertMode
	maxQueryLimit int64
}

type newBlockFn func(time.Time, time.Duration, index.Options) (index.Block, error)

// newNamespaceIndex returns a new namespaceIndex for the provided namespace.
func newNamespaceIndex(
	nsMD namespace.Metadata,
	opts index.Options,
) (namespaceIndex, error) {
	return newNamespaceIndexWithOptions(newNamespaceIndexOpts{
		md:              nsMD,
		indexOpts:       opts,
		newIndexQueueFn: newNamespaceIndexInsertQueue,
		newBlockFn:      index.NewBlock,
	})
}

// newNamespaceIndexWithInsertQueueFn is a ctor used in tests to override the insert queue.
func newNamespaceIndexWithInsertQueueFn(
	nsMD namespace.Metadata,
	newIndexQueueFn newNamespaceIndexInsertQueueFn,
	opts index.Options,
) (namespaceIndex, error) {
	return newNamespaceIndexWithOptions(newNamespaceIndexOpts{
		md:              nsMD,
		indexOpts:       opts,
		newIndexQueueFn: newIndexQueueFn,
		newBlockFn:      index.NewBlock,
	})
}

// newNamespaceIndexWithNewBlockFn is a ctor used in tests to inject blocks.
func newNamespaceIndexWithNewBlockFn(
	nsMD namespace.Metadata,
	newBlockFn newBlockFn,
	opts index.Options,
) (namespaceIndex, error) {
	return newNamespaceIndexWithOptions(newNamespaceIndexOpts{
		md:              nsMD,
		indexOpts:       opts,
		newIndexQueueFn: newNamespaceIndexInsertQueue,
		newBlockFn:      newBlockFn,
	})
}

type newNamespaceIndexOpts struct {
	md              namespace.Metadata
	indexOpts       index.Options
	newIndexQueueFn newNamespaceIndexInsertQueueFn
	newBlockFn      newBlockFn
}

// newNamespaceIndexWithOptions returns a new namespaceIndex with the provided configuration options.
func newNamespaceIndexWithOptions(
	newIndexOpts newNamespaceIndexOpts,
) (namespaceIndex, error) {
	var (
		nsMD            = newIndexOpts.md
		opts            = newIndexOpts.indexOpts
		newIndexQueueFn = newIndexOpts.newIndexQueueFn
		newBlockFn      = newIndexOpts.newBlockFn
	)
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	nowFn := opts.ClockOptions().NowFn()
	idx := &nsIndex{
		state: nsIndexState{
			runtimeOpts: nsIndexRuntimeOptions{
				insertMode: opts.InsertMode(), // FOLLOWUP(prateek): wire to allow this to be tweaked at runtime
			},
			blocksByTime: make(map[xtime.UnixNano]index.Block),
		},

		nowFn:           nowFn,
		blockSize:       nsMD.Options().IndexOptions().BlockSize(),
		retentionPeriod: nsMD.Options().RetentionOptions().RetentionPeriod(),
		bufferPast:      nsMD.Options().RetentionOptions().BufferPast(),
		bufferFuture:    nsMD.Options().RetentionOptions().BufferFuture(),

		newBlockFn: newBlockFn,
		opts:       opts,
		logger:     opts.InstrumentOptions().Logger(),
		metrics:    newNamespaceIndexMetrics(opts.InstrumentOptions().MetricsScope()),
		nsMetadata: nsMD,
	}

	// allocate indexing queue and start it up.
	queue := newIndexQueueFn(idx.writeBatches, nowFn, opts.InstrumentOptions().MetricsScope())
	if err := queue.Start(); err != nil {
		return nil, err
	}
	idx.state.insertQueue = queue

	// allocate the current block to ensure we're able to index as soon as we return
	currentBlock := nowFn().Truncate(idx.blockSize)
	idx.state.RLock()
	defer idx.state.RUnlock()
	if _, err := idx.ensureBlockPresentWithRLock(currentBlock); err != nil {
		return nil, err
	}

	return idx, nil
}

// NB(prateek): including the call chains leading to this point:
//
// - For new entry (previously unseen in the shard):
//     shard.WriteTagged()
//       => shardInsertQueue.Insert()
//       => shard.writeBatch()
//       => index.Write()
//       => indexQueue.Insert()
//       => index.writeBatch()
//
// - For entry which exists in the shard, but needs indexing (either past
//   the TTL or the last indexing hasn't happened/failed):
//      shard.WriteTagged()
//        => index.Write()
//        => indexQueue.Insert()
//      	=> index.writeBatch()

func (i *nsIndex) WriteBatch(
	entries []index.WriteBatchEntry,
) error {
	// Ensure timestamp is not too old/new based on retention policies.
	now := i.nowFn()
	futureLimit := now.Add(1 * i.bufferFuture)
	pastLimit := now.Add(-1 * i.bufferPast)

	var emptyEntry index.WriteBatchEntry
	for j := range entries {
		var (
			timestamp = entries[j].Timestamp
			onIndexFn = entries[j].OnIndexSeries
		)
		if !futureLimit.After(timestamp) {
			onIndexFn.OnIndexFinalize()
			entries[j] = emptyEntry // indicate we don't need to index this.
			// TODO(prateek): capture that this needs to return m3dberrors.ErrTooFuture
			continue
		}

		if !pastLimit.Before(timestamp) {
			onIndexFn.OnIndexFinalize()
			entries[j] = emptyEntry // indicate we don't need to index this.
			// TODO(prateek): capture that this needs to return m3dberrors.ErrTooPast
			continue
		}

		// update the timestamp to the blockstart for the block it needs to be sent to
		entries[j].Timestamp = timestamp.Truncate(i.blockSize)
	}
	return i.enqueueBatch(entries)
}

func (i *nsIndex) enqueueBatch(
	entries []index.WriteBatchEntry,
) error {
	i.state.RLock()
	if !i.isOpenWithRLock() {
		i.state.RUnlock()
		i.metrics.InsertAfterClose.Inc(1)
		index.WriteBatchEntriesFinalizer(entries).Finalize()
		return errDbIndexUnableToWriteClosed
	}

	// NB(prateek): retrieving insertMode here while we have the RLock.
	insertMode := i.state.runtimeOpts.insertMode
	wg, err := i.state.insertQueue.InsertBatch(entries)

	// release the lock because we don't need it past this point.
	i.state.RUnlock()

	// if we're unable to index, we still have to finalize the reference we hold.
	if err != nil {
		index.WriteBatchEntriesFinalizer(entries).Finalize()
		return err
	}
	// once the write has been queued in the indexInsertQueue, it assumes
	// responsibility for calling the resource hooks.

	// wait/terminate depending on if we are indexing synchronously or not.
	if insertMode != index.InsertAsync {
		// FOLLOWUP(prateek): to correctly propagate indexing error to the user in
		// the sync case, we need a mechanism to receive notifications from
		// the index insert queue path. Some ways to do this:
		// (0) alloc a slice for the errors in queue, and return a wait group
		// the slice and an index into the slice.
		// (1) provide an error channel as input to i.insertQueue.Insert, and
		// guarantee that receives a value on success/failure in the async
		// insertion code path. We could eliminate the wait group if we did that.
		// (2) we could provide an OnIndexError callback within OnIndexSeries,
		// again ensuring it was called upon failure, and cache the error within
		// the dbShardEntry. Then we're guaranteed that the error would be set
		// once wg.Wait returns, and the shard insert code-paths would be able
		// to retrieve the error from the dbShardEntry. Not pretty, but probably
		// a lot cheaper than (1).
		wg.Wait()
	}

	return nil
}

// WriteBatches is called by the indexInsertQueue.
// FOLLOWUP(prateek): propagate error back up from here to the indexInsertQueue
// so that we can notify users of success/failure correctly in the case of
// sync'd inserts.
func (i *nsIndex) writeBatches(
	batches [][]index.WriteBatchEntry,
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

	for _, batch := range batches {
		// we sort the inserts by which block they're applicable for, and do the inserts
		// for each block.
		writesByBlockStart := index.WriteBatchEntryByBlockStartAndID(batch)
		sort.Sort(writesByBlockStart)
		writesByBlockStart.ForEachBlockStart(i.writeBatchForBlockStartWithRLock)
	}
}

func (i *nsIndex) writeBatchForBlockStartWithRLock(
	blockStart time.Time, inserts index.WriteBatchEntryByBlockStartAndID,
) {
	// ensure we have an index block for the specified blockStart.
	block, err := i.ensureBlockPresentWithRLock(blockStart)
	if err != nil {
		index.WriteBatchEntriesFinalizer(inserts).Finalize()
		i.logger.WithFields(
			xlog.NewField("blockStart", blockStart),
			xlog.NewField("numWrites", len(inserts)),
			xlog.NewField("err", err.Error()),
		).Error("unable to write to index, dropping inserts.")
		i.metrics.AsyncInsertErrors.Inc(int64(len(inserts)))
		return
	}

	// i.e. we have the block and the inserts, perform the writes.
	result, err := block.WriteBatch(inserts)

	// NB: we don't need to do anything to the OnIndexSeries refs in `inserts` at this point,
	// the index.Block WriteBatch assumes responsibility for calling the appropriate methods.
	if numErr := result.NumError; numErr != 0 {
		i.metrics.AsyncInsertErrors.Inc(numErr)
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
	for blockStart, block := range bootstrapResults {
		segments := block.Segments()
		block, err := i.ensureBlockPresentWithRLock(blockStart.ToTime())
		if err != nil { // should never happen
			multiErr = multiErr.Add(i.unableToAllocBlockInvariantError(err))
			continue
		}
		if err := block.Bootstrap(segments); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (i *nsIndex) Tick(c context.Cancellable) (namespaceIndexTickResult, error) {
	var (
		result                     = namespaceIndexTickResult{}
		now                        = i.nowFn()
		earliestBlockStartToRetain = retention.FlushTimeStartForRetentionPeriod(i.retentionPeriod, i.blockSize, now)
		lastSealableBlockStart     = retention.FlushTimeEndForBlockSize(i.blockSize, now.Add(-i.bufferPast))
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
		result.NumTotalDocs += blockTickResult.NumDocs

		// seal any blocks that are sealable
		if !blockStart.ToTime().After(lastSealableBlockStart) && !block.IsSealed() {
			multiErr = multiErr.Add(block.Seal())
			result.NumBlocksSealed++
		}
	}

	return result, multiErr.FinalError()
}

func (i *nsIndex) Query(
	ctx context.Context,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResults, error) {
	i.state.RLock()
	defer i.state.RUnlock()
	if !i.isOpenWithRLock() {
		return index.QueryResults{}, errDbIndexUnableToQueryClosed
	}

	// override query response limit if needed.
	if i.state.runtimeOpts.maxQueryLimit > 0 && (opts.Limit == 0 ||
		int64(opts.Limit) > i.state.runtimeOpts.maxQueryLimit) {
		i.logger.Debugf("overriding query response limit, requested: %d, max-allowed: %d",
			opts.Limit, i.state.runtimeOpts.maxQueryLimit) // FOLLOWUP(prateek): log query too once it's serializable.
		opts.Limit = int(i.state.runtimeOpts.maxQueryLimit)
	}

	var (
		exhaustive = true
		results    = i.opts.ResultsPool().Get()
		err        error
	)
	results.Reset(i.nsMetadata.ID())
	ctx.RegisterFinalizer(results)

	// Chunk the query request into bounds based on applicable blocks and
	// execute the requests to each of them; and merge results.
	queryRange := xtime.Ranges{}.AddRange(xtime.Range{
		Start: opts.StartInclusive, End: opts.EndExclusive})

	// iterate known blocks in a defined order of time (newest first) to enforce
	// some determinism about the results returned.
	for _, start := range i.state.blockStartsDescOrder {
		block, ok := i.state.blocksByTime[start]
		if !ok { // should never happen
			return index.QueryResults{}, i.missingBlockInvariantError(start)
		}

		// ensure the block has data requested by the query
		blockRange := xtime.Range{Start: block.StartTime(), End: block.EndTime()}
		if !queryRange.Overlaps(blockRange) {
			continue
		}

		// terminate early if we know we don't need any more results
		if opts.Limit > 0 && results.Size() >= opts.Limit {
			exhaustive = false
			break
		}

		exhaustive, err = block.Query(query, opts, results)
		if err != nil {
			return index.QueryResults{}, err
		}

		if !exhaustive {
			// i.e. block had more data but we stopped early, we know
			// we have hit the limit and don't need to query any more.
			break
		}

		// terminate if queryRange doesn't need any more data
		queryRange = queryRange.RemoveRange(blockRange)
		if queryRange.IsEmpty() {
			break
		}
	}

	// FOLLOWUP(prateek): do the above operation with controllable parallelism to optimize
	// for latency at the cost of higher mem-usage.

	return index.QueryResults{
		Exhaustive: exhaustive,
		Results:    results,
	}, nil
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
	block, err := i.newBlockFn(blockStart, i.blockSize, i.opts)
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

func (i *nsIndex) Close() error {
	i.state.Lock()
	defer i.state.Unlock()
	if !i.isOpenWithRLock() {
		return errDbIndexAlreadyClosed
	}
	i.state.closed = true

	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(i.state.insertQueue.Stop())

	for t := range i.state.blocksByTime {
		blk := i.state.blocksByTime[t]
		multiErr = multiErr.Add(blk.Close())
	}

	i.state.latestBlock = nil
	i.state.blocksByTime = nil
	i.state.blockStartsDescOrder = nil

	return multiErr.FinalError()
}

func (i *nsIndex) missingBlockInvariantError(t xtime.UnixNano) error {
	err := fmt.Errorf("index query did not find block %d despite seeing it in slice", t)
	instrument.EmitInvariantViolationAndGetLogger(i.opts.InstrumentOptions()).Errorf(err.Error())
	return err
}

func (i *nsIndex) unableToAllocBlockInvariantError(err error) error {
	ierr := fmt.Errorf("index unable to allocate block: %v", err)
	instrument.EmitInvariantViolationAndGetLogger(i.opts.InstrumentOptions()).Errorf(ierr.Error())
	return ierr
}

type nsIndexMetrics struct {
	AsyncInsertErrors tally.Counter
	InsertAfterClose  tally.Counter
	QueryAfterClose   tally.Counter
}

func newNamespaceIndexMetrics(scope tally.Scope) nsIndexMetrics {
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
	}
}
