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
	"container/list"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/generated/proto/pagetoken"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/repair"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/storage/series/lookup"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/m3ninx/doc"
	xclose "github.com/m3db/m3x/close"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/gogo/protobuf/proto"
	"github.com/uber-go/tally"
)

const (
	shardIterateBatchPercent = 0.01
	shardIterateBatchMinSize = 16
)

var (
	errShardEntryNotFound                  = errors.New("shard entry not found")
	errShardNotOpen                        = errors.New("shard is not open")
	errShardAlreadyTicking                 = errors.New("shard is already ticking")
	errShardClosingTickTerminated          = errors.New("shard is closing, terminating tick")
	errShardInvalidPageToken               = errors.New("shard could not unmarshal page token")
	errNewShardEntryTagsTypeInvalid        = errors.New("new shard entry options error: tags type invalid")
	errNewShardEntryTagsIterNotAtIndexZero = errors.New("new shard entry options error: tags iter not at index zero")
)

type filesetBeforeFn func(
	filePathPrefix string,
	namespace ident.ID,
	shardID uint32,
	t time.Time,
) ([]string, error)

type tickPolicy int

const (
	tickPolicyRegular tickPolicy = iota
	tickPolicyCloseShard
)

type dbShardState int

const (
	dbShardStateOpen dbShardState = iota
	dbShardStateClosing
)

type tagsArgType uint

const (
	// nolint: varcheck, unused
	tagsInvalidArg tagsArgType = iota
	tagsIterArg
	tagsArg
)

// tagsArgOptions is a union type that allows
// callers to pass either an ident.TagIterator or
// ident.Tags based on what access they have to
type tagsArgOptions struct {
	arg      tagsArgType
	tagsIter ident.TagIterator
	tags     ident.Tags
}

func newTagsIterArg(
	tagsIter ident.TagIterator,
) tagsArgOptions {
	return tagsArgOptions{
		arg:      tagsIterArg,
		tagsIter: tagsIter,
	}
}

func newTagsArg(
	tags ident.Tags,
) tagsArgOptions {
	return tagsArgOptions{
		arg:  tagsArg,
		tags: tags,
	}
}

type dbShard struct {
	sync.RWMutex
	block.DatabaseBlockRetriever
	opts                     Options
	seriesOpts               series.Options
	nowFn                    clock.NowFn
	state                    dbShardState
	namespace                namespace.Metadata
	seriesBlockRetriever     series.QueryableBlockRetriever
	seriesOnRetrieveBlock    block.OnRetrieveBlock
	namespaceReaderMgr       databaseNamespaceReaderManager
	increasingIndex          increasingIndex
	seriesPool               series.DatabaseSeriesPool
	reverseIndex             namespaceIndex
	insertQueue              *dbShardInsertQueue
	lookup                   *shardMap
	list                     *list.List
	bootstrapState           BootstrapState
	filesetBeforeFn          filesetBeforeFn
	deleteFilesFn            deleteFilesFn
	snapshotFilesFn          snapshotFilesFn
	sleepFn                  func(time.Duration)
	identifierPool           ident.Pool
	contextPool              context.Pool
	flushState               shardFlushState
	snapshotState            shardSnapshotState
	tickWg                   *sync.WaitGroup
	runtimeOptsListenClosers []xclose.SimpleCloser
	currRuntimeOptions       dbShardRuntimeOptions
	logger                   xlog.Logger
	metrics                  dbShardMetrics
	newSeriesBootstrapped    bool
	ticking                  bool
	shard                    uint32
}

// NB(r): dbShardRuntimeOptions does not contain its own
// mutex as some of the variables are needed each write
// which already at least acquires read lock from the shard
// mutex, so to keep the lock acquisitions to a minimum
// these are protected under the same shard mutex.
type dbShardRuntimeOptions struct {
	writeNewSeriesAsync      bool
	tickSleepSeriesBatchSize int
	tickSleepPerSeries       time.Duration
}

type dbShardMetrics struct {
	create                        tally.Counter
	close                         tally.Counter
	closeStart                    tally.Counter
	closeLatency                  tally.Timer
	insertAsyncInsertErrors       tally.Counter
	insertAsyncBootstrapErrors    tally.Counter
	insertAsyncWriteErrors        tally.Counter
	seriesBootstrapBlocksToBuffer tally.Counter
	seriesBootstrapBlocksMerged   tally.Counter
}

func newDatabaseShardMetrics(scope tally.Scope) dbShardMetrics {
	seriesBootstrapScope := scope.SubScope("series-bootstrap")
	return dbShardMetrics{
		create:       scope.Counter("create"),
		close:        scope.Counter("close"),
		closeStart:   scope.Counter("close-start"),
		closeLatency: scope.Timer("close-latency"),
		insertAsyncInsertErrors: scope.Tagged(map[string]string{
			"error_type": "insert-series",
		}).Counter("insert-async.errors"),
		insertAsyncBootstrapErrors: scope.Tagged(map[string]string{
			"error_type": "bootstrap-series",
		}).Counter("insert-async.errors"),
		insertAsyncWriteErrors: scope.Tagged(map[string]string{
			"error_type": "write-value",
		}).Counter("insert-async.errors"),
		seriesBootstrapBlocksToBuffer: seriesBootstrapScope.Counter("blocks-to-buffer"),
		seriesBootstrapBlocksMerged:   seriesBootstrapScope.Counter("blocks-merged"),
	}
}

type dbShardEntryWorkFn func(entry *lookup.Entry) bool

type dbShardEntryBatchWorkFn func(entries []*lookup.Entry) bool

type shardListElement *list.Element

type shardFlushState struct {
	sync.RWMutex
	statesByTime map[xtime.UnixNano]fileOpState
}

func newShardFlushState() shardFlushState {
	return shardFlushState{
		statesByTime: make(map[xtime.UnixNano]fileOpState),
	}
}

type shardSnapshotState struct {
	sync.RWMutex
	isSnapshotting         bool
	lastSuccessfulSnapshot time.Time
}

func newDatabaseShard(
	namespaceMetadata namespace.Metadata,
	shard uint32,
	blockRetriever block.DatabaseBlockRetriever,
	namespaceReaderMgr databaseNamespaceReaderManager,
	increasingIndex increasingIndex,
	reverseIndex namespaceIndex,
	needsBootstrap bool,
	opts Options,
	seriesOpts series.Options,
) databaseShard {
	scope := opts.InstrumentOptions().MetricsScope().
		SubScope("dbshard")

	s := &dbShard{
		opts:               opts,
		seriesOpts:         seriesOpts,
		nowFn:              opts.ClockOptions().NowFn(),
		state:              dbShardStateOpen,
		namespace:          namespaceMetadata,
		shard:              shard,
		namespaceReaderMgr: namespaceReaderMgr,
		increasingIndex:    increasingIndex,
		seriesPool:         opts.DatabaseSeriesPool(),
		reverseIndex:       reverseIndex,
		lookup:             newShardMap(shardMapOptions{}),
		list:               list.New(),
		filesetBeforeFn:    fs.DataFileSetsBefore,
		deleteFilesFn:      fs.DeleteFiles,
		snapshotFilesFn:    fs.SnapshotFiles,
		sleepFn:            time.Sleep,
		identifierPool:     opts.IdentifierPool(),
		contextPool:        opts.ContextPool(),
		flushState:         newShardFlushState(),
		tickWg:             &sync.WaitGroup{},
		logger:             opts.InstrumentOptions().Logger(),
		metrics:            newDatabaseShardMetrics(scope),
	}
	s.insertQueue = newDatabaseShardInsertQueue(s.insertSeriesBatch,
		s.nowFn, scope)

	registerRuntimeOptionsListener := func(listener runtime.OptionsListener) {
		elem := opts.RuntimeOptionsManager().RegisterListener(listener)
		s.runtimeOptsListenClosers = append(s.runtimeOptsListenClosers, elem)
	}
	registerRuntimeOptionsListener(s)
	registerRuntimeOptionsListener(s.insertQueue)

	// Start the insert queue after registering runtime options listeners
	// that may immediately fire with values
	s.insertQueue.Start()

	if !needsBootstrap {
		s.bootstrapState = Bootstrapped
		s.newSeriesBootstrapped = true
	}

	if blockRetriever != nil {
		s.setBlockRetriever(blockRetriever)
	}

	s.metrics.create.Inc(1)

	return s
}

func (s *dbShard) setBlockRetriever(retriever block.DatabaseBlockRetriever) {
	// If using the block retriever then set the block retriever field
	// and set the series block retriever as the shard itself and
	// the on retrieve block callback as the shard itself as well
	s.DatabaseBlockRetriever = retriever
	s.seriesBlockRetriever = s
	s.seriesOnRetrieveBlock = s
}

func (s *dbShard) SetRuntimeOptions(value runtime.Options) {
	s.Lock()
	s.currRuntimeOptions = dbShardRuntimeOptions{
		writeNewSeriesAsync:      value.WriteNewSeriesAsync(),
		tickSleepSeriesBatchSize: value.TickSeriesBatchSize(),
		tickSleepPerSeries:       value.TickPerSeriesSleepDuration(),
	}
	s.Unlock()
}

func (s *dbShard) ID() uint32 {
	return s.shard
}

func (s *dbShard) NumSeries() int64 {
	s.RLock()
	n := s.list.Len()
	s.RUnlock()
	return int64(n)
}

// Stream implements series.QueryableBlockRetriever
func (s *dbShard) Stream(
	ctx context.Context,
	id ident.ID,
	blockStart time.Time,
	onRetrieve block.OnRetrieveBlock,
) (xio.BlockReader, error) {
	return s.DatabaseBlockRetriever.Stream(ctx, s.shard, id, blockStart, onRetrieve)
}

// IsBlockRetrievable implements series.QueryableBlockRetriever
func (s *dbShard) IsBlockRetrievable(blockStart time.Time) bool {
	flushState := s.FlushState(blockStart)
	switch flushState.Status {
	case fileOpNotStarted, fileOpInProgress, fileOpFailed:
		return false
	case fileOpSuccess:
		return true
	}
	panic(fmt.Errorf("shard queried is retrievable with bad flush state %d",
		flushState.Status))
}

func (s *dbShard) OnRetrieveBlock(
	id ident.ID,
	tags ident.TagIterator,
	startTime time.Time,
	segment ts.Segment,
) {
	s.RLock()
	entry, _, err := s.lookupEntryWithLock(id)
	if entry != nil {
		entry.IncrementReaderWriterCount()
		defer entry.DecrementReaderWriterCount()
	}
	s.RUnlock()

	if err != nil && err != errShardEntryNotFound {
		return // Likely closing
	}

	if entry != nil {
		entry.Series.OnRetrieveBlock(id, tags, startTime, segment)
		return
	}

	entry, err = s.newShardEntry(id, newTagsIterArg(tags))
	if err != nil {
		// should never happen
		s.logger.WithFields(
			xlog.NewField("id", id.String()),
			xlog.NewField("startTime", startTime.String()),
		).Errorf("[invariant violated] unable to create shardEntry from retrieved block data")
		return
	}

	// NB(r): Do not need to specify that needs to be indexed as series would
	// have been already been indexed when it was written
	copiedID := entry.Series.ID()
	copiedTagsIter := s.identifierPool.TagsIterator()
	copiedTagsIter.Reset(entry.Series.Tags())
	s.insertQueue.Insert(dbShardInsert{
		entry: entry,
		opts: dbShardInsertAsyncOptions{
			hasPendingRetrievedBlock: true,
			pendingRetrievedBlock: dbShardPendingRetrievedBlock{
				id:      copiedID,
				tags:    copiedTagsIter,
				start:   startTime,
				segment: segment,
			},
		},
	})
}

func (s *dbShard) OnEvictedFromWiredList(id ident.ID, blockStart time.Time) {
	s.RLock()
	entry, _, err := s.lookupEntryWithLock(id)
	s.RUnlock()

	if err != nil && err != errShardEntryNotFound {
		return // Shard is probably closing
	}

	if entry == nil {
		// Its counter-intuitive that this can ever occur because the series should
		// always exist if it has any active blocks, and if we've reached this point
		// then the WiredList had a reference to a block that should still be in the
		// series, and thus the series should exist. The reason this can occur is that
		// even though the WiredList controls the lifecycle of blocks retrieved from
		// disk, those blocks can still be removed from the series if they've completely
		// fallen out of the retention period. In that case, the series tick will still
		// remove the block, and then the shard tick can remove the series. At that point,
		// it's possible for the WiredList to have a reference to an expired block for a
		// series that is no longer in the shard.
		return
	}

	entry.Series.OnEvictedFromWiredList(id, blockStart)
}

func (s *dbShard) forEachShardEntry(entryFn dbShardEntryWorkFn) error {
	return s.forEachShardEntryBatch(func(currEntries []*lookup.Entry) bool {
		for _, entry := range currEntries {
			if continueForEach := entryFn(entry); !continueForEach {
				return false
			}
		}
		return true
	})
}

func iterateBatchSize(elemsLen int) int {
	if elemsLen < shardIterateBatchMinSize {
		return elemsLen
	}
	t := math.Ceil(float64(shardIterateBatchPercent) * float64(elemsLen))
	return int(math.Max(shardIterateBatchMinSize, t))
}

func (s *dbShard) forEachShardEntryBatch(entriesBatchFn dbShardEntryBatchWorkFn) error {
	// NB(r): consider using a lockless list for ticking.
	s.RLock()
	elemsLen := s.list.Len()
	s.RUnlock()

	batchSize := iterateBatchSize(elemsLen)
	decRefElem := func(e *list.Element) {
		if e == nil {
			return
		}
		e.Value.(*lookup.Entry).DecrementReaderWriterCount()
	}

	var (
		currEntries = make([]*lookup.Entry, 0, batchSize)
		first       = true
		nextElem    *list.Element
	)
	for nextElem != nil || first {
		s.RLock()
		// NB(prateek): release held reference on the next element pointer now
		// that we have the read lock and are guaranteed it cannot be changed
		// from under us.
		decRefElem(nextElem)

		// lazily pull from the head of the list at first
		if first {
			nextElem = s.list.Front()
			first = false
		}

		elem := nextElem
		for ticked := 0; ticked < batchSize && elem != nil; ticked++ {
			nextElem = elem.Next()
			entry := elem.Value.(*lookup.Entry)
			entry.IncrementReaderWriterCount()
			currEntries = append(currEntries, entry)
			elem = nextElem
		}

		// NB(prateek): inc a reference to the next element while we have a lock,
		// to guarantee the element pointer cannot be changed from under us.
		if nextElem != nil {
			nextElem.Value.(*lookup.Entry).IncrementReaderWriterCount()
		}
		s.RUnlock()

		continueExecution := entriesBatchFn(currEntries)
		for i := range currEntries {
			currEntries[i].DecrementReaderWriterCount()
			currEntries[i] = nil
		}
		currEntries = currEntries[:0]
		if !continueExecution {
			decRefElem(nextElem)
			return nil
		}
	}

	return nil
}

func (s *dbShard) IsBootstrapped() bool {
	return s.BootstrapState() == Bootstrapped
}

func (s *dbShard) Close() error {
	s.Lock()
	if s.state != dbShardStateOpen {
		s.Unlock()
		return errShardNotOpen
	}
	s.state = dbShardStateClosing
	s.Unlock()

	s.insertQueue.Stop()

	for _, closer := range s.runtimeOptsListenClosers {
		closer.Close()
	}

	s.metrics.closeStart.Inc(1)
	stopwatch := s.metrics.closeLatency.Start()
	defer func() {
		s.metrics.close.Inc(1)
		stopwatch.Stop()
	}()

	// NB(prateek): wait till any existing ticks are finished. In the usual
	// case, no other ticks are running, and tickWg count is at 0, so the
	// call to Wait() will return immediately.
	// In the case when there is an existing Tick running, the count for
	// tickWg will be > 0, and we'll wait until it's reset to zero, which
	// will happen because earlier in this function we set the shard state
	// to dbShardStateClosing, which triggers an early termination of
	// any active ticks.
	s.tickWg.Wait()

	// NB(r): Asynchronously we purge expired series to ensure pressure on the
	// GC is not placed all at one time.  If the deadline is too low and still
	// causes the GC to impact performance when closing shards the deadline
	// should be increased.
	cancellable := context.NewNoOpCanncellable()
	_, err := s.tickAndExpire(cancellable, tickPolicyCloseShard)
	return err
}

func (s *dbShard) isClosing() bool {
	s.RLock()
	closing := s.isClosingWithLock()
	s.RUnlock()
	return closing
}

func (s *dbShard) isClosingWithLock() bool {
	return s.state == dbShardStateClosing
}

func (s *dbShard) Tick(c context.Cancellable, tickStart time.Time) (tickResult, error) {
	s.removeAnyFlushStatesTooEarly(tickStart)
	return s.tickAndExpire(c, tickPolicyRegular)
}

func (s *dbShard) tickAndExpire(
	c context.Cancellable,
	policy tickPolicy,
) (tickResult, error) {
	s.Lock()
	// ensure only one tick can execute at a time
	if s.ticking {
		s.Unlock()
		// i.e. we were previously ticking
		return tickResult{}, errShardAlreadyTicking
	}

	// NB(prateek): we bail out early if the shard is closing,
	// unless it's the final tick issued during the Close(). This
	// final tick is required to release resources back to our pools.
	if policy != tickPolicyCloseShard && s.isClosingWithLock() {
		s.Unlock()
		return tickResult{}, errShardClosingTickTerminated
	}

	// enable Close() to track the lifecycle of the tick
	s.ticking = true
	s.tickWg.Add(1)
	s.Unlock()

	// reset ticking state
	defer func() {
		s.Lock()
		s.ticking = false
		s.tickWg.Done()
		s.Unlock()
	}()

	var (
		r                             tickResult
		terminatedTickingDueToClosing bool
		i                             int
		slept                         time.Duration
		expired                       []*lookup.Entry
	)
	s.RLock()
	tickSleepBatch := s.currRuntimeOptions.tickSleepSeriesBatchSize
	tickSleepPerSeries := s.currRuntimeOptions.tickSleepPerSeries
	s.RUnlock()
	s.forEachShardEntryBatch(func(currEntries []*lookup.Entry) bool {
		// re-using `expired` to amortize allocs, still need to reset it
		// to be safe for re-use.
		for i := range expired {
			expired[i] = nil
		}
		expired = expired[:0]
		for _, entry := range currEntries {
			if i > 0 && i%tickSleepBatch == 0 {
				// NB(xichen): if the tick is cancelled, we bail out immediately.
				// The cancellation check is performed on every batch of entries
				// instead of every entry to reduce load.
				if c.IsCancelled() {
					return false
				}
				// NB(prateek): Also bail out early if the shard is closing,
				// unless it's the final tick issued during the Close(). This
				// final tick is required to release resources back to our pools.
				if policy != tickPolicyCloseShard && s.isClosing() {
					terminatedTickingDueToClosing = true
					return false
				}
				// Throttle the tick
				sleepFor := time.Duration(tickSleepBatch) * tickSleepPerSeries
				s.sleepFn(sleepFor)
				slept += sleepFor
			}

			var (
				result series.TickResult
				err    error
			)
			switch policy {
			case tickPolicyRegular:
				result, err = entry.Series.Tick()
			case tickPolicyCloseShard:
				err = series.ErrSeriesAllDatapointsExpired
			}
			if err == series.ErrSeriesAllDatapointsExpired {
				expired = append(expired, entry)
				r.expiredSeries++
			} else {
				r.activeSeries++
				if err != nil {
					r.errors++
				}
			}
			r.activeBlocks += result.ActiveBlocks
			r.openBlocks += result.OpenBlocks
			r.wiredBlocks += result.WiredBlocks
			r.unwiredBlocks += result.UnwiredBlocks
			r.pendingMergeBlocks += result.PendingMergeBlocks
			r.madeExpiredBlocks += result.MadeExpiredBlocks
			r.madeUnwiredBlocks += result.MadeUnwiredBlocks
			r.mergedOutOfOrderBlocks += result.MergedOutOfOrderBlocks
			i++
		}

		// Purge any series requiring purging.
		if len(expired) > 0 {
			s.purgeExpiredSeries(expired)
			for i := range expired {
				expired[i] = nil
			}
			expired = expired[:0]
		}
		// Continue
		return true
	})

	if terminatedTickingDueToClosing {
		return tickResult{}, errShardClosingTickTerminated
	}

	return r, nil
}

// NB(prateek): purgeExpiredSeries requires that all entries passed to it have at least one reader/writer,
// i.e. have a readWriteCount of at least 1.
// Currently, this function is only called by the lambda inside `tickAndExpire`'s `forEachShardEntryBatch`
// call. This satisfies the contract of all entries it operating upon being guaranteed to have a
// readerWriterEntryCount of at least 1, by virtue of the implementation of `forEachShardEntryBatch`.
func (s *dbShard) purgeExpiredSeries(expiredEntries []*lookup.Entry) {
	// Remove all expired series from lookup and list.
	s.Lock()
	for _, entry := range expiredEntries {
		series := entry.Series
		id := series.ID()
		elem, exists := s.lookup.Get(id)
		if !exists {
			continue
		}

		count := entry.ReaderWriterCount()
		// The contract requires all entries to have count >= 1.
		if count < 1 {
			s.logger.WithFields(
				xlog.NewField("series", series.ID().String()),
				xlog.NewField("ReaderWriterCount", count),
			).Errorf("observed series with invalid ReaderWriterCount in `purgeExpiredSeries`")
			continue
		}
		// If this series is currently being written to or read from, we don't
		// remove to ensure a consistent view of the series to other users.
		if count > 1 {
			continue
		}
		// If there have been datapoints written to the series since its
		// last empty check, we don't remove it.
		if !series.IsEmpty() {
			continue
		}
		// NB(xichen): if we get here, we are guaranteed that there can be
		// no more reads/writes to this series while the lock is held, so it's
		// safe to remove it.
		series.Close()
		s.list.Remove(elem)
		s.lookup.Delete(id)
	}
	s.Unlock()
}

func (s *dbShard) WriteTagged(
	ctx context.Context,
	id ident.ID,
	tags ident.TagIterator,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) (ts.Series, error) {
	return s.writeAndIndex(ctx, id, tags, timestamp,
		value, unit, annotation, true)
}

func (s *dbShard) Write(
	ctx context.Context,
	id ident.ID,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) (ts.Series, error) {
	return s.writeAndIndex(ctx, id, ident.EmptyTagIterator, timestamp,
		value, unit, annotation, false)
}

func (s *dbShard) writeAndIndex(
	ctx context.Context,
	id ident.ID,
	tags ident.TagIterator,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
	shouldReverseIndex bool,
) (ts.Series, error) {
	// Prepare write
	entry, opts, err := s.tryRetrieveWritableSeries(id)
	if err != nil {
		return ts.Series{}, err
	}

	writable := entry != nil

	// If no entry and we are not writing new series asynchronously
	if !writable && !opts.writeNewSeriesAsync {
		// Avoid double lookup by enqueueing insert immediately
		result, err := s.insertSeriesAsyncBatched(id, tags, dbShardInsertAsyncOptions{
			hasPendingIndexing: shouldReverseIndex,
			pendingIndex: dbShardPendingIndex{
				timestamp:  timestamp,
				enqueuedAt: s.nowFn(),
			},
		})
		if err != nil {
			return ts.Series{}, err
		}

		// Wait for the insert to be batched together and inserted
		result.wg.Wait()

		// Retrieve the inserted entry
		entry, err = s.writableSeries(id, tags)
		if err != nil {
			return ts.Series{}, err
		}
		writable = true

		// NB(r): We just indexed this series if shouldReverseIndex was true
		shouldReverseIndex = false
	}

	var (
		commitLogSeriesID          ident.ID
		commitLogSeriesTags        ident.Tags
		commitLogSeriesUniqueIndex uint64
	)
	if writable {
		// Perform write
		err = entry.Series.Write(ctx, timestamp, value, unit, annotation)
		// Load series metadata before decrementing the writer count
		// to ensure this metadata is snapshotted at a consistent state
		// NB(r): We explicitly do not place the series ID back into a
		// pool as high frequency users of series IDs such
		// as the commit log need to use the reference without the
		// overhead of ownership tracking. This makes taking a ref here safe.
		commitLogSeriesID = entry.Series.ID()
		commitLogSeriesTags = entry.Series.Tags()
		commitLogSeriesUniqueIndex = entry.Index
		if err == nil && shouldReverseIndex {
			if entry.NeedsIndexUpdate(s.reverseIndex.BlockStartForWriteTime(timestamp)) {
				err = s.insertSeriesForIndexingAsyncBatched(entry, timestamp,
					opts.writeNewSeriesAsync)
			}
		}
		// release the reference we got on entry from `writableSeries`
		entry.DecrementReaderWriterCount()
		if err != nil {
			return ts.Series{}, err
		}
	} else {
		// This is an asynchronous insert and write
		result, err := s.insertSeriesAsyncBatched(id, tags, dbShardInsertAsyncOptions{
			hasPendingWrite: true,
			pendingWrite: dbShardPendingWrite{
				timestamp:  timestamp,
				value:      value,
				unit:       unit,
				annotation: annotation,
			},
			hasPendingIndexing: shouldReverseIndex,
			pendingIndex: dbShardPendingIndex{
				timestamp:  timestamp,
				enqueuedAt: s.nowFn(),
			},
		})
		if err != nil {
			return ts.Series{}, err
		}
		// NB(r): Make sure to use the copied ID which will eventually
		// be set to the newly series inserted ID.
		// The `id` var here is volatile after the context is closed
		// and adding ownership tracking to use it in the commit log
		// (i.e. registering a dependency on the context) is too expensive.
		commitLogSeriesID = result.copiedID
		commitLogSeriesTags = result.copiedTags
		commitLogSeriesUniqueIndex = result.entry.Index
	}

	// Write commit log
	series := ts.Series{
		UniqueIndex: commitLogSeriesUniqueIndex,
		Namespace:   s.namespace.ID(),
		ID:          commitLogSeriesID,
		Tags:        commitLogSeriesTags,
		Shard:       s.shard,
	}

	return series, nil
}

func (s *dbShard) ReadEncoded(
	ctx context.Context,
	id ident.ID,
	start, end time.Time,
) ([][]xio.BlockReader, error) {
	s.RLock()
	entry, _, err := s.lookupEntryWithLock(id)
	if entry != nil {
		// NB(r): Ensure readers have consistent view of this series, do
		// not expire the series while being read from.
		entry.IncrementReaderWriterCount()
		defer entry.DecrementReaderWriterCount()
	}
	s.RUnlock()

	if err == errShardEntryNotFound {
		switch s.opts.SeriesCachePolicy() {
		case series.CacheAll:
			// No-op, would be in memory if cached
			return nil, nil
		}
	} else if err != nil {
		return nil, err
	}

	if entry != nil {
		return entry.Series.ReadEncoded(ctx, start, end)
	}

	retriever := s.seriesBlockRetriever
	onRetrieve := s.seriesOnRetrieveBlock
	opts := s.seriesOpts
	reader := series.NewReaderUsingRetriever(id, retriever, onRetrieve, nil, opts)
	return reader.ReadEncoded(ctx, start, end)
}

// lookupEntryWithLock returns the entry for a given id while holding a read lock or a write lock.
func (s *dbShard) lookupEntryWithLock(id ident.ID) (*lookup.Entry, *list.Element, error) {
	if s.state != dbShardStateOpen {
		// NB(r): Return an invalid params error here so any upstream
		// callers will not retry this operation
		return nil, nil, xerrors.NewInvalidParamsError(errShardNotOpen)
	}
	elem, exists := s.lookup.Get(id)
	if !exists {
		return nil, nil, errShardEntryNotFound
	}
	return elem.Value.(*lookup.Entry), elem, nil
}

func (s *dbShard) writableSeries(id ident.ID, tags ident.TagIterator) (*lookup.Entry, error) {
	for {
		entry, _, err := s.tryRetrieveWritableSeries(id)
		if entry != nil {
			return entry, nil
		}
		if err != nil {
			return nil, err
		}

		// Not inserted, attempt a batched insert
		result, err := s.insertSeriesAsyncBatched(id, tags, dbShardInsertAsyncOptions{})
		if err != nil {
			return nil, err
		}

		// Wait for the insert attempt
		result.wg.Wait()
	}
}

type writableSeriesOptions struct {
	writeNewSeriesAsync bool
}

func (s *dbShard) tryRetrieveWritableSeries(id ident.ID) (
	*lookup.Entry,
	writableSeriesOptions,
	error,
) {
	s.RLock()
	opts := writableSeriesOptions{
		writeNewSeriesAsync: s.currRuntimeOptions.writeNewSeriesAsync,
	}
	if entry, _, err := s.lookupEntryWithLock(id); err == nil {
		entry.IncrementReaderWriterCount()
		s.RUnlock()
		return entry, opts, nil
	} else if err != errShardEntryNotFound {
		s.RUnlock()
		return nil, opts, err
	}
	s.RUnlock()
	return nil, opts, nil
}

func (s *dbShard) newShardEntry(
	id ident.ID,
	tagsArgOpts tagsArgOptions,
) (*lookup.Entry, error) {
	// NB(r): As documented in storage/series.DatabaseSeries the series IDs
	// are garbage collected, hence we cast the ID to a BytesID that can't be
	// finalized.
	// Since series are purged so infrequently the overhead of not releasing
	// back an ID to a pool is amortized over a long period of time.
	var (
		seriesID   ident.BytesID
		seriesTags ident.Tags
		err        error
	)
	if id.IsNoFinalize() {
		// If the ID is already marked as NoFinalize, meaning it won't be returned
		// to any pools, then we can directly take reference to it.
		// We make sure to use ident.BytesID for this ID to avoid inc/decref when
		// accessing the ID since it's not pooled and therefore the safety is not
		// required.
		seriesID = ident.BytesID(id.Bytes())
	} else {
		seriesID = ident.BytesID(append([]byte(nil), id.Bytes()...))
		seriesID.NoFinalize()
	}

	switch tagsArgOpts.arg {
	case tagsIterArg:
		// NB(r): Take a duplicate so that we don't double close the tag iterator
		// passed to this method
		tagsIter := tagsArgOpts.tagsIter.Duplicate()

		// Ensure tag iterator at start
		if tagsIter.CurrentIndex() != 0 {
			return nil, errNewShardEntryTagsIterNotAtIndexZero
		}
		seriesTags, err = convert.TagsFromTagsIter(
			seriesID, tagsIter, s.identifierPool)
		tagsIter.Close()
		if err != nil {
			return nil, err
		}

		if err := convert.ValidateMetric(seriesID, seriesTags); err != nil {
			return nil, err
		}

	case tagsArg:
		seriesTags = tagsArgOpts.tags

	default:
		return nil, errNewShardEntryTagsTypeInvalid

	}

	series := s.seriesPool.Get()
	series.Reset(seriesID, seriesTags, s.seriesBlockRetriever,
		s.seriesOnRetrieveBlock, s, s.seriesOpts)
	uniqueIndex := s.increasingIndex.nextIndex()
	return lookup.NewEntry(series, uniqueIndex), nil
}

type insertAsyncResult struct {
	wg         *sync.WaitGroup
	copiedID   ident.ID
	copiedTags ident.Tags
	// entry is not guaranteed to be the final entry
	// inserted into the shard map in case there is already
	// an existing entry waiting in the insert queue
	entry *lookup.Entry
}

func (s *dbShard) insertSeriesForIndexingAsyncBatched(
	entry *lookup.Entry,
	timestamp time.Time,
	async bool,
) error {
	indexBlockStart := s.reverseIndex.BlockStartForWriteTime(timestamp)
	// inc a ref on the entry to ensure it's valid until the queue acts upon it.
	entry.OnIndexPrepare()
	wg, err := s.insertQueue.Insert(dbShardInsert{
		entry: entry,
		opts: dbShardInsertAsyncOptions{
			hasPendingIndexing: true,
			pendingIndex: dbShardPendingIndex{
				timestamp:  timestamp,
				enqueuedAt: s.nowFn(),
			},
			// indicate we already have inc'd the entry's ref count, so we can correctly
			// handle the ref counting semantics in `insertSeriesBatch`.
			entryRefCountIncremented: true,
		},
	})

	// i.e. unable to enqueue into shard insert queue
	if err != nil {
		entry.OnIndexFinalize(indexBlockStart) // release any reference's we've held for indexing
		return err
	}

	// if operating in async mode, we're done
	if async {
		return nil
	}

	// if indexing in sync mode, wait till we're done and ensure we have have indexed the entry
	wg.Wait()
	if !entry.IndexedForBlockStart(indexBlockStart) {
		// i.e. indexing failed
		return fmt.Errorf("internal error: unable to index series")
	}

	return nil
}

func (s *dbShard) insertSeriesAsyncBatched(
	id ident.ID,
	tags ident.TagIterator,
	opts dbShardInsertAsyncOptions,
) (insertAsyncResult, error) {
	entry, err := s.newShardEntry(id, newTagsIterArg(tags))
	if err != nil {
		return insertAsyncResult{}, err
	}

	wg, err := s.insertQueue.Insert(dbShardInsert{
		entry: entry,
		opts:  opts,
	})
	return insertAsyncResult{
		wg: wg,
		// Make sure to return the copied ID from the new series
		copiedID:   entry.Series.ID(),
		copiedTags: entry.Series.Tags(),
		entry:      entry,
	}, err
}

type insertSyncType uint8

// nolint: varcheck, unused
const (
	insertSync insertSyncType = iota
	insertSyncIncReaderWriterCount
)

func (s *dbShard) insertSeriesSync(
	id ident.ID,
	tagsArgOpts tagsArgOptions,
	insertType insertSyncType,
) (*lookup.Entry, error) {
	var (
		entry *lookup.Entry
		err   error
	)

	s.Lock()
	defer func() {
		// Check if we're making a modification to this entry, be sure
		// to increment the writer count so it's visible when we release
		// the lock
		if entry != nil && insertType == insertSyncIncReaderWriterCount {
			entry.IncrementReaderWriterCount()
		}
		s.Unlock()
	}()

	entry, _, err = s.lookupEntryWithLock(id)
	if err != nil && err != errShardEntryNotFound {
		// Shard not taking inserts likely
		return nil, err
	}
	if entry != nil {
		// Already inserted
		return entry, nil
	}

	entry, err = s.newShardEntry(id, tagsArgOpts)
	if err != nil {
		// should never happen
		s.logger.WithFields(
			xlog.NewField("id", id.String()),
			xlog.NewField("err", err.Error()),
		).Errorf("[invariant violated] unable to create shardEntry in insertSeriesSync")
		return nil, err
	}

	if s.newSeriesBootstrapped {
		_, err := entry.Series.Bootstrap(nil)
		if err != nil {
			entry = nil // Don't increment the writer count for this series
			return nil, err
		}
	}

	s.insertNewShardEntryWithLock(entry)
	return entry, nil
}

func (s *dbShard) insertNewShardEntryWithLock(entry *lookup.Entry) {
	// Set the lookup value, we use the copied ID and since it is GC'd
	// we explicitly set it with options to not copy the key and not to
	// finalize it
	copiedID := entry.Series.ID()
	listElem := s.list.PushBack(entry)
	s.lookup.SetUnsafe(copiedID, listElem, shardMapSetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	})
}

func (s *dbShard) insertSeriesBatch(inserts []dbShardInsert) error {
	var (
		anyPendingAction   = false
		numPendingIndexing = 0
	)

	s.Lock()
	for i := range inserts {
		// If we are going to write to this entry then increment the
		// writer count so it does not look empty immediately after
		// we release the write lock.
		hasPendingWrite := inserts[i].opts.hasPendingWrite
		hasPendingIndexing := inserts[i].opts.hasPendingIndexing
		hasPendingRetrievedBlock := inserts[i].opts.hasPendingRetrievedBlock
		anyPendingAction = anyPendingAction || hasPendingWrite ||
			hasPendingRetrievedBlock || hasPendingIndexing

		if hasPendingIndexing {
			numPendingIndexing++
		}

		// we don't need to inc the entry ref count if we already have a ref on the entry. check if
		// that's the case.
		if inserts[i].opts.entryRefCountIncremented {
			// don't need to inc a ref on the entry, we were given as writable entry as input.
			continue
		}

		// i.e. we don't have a ref on provided entry, so we check if between the operation being
		// enqueue in the shard insert queue, and this function executing, an entry was created
		// for the same ID.
		entry, _, err := s.lookupEntryWithLock(inserts[i].entry.Series.ID())
		if entry != nil {
			// Already exists so update the entry we're pointed at for this insert
			inserts[i].entry = entry
		}

		if hasPendingIndexing || hasPendingWrite || hasPendingRetrievedBlock {
			// We're definitely writing a value, ensure that the pending write is
			// visible before we release the lookup write lock
			inserts[i].entry.IncrementReaderWriterCount()
			// also indicate that we have a ref count on this entry for this operation
			inserts[i].opts.entryRefCountIncremented = true
		}

		if err == nil {
			// Already inserted
			continue
		}

		if err != errShardEntryNotFound {
			// Shard is not taking inserts
			s.Unlock()
			// FOLLOWUP(prateek): is this an existing bug? why don't we need to release any ref's we've inc'd
			// on entries in the loop before this point, i.e. in range [0, i). Otherwise, how are those entries
			// going to get cleaned up?
			s.metrics.insertAsyncInsertErrors.Inc(int64(len(inserts) - i))
			return err
		}

		// Insert still pending, perform the insert
		entry = inserts[i].entry
		if s.newSeriesBootstrapped {
			_, err := entry.Series.Bootstrap(nil)
			if err != nil {
				s.metrics.insertAsyncBootstrapErrors.Inc(1)
			}
		}
		s.insertNewShardEntryWithLock(entry)
	}
	s.Unlock()

	if !anyPendingAction {
		return nil
	}

	// Perform any indexing, pending writes or pending retrieved blocks outside of lock
	ctx := s.contextPool.Get()
	// TODO(prateek): pool this type
	indexBlockSize := s.namespace.Options().IndexOptions().BlockSize()
	indexBatch := index.NewWriteBatch(index.WriteBatchOptions{
		InitialCapacity: numPendingIndexing,
		IndexBlockSize:  indexBlockSize,
	})
	for i := range inserts {
		var (
			entry           = inserts[i].entry
			releaseEntryRef = inserts[i].opts.entryRefCountIncremented
		)

		if inserts[i].opts.hasPendingWrite {
			write := inserts[i].opts.pendingWrite
			err := entry.Series.Write(ctx, write.timestamp, write.value,
				write.unit, write.annotation)
			if err != nil {
				s.metrics.insertAsyncWriteErrors.Inc(1)
			}
		}

		if inserts[i].opts.hasPendingIndexing {
			pendingIndex := inserts[i].opts.pendingIndex
			// increment the ref on the entry, as the original one was transferred to the
			// this method (insertSeriesBatch) via `entryRefCountIncremented` mechanism.
			entry.OnIndexPrepare()

			id := entry.Series.ID()
			tags := entry.Series.Tags().Values()

			var d doc.Document
			d.ID = id.Bytes() // IDs from shard entries are always set NoFinalize
			d.Fields = make(doc.Fields, 0, len(tags))
			for _, tag := range tags {
				d.Fields = append(d.Fields, doc.Field{
					Name:  tag.Name.Bytes(),  // Tags from shard entries are always set NoFinalize
					Value: tag.Value.Bytes(), // Tags from shard entries are always set NoFinalize
				})
			}
			indexBatch.Append(index.WriteBatchEntry{
				Timestamp:     pendingIndex.timestamp,
				OnIndexSeries: entry,
				EnqueuedAt:    pendingIndex.enqueuedAt,
			}, d)
		}

		if inserts[i].opts.hasPendingRetrievedBlock {
			block := inserts[i].opts.pendingRetrievedBlock
			entry.Series.OnRetrieveBlock(block.id, block.tags, block.start, block.segment)
		}

		if releaseEntryRef {
			entry.DecrementReaderWriterCount()
		}
	}

	var err error
	// index all requested entries in batch.
	if indexBatch.Len() > 0 {
		err = s.reverseIndex.WriteBatch(indexBatch)
	}

	// Avoid goroutine spinning up to close this context
	ctx.BlockingClose()

	return err
}

func (s *dbShard) FetchBlocks(
	ctx context.Context,
	id ident.ID,
	starts []time.Time,
) ([]block.FetchBlockResult, error) {
	s.RLock()
	entry, _, err := s.lookupEntryWithLock(id)
	if entry != nil {
		// NB(r): Ensure readers have consistent view of this series, do
		// not expire the series while being read from.
		entry.IncrementReaderWriterCount()
		defer entry.DecrementReaderWriterCount()
	}
	s.RUnlock()

	if err == errShardEntryNotFound {
		switch s.opts.SeriesCachePolicy() {
		case series.CacheAll:
			// No-op, would be in memory if cached
			return nil, nil
		}
	} else if err != nil {
		return nil, err
	}

	if entry != nil {
		return entry.Series.FetchBlocks(ctx, starts)
	}

	retriever := s.seriesBlockRetriever
	onRetrieve := s.seriesOnRetrieveBlock
	opts := s.seriesOpts
	// Nil for onRead callback because we don't want peer bootstrapping to impact
	// the behavior of the LRU
	var onReadCb block.OnReadBlock
	reader := series.NewReaderUsingRetriever(id, retriever, onRetrieve, onReadCb, opts)
	return reader.FetchBlocks(ctx, starts)
}

func (s *dbShard) fetchActiveBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	limit int64,
	indexCursor int64,
	opts series.FetchBlocksMetadataOptions,
) (block.FetchBlocksMetadataResults, *int64, error) {
	var (
		res             = s.opts.FetchBlocksMetadataResultsPool().Get()
		tmpCtx          = context.NewContext()
		nextIndexCursor *int64
	)

	var loopErr error
	s.forEachShardEntry(func(entry *lookup.Entry) bool {
		// Break out of the iteration loop once we've accumulated enough entries.
		if int64(len(res.Results())) >= limit {
			next := int64(entry.Index)
			nextIndexCursor = &next
			return false
		}

		// Fast forward past indexes lower than page token
		if int64(entry.Index) < indexCursor {
			return true
		}

		// Use a temporary context here so the stream readers can be returned to
		// pool after we finish fetching the metadata for this series.
		tmpCtx.Reset()
		metadata, err := entry.Series.FetchBlocksMetadata(tmpCtx, start, end, opts)
		tmpCtx.BlockingClose()
		if err != nil {
			loopErr = err
			return false
		}

		// If the blocksMetadata is empty, the series have no data within the specified
		// time range so we don't return it to the client
		if len(metadata.Blocks.Results()) == 0 {
			metadata.Blocks.Close()
			return true
		}

		// Otherwise add it to the result which takes care of closing the metadata
		res.Add(metadata)

		return true
	})

	return res, nextIndexCursor, loopErr
}

func (s *dbShard) FetchBlocksMetadataV2(
	ctx context.Context,
	start, end time.Time,
	limit int64,
	encodedPageToken PageToken,
	opts block.FetchBlocksMetadataOptions,
) (block.FetchBlocksMetadataResults, PageToken, error) {
	token := new(pagetoken.PageToken)
	if encodedPageToken != nil {
		if err := proto.Unmarshal(encodedPageToken, token); err != nil {
			return nil, nil, xerrors.NewInvalidParamsError(errShardInvalidPageToken)
		}
	}

	activePhase := token.ActiveSeriesPhase
	flushedPhase := token.FlushedSeriesPhase

	cachePolicy := s.opts.SeriesCachePolicy()
	if cachePolicy == series.CacheAll {
		// If we are using a series cache policy that caches all block metadata
		// in memory then we only ever perform the active phase as all metadata
		// is actively held in memory
		indexCursor := int64(0)
		if activePhase != nil {
			indexCursor = activePhase.IndexCursor
		}
		// We always include cached blocks
		seriesFetchBlocksMetadataOpts := series.FetchBlocksMetadataOptions{
			FetchBlocksMetadataOptions: opts,
			IncludeCachedBlocks:        true,
		}
		result, nextIndexCursor, err := s.fetchActiveBlocksMetadata(ctx, start, end,
			limit, indexCursor, seriesFetchBlocksMetadataOpts)
		if err != nil {
			return nil, nil, err
		}

		if nextIndexCursor == nil {
			// No more results and only enacting active phase since we are using
			// series policy that caches all block metadata in memory
			return result, nil, nil
		}
		token = &pagetoken.PageToken{
			ActiveSeriesPhase: &pagetoken.PageToken_ActiveSeriesPhase{
				IndexCursor: *nextIndexCursor,
			},
		}
		data, err := proto.Marshal(token)
		if err != nil {
			return nil, nil, err
		}
		return result, PageToken(data), nil
	}

	// NB(r): If returning mixed in memory and disk results, then we return anything
	// that's mutable in memory first then all disk results.
	// We work backwards so we don't hit race conditions with blocks
	// being flushed and potentially missed between paginations. Working
	// backwards means that we might duplicate metadata sent back switching
	// between active phase and flushed phase, but that's better than missing
	// data working in the opposite direction. De-duping which block time ranges
	// were actually sent is also difficult as it's not always a consistent view
	// across async pagination.
	// Duplicating the metadata sent back means that consumers get a consistent
	// view of the world if they merge all the results together.
	// In the future we should consider the lifecycle of fileset files rather
	// than directly working with them here while filesystem cleanup manager
	// could delete them mid-read, on linux this is ok as it's just an unlink
	// and we'll finish our read cleanly. If there's a race between us thinking
	// the file is accessible and us opening a reader to it then this will bubble
	// an error to the client which will be retried.
	if flushedPhase == nil {
		// If first phase started or no phases started then return active
		// series metadata until we find a block start time that we have fileset
		// files for
		indexCursor := int64(0)
		if activePhase != nil {
			indexCursor = activePhase.IndexCursor
		}
		// We do not include cached blocks because we'll send metadata for
		// those blocks when we send metadata directly from the flushed files
		seriesFetchBlocksMetadataOpts := series.FetchBlocksMetadataOptions{
			FetchBlocksMetadataOptions: opts,
			IncludeCachedBlocks:        false,
		}
		result, nextIndexCursor, err := s.fetchActiveBlocksMetadata(ctx, start, end,
			limit, indexCursor, seriesFetchBlocksMetadataOpts)
		if err != nil {
			return nil, nil, err
		}

		// Encode the next page token
		if nextIndexCursor == nil {
			// Next phase, no more results from active series
			token = &pagetoken.PageToken{
				FlushedSeriesPhase: &pagetoken.PageToken_FlushedSeriesPhase{},
			}
		} else {
			// This phase is still active
			token = &pagetoken.PageToken{
				ActiveSeriesPhase: &pagetoken.PageToken_ActiveSeriesPhase{
					IndexCursor: *nextIndexCursor,
				},
			}
		}

		data, err := proto.Marshal(token)
		if err != nil {
			return nil, nil, err
		}

		return result, PageToken(data), nil
	}

	// Must be in the second phase, start with checking the latest possible
	// flushed block and work backwards.
	var (
		result    = s.opts.FetchBlocksMetadataResultsPool().Get()
		ropts     = s.namespace.Options().RetentionOptions()
		blockSize = ropts.BlockSize()
		// Subtract one blocksize because all fetch requests are exclusive on the end side
		blockStart      = end.Truncate(blockSize).Add(-1 * blockSize)
		tokenBlockStart time.Time
		numResults      int64
	)
	if flushedPhase.CurrBlockStartUnixNanos > 0 {
		tokenBlockStart = time.Unix(0, flushedPhase.CurrBlockStartUnixNanos)
		blockStart = tokenBlockStart
	}

	// Work backwards while in requested range and not before retention
	for !blockStart.Before(start) &&
		!blockStart.Before(retention.FlushTimeStart(ropts, s.nowFn())) {
		exists, err := s.namespaceReaderMgr.filesetExistsAt(s.shard, blockStart)
		if err != nil {
			return nil, nil, err
		}
		if !exists {
			// No fileset files here
			blockStart = blockStart.Add(-1 * blockSize)
			continue
		}

		var pos readerPosition
		if !tokenBlockStart.IsZero() {
			// Was previously seeking through a previous block, need to validate
			// this is the correct one we found otherwise the file just went missing
			if !blockStart.Equal(tokenBlockStart) {
				return nil, nil, fmt.Errorf(
					"was reading block at %v but next available block is: %v",
					tokenBlockStart, blockStart)
			}

			// Do not need to check if we move onto the next block that it matches
			// the token's block start on next iteration
			tokenBlockStart = time.Time{}

			pos.metadataIdx = int(flushedPhase.CurrBlockEntryIdx)
		}

		// Open a reader at this position, potentially from cache
		reader, err := s.namespaceReaderMgr.get(s.shard, blockStart, pos)
		if err != nil {
			return nil, nil, err
		}

		for numResults < limit {
			id, tags, size, checksum, err := reader.ReadMetadata()
			if err == io.EOF {
				// Clean end of volume, we can break now
				if err := reader.Close(); err != nil {
					return nil, nil, fmt.Errorf(
						"could not close metadata reader for block %v: %v",
						blockStart, err)
				}
				break
			}
			if err != nil {
				// Best effort to close the reader on a read error
				if err := reader.Close(); err != nil {
					s.logger.Errorf("could not close reader on unexpected err: %v", err)
				}
				return nil, nil, fmt.Errorf(
					"could not read metadata for block %v: %v",
					blockStart, err)
			}

			blockResult := s.opts.FetchBlockMetadataResultsPool().Get()
			value := block.FetchBlockMetadataResult{
				Start: blockStart,
			}
			if opts.IncludeSizes {
				value.Size = int64(size)
			}
			if opts.IncludeChecksums {
				v := checksum
				value.Checksum = &v
			}
			blockResult.Add(value)

			numResults++
			result.Add(block.NewFetchBlocksMetadataResult(id, tags,
				blockResult))
		}

		// Return the reader to the cache
		endPos := int64(reader.MetadataRead())
		s.namespaceReaderMgr.put(reader)

		if numResults >= limit {
			// We hit the limit, return results with page token
			token = &pagetoken.PageToken{
				FlushedSeriesPhase: &pagetoken.PageToken_FlushedSeriesPhase{
					CurrBlockStartUnixNanos: blockStart.UnixNano(),
					CurrBlockEntryIdx:       endPos,
				},
			}
			data, err := proto.Marshal(token)
			if err != nil {
				return nil, nil, err
			}
			return result, PageToken(data), nil
		}

		// Otherwise we move on to the previous block
		blockStart = blockStart.Add(-1 * blockSize)
	}

	// No more results if we fall through
	return result, nil, nil
}

func (s *dbShard) Bootstrap(
	bootstrappedSeries *result.Map,
) error {
	s.Lock()
	if s.bootstrapState == Bootstrapped {
		s.Unlock()
		return nil
	}
	if s.bootstrapState == Bootstrapping {
		s.Unlock()
		return errShardIsBootstrapping
	}
	s.bootstrapState = Bootstrapping
	s.Unlock()

	var (
		shardBootstrapResult = dbShardBootstrapResult{}
		multiErr             = xerrors.NewMultiError()
	)
	for _, elem := range bootstrappedSeries.Iter() {
		dbBlocks := elem.Value()

		// First lookup if series already exists
		entry, _, err := s.tryRetrieveWritableSeries(dbBlocks.ID)
		if err != nil {
			multiErr = multiErr.Add(err)
			continue
		}
		if entry == nil {
			// Synchronously insert to avoid waiting for
			// the insert queue potential delayed insert
			entry, err = s.insertSeriesSync(dbBlocks.ID, newTagsArg(dbBlocks.Tags),
				insertSyncIncReaderWriterCount)
			if err != nil {
				multiErr = multiErr.Add(err)
				continue
			}
		} else {
			// No longer needed as we found the series and we don't require
			// them for insertion.
			// FOLLOWUP(r): Audit places that keep refs to the ID from a
			// bootstrap result, newShardEntry copies it but some of the
			// bootstrapped blocks when using certain series cache policies
			// keeps refs to the ID with seriesID, so for now these IDs will
			// be garbage collected)
			dbBlocks.Tags.Finalize()
		}

		// Cannot close blocks once done as series takes ref to these
		bsResult, err := entry.Series.Bootstrap(dbBlocks.Blocks)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
		shardBootstrapResult.update(bsResult)

		// Always decrement the writer count, avoid continue on bootstrap error
		entry.DecrementReaderWriterCount()
	}

	s.emitBootstrapResult(shardBootstrapResult)

	// From this point onwards, all newly created series that aren't in
	// the existing map should be considered bootstrapped because they
	// have no data within the retention period.
	s.Lock()
	s.newSeriesBootstrapped = true
	s.Unlock()

	// Find the series with no data within the retention period but has
	// buffered data points since server start. Any new series added
	// after this will be marked as bootstrapped.
	s.forEachShardEntry(func(entry *lookup.Entry) bool {
		series := entry.Series
		if series.IsBootstrapped() {
			return true
		}
		_, err := series.Bootstrap(nil)
		multiErr = multiErr.Add(err)
		return true
	})

	// Now iterate flushed time ranges to determine which blocks are
	// retrievable before servicing reads
	fsOpts := s.opts.CommitLogOptions().FilesystemOptions()
	readInfoFilesResults := fs.ReadInfoFiles(fsOpts.FilePathPrefix(), s.namespace.ID(), s.shard,
		fsOpts.InfoReaderBufferSize(), fsOpts.DecodingOptions())

	for _, result := range readInfoFilesResults {
		if result.Err.Error() != nil {
			s.logger.WithFields(
				xlog.NewField("shard", s.ID()),
				xlog.NewField("namespace", s.namespace.ID()),
				xlog.NewField("error", result.Err.Error()),
				xlog.NewField("filepath", result.Err.Filepath()),
			).Error("unable to read info files in shard bootstrap")
			continue
		}
		info := result.Info
		at := xtime.FromNanoseconds(info.BlockStart)
		fs := s.FlushState(at)
		if fs.Status != fileOpNotStarted {
			continue // Already recorded progress
		}
		s.markFlushStateSuccess(at)
	}

	s.Lock()
	s.bootstrapState = Bootstrapped
	s.Unlock()

	return multiErr.FinalError()
}

func (s *dbShard) Flush(
	blockStart time.Time,
	flushPreparer persist.FlushPreparer,
) error {
	// We don't flush data when the shard is still bootstrapping
	s.RLock()
	if s.bootstrapState != Bootstrapped {
		s.RUnlock()
		return errShardNotBootstrappedToFlush
	}
	s.RUnlock()

	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: s.namespace,
		Shard:             s.ID(),
		BlockStart:        blockStart,
		// We explicitly set delete if exists to false here as we track which
		// filesets exists at bootstrap time so we should never encounter a time
		// when we attempt to flush and a fileset already exists unless there is
		// racing competing processes.
		DeleteIfExists: false,
	}
	prepared, err := flushPreparer.PrepareData(prepareOpts)
	if err != nil {
		return s.markFlushStateSuccessOrError(blockStart, err)
	}

	var multiErr xerrors.MultiError
	tmpCtx := context.NewContext()

	flushResult := dbShardFlushResult{}
	s.forEachShardEntry(func(entry *lookup.Entry) bool {
		curr := entry.Series
		// Use a temporary context here so the stream readers can be returned to
		// the pool after we finish fetching flushing the series.
		tmpCtx.Reset()
		flushOutcome, err := curr.Flush(tmpCtx, blockStart, prepared.Persist)
		tmpCtx.BlockingClose()

		if err != nil {
			multiErr = multiErr.Add(err)
			// If we encounter an error when persisting a series, don't continue as
			// the file on disk could be in a corrupt state.
			return false
		}

		flushResult.update(flushOutcome)

		return true
	})

	s.logFlushResult(flushResult)

	if err := prepared.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}

	return s.markFlushStateSuccessOrError(blockStart, multiErr.FinalError())
}

func (s *dbShard) Snapshot(
	blockStart time.Time,
	snapshotTime time.Time,
	snapshotPreparer persist.SnapshotPreparer,
) error {
	// We don't snapshot data when the shard is still bootstrapping
	s.RLock()
	if s.bootstrapState != Bootstrapped {
		s.RUnlock()
		return errShardNotBootstrappedToSnapshot
	}
	s.RUnlock()

	var multiErr xerrors.MultiError

	s.markIsSnapshotting()
	defer func() {
		s.markDoneSnapshotting(multiErr.Empty(), snapshotTime)
	}()

	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: s.namespace,
		Shard:             s.ID(),
		BlockStart:        blockStart,
		FileSetType:       persist.FileSetSnapshotType,
		// We explicitly set delete if exists to false here as we do not
		// expect there to be a collision as snapshots files are appended
		// with a monotonically increasing number to avoid collisions, there
		// would have to be a competing process to cause a collision.
		DeleteIfExists: false,
		Snapshot: persist.DataPrepareSnapshotOptions{
			SnapshotTime: snapshotTime,
		},
	}
	prepared, err := snapshotPreparer.PrepareData(prepareOpts)
	// Add the err so the defer will capture it
	multiErr = multiErr.Add(err)
	if err != nil {
		return err
	}

	tmpCtx := context.NewContext()
	s.forEachShardEntry(func(entry *lookup.Entry) bool {
		series := entry.Series
		// Use a temporary context here so the stream readers can be returned to
		// pool after we finish fetching flushing the series
		tmpCtx.Reset()
		err := series.Snapshot(tmpCtx, blockStart, prepared.Persist)
		tmpCtx.BlockingClose()

		if err != nil {
			multiErr = multiErr.Add(err)
			// If we encounter an error when persisting a series, don't continue as
			// the file on disk could be in a corrupt state.
			return false
		}

		return true
	})

	if err := prepared.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}

	return multiErr.FinalError()
}

func (s *dbShard) FlushState(blockStart time.Time) fileOpState {
	s.flushState.RLock()
	state, ok := s.flushState.statesByTime[xtime.ToUnixNano(blockStart)]
	if !ok {
		s.flushState.RUnlock()
		return fileOpState{Status: fileOpNotStarted}
	}
	s.flushState.RUnlock()
	return state
}

func (s *dbShard) markFlushStateSuccessOrError(blockStart time.Time, err error) error {
	// Track flush state for block state
	if err == nil {
		s.markFlushStateSuccess(blockStart)
	} else {
		s.markFlushStateFail(blockStart)
	}
	return err
}

func (s *dbShard) markFlushStateSuccess(blockStart time.Time) {
	s.flushState.Lock()
	s.flushState.statesByTime[xtime.ToUnixNano(blockStart)] = fileOpState{Status: fileOpSuccess}
	s.flushState.Unlock()
}

func (s *dbShard) markFlushStateFail(blockStart time.Time) {
	s.flushState.Lock()
	state := s.flushState.statesByTime[xtime.ToUnixNano(blockStart)]
	state.Status = fileOpFailed
	state.NumFailures++
	s.flushState.statesByTime[xtime.ToUnixNano(blockStart)] = state
	s.flushState.Unlock()
}

func (s *dbShard) removeAnyFlushStatesTooEarly(tickStart time.Time) {
	s.flushState.Lock()
	earliestFlush := retention.FlushTimeStart(s.namespace.Options().RetentionOptions(), tickStart)
	for t := range s.flushState.statesByTime {
		if t.ToTime().Before(earliestFlush) {
			delete(s.flushState.statesByTime, t)
		}
	}
	s.flushState.Unlock()
}

func (s *dbShard) SnapshotState() (bool, time.Time) {
	s.snapshotState.RLock()
	defer s.snapshotState.RUnlock()
	return s.snapshotState.isSnapshotting, s.snapshotState.lastSuccessfulSnapshot
}

func (s *dbShard) markIsSnapshotting() {
	s.snapshotState.Lock()
	s.snapshotState.isSnapshotting = true
	s.snapshotState.Unlock()
}

func (s *dbShard) markDoneSnapshotting(success bool, completionTime time.Time) {
	s.snapshotState.Lock()
	s.snapshotState.isSnapshotting = false
	if success {
		s.snapshotState.lastSuccessfulSnapshot = completionTime
	}
	s.snapshotState.Unlock()
}

func (s *dbShard) CleanupExpiredFileSets(earliestToRetain time.Time) error {
	filePathPrefix := s.opts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
	multiErr := xerrors.NewMultiError()
	expired, err := s.filesetBeforeFn(filePathPrefix, s.namespace.ID(), s.ID(), earliestToRetain)
	if err != nil {
		detailedErr :=
			fmt.Errorf("encountered errors when getting fileset files for prefix %s namespace %s shard %d: %v",
				filePathPrefix, s.namespace.ID(), s.ID(), err)
		multiErr = multiErr.Add(detailedErr)
	}
	if err := s.deleteFilesFn(expired); err != nil {
		multiErr = multiErr.Add(err)
	}
	return multiErr.FinalError()
}

func (s *dbShard) Repair(
	ctx context.Context,
	tr xtime.Range,
	repairer databaseShardRepairer,
) (repair.MetadataComparisonResult, error) {
	return repairer.Repair(ctx, s.namespace.ID(), tr, s)
}

func (s *dbShard) BootstrapState() BootstrapState {
	s.RLock()
	bs := s.bootstrapState
	s.RUnlock()
	return bs
}

func (s *dbShard) emitBootstrapResult(r dbShardBootstrapResult) {
	s.metrics.seriesBootstrapBlocksToBuffer.Inc(r.numBlocksMovedToBuffer)
	s.metrics.seriesBootstrapBlocksMerged.Inc(r.numBlocksMerged)
}

func (s *dbShard) logFlushResult(r dbShardFlushResult) {
	s.logger.WithFields(
		xlog.NewField("shard", s.ID()),
		xlog.NewField("numBlockDoesNotExist", r.numBlockDoesNotExist),
	).Debug("shard flush outcome")
}

// dbShardBootstrapResult is a helper struct for keeping track of the result of bootstrapping all the
// series in the shard.
type dbShardBootstrapResult struct {
	numBlocksMovedToBuffer int64
	numBlocksMerged        int64
}

func (r *dbShardBootstrapResult) update(u series.BootstrapResult) {
	r.numBlocksMovedToBuffer += u.NumBlocksMovedToBuffer
	r.numBlocksMerged += u.NumBlocksMerged
}

// dbShardFlushResult is a helper struct for keeping track of the result of flushing all the
// series in the shard.
type dbShardFlushResult struct {
	numBlockDoesNotExist int64
}

func (r *dbShardFlushResult) update(u series.FlushOutcome) {
	if u == series.FlushOutcomeBlockDoesNotExist {
		r.numBlockDoesNotExist++
	}
}
