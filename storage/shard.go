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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/generated/proto/pagetoken"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/xio"
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
	errShardEntryNotFound         = errors.New("shard entry not found")
	errShardNotOpen               = errors.New("shard is not open")
	errShardAlreadyTicking        = errors.New("shard is already ticking")
	errShardClosingTickTerminated = errors.New("shard is closing, terminating tick")
	errShardInvalidPageToken      = errors.New("shard could not unmarshal page token")
)

type filesetBeforeFn func(
	filePathPrefix string,
	namespace ident.ID,
	shardID uint32,
	t time.Time,
) ([]string, error)

type snapshotFilesFn func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.SnapshotFilesSlice, error)

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
	commitLogWriter          commitLogWriter
	reverseIndex             namespaceIndex
	insertQueue              *dbShardInsertQueue
	lookup                   *shardMap
	list                     *list.List
	bs                       BootstrapState
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

type dbShardEntry struct {
	series         series.DatabaseSeries
	index          uint64
	curReadWriters int32
	reverseIndex   struct {
		// NB(prateek): writeAttemptedBlockstart[1-2]Nanos is used to indicate the index block start(s)
		// for which this entry has a pending indexing operation (if any). The value for
		// the blockstart is computed based upon timestamp of writes to the entry. When
		// a write comes in for series, we compute this token and compare against the value
		// set on the entry. If the entry has the same value, then we know there is already
		// a write pending for the entry in the index for the given block start, and don't
		// need to attempt another one. If not, we need to index it.
		// The following semantics hold for this type:
		//   - `writeAttemptedBlockstartNanos == 0` indicates no write is pending for this entry.
		//   - `writeAttemptedBlockStartNanos != 0` indicates a write is pending for this entry.
		// We actually require two blockStarts, because there can be writes issued for two blocks at the
		// same time, and we need to track both of them.
		writeAttemptedBlockStart1Nanos int64
		writeAttemptedBlockStart2Nanos int64
		// NB(prateek): nextWriteTimeNanos is the UnixNanos until
		// the next index write is required. We use an atomic instead
		// of a time.Time to avoid using a Mutex to guard it.
		nextWriteTimeNanos int64
	}
}

func (entry *dbShardEntry) readerWriterCount() int32 {
	return atomic.LoadInt32(&entry.curReadWriters)
}

func (entry *dbShardEntry) incrementReaderWriterCount() {
	atomic.AddInt32(&entry.curReadWriters, 1)
}

func (entry *dbShardEntry) decrementReaderWriterCount() {
	atomic.AddInt32(&entry.curReadWriters, -1)
}

func (entry *dbShardEntry) hasIndexTTLGreaterThan(writeTime time.Time) bool {
	nextWriteNanos := atomic.LoadInt64(&entry.reverseIndex.nextWriteTimeNanos)
	return nextWriteNanos > int64(xtime.ToUnixNano(writeTime))
}

// NB(prateek): needsIndexUpdate is a CAS, i.e. when this method returns true, it
// also sets state on the entry to indicate that a write for the given blockStart
// is going to be sent to the index, and other go routines should not attempt the
// same write. Callers are expected to ensure they follow this guideline.
// Further, every call to needsIndexUpdate which returns true needs to have a corresponding
// OnIndexFinalze() call. This is reqiured for correct lifecycle maintenance.
func (entry *dbShardEntry) needsIndexUpdate(indexblockStartForWrite xtime.UnixNano) bool {
	// check if the entry has a TTL indicating it does not need a write till a
	// time later than the provided windexblockStartForWrite. If so, we know the
	// entry does not need to be indexed. More on this in a NB at the end of this function.
	nextWriteNanos := atomic.LoadInt64(&entry.reverseIndex.nextWriteTimeNanos)
	if nextWriteNanos > int64(indexblockStartForWrite) {
		return false
	}

	var (
		entryBlock1 = &entry.reverseIndex.writeAttemptedBlockStart1Nanos
		entryBlock2 = &entry.reverseIndex.writeAttemptedBlockStart1Nanos
	)

	// i.e. no TTL has been marked on the entry yet. check if a write has been attempted for the entry at all.
	if atomic.CompareAndSwapInt64(entryBlock1, int64(indexblockStartForWrite), int64(indexblockStartForWrite)) {
		// i.e. we're already attempting a write for this block start and are tracking this at entryBlock1
		return false
	}

	if atomic.CompareAndSwapInt64(entryBlock2, int64(indexblockStartForWrite), int64(indexblockStartForWrite)) {
		// i.e. we're already attempting a write for this block start and are tracking this at entryBlock2
		return false
	}

	// now we attempt to grab either of the slots and indicate success if so
	if atomic.CompareAndSwapInt64(entryBlock1, 0, int64(indexblockStartForWrite)) {
		// i.e. we're are now attempting a write for this block start and are tracking this at entryBlock1
		return true
	}

	if atomic.CompareAndSwapInt64(entryBlock2, 0, int64(indexblockStartForWrite)) {
		// i.e. we're are now attempting a write for this block start and are tracking this at entryBlock2
		return true
	}

	// i.e. both the slots are occupied. we can reach here in a couple of ways:
	// (1) There are lots of writes for this series, and another go routine has taken the slots. This is
	// expected, and we can indicate false.
	return false

	// (2) There's a code bug, where we haven't called OnIndexFinalize() despite this method returning
	// true. We can possibly check for this by seeing if the values in entryBlock1/2 are more than
	// 2 * indexBlockSize away from the provided blockStart. Should I do that here? // TODO(prateek): <---

	// NB(prateek): There's still a design flaw in case of out of order, delayed writes.
	// Consider an index block size of 2h, and buffer past of 10m.
	// Say a write comes in at 2.05p (wallclock) for 2.05p (timestamp in the write), we'd index the
	// entry, and update the entry to have a nextWriteTimeNanos of 4p. Now imagine another write
	// comes in at 2.06p (wallclock) for 1.57p (timestamp in the write). The current design would
	// see 1.57p points to the 12p blockStart, which is before the nextWriteTimeNanos of 4p and
	// decide that we have already indexed this entry for the earlier block. i.e. we'd drop the
	// indexing write.
	// This can be addresssed by doing somthing like keeping 4 atomics (instead of the current 3), to
	// capture any possible blocks that could be written to and/or if such a write has been attempted.
	// IMO the complexity of that isn't worth maintaining. Much simpler to explicitly call out that out
	// out order writes for the same ids have this issue. Will bring up with other devs in the PR.
}

// ensure dbShardEntry satisfies the `index.OnIndexSeries` interface.
var _ index.OnIndexSeries = &dbShardEntry{}

func (entry *dbShardEntry) OnIndexSuccess(nextWriteTime xtime.UnixNano) {
	atomic.StoreInt64(&entry.reverseIndex.nextWriteTimeNanos, int64(nextWriteTime))
}

func (entry *dbShardEntry) OnIndexFinalize(blockStartNanos xtime.UnixNano) {
	// indicate the index has released held reference for provided write
	entry.decrementReaderWriterCount()

	var (
		entryBlock1 = &entry.reverseIndex.writeAttemptedBlockStart1Nanos
		entryBlock2 = &entry.reverseIndex.writeAttemptedBlockStart1Nanos
	)

	if atomic.CompareAndSwapInt64(entryBlock1, int64(blockStartNanos), 0) {
		// i.e. we were attempting a write for this block start at entryBlock1 and have cleared it.
		return
	}

	if atomic.CompareAndSwapInt64(entryBlock2, int64(blockStartNanos), 0) {
		// i.e. we were attempting a write for this block start at entryBlock2 and have cleared it.
		return
	}

	// TODO(prateek): capture some kind of log for this failure mode.
	// should never get here, on index finalize should only be called for a blockstart we're
	// tracking.
}

func (entry *dbShardEntry) onIndexPrepare() {
	// NB(prateek): we retain the ref count on the entry while the indexing is pending,
	// the callback executed on the entry once the indexing is completed releases this
	// reference.
	entry.incrementReaderWriterCount()
}

type dbShardEntryWorkFn func(entry *dbShardEntry) bool

type dbShardEntryBatchWorkFn func(entries []*dbShardEntry) bool

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
	commitLogWriter commitLogWriter,
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
		commitLogWriter:    commitLogWriter,
		reverseIndex:       reverseIndex,
		lookup:             newShardMap(shardMapOptions{}),
		list:               list.New(),
		filesetBeforeFn:    fs.FileSetBefore,
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
		s.bs = Bootstrapped
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
		entry.incrementReaderWriterCount()
		defer entry.decrementReaderWriterCount()
	}
	s.RUnlock()

	if err != nil && err != errShardEntryNotFound {
		return // Likely closing
	}

	if entry != nil {
		entry.series.OnRetrieveBlock(id, tags, startTime, segment)
		return
	}

	entry, err = s.newShardEntry(id, tags)
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
	copiedID := entry.series.ID()
	// TODO(r): Pool the slice iterators here.
	copiedTags := ident.NewTagSliceIterator(entry.series.Tags())
	s.insertQueue.Insert(dbShardInsert{
		entry: entry,
		opts: dbShardInsertAsyncOptions{
			hasPendingRetrievedBlock: true,
			pendingRetrievedBlock: dbShardPendingRetrievedBlock{
				id:      copiedID,
				tags:    copiedTags,
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

	entry.series.OnEvictedFromWiredList(id, blockStart)
}

func (s *dbShard) forEachShardEntry(entryFn dbShardEntryWorkFn) error {
	return s.forEachShardEntryBatch(func(currEntries []*dbShardEntry) bool {
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
		e.Value.(*dbShardEntry).decrementReaderWriterCount()
	}

	var (
		currEntries = make([]*dbShardEntry, 0, batchSize)
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
			entry := elem.Value.(*dbShardEntry)
			entry.incrementReaderWriterCount()
			currEntries = append(currEntries, entry)
			elem = nextElem
		}

		// NB(prateek): inc a reference to the next element while we have a lock,
		// to guarantee the element pointer cannot be changed from under us.
		if nextElem != nil {
			nextElem.Value.(*dbShardEntry).incrementReaderWriterCount()
		}
		s.RUnlock()

		continueExecution := entriesBatchFn(currEntries)
		for i := range currEntries {
			currEntries[i].decrementReaderWriterCount()
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

func (s *dbShard) Tick(c context.Cancellable) (tickResult, error) {
	s.removeAnyFlushStatesTooEarly()
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
		expired                       []*dbShardEntry
	)
	s.RLock()
	tickSleepBatch := s.currRuntimeOptions.tickSleepSeriesBatchSize
	tickSleepPerSeries := s.currRuntimeOptions.tickSleepPerSeries
	s.RUnlock()
	s.forEachShardEntryBatch(func(currEntries []*dbShardEntry) bool {
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
				result, err = entry.series.Tick()
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
func (s *dbShard) purgeExpiredSeries(expiredEntries []*dbShardEntry) {
	// Remove all expired series from lookup and list.
	s.Lock()
	for _, entry := range expiredEntries {
		series := entry.series
		id := series.ID()
		elem, exists := s.lookup.Get(id)
		if !exists {
			continue
		}

		count := entry.readerWriterCount()
		// The contract requires all entries to have count >= 1.
		if count < 1 {
			s.logger.WithFields(
				xlog.NewField("series", series.ID().String()),
				xlog.NewField("readerWriterCount", count),
			).Errorf("observed series with invalid readerWriterCount in `purgeExpiredSeries`")
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
) error {
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
) error {
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
) error {
	// Prepare write
	entry, opts, err := s.tryRetrieveWritableSeries(id)
	if err != nil {
		return err
	}

	writable := entry != nil

	// If no entry and we are not writing new series asynchronously
	if !writable && !opts.writeNewSeriesAsync {
		// Avoid double lookup by enqueueing insert immediately
		result, err := s.insertSeriesAsyncBatched(id, tags, dbShardInsertAsyncOptions{
			hasPendingIndexing: shouldReverseIndex,
			pendingIndex: dbShardPendingIndex{
				timestamp: timestamp,
			},
		})
		if err != nil {
			return err
		}

		// Wait for the insert to be batched together and inserted
		result.wg.Wait()

		// Retrieve the inserted entry
		entry, err = s.writableSeries(id, tags)
		if err != nil {
			return err
		}
		writable = true
	}

	var (
		commitLogSeriesID          ident.ID
		commitLogSeriesTags        ident.Tags
		commitLogSeriesUniqueIndex uint64
	)
	if writable {
		// Perform write
		err = entry.series.Write(ctx, timestamp, value, unit, annotation)
		// Load series metadata before decrementing the writer count
		// to ensure this metadata is snapshotted at a consistent state
		// NB(r): We explicitly do not place the series ID back into a
		// pool as high frequency users of series IDs such
		// as the commit log need to use the reference without the
		// overhead of ownership tracking. This makes taking a ref here safe.
		commitLogSeriesID = entry.series.ID()
		commitLogSeriesTags = entry.series.Tags()
		commitLogSeriesUniqueIndex = entry.index
		needsIndex := shouldReverseIndex && entry.needsIndexUpdate(s.reverseIndex.BlockStartForWriteTime(timestamp))
		if err == nil && needsIndex {
			err = s.insertSeriesForIndexing(entry, timestamp, opts.writeNewSeriesAsync)
		}
		// release the reference we got on entry from `writableSeries`
		entry.decrementReaderWriterCount()
		if err != nil {
			return err
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
				timestamp: timestamp,
			},
		})
		if err != nil {
			return err
		}
		// NB(r): Make sure to use the copied ID which will eventually
		// be set to the newly series inserted ID.
		// The `id` var here is volatile after the context is closed
		// and adding ownership tracking to use it in the commit log
		// (i.e. registering a dependency on the context) is too expensive.
		commitLogSeriesID = result.copiedID
		commitLogSeriesTags = result.copiedTags
		commitLogSeriesUniqueIndex = result.entry.index
	}

	// Write commit log
	series := commitlog.Series{
		UniqueIndex: commitLogSeriesUniqueIndex,
		Namespace:   s.namespace.ID(),
		ID:          commitLogSeriesID,
		Tags:        commitLogSeriesTags,
		Shard:       s.shard,
	}

	datapoint := ts.Datapoint{
		Timestamp: timestamp,
		Value:     value,
	}

	return s.commitLogWriter.Write(ctx, series, datapoint,
		unit, annotation)
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
		entry.incrementReaderWriterCount()
		defer entry.decrementReaderWriterCount()
	}
	s.RUnlock()

	if err == errShardEntryNotFound {
		switch s.opts.SeriesCachePolicy() {
		case series.CacheAll:
			// No-op, would be in memory if cached
			return nil, nil
		case series.CacheAllMetadata:
			// No-op, would be in memory if metadata cached
			return nil, nil
		}
	} else if err != nil {
		return nil, err
	}

	if entry != nil {
		return entry.series.ReadEncoded(ctx, start, end)
	}

	retriever := s.seriesBlockRetriever
	onRetrieve := s.seriesOnRetrieveBlock
	opts := s.seriesOpts
	reader := series.NewReaderUsingRetriever(id, retriever, onRetrieve, nil, opts)
	return reader.ReadEncoded(ctx, start, end)
}

// lookupEntryWithLock returns the entry for a given id while holding a read lock or a write lock.
func (s *dbShard) lookupEntryWithLock(id ident.ID) (*dbShardEntry, *list.Element, error) {
	if s.state != dbShardStateOpen {
		// NB(r): Return an invalid params error here so any upstream
		// callers will not retry this operation
		return nil, nil, xerrors.NewInvalidParamsError(errShardNotOpen)
	}
	elem, exists := s.lookup.Get(id)
	if !exists {
		return nil, nil, errShardEntryNotFound
	}
	return elem.Value.(*dbShardEntry), elem, nil
}

func (s *dbShard) writableSeries(id ident.ID, tags ident.TagIterator) (*dbShardEntry, error) {
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
	*dbShardEntry,
	writableSeriesOptions,
	error,
) {
	s.RLock()
	opts := writableSeriesOptions{
		writeNewSeriesAsync: s.currRuntimeOptions.writeNewSeriesAsync,
	}
	if entry, _, err := s.lookupEntryWithLock(id); err == nil {
		entry.incrementReaderWriterCount()
		s.RUnlock()
		return entry, opts, nil
	} else if err != errShardEntryNotFound {
		s.RUnlock()
		return nil, opts, err
	}
	s.RUnlock()
	return nil, opts, nil
}

func (s *dbShard) newShardEntry(id ident.ID, tags ident.TagIterator) (*dbShardEntry, error) {
	clonedTags, err := s.cloneTags(tags)
	if err != nil {
		return nil, err
	}
	series := s.seriesPool.Get()
	clonedID := s.identifierPool.Clone(id)
	series.Reset(clonedID, clonedTags, s.seriesBlockRetriever,
		s.seriesOnRetrieveBlock, s, s.seriesOpts)
	uniqueIndex := s.increasingIndex.nextIndex()
	return &dbShardEntry{series: series, index: uniqueIndex}, nil
}

func (s *dbShard) cloneTags(tags ident.TagIterator) (ident.Tags, error) {
	tags = tags.Duplicate()
	clone := make(ident.Tags, 0, tags.Remaining())
	defer tags.Close()
	for tags.Next() {
		t := tags.Current()
		clone = append(clone, s.identifierPool.CloneTag(t))
	}
	if err := tags.Err(); err != nil {
		return nil, err
	}
	return clone, nil
}

type insertAsyncResult struct {
	wg         *sync.WaitGroup
	copiedID   ident.ID
	copiedTags ident.Tags
	// entry is not guaranteed to be the final entry
	// inserted into the shard map in case there is already
	// an existing entry waiting in the insert queue
	entry *dbShardEntry
}

func (s *dbShard) insertSeriesForIndexing(
	entry *dbShardEntry,
	timestamp time.Time,
	async bool,
) error {
	// inc a ref on the entry to ensure it's valid until the queue acts upon it.
	entry.onIndexPrepare()
	wg, err := s.insertQueue.Insert(dbShardInsert{
		entry: entry,
		opts: dbShardInsertAsyncOptions{
			hasPendingIndexing: true,
			pendingIndex: dbShardPendingIndex{
				timestamp: timestamp,
			},
			// indicate we already have inc'd the entry's ref count, so we can correctly
			// handle the ref counting semantics in `insertSeriesBatch`.
			entryRefCountIncremented: true,
		},
	})

	// i.e. unable to enqueue into shard insert queue
	if err != nil {
		indexBlockStart := s.reverseIndex.BlockStartForWriteTime(timestamp)
		entry.OnIndexFinalize(indexBlockStart) // release any reference's we've held for indexing
		return err
	}

	// if operating in async mode, we're done
	if async {
		return nil
	}

	// if indexing in sync mode, wait till we're done and ensure we have have indexed the entry
	wg.Wait()
	if entry.hasIndexTTLGreaterThan(timestamp) {
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
	entry, err := s.newShardEntry(id, tags)
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
		copiedID:   entry.series.ID(),
		copiedTags: entry.series.Tags(),
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
	tags ident.TagIterator,
	insertType insertSyncType,
) (*dbShardEntry, error) {
	var (
		entry *dbShardEntry
		err   error
	)

	s.Lock()
	defer func() {
		// Check if we're making a modification to this entry, be sure
		// to increment the writer count so it's visible when we release
		// the lock
		if entry != nil && insertType == insertSyncIncReaderWriterCount {
			entry.incrementReaderWriterCount()
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

	entry, err = s.newShardEntry(id, tags)
	if err != nil {
		// should never happen
		s.logger.WithFields(
			xlog.NewField("id", id.String()),
			xlog.NewField("err", err.Error()),
		).Errorf("[invariant violated] unable to create shardEntry in insertSeriesSync")
		return nil, err
	}

	if s.newSeriesBootstrapped {
		_, err := entry.series.Bootstrap(nil)
		if err != nil {
			entry = nil // Don't increment the writer count for this series
			return nil, err
		}
	}

	s.insertNewShardEntryWithLock(entry)
	return entry, nil
}

func (s *dbShard) insertNewShardEntryWithLock(entry *dbShardEntry) {
	// Set the lookup value, we use the copied ID and since it is GC'd
	// we explicitly set it with options to not copy the key and not to
	// finalize it
	copiedID := entry.series.ID()
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
		entry, _, err := s.lookupEntryWithLock(inserts[i].entry.series.ID())
		if entry != nil {
			// Already exists so update the entry we're pointed at for this insert
			inserts[i].entry = entry
		}

		if hasPendingIndexing || hasPendingWrite || hasPendingRetrievedBlock {
			// We're definitely writing a value, ensure that the pending write is
			// visible before we release the lookup write lock
			inserts[i].entry.incrementReaderWriterCount()
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
			_, err := entry.series.Bootstrap(nil)
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
	indexBatch := make([]index.WriteBatchEntry, 0, numPendingIndexing)
	for i := range inserts {
		var (
			entry           = inserts[i].entry
			releaseEntryRef = inserts[i].opts.entryRefCountIncremented
		)

		if inserts[i].opts.hasPendingWrite {
			write := inserts[i].opts.pendingWrite
			err := entry.series.Write(ctx, write.timestamp, write.value,
				write.unit, write.annotation)
			if err != nil {
				s.metrics.insertAsyncWriteErrors.Inc(1)
			}
		}

		if inserts[i].opts.hasPendingIndexing {
			pendingIndex := inserts[i].opts.pendingIndex
			// increment the ref on the entry, as the original one was transferred to the
			// this method (insertSeriesBatch) via `entryRefCountIncremented` mechanism.
			entry.onIndexPrepare()
			indexBatch = append(indexBatch, index.WriteBatchEntry{
				ID:            entry.series.ID(),
				Tags:          entry.series.Tags(),
				Timestamp:     pendingIndex.timestamp,
				OnIndexSeries: entry,
			})
		}

		if inserts[i].opts.hasPendingRetrievedBlock {
			block := inserts[i].opts.pendingRetrievedBlock
			entry.series.OnRetrieveBlock(block.id, block.tags, block.start, block.segment)
		}

		if releaseEntryRef {
			entry.decrementReaderWriterCount()
		}
	}

	var err error
	// index all requested entries in batch.
	if len(indexBatch) != 0 {
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
		entry.incrementReaderWriterCount()
		defer entry.decrementReaderWriterCount()
	}
	s.RUnlock()

	if err == errShardEntryNotFound {
		switch s.opts.SeriesCachePolicy() {
		case series.CacheAll:
			// No-op, would be in memory if cached
			return nil, nil
		case series.CacheAllMetadata:
			// No-op, would be in memory if metadata cached
			return nil, nil
		}
	} else if err != nil {
		return nil, err
	}

	if entry != nil {
		return entry.series.FetchBlocks(ctx, starts)
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
	s.forEachShardEntry(func(entry *dbShardEntry) bool {
		// Break out of the iteration loop once we've accumulated enough entries.
		if int64(len(res.Results())) >= limit {
			next := int64(entry.index)
			nextIndexCursor = &next
			return false
		}

		// Fast forward past indexes lower than page token
		if int64(entry.index) < indexCursor {
			return true
		}

		// Use a temporary context here so the stream readers can be returned to
		// pool after we finish fetching the metadata for this series.
		tmpCtx.Reset()
		metadata, err := entry.series.FetchBlocksMetadata(tmpCtx, start, end, opts)
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

func (s *dbShard) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	limit int64,
	pageToken int64,
	opts block.FetchBlocksMetadataOptions,
) (block.FetchBlocksMetadataResults, *int64, error) {
	switch s.opts.SeriesCachePolicy() {
	case series.CacheAll:
	case series.CacheAllMetadata:
	default:
		// If not using CacheAll or CacheAllMetadata then calling the v1
		// API will only return active block metadata (mutable and cached)
		// hence this call is invalid
		return nil, nil, fmt.Errorf(
			"fetch blocks metadata v1 endpoint invalid with cache policy: %s",
			s.opts.SeriesCachePolicy().String())
	}

	// For v1 endpoint we always include cached blocks because when using
	// CacheAllMetadata the blocks will appear cached
	seriesFetchBlocksMetadataOpts := series.FetchBlocksMetadataOptions{
		FetchBlocksMetadataOptions: opts,
		IncludeCachedBlocks:        true,
	}
	return s.fetchActiveBlocksMetadata(ctx, start, end,
		limit, pageToken, seriesFetchBlocksMetadataOpts)
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
	if cachePolicy == series.CacheAll || cachePolicy == series.CacheAllMetadata {
		// If we are using a series cache policy that caches all block metadata
		// in memory then we only ever perform the active phase as all metadata
		// is actively held in memory
		indexCursor := int64(0)
		if activePhase != nil {
			indexCursor = activePhase.IndexCursor
		}
		// We always include cached blocks because when using
		// CacheAllMetadata the blocks will appear cached
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

			// Make sure ID and tags get cleaned up after read is done
			ctx.RegisterFinalizer(id)
			ctx.RegisterCloser(tags)

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
	if s.bs == Bootstrapped {
		s.Unlock()
		return nil
	}
	if s.bs == Bootstrapping {
		s.Unlock()
		return errShardIsBootstrapping
	}
	s.bs = Bootstrapping
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
			entry, err = s.insertSeriesSync(dbBlocks.ID,
				ident.NewTagSliceIterator(dbBlocks.Tags),
				insertSyncIncReaderWriterCount)
			if err != nil {
				multiErr = multiErr.Add(err)
				continue
			}
		}

		bsResult, err := entry.series.Bootstrap(dbBlocks.Blocks)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
		shardBootstrapResult.update(bsResult)

		// Always decrement the writer count, avoid continue on bootstrap error
		entry.decrementReaderWriterCount()
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
	s.forEachShardEntry(func(entry *dbShardEntry) bool {
		series := entry.series
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
	s.bs = Bootstrapped
	s.Unlock()

	return multiErr.FinalError()
}

func (s *dbShard) Flush(
	blockStart time.Time,
	flush persist.Flush,
) error {
	// We don't flush data when the shard is still bootstrapping
	s.RLock()
	if s.bs != Bootstrapped {
		s.RUnlock()
		return errShardNotBootstrappedToFlush
	}
	s.RUnlock()

	prepareOpts := persist.PrepareOptions{
		NamespaceMetadata: s.namespace,
		Shard:             s.ID(),
		BlockStart:        blockStart,
		// We explicitly set delete if exists to false here as we track which
		// filesets exists at bootstrap time so we should never encounter a time
		// when we attempt to flush and a fileset already exists unless there is
		// racing competing processes.
		DeleteIfExists: false,
	}
	prepared, err := flush.Prepare(prepareOpts)
	if err != nil {
		return s.markFlushStateSuccessOrError(blockStart, err)
	}

	var multiErr xerrors.MultiError
	tmpCtx := context.NewContext()

	flushResult := dbShardFlushResult{}
	s.forEachShardEntry(func(entry *dbShardEntry) bool {
		curr := entry.series
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
	flush persist.Flush,
) error {
	// We don't snapshot data when the shard is still bootstrapping
	s.RLock()
	if s.bs != Bootstrapped {
		s.RUnlock()
		return errShardNotBootstrappedToSnapshot
	}
	s.RUnlock()

	var multiErr xerrors.MultiError

	s.markIsSnapshotting()
	defer func() {
		s.markDoneSnapshotting(multiErr.Empty(), snapshotTime)
	}()

	prepareOpts := persist.PrepareOptions{
		NamespaceMetadata: s.namespace,
		Shard:             s.ID(),
		BlockStart:        blockStart,
		SnapshotTime:      snapshotTime,
		FileSetType:       persist.FileSetSnapshotType,
		// We explicitly set delete if exists to false here as we do not
		// expect there to be a collision as snapshots files are appended
		// with a monotonically increasing number to avoid collisions, there
		// would have to be a competing process to cause a collision.
		DeleteIfExists: false,
	}
	prepared, err := flush.Prepare(prepareOpts)
	// Add the err so the defer will capture it
	multiErr = multiErr.Add(err)
	if err != nil {
		return err
	}

	tmpCtx := context.NewContext()
	s.forEachShardEntry(func(entry *dbShardEntry) bool {
		series := entry.series
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

func (s *dbShard) removeAnyFlushStatesTooEarly() {
	s.flushState.Lock()
	now := s.nowFn()
	earliestFlush := retention.FlushTimeStart(s.namespace.Options().RetentionOptions(), now)
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

// CleanupSnapshots examines the snapshot files for the shard that are on disk and
// determines which can be safely deleted. A snapshot file is safe to delete if it
// meets one of the following criteria:
// 		1) It contains data for a block start that is out of retention (as determined
// 		   by the earliestToRetain argument.)
// 		2) It contains data for a block start that has already been successfully flushed.
// 		3) It contains data for a block start that hasn't been flushed yet, but a more
// 		   recent set of snapshot files (higher index) exists for the same block start.
// 		   This is because snapshot files are cumulative, so once a new one has been
//         written out it's safe to delete any previous ones for that block start.
func (s *dbShard) CleanupSnapshots(earliestToRetain time.Time) error {
	filePathPrefix := s.opts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
	snapshotFiles, err := s.snapshotFilesFn(filePathPrefix, s.namespace.ID(), s.ID())
	if err != nil {
		return err
	}

	sort.Slice(snapshotFiles, func(i, j int) bool {
		// Make sure they're sorted by blockStart/Index in ascending order.
		if snapshotFiles[i].ID.BlockStart.Equal(snapshotFiles[j].ID.BlockStart) {
			return snapshotFiles[i].ID.Index < snapshotFiles[j].ID.Index
		}
		return snapshotFiles[i].ID.BlockStart.Before(snapshotFiles[j].ID.BlockStart)
	})

	filesToDelete := []string{}

	for i := 0; i < len(snapshotFiles); i++ {
		curr := snapshotFiles[i]

		if curr.ID.BlockStart.Before(earliestToRetain) {
			// Delete snapshot files for blocks that have fallen out
			// of retention.
			filesToDelete = append(filesToDelete, curr.AbsoluteFilepaths...)
			continue
		}

		if s.FlushState(curr.ID.BlockStart).Status == fileOpSuccess {
			// Delete snapshot files for any block starts that have been
			// successfully flushed.
			filesToDelete = append(filesToDelete, curr.AbsoluteFilepaths...)
			continue
		}

		if i+1 < len(snapshotFiles) &&
			snapshotFiles[i+1].ID.BlockStart == curr.ID.BlockStart &&
			snapshotFiles[i+1].ID.Index > curr.ID.Index &&
			snapshotFiles[i+1].HasCheckpointFile() {
			// Delete any snapshot files which are not the most recent
			// for that block start, but only of the set of snapshot files
			// with the higher index is complete (checkpoint file exists)
			filesToDelete = append(filesToDelete, curr.AbsoluteFilepaths...)
			continue
		}
	}

	return s.deleteFilesFn(filesToDelete)
}

func (s *dbShard) CleanupFileSet(earliestToRetain time.Time) error {
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
	bs := s.bs
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
