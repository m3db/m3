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
	expireBatchLength        = 1024
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
	indexWriteFn             databaseIndexWriteFn
	insertQueue              *dbShardInsertQueue
	lookup                   map[ident.Hash]*list.Element
	list                     *list.List
	bs                       bootstrapState
	filesetBeforeFn          filesetBeforeFn
	deleteFilesFn            deleteFilesFn
	sleepFn                  func(time.Duration)
	identifierPool           ident.Pool
	contextPool              context.Pool
	flushState               shardFlushState
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
	create                     tally.Counter
	close                      tally.Counter
	closeStart                 tally.Counter
	closeLatency               tally.Timer
	insertAsyncInsertErrors    tally.Counter
	insertAsyncBootstrapErrors tally.Counter
	insertAsyncWriteErrors     tally.Counter
	insertAsyncIndexErrors     tally.Counter
}

func newDatabaseShardMetrics(scope tally.Scope) dbShardMetrics {
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
		insertAsyncIndexErrors: scope.Tagged(map[string]string{
			"error_type": "index-series",
		}).Counter("insert-async.errors"),
	}
}

type dbShardEntry struct {
	series         series.DatabaseSeries
	index          uint64
	curReadWriters int32
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

type dbShardEntryWorkFn func(entry *dbShardEntry) bool

type shardFlushState struct {
	sync.RWMutex
	statesByTime map[xtime.UnixNano]fileOpState
}

func newShardFlushState() shardFlushState {
	return shardFlushState{
		statesByTime: make(map[xtime.UnixNano]fileOpState),
	}
}

func newDatabaseShard(
	namespaceMetadata namespace.Metadata,
	shard uint32,
	blockRetriever block.DatabaseBlockRetriever,
	namespaceReaderMgr databaseNamespaceReaderManager,
	increasingIndex increasingIndex,
	commitLogWriter commitLogWriter,
	indexWriteFn databaseIndexWriteFn,
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
		indexWriteFn:       indexWriteFn,
		lookup:             make(map[ident.Hash]*list.Element),
		list:               list.New(),
		filesetBeforeFn:    fs.FilesetBefore,
		deleteFilesFn:      fs.DeleteFiles,
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
		s.bs = bootstrapped
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
	start time.Time,
	onRetrieve block.OnRetrieveBlock,
) (xio.SegmentReader, error) {
	return s.DatabaseBlockRetriever.Stream(ctx, s.shard, id, start, onRetrieve)
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
		entry.series.OnRetrieveBlock(id, startTime, segment)
		return
	}

	// Insert batched with the retrieved block
	// NB(prateek): we do not retrieve tags during the retrieval process,
	// as the index was already given the requisite information when the
	// series was first inserted.
	entry = s.newShardEntry(id, ident.EmptyTagIterator)
	copiedID := entry.series.ID()
	s.insertQueue.Insert(dbShardInsert{
		entry: entry,
		opts: dbShardInsertAsyncOptions{
			hasPendingRetrievedBlock: true,
			pendingRetrievedBlock: dbShardPendingRetrievedBlock{
				id:      copiedID,
				start:   startTime,
				segment: segment,
			},
		},
	})
}

func (s *dbShard) forEachShardEntry(entryFn dbShardEntryWorkFn) error {
	// NB(r): consider using a lockless list for ticking.
	s.RLock()
	elemsLen := s.list.Len()
	batchSize := int(math.Ceil(shardIterateBatchPercent * float64(elemsLen)))
	nextElem := s.list.Front()
	s.RUnlock()

	// TODO(xichen): pool or cache this.
	currEntries := make([]*dbShardEntry, 0, batchSize)
	for nextElem != nil {
		s.RLock()
		elem := nextElem
		for ticked := 0; ticked < batchSize && elem != nil; ticked++ {
			nextElem = elem.Next()
			entry := elem.Value.(*dbShardEntry)
			entry.incrementReaderWriterCount()
			currEntries = append(currEntries, entry)
			elem = nextElem
		}
		s.RUnlock()
		for _, entry := range currEntries {
			if continueForEach := entryFn(entry); !continueForEach {
				// Abort early, decrement reader writer count for all entries first
				for _, e := range currEntries {
					e.decrementReaderWriterCount()
				}
				return nil
			}
		}
		for i := range currEntries {
			currEntries[i].decrementReaderWriterCount()
			currEntries[i] = nil
		}
		currEntries = currEntries[:0]
	}
	return nil
}

func (s *dbShard) IsBootstrapping() bool {
	s.RLock()
	state := s.bs
	s.RUnlock()
	return state == bootstrapping
}

func (s *dbShard) IsBootstrapped() bool {
	s.RLock()
	state := s.bs
	s.RUnlock()
	return state == bootstrapped
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
		expired                       []series.DatabaseSeries
		terminatedTickingDueToClosing bool
		i                             int
		slept                         time.Duration
	)
	s.RLock()
	tickSleepBatch := s.currRuntimeOptions.tickSleepSeriesBatchSize
	tickSleepPerSeries := s.currRuntimeOptions.tickSleepPerSeries
	s.RUnlock()
	s.forEachShardEntry(func(entry *dbShardEntry) bool {
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
			expired = append(expired, entry.series)
			r.expiredSeries++
			if len(expired) >= expireBatchLength {
				// Purge when reaching max batch size to avoid large array growth
				// and ensure smooth rate of elements being returned to pools.
				// This method does not run using a lock so this is safe to
				// perform inline.
				s.purgeExpiredSeries(expired)
				for i := range expired {
					expired[i] = nil
				}
				expired = expired[:0]
			}
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
		// Continue
		return true
	})

	if len(expired) > 0 {
		// Purge any series that still haven't been purged yet
		s.purgeExpiredSeries(expired)
	}

	if terminatedTickingDueToClosing {
		return tickResult{}, errShardClosingTickTerminated
	}

	return r, nil
}

func (s *dbShard) purgeExpiredSeries(expired []series.DatabaseSeries) {
	// Remove all expired series from lookup and list.
	s.Lock()
	for _, series := range expired {
		hash := series.ID().Hash()
		elem, exists := s.lookup[hash]
		if !exists {
			continue
		}
		entry := elem.Value.(*dbShardEntry)
		// If this series is currently being written to or read from, we don't
		// remove it even though it's empty in that it might become non-empty
		// soon.
		// We avoid closing the series during a read to ensure a consistent view
		// of the series if they manage to take a reference to it.
		if entry.readerWriterCount() > 0 {
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
		delete(s.lookup, hash)
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
	return s.writeAndIndex(ctx, id, tags, timestamp, value, unit, annotation, s.indexWriteFn)
}

func (s *dbShard) Write(
	ctx context.Context,
	id ident.ID,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	return s.writeAndIndex(ctx, id, ident.EmptyTagIterator, timestamp, value, unit, annotation, databaseIndexNoOpWriteFn)
}

func (s *dbShard) writeAndIndex(
	ctx context.Context,
	id ident.ID,
	tags ident.TagIterator,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
	indexWriteFn databaseIndexWriteFn,
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
			indexWriteFn: indexWriteFn,
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
			indexWriteFn: indexWriteFn,
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
) ([][]xio.SegmentReader, error) {
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
	reader := series.NewReaderUsingRetriever(id, retriever, onRetrieve, opts)
	return reader.ReadEncoded(ctx, start, end)
}

// lookupEntryWithLock returns the entry for a given id while holding a read lock or a write lock.
func (s *dbShard) lookupEntryWithLock(id ident.ID) (*dbShardEntry, *list.Element, error) {
	if s.state != dbShardStateOpen {
		// NB(r): Return an invalid params error here so any upstream
		// callers will not retry this operation
		return nil, nil, xerrors.NewInvalidParamsError(errShardNotOpen)
	}
	elem, exists := s.lookup[id.Hash()]
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

func (s *dbShard) newShardEntry(id ident.ID, tags ident.TagIterator) *dbShardEntry {
	series := s.seriesPool.Get()
	clonedID := s.identifierPool.Clone(id)
	clonedTags := s.cloneTags(tags)
	series.Reset(clonedID, clonedTags, s.seriesBlockRetriever,
		s.seriesOnRetrieveBlock, s.seriesOpts)
	uniqueIndex := s.increasingIndex.nextIndex()
	return &dbShardEntry{series: series, index: uniqueIndex}
}

// TODO(prateek): add ident.Pool method: `CloneTags(TagIterator) Tags`
func (s *dbShard) cloneTags(tags ident.TagIterator) ident.Tags {
	tags = tags.Clone()
	clone := make(ident.Tags, 0, tags.Remaining())
	defer tags.Close()
	for tags.Next() {
		t := tags.Current()
		clone = append(clone, ident.Tag{
			Name:  s.identifierPool.Clone(t.Name),
			Value: s.identifierPool.Clone(t.Value),
		})
	}
	return clone
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

func (s *dbShard) insertSeriesAsyncBatched(
	id ident.ID,
	tags ident.TagIterator,
	opts dbShardInsertAsyncOptions,
) (insertAsyncResult, error) {
	entry := s.newShardEntry(id, tags)

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

// nolint: deadcode, varcheck, unused
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

	entry = s.newShardEntry(id, tags)
	if s.newSeriesBootstrapped {
		if err := entry.series.Bootstrap(nil); err != nil {
			entry = nil // Don't increment the writer count for this series
			return nil, err
		}
	}
	s.lookup[entry.series.ID().Hash()] = s.list.PushBack(entry)
	return entry, nil
}

func (s *dbShard) insertSeriesBatch(inserts []dbShardInsert) error {
	anyPendingAction := false
	anyPendingIndexing := false

	s.Lock()
	for i := range inserts {
		entry, _, err := s.lookupEntryWithLock(inserts[i].entry.series.ID())
		if entry != nil {
			// Already exists so update the entry we're pointed at for this insert
			inserts[i].entry = entry

			// Entry already exists, don't need to re-index.
			inserts[i].opts.indexWriteFn = nil
		}

		// If we are going to write to this entry then increment the
		// writer count so it does not look empty immediately after
		// we release the write lock
		hasPendingWrite := inserts[i].opts.hasPendingWrite
		anyPendingIndexing = anyPendingIndexing || inserts[i].opts.indexWriteFn != nil
		hasPendingRetrievedBlock := inserts[i].opts.hasPendingRetrievedBlock
		anyPendingAction = anyPendingAction || hasPendingWrite || hasPendingRetrievedBlock

		if hasPendingWrite || hasPendingRetrievedBlock {
			// We're definitely writing a value, ensure that the pending write is
			// visible before we release the lookup write lock
			inserts[i].entry.incrementReaderWriterCount()
		}
		if err == nil {
			// Already inserted
			continue
		}

		if err != errShardEntryNotFound {
			// Shard is not taking inserts
			s.Unlock()
			s.metrics.insertAsyncInsertErrors.Inc(int64(len(inserts) - i))
			return err
		}

		// Insert still pending, perform the insert
		entry = inserts[i].entry
		if s.newSeriesBootstrapped {
			if err := entry.series.Bootstrap(nil); err != nil {
				s.metrics.insertAsyncBootstrapErrors.Inc(1)
			}
		}
		s.lookup[entry.series.ID().Hash()] = s.list.PushBack(entry)
	}
	s.Unlock()

	if anyPendingIndexing {
		for i := range inserts {
			insert := inserts[i]
			indexFn := insert.opts.indexWriteFn
			if indexFn == nil {
				continue
			}

			ns, id, tags := s.namespace.ID(), insert.entry.series.ID(), insert.entry.series.Tags()
			if err := indexFn(ns, id, tags); err != nil {
				s.metrics.insertAsyncIndexErrors.Inc(int64(len(inserts) - i))
				return err
			}
		}
	}

	if !anyPendingAction {
		return nil
	}

	// Perform any pending writes or pending retrieved blocks outside of lock
	ctx := s.contextPool.Get()
	for i := range inserts {
		var (
			entry       = inserts[i].entry
			readOrWrite = false
		)
		switch {
		case inserts[i].opts.hasPendingWrite:
			readOrWrite = true
			write := inserts[i].opts.pendingWrite
			err := entry.series.Write(ctx, write.timestamp, write.value,
				write.unit, write.annotation)
			if err != nil {
				s.metrics.insertAsyncWriteErrors.Inc(1)
			}
		case inserts[i].opts.hasPendingRetrievedBlock:
			readOrWrite = true
			block := inserts[i].opts.pendingRetrievedBlock
			entry.series.OnRetrieveBlock(block.id, block.start, block.segment)
		}
		if readOrWrite {
			entry.decrementReaderWriterCount()
		}
	}

	// Avoid goroutine spinning up to close this context
	ctx.BlockingClose()

	return nil
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
	reader := series.NewReaderUsingRetriever(id, retriever, onRetrieve, opts)
	return reader.FetchBlocks(ctx, starts)
}

func (s *dbShard) fetchActiveBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	limit int64,
	indexCursor int64,
	opts series.FetchBlocksMetadataOptions,
) (block.FetchBlocksMetadataResults, *int64) {
	var (
		res             = s.opts.FetchBlocksMetadataResultsPool().Get()
		tmpCtx          = context.NewContext()
		nextIndexCursor *int64
	)

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
		metadata := entry.series.FetchBlocksMetadata(tmpCtx, start, end, opts)
		tmpCtx.BlockingClose()

		// If the blocksMetadata is empty, the series have no data within the specified
		// time range so we don't return it to the client
		if len(metadata.Blocks.Results()) == 0 {
			if metadata.ID != nil {
				metadata.ID.Finalize()
			}
			metadata.Blocks.Close()
			return true
		}

		// Otherwise add it to the result which takes care of closing the metadata
		res.Add(metadata)

		return true
	})

	return res, nextIndexCursor
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
	result, nextPageToken := s.fetchActiveBlocksMetadata(ctx, start, end,
		limit, pageToken, seriesFetchBlocksMetadataOpts)
	return result, nextPageToken, nil
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
		result, nextIndexCursor := s.fetchActiveBlocksMetadata(ctx, start, end,
			limit, indexCursor, seriesFetchBlocksMetadataOpts)
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
		result, nextIndexCursor := s.fetchActiveBlocksMetadata(ctx, start, end,
			limit, indexCursor, seriesFetchBlocksMetadataOpts)
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
		result          = s.opts.FetchBlocksMetadataResultsPool().Get()
		ropts           = s.namespace.Options().RetentionOptions()
		blockSize       = ropts.BlockSize()
		blockStart      = end.Truncate(blockSize)
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
		exists := s.namespaceReaderMgr.filesetExistsAt(s.shard, blockStart)
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
			id, size, checksum, err := reader.ReadMetadata()
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
			result.Add(block.NewFetchBlocksMetadataResult(id, blockResult))
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
	bootstrappedSeries map[ident.Hash]result.DatabaseSeriesBlocks,
) error {
	s.Lock()
	if s.bs == bootstrapped {
		s.Unlock()
		return nil
	}
	if s.bs == bootstrapping {
		s.Unlock()
		return errShardIsBootstrapping
	}
	s.bs = bootstrapping
	s.Unlock()

	multiErr := xerrors.NewMultiError()
	for _, dbBlocks := range bootstrappedSeries {
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
				ident.EmptyTagIterator, // TODO(prateek): retrieve tags during bootstrap process, and insert into index
				insertSyncIncReaderWriterCount)
			if err != nil {
				multiErr = multiErr.Add(err)
				continue
			}
		}

		err = entry.series.Bootstrap(dbBlocks.Blocks)
		if err != nil {
			multiErr = multiErr.Add(err)
		}

		// Always decrement the writer count, avoid continue on bootstrap error
		entry.decrementReaderWriterCount()
	}

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
		err := series.Bootstrap(nil)
		multiErr = multiErr.Add(err)
		return true
	})

	// Now iterate flushed time ranges to determine which blocks are
	// retrievable before servicing reads
	fsOpts := s.opts.CommitLogOptions().FilesystemOptions()
	entries := fs.ReadInfoFiles(fsOpts.FilePathPrefix(), s.namespace.ID(), s.shard,
		fsOpts.InfoReaderBufferSize(), fsOpts.DecodingOptions())
	for _, info := range entries {
		at := xtime.FromNanoseconds(info.Start)
		fs := s.FlushState(at)
		if fs.Status != fileOpNotStarted {
			continue // Already recorded progress
		}
		s.markFlushStateSuccess(at)
	}

	s.Lock()
	s.bs = bootstrapped
	s.Unlock()

	return multiErr.FinalError()
}

func (s *dbShard) Flush(
	blockStart time.Time,
	flush persist.Flush,
) error {
	// We don't flush data when the shard is still bootstrapping
	s.RLock()
	if s.bs != bootstrapped {
		s.RUnlock()
		return errShardNotBootstrappedToFlush
	}
	s.RUnlock()

	var multiErr xerrors.MultiError
	prepared, err := flush.Prepare(s.namespace, s.ID(), blockStart)
	multiErr = multiErr.Add(err)

	// No action is necessary therefore we bail out early and there is no need to close.
	if prepared.Persist == nil {
		// NB(r): Need to mark state without mulitErr as success so IsBlockRetrievable can
		// return true when querying if a block is retrievable for this time
		return s.markFlushStateSuccessOrError(blockStart, multiErr.FinalError())
	}

	// If we encounter an error when persisting a series, we continue regardless.
	tmpCtx := context.NewContext()
	s.forEachShardEntry(func(entry *dbShardEntry) bool {
		series := entry.series
		// Use a temporary context here so the stream readers can be returned to
		// pool after we finish fetching flushing the series
		tmpCtx.Reset()
		err := series.Flush(tmpCtx, blockStart, prepared.Persist)
		tmpCtx.BlockingClose()
		multiErr = multiErr.Add(err)
		return true
	})

	if err := prepared.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}

	return s.markFlushStateSuccessOrError(blockStart, multiErr.FinalError())
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

func (s *dbShard) CleanupFileset(earliestToRetain time.Time) error {
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
