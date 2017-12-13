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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
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
	xio "github.com/m3db/m3db/x/io"
	xclose "github.com/m3db/m3x/close"
	xerrors "github.com/m3db/m3x/errors"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

const (
	shardIterateBatchPercent = 0.01
	expireBatchLength        = 1024
)

var (
	errShardEntryNotFound = errors.New("shard entry not found")
	errShardNotOpen       = errors.New("shard is not open")
)

type filesetBeforeFn func(filePathPrefix string, namespace ts.ID, shardID uint32, t time.Time) ([]string, error)

type tickPolicy int

const (
	tickPolicyRegular tickPolicy = iota
	tickPolicyForceExpiry
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
	nsMetadata               namespace.Metadata
	seriesBlockRetriever     series.QueryableBlockRetriever
	shard                    uint32
	increasingIndex          increasingIndex
	seriesPool               series.DatabaseSeriesPool
	commitLogWriter          commitLogWriter
	insertQueue              *dbShardInsertQueue
	lookup                   map[ts.Hash]*list.Element
	list                     *list.List
	bs                       bootstrapState
	newSeriesBootstrapped    bool
	filesetBeforeFn          filesetBeforeFn
	deleteFilesFn            deleteFilesFn
	sleepFn                  func(time.Duration)
	identifierPool           ts.IdentifierPool
	contextPool              context.Pool
	flushState               shardFlushState
	runtimeOptsListenClosers []xclose.SimpleCloser
	currRuntimeOptions       dbShardRuntimeOptions
	metrics                  dbShardMetrics
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
	}
}

type dbShardEntry struct {
	series     series.DatabaseSeries
	index      uint64
	curWriters int32
}

func (entry *dbShardEntry) writerCount() int32 {
	return atomic.LoadInt32(&entry.curWriters)
}

func (entry *dbShardEntry) incrementWriterCount() {
	atomic.AddInt32(&entry.curWriters, 1)
}

func (entry *dbShardEntry) decrementWriterCount() {
	atomic.AddInt32(&entry.curWriters, -1)
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
	increasingIndex increasingIndex,
	commitLogWriter commitLogWriter,
	needsBootstrap bool,
	opts Options,
	seriesOpts series.Options,
) databaseShard {
	scope := opts.InstrumentOptions().MetricsScope().
		SubScope("dbshard")

	d := &dbShard{
		opts:            opts,
		seriesOpts:      seriesOpts,
		nowFn:           opts.ClockOptions().NowFn(),
		state:           dbShardStateOpen,
		nsMetadata:      namespaceMetadata,
		shard:           shard,
		increasingIndex: increasingIndex,
		seriesPool:      opts.DatabaseSeriesPool(),
		commitLogWriter: commitLogWriter,
		lookup:          make(map[ts.Hash]*list.Element),
		list:            list.New(),
		filesetBeforeFn: fs.FilesetBefore,
		deleteFilesFn:   fs.DeleteFiles,
		sleepFn:         time.Sleep,
		identifierPool:  opts.IdentifierPool(),
		contextPool:     opts.ContextPool(),
		flushState:      newShardFlushState(),
		metrics:         newDatabaseShardMetrics(scope),
	}
	d.insertQueue = newDatabaseShardInsertQueue(d.insertSeriesBatch,
		d.nowFn, scope)

	registerRuntimeOptionsListener := func(listener runtime.OptionsListener) {
		elem := opts.RuntimeOptionsManager().RegisterListener(listener)
		d.runtimeOptsListenClosers = append(d.runtimeOptsListenClosers, elem)
	}
	registerRuntimeOptionsListener(d)
	registerRuntimeOptionsListener(d.insertQueue)

	// Start the insert queue after registering runtime options listeners
	// that may immediately fire with values
	d.insertQueue.Start()

	if !needsBootstrap {
		d.bs = bootstrapped
		d.newSeriesBootstrapped = true
	}

	if blockRetriever != nil {
		// If passing the block retriever then set the block retriever
		// and set the series block retriever as the shard itself
		d.DatabaseBlockRetriever = blockRetriever
		d.seriesBlockRetriever = d
	}

	d.metrics.create.Inc(1)

	return d
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

// Stream implements series.SeriesBlockRetriever
func (s *dbShard) Stream(
	id ts.ID,
	start time.Time,
	onRetrieve block.OnRetrieveBlock,
) (xio.SegmentReader, error) {
	return s.DatabaseBlockRetriever.Stream(s.shard, id, start, onRetrieve)
}

// IsBlockRetrievable implements series.SeriesBlockRetriever
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

func (s *dbShard) forEachShardEntry(entryFn dbShardEntryWorkFn) error {
	// NB(r): consider using a lockless list for ticking
	s.RLock()
	elemsLen := s.list.Len()
	batchSize := int(math.Ceil(shardIterateBatchPercent * float64(elemsLen)))
	nextElem := s.list.Front()
	s.RUnlock()

	// TODO(xichen): pool or cache this.
	currEntries := make([]*dbShardEntry, 0, batchSize)
	for nextElem != nil {
		s.RLock()
		nextElem = s.forBatchWithLock(nextElem, &currEntries, batchSize)
		s.RUnlock()
		for _, entry := range currEntries {
			if continueForEach := entryFn(entry); !continueForEach {
				return nil
			}
		}
		for i := range currEntries {
			currEntries[i] = nil
		}
		currEntries = currEntries[:0]
	}
	return nil
}

func (s *dbShard) forBatchWithLock(
	elem *list.Element,
	currEntries *[]*dbShardEntry,
	batchSize int,
) *list.Element {
	var nextElem *list.Element
	for ticked := 0; ticked < batchSize && elem != nil; ticked++ {
		nextElem = elem.Next()
		entry := elem.Value.(*dbShardEntry)
		*currEntries = append(*currEntries, entry)
		elem = nextElem
	}
	return nextElem
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

	// NB(r): Asynchronously we purge expired series to ensure pressure on the
	// GC is not placed all at one time.  If the deadline is too low and still
	// causes the GC to impact performance when closing shards the deadline
	// should be increased.
	cancellable := context.NewNoOpCanncellable()
	s.tickAndExpire(cancellable, tickPolicyForceExpiry)

	return nil
}

func (s *dbShard) Tick(c context.Cancellable) tickResult {
	s.removeAnyFlushStatesTooEarly()
	return s.tickAndExpire(c, tickPolicyRegular)
}

func (s *dbShard) tickAndExpire(
	c context.Cancellable,
	policy tickPolicy,
) tickResult {
	var (
		r       tickResult
		expired []series.DatabaseSeries
		i       int
		slept   time.Duration
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
		case tickPolicyForceExpiry:
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

	return r
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
		// If this series is currently being written to, we don't remove
		// it even though it's empty in that it might become non-empty soon.
		if entry.writerCount() > 0 {
			continue
		}
		// If there have been datapoints written to the series since its
		// last empty check, we don't remove it.
		if !series.IsEmpty() {
			continue
		}
		// NB(xichen): if we get here, we are guaranteed that there can be
		// no more writes to this series while the lock is held, so it's
		// safe to remove it.
		series.Close()
		s.list.Remove(elem)
		delete(s.lookup, hash)
	}
	s.Unlock()
}

func (s *dbShard) Write(
	ctx context.Context,
	id ts.ID,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
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
		result, err := s.insertSeriesAsyncBatched(id, insertAsyncOptions{
			hasPendingWrite: false,
		})
		if err != nil {
			return err
		}

		// Wait for the insert to be batched together and inserted
		result.wg.Wait()

		// Retrieve the inserted entry
		entry, err = s.writableSeries(id)
		if err != nil {
			return err
		}
		writable = true
	}

	var (
		commitLogSeriesID          ts.ID
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
		commitLogSeriesUniqueIndex = entry.index
		entry.decrementWriterCount()
		if err != nil {
			return err
		}
	} else {
		// This is an asynchronous insert and write
		result, err := s.insertSeriesAsyncBatched(id, insertAsyncOptions{
			hasPendingWrite: true,
			pendingWrite: dbShardPendingWrite{
				timestamp:  timestamp,
				value:      value,
				unit:       unit,
				annotation: annotation,
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
		commitLogSeriesUniqueIndex = result.entry.index
	}

	// Write commit log
	series := commitlog.Series{
		UniqueIndex: commitLogSeriesUniqueIndex,
		Namespace:   s.nsMetadata.ID(),
		ID:          commitLogSeriesID,
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
	id ts.ID,
	start, end time.Time,
) ([][]xio.SegmentReader, error) {
	s.RLock()
	entry, _, err := s.lookupEntryWithLock(id)
	s.RUnlock()
	if err == errShardEntryNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return entry.series.ReadEncoded(ctx, start, end)
}

// lookupEntryWithLock returns the entry for a given id while holding a read lock or a write lock.
func (s *dbShard) lookupEntryWithLock(id ts.ID) (*dbShardEntry, *list.Element, error) {
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

func (s *dbShard) writableSeries(id ts.ID) (*dbShardEntry, error) {
	for {
		entry, _, err := s.tryRetrieveWritableSeries(id)
		if entry != nil {
			return entry, nil
		}
		if err != nil {
			return nil, err
		}

		// Not inserted, attempt a batched insert
		result, err := s.insertSeriesAsyncBatched(id, insertAsyncOptions{
			hasPendingWrite: false,
		})
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

func (s *dbShard) tryRetrieveWritableSeries(id ts.ID) (
	*dbShardEntry,
	writableSeriesOptions,
	error,
) {
	s.RLock()
	opts := writableSeriesOptions{
		writeNewSeriesAsync: s.currRuntimeOptions.writeNewSeriesAsync,
	}
	if entry, _, err := s.lookupEntryWithLock(id); err == nil {
		entry.incrementWriterCount()
		s.RUnlock()
		return entry, opts, nil
	} else if err != errShardEntryNotFound {
		s.RUnlock()
		return nil, opts, err
	}
	s.RUnlock()
	return nil, opts, nil
}

func (s *dbShard) newShardEntry(id ts.ID) *dbShardEntry {
	series := s.seriesPool.Get()
	seriesID := s.identifierPool.Clone(id)
	series.Reset(seriesID, s.seriesBlockRetriever, s.seriesOpts)
	uniqueIndex := s.increasingIndex.nextIndex()
	return &dbShardEntry{series: series, index: uniqueIndex}
}

type insertAsyncOptions struct {
	hasPendingWrite bool
	pendingWrite    dbShardPendingWrite
}

type insertAsyncResult struct {
	wg       *sync.WaitGroup
	copiedID ts.ID
	// entry is not guaranteed to be the final entry
	// inserted into the shard map in case there is already
	// an existing entry waiting in the insert queue
	entry *dbShardEntry
}

func (s *dbShard) insertSeriesAsyncBatched(
	id ts.ID,
	opts insertAsyncOptions,
) (insertAsyncResult, error) {
	entry := s.newShardEntry(id)

	wg, err := s.insertQueue.Insert(dbShardInsert{
		entry:           entry,
		hasPendingWrite: opts.hasPendingWrite,
		pendingWrite:    opts.pendingWrite,
	})
	return insertAsyncResult{
		wg: wg,
		// Make sure to return the copied ID from the new series
		copiedID: entry.series.ID(),
		entry:    entry,
	}, err
}

type insertSyncType uint8

// nolint: deadcode, varcheck, unused
const (
	insertSync insertSyncType = iota
	insertSyncIncWriterCount
)

func (s *dbShard) insertSeriesSync(
	id ts.ID,
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
		if entry != nil && insertType == insertSyncIncWriterCount {
			entry.incrementWriterCount()
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

	entry = s.newShardEntry(id)
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
	anyPendingWrites := false

	s.Lock()
	for i := range inserts {
		// If we are going to write to this entry then increment the
		// writer count so it does not look empty immediately after
		// we release the write lock
		hasPendingWrite := inserts[i].hasPendingWrite
		anyPendingWrites = anyPendingWrites || hasPendingWrite

		entry, _, err := s.lookupEntryWithLock(inserts[i].entry.series.ID())
		if entry != nil {
			// Already exists so update the entry we're pointed at for this insert
			inserts[i].entry = entry
		}
		if hasPendingWrite {
			// We're definitely writing a value, ensure that the pending write is
			// visible before we release the lookup write lock
			inserts[i].entry.incrementWriterCount()
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

	if !anyPendingWrites {
		return nil
	}

	// Perform any pending writes outside of the lock
	ctx := s.contextPool.Get()
	for i := range inserts {
		if !inserts[i].hasPendingWrite {
			continue
		}
		entry := inserts[i].entry
		write := inserts[i].pendingWrite
		err := entry.series.Write(ctx, write.timestamp, write.value, write.unit, write.annotation)
		if err != nil {
			s.metrics.insertAsyncWriteErrors.Inc(1)
		}
		entry.decrementWriterCount()
	}

	// Avoid goroutine spinning up to close this context
	ctx.BlockingClose()

	return nil
}

func (s *dbShard) FetchBlocks(
	ctx context.Context,
	id ts.ID,
	starts []time.Time,
) ([]block.FetchBlockResult, error) {
	s.RLock()
	entry, _, err := s.lookupEntryWithLock(id)
	s.RUnlock()
	if err == errShardEntryNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return entry.series.FetchBlocks(ctx, starts), nil
}

func (s *dbShard) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	limit int64,
	pageToken int64,
	opts block.FetchBlocksMetadataOptions,
) (block.FetchBlocksMetadataResults, *int64) {
	var (
		res            = s.opts.FetchBlocksMetadataResultsPool().Get()
		tmpCtx         = context.NewContext()
		pNextPageToken *int64
	)

	s.forEachShardEntry(func(entry *dbShardEntry) bool {
		// Break out of the iteration loop once we've accumulated enough entries.
		if int64(len(res.Results())) >= limit {
			nextPageToken := int64(entry.index)
			pNextPageToken = &nextPageToken
			return false
		}

		// Fast forward past indexes lower than page token
		if int64(entry.index) < pageToken {
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

	return res, pNextPageToken
}

func (s *dbShard) Bootstrap(
	bootstrappedSeries map[ts.Hash]result.DatabaseSeriesBlocks,
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
			entry, err = s.insertSeriesSync(dbBlocks.ID, insertSyncIncWriterCount)
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
		entry.decrementWriterCount()
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
	prepared, err := flush.Prepare(s.nsMetadata, s.ID(), blockStart)
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
	earliestFlush := retention.FlushTimeStart(s.nsMetadata.Options().RetentionOptions(), now)
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
	expired, err := s.filesetBeforeFn(filePathPrefix, s.nsMetadata.ID(), s.ID(), earliestToRetain)
	if err != nil {
		detailedErr :=
			fmt.Errorf("encountered errors when getting fileset files for prefix %s namespace %s shard %d: %v",
				filePathPrefix, s.nsMetadata.ID(), s.ID(), err)
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
	return repairer.Repair(ctx, s.nsMetadata.ID(), tr, s)
}
