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

package commitlog

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/context"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	// ErrCommitLogQueueFull is raised when trying to write to the commit log
	// when the queue is full
	ErrCommitLogQueueFull = errors.New("commit log queue is full")

	errCommitLogClosed = errors.New("commit log is closed")

	timeZero = time.Time{}
)

type newCommitLogWriterFn func(
	flushFn flushFn,
	opts Options,
) commitLogWriter

type writeCommitLogFn func(
	ctx context.Context,
	writes writeOrWriteBatch,
) error

type commitLogFailFn func(err error)

// writeOrWriteBatch is a union type of write or writeBatch so that
// we can handle both cases without having to allocate as slice of size
// 1 to handle a single write.
type writeOrWriteBatch struct {
	write      ts.Write
	writeBatch ts.WriteBatch
}

type commitLog struct {
	// The commitlog has two different locks that it maintains:
	//
	// 1) The closedState lock is acquired and held for any actions taking place that
	// the commitlog must remain open for the duration of (or for changing the state
	// of the commitlog to closed).
	//
	//
	// 2) The flushState is only used for reading and writing the lastFlushAt variable. The scope
	// of the flushState lock is very limited and is hidden behind helper methods for getting and
	// setting the value of lastFlushAt.
	closedState closedState
	flushState  flushState

	writerState writerState

	// Associated with the closedState, but stored separately since
	// it does not require the closedState lock to be acquired before
	// being accessed.
	closeErr chan error

	writes          chan commitLogWrite
	pendingFlushFns []callbackFn
	maxQueueSize    int64

	opts  Options
	nowFn clock.NowFn
	log   xlog.Logger

	newCommitLogWriterFn newCommitLogWriterFn
	writeFn              writeCommitLogFn
	commitLogFailFn      commitLogFailFn

	metrics commitLogMetrics

	numWritesInQueue int64
}

// Use the helper methods when interacting with this struct, the mutex
// should never need to be manually interacted with.
type flushState struct {
	sync.RWMutex
	lastFlushAt time.Time
}

func (f *flushState) setLastFlushAt(t time.Time) {
	f.Lock()
	f.lastFlushAt = t
	f.Unlock()
}

func (f *flushState) getLastFlushAt() time.Time {
	f.RLock()
	lastFlush := f.lastFlushAt
	f.RUnlock()
	return lastFlush
}

type writerState struct {
	writer         commitLogWriter
	writerExpireAt time.Time
	activeFile     *persist.CommitlogFile
}

type closedState struct {
	sync.RWMutex
	closed bool
}

type commitLogMetrics struct {
	numWritesInQueue tally.Gauge
	queueLength      tally.Gauge
	queueCapacity    tally.Gauge
	success          tally.Counter
	errors           tally.Counter
	openErrors       tally.Counter
	closeErrors      tally.Counter
	flushErrors      tally.Counter
	flushDone        tally.Counter
}

type eventType int

// nolint: varcheck, unused
const (
	writeEventType eventType = iota
	flushEventType
	activeLogsEventType
	rotateLogsEventType
)

type callbackFn func(callbackResult)

type callbackResult struct {
	eventType  eventType
	err        error
	activeLogs activeLogsCallbackResult
	rotateLogs rotateLogsResult
}

type activeLogsCallbackResult struct {
	file *persist.CommitlogFile
}

type rotateLogsResult struct {
	file persist.CommitlogFile
}

func (r callbackResult) activeLogsCallbackResult() (activeLogsCallbackResult, error) {
	if r.eventType != activeLogsEventType {
		return activeLogsCallbackResult{}, fmt.Errorf(
			"wrong event type: expected %d but got %d",
			activeLogsEventType, r.eventType)
	}

	if r.err != nil {
		return activeLogsCallbackResult{}, nil
	}

	return r.activeLogs, nil
}

func (r callbackResult) rotateLogsResult() (rotateLogsResult, error) {
	if r.eventType != rotateLogsEventType {
		return rotateLogsResult{}, fmt.Errorf(
			"wrong event type: expected %d but got %d",
			rotateLogsEventType, r.eventType)
	}

	if r.err != nil {
		return rotateLogsResult{}, nil
	}

	return r.rotateLogs, nil
}

type commitLogWrite struct {
	eventType  eventType
	write      writeOrWriteBatch
	callbackFn callbackFn
}

// NewCommitLog creates a new commit log
func NewCommitLog(opts Options) (CommitLog, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	iopts := opts.InstrumentOptions().SetMetricsScope(
		opts.InstrumentOptions().MetricsScope().SubScope("commitlog"))
	scope := iopts.MetricsScope()

	commitLog := &commitLog{
		opts:                 opts,
		nowFn:                opts.ClockOptions().NowFn(),
		log:                  iopts.Logger(),
		newCommitLogWriterFn: newCommitLogWriter,
		writes:               make(chan commitLogWrite, opts.BacklogQueueChannelSize()),
		maxQueueSize:         int64(opts.BacklogQueueSize()),
		closeErr:             make(chan error),
		metrics: commitLogMetrics{
			numWritesInQueue: scope.Gauge("writes.queued"),
			queueLength:      scope.Gauge("writes.queue-length"),
			queueCapacity:    scope.Gauge("writes.queue-capacity"),
			success:          scope.Counter("writes.success"),
			errors:           scope.Counter("writes.errors"),
			openErrors:       scope.Counter("writes.open-errors"),
			closeErrors:      scope.Counter("writes.close-errors"),
			flushErrors:      scope.Counter("writes.flush-errors"),
			flushDone:        scope.Counter("writes.flush-done"),
		},
	}

	switch opts.Strategy() {
	case StrategyWriteWait:
		commitLog.writeFn = commitLog.writeWait
	default:
		commitLog.writeFn = commitLog.writeBehind
	}

	return commitLog, nil
}

func (l *commitLog) Open() error {
	l.closedState.Lock()
	defer l.closedState.Unlock()

	// Open the buffered commit log writer
	if _, err := l.openWriter(l.nowFn()); err != nil {
		return err
	}

	// Sync the info header to ensure we can write to disk and make sure that we can at least
	// read the info about the commitlog file later.
	if err := l.writerState.writer.Flush(true); err != nil {
		return err
	}

	// NB(r): In the future we can introduce a commit log failure policy
	// similar to Cassandra's "stop", for example see:
	// https://github.com/apache/cassandra/blob/6dfc1e7eeba539774784dfd650d3e1de6785c938/conf/cassandra.yaml#L232
	// Right now it is a large amount of coordination to implement something similar.
	l.commitLogFailFn = func(err error) {
		l.log.Fatalf("fatal commit log error: %v", err)
	}

	// Asynchronously write
	go l.write()

	if flushInterval := l.opts.FlushInterval(); flushInterval > 0 {
		// Continually flush the commit log at given interval if set
		go l.flushEvery(flushInterval)
	}

	return nil
}

func (l *commitLog) ActiveLogs() ([]persist.CommitlogFile, error) {
	l.closedState.RLock()
	defer l.closedState.RUnlock()

	if l.closedState.closed {
		return nil, errCommitLogClosed
	}

	var (
		err   error
		files []persist.CommitlogFile
		wg    sync.WaitGroup
	)
	wg.Add(1)

	l.writes <- commitLogWrite{
		eventType: activeLogsEventType,
		callbackFn: func(r callbackResult) {
			defer wg.Done()

			result, e := r.activeLogsCallbackResult()
			if e != nil {
				err = e
				return
			}

			if result.file != nil {
				files = append(files, *result.file)
			}
		},
	}

	wg.Wait()

	if err != nil {
		return nil, err
	}

	return files, nil
}

func (l *commitLog) RotateLogs() (persist.CommitlogFile, error) {
	l.closedState.RLock()
	defer l.closedState.RUnlock()

	if l.closedState.closed {
		return persist.CommitlogFile{}, errCommitLogClosed
	}

	var (
		err  error
		file persist.CommitlogFile
		wg   sync.WaitGroup
	)
	wg.Add(1)

	l.writes <- commitLogWrite{
		eventType: rotateLogsEventType,
		callbackFn: func(r callbackResult) {
			defer wg.Done()

			result, e := r.rotateLogsResult()
			file, err = result.file, e
		},
	}

	wg.Wait()

	if err != nil {
		return persist.CommitlogFile{}, err
	}

	return file, nil
}

func (l *commitLog) flushEvery(interval time.Duration) {
	// Periodically flush the underlying commit log writer to cover
	// the case when writes stall for a considerable time
	var sleepForOverride time.Duration

	for {
		// The number of actual metrics / writes in the queue.
		l.metrics.numWritesInQueue.Update(float64(atomic.LoadInt64(&l.numWritesInQueue)))
		// The current length of the queue, different from number of writes due to each
		// item in the queue could (potentially) be a batch of many writes.
		l.metrics.queueLength.Update(float64(len(l.writes)))
		l.metrics.queueCapacity.Update(float64(cap(l.writes)))

		sleepFor := interval

		if sleepForOverride > 0 {
			sleepFor = sleepForOverride
			sleepForOverride = 0
		}

		time.Sleep(sleepFor)

		lastFlushAt := l.flushState.getLastFlushAt()
		if sinceFlush := l.nowFn().Sub(lastFlushAt); sinceFlush < interval {
			// Flushed already recently, sleep until we would next consider flushing
			sleepForOverride = interval - sinceFlush
			continue
		}

		// Request a flush
		l.closedState.RLock()
		if l.closedState.closed {
			l.closedState.RUnlock()
			return
		}

		l.writes <- commitLogWrite{eventType: flushEventType}
		l.closedState.RUnlock()
	}
}

func (l *commitLog) write() {
	// We use these to make the batch and non-batched write paths the same
	// by turning non-batched writes into a batch of size one while avoiding
	// any allocations.
	var singleBatch = make([]ts.BatchWrite, 1)
	var batch []ts.BatchWrite

	for write := range l.writes {
		if write.eventType == flushEventType {
			l.writerState.writer.Flush(false)
			continue
		}

		if write.eventType == activeLogsEventType {
			write.callbackFn(callbackResult{
				eventType: write.eventType,
				err:       nil,
				activeLogs: activeLogsCallbackResult{
					file: l.writerState.activeFile,
				},
			})
			continue
		}

		// For writes requiring acks add to pending acks
		if write.eventType == writeEventType && write.callbackFn != nil {
			l.pendingFlushFns = append(l.pendingFlushFns, write.callbackFn)
		}

		var (
			now                         = l.nowFn()
			isWriteForNextCommitLogFile = !now.Before(l.writerState.writerExpireAt)
			isRotateLogsEvent           = write.eventType == rotateLogsEventType
			shouldRotate                = isRotateLogsEvent || isWriteForNextCommitLogFile
		)

		if shouldRotate {
			file, err := l.openWriter(now)
			if err != nil {
				l.metrics.errors.Inc(1)
				l.metrics.openErrors.Inc(1)
				l.log.Errorf("failed to open commit log: %v", err)

				if l.commitLogFailFn != nil {
					l.commitLogFailFn(err)
				}
			}

			if isRotateLogsEvent {
				write.callbackFn(callbackResult{
					eventType: write.eventType,
					err:       err,
					rotateLogs: rotateLogsResult{
						file: file,
					},
				})
			}

			if err != nil || isRotateLogsEvent {
				continue
			}
		}

		var (
			numWritesSuccess int64
			numDequeued      int
		)

		if write.write.writeBatch == nil {
			singleBatch[0].Write = write.write.write
			batch = singleBatch
		} else {
			batch = write.write.writeBatch.Iter()
		}
		numDequeued = len(batch)

		for _, writeBatch := range batch {
			if writeBatch.Err != nil {
				// This entry was not written successfully to the in-memory datastructures so
				// we should not persist it to the commitlog. This is important to maintain
				// consistency and the integrity of M3DB's business logic, but also because if
				// the write does not succeed to the in-memory datastructures then we don't have
				// access to long-lived identifiers like the seriesID (which is pooled) so
				// attempting to write would cause pooling / lifecycle issues as well.
				continue
			}

			write := writeBatch.Write
			err := l.writerState.writer.Write(write.Series,
				write.Datapoint, write.Unit, write.Annotation)
			if err != nil {
				l.handleWriteErr(err)
				continue
			}
			numWritesSuccess++
		}

		// Return the write batch to the pool.
		if write.write.writeBatch != nil {
			write.write.writeBatch.Finalize()
		}

		atomic.AddInt64(&l.numWritesInQueue, int64(-numDequeued))
		l.metrics.success.Inc(numWritesSuccess)
	}

	writer := l.writerState.writer
	l.writerState.writer = nil

	l.closeErr <- writer.Close()
}

func (l *commitLog) onFlush(err error) {
	l.flushState.setLastFlushAt(l.nowFn())

	if err != nil {
		l.metrics.errors.Inc(1)
		l.metrics.flushErrors.Inc(1)
		l.log.Errorf("failed to flush commit log: %v", err)

		if l.commitLogFailFn != nil {
			l.commitLogFailFn(err)
		}
	}

	// onFlush only ever called by "write()" and "openWriter" or
	// before "write()" begins on "Open()" and there are no other
	// accessors of "pendingFlushFns" so it is safe to read and mutate
	// without a lock here
	if len(l.pendingFlushFns) == 0 {
		l.metrics.flushDone.Inc(1)
		return
	}

	for i := range l.pendingFlushFns {
		l.pendingFlushFns[i](callbackResult{
			eventType: flushEventType,
			err:       err,
		})
		l.pendingFlushFns[i] = nil
	}
	l.pendingFlushFns = l.pendingFlushFns[:0]
	l.metrics.flushDone.Inc(1)
}

// writerState lock must be held for the duration of this function call.
func (l *commitLog) openWriter(now time.Time) (persist.CommitlogFile, error) {
	if l.writerState.writer != nil {
		if err := l.writerState.writer.Close(); err != nil {
			l.metrics.closeErrors.Inc(1)
			l.log.Errorf("failed to close commit log: %v", err)

			// If we failed to close then create a new commit log writer
			l.writerState.writer = nil
		}
	}

	if l.writerState.writer == nil {
		l.writerState.writer = l.newCommitLogWriterFn(l.onFlush, l.opts)
	}

	blockSize := l.opts.BlockSize()
	start := now.Truncate(blockSize)

	file, err := l.writerState.writer.Open(start, blockSize)
	if err != nil {
		return persist.CommitlogFile{}, err
	}

	l.writerState.activeFile = &file
	l.writerState.writerExpireAt = start.Add(blockSize)

	return file, nil
}

func (l *commitLog) Write(
	ctx context.Context,
	series ts.Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error {
	return l.writeFn(ctx, writeOrWriteBatch{
		write: ts.Write{
			Series:     series,
			Datapoint:  datapoint,
			Unit:       unit,
			Annotation: annotation,
		},
	})
}

func (l *commitLog) WriteBatch(
	ctx context.Context,
	writes ts.WriteBatch,
) error {
	return l.writeFn(ctx, writeOrWriteBatch{
		writeBatch: writes,
	})
}

func (l *commitLog) writeWait(
	ctx context.Context,
	write writeOrWriteBatch,
) error {
	l.closedState.RLock()
	if l.closedState.closed {
		l.closedState.RUnlock()
		return errCommitLogClosed
	}

	var (
		wg     sync.WaitGroup
		result error
	)

	wg.Add(1)

	completion := func(r callbackResult) {
		result = r.err
		wg.Done()
	}

	writeToEnqueue := commitLogWrite{
		write:      write,
		callbackFn: completion,
	}

	numToEnqueue := int64(1)
	if writeToEnqueue.write.writeBatch != nil {
		numToEnqueue = int64(len(writeToEnqueue.write.writeBatch.Iter()))
	}

	// Optimistically increment the number of enqueued writes.
	numEnqueued := atomic.AddInt64(&l.numWritesInQueue, int64(numToEnqueue))

	// If we exceeded the limit, decrement the number of enqueued writes and bail.
	if numEnqueued > l.maxQueueSize {
		atomic.AddInt64(&l.numWritesInQueue, int64(-numToEnqueue))
		l.closedState.RUnlock()

		if write.writeBatch != nil {
			// Make sure to finalize the write batch even though we didn't accept the writes
			// so it can be returned to the pool.
			write.writeBatch.Finalize()
		}

		return ErrCommitLogQueueFull
	}

	// Otherwise submit the write.
	l.writes <- commitLogWrite{
		write:      write,
		callbackFn: completion,
	}

	l.closedState.RUnlock()

	wg.Wait()

	return result
}

func (l *commitLog) writeBehind(
	ctx context.Context,
	write writeOrWriteBatch,
) error {
	l.closedState.RLock()
	if l.closedState.closed {
		l.closedState.RUnlock()
		return errCommitLogClosed
	}

	numToEnqueue := int64(1)
	if write.writeBatch != nil {
		numToEnqueue = int64(len(write.writeBatch.Iter()))
	}

	// Optimistically increment the number of enqueued writes.
	numEnqueued := atomic.AddInt64(&l.numWritesInQueue, int64(numToEnqueue))

	// If we exceeded the limit, decrement the number of enqueued writes and bail.
	if numEnqueued > l.maxQueueSize {
		atomic.AddInt64(&l.numWritesInQueue, int64(-numToEnqueue))
		l.closedState.RUnlock()

		if write.writeBatch != nil {
			// Make sure to finalize the write batch even though we didn't accept the writes
			// so it can be returned to the pool.
			write.writeBatch.Finalize()
		}

		return ErrCommitLogQueueFull
	}

	// Otherwise submit the write.
	l.writes <- commitLogWrite{
		write: write,
	}

	l.closedState.RUnlock()

	return nil
}

func (l *commitLog) Close() error {
	l.closedState.Lock()
	if l.closedState.closed {
		l.closedState.Unlock()
		return nil
	}

	l.closedState.closed = true
	close(l.writes)
	l.closedState.Unlock()

	// Receive the result of closing the writer from asynchronous writer
	return <-l.closeErr
}

func (l *commitLog) handleWriteErr(err error) {
	l.metrics.errors.Inc(1)
	l.log.Errorf("failed to write to commit log: %v", err)

	if l.commitLogFailFn != nil {
		l.commitLogFailFn(err)
	}
}
