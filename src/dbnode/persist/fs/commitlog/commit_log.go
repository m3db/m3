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
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	// ErrCommitLogQueueFull is raised when trying to write to the commit log
	// when the queue is full
	ErrCommitLogQueueFull = errors.New("commit log queue is full")

	errCommitLogClosed = errors.New("commit log is closed")
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

	writes       chan commitLogWrite
	maxQueueSize int64

	opts  Options
	nowFn clock.NowFn
	log   *zap.Logger

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
	// See "Rotating Files" section of README.md for an explanation of how the
	// primaryWriter and secondaryWriter fields are used during commitlog rotation.
	primaryWriter   asyncResettableWriter
	secondaryWriter asyncResettableWriter
	activeFiles     persist.CommitLogFiles
}

type asyncResettableWriter struct {
	// The commitlog writer is single-threaded, so normally the commitLogWriter can be
	// accessed without synchronization. However, since the secondaryWriter is reset by
	// a background goroutine, a waitgroup is used to ensure that the previous background
	// reset has completed before attempting to access the secondary writers state and/or
	// begin a new hot-swap.
	*sync.WaitGroup
	writer commitLogWriter
	// Each writer maintains its own slice of pending flushFns because each writer will get
	// flushed independently. This is important for maintaining correctness in code paths
	// that care about durability, particularly during commitlog rotations.
	//
	// For example, imagine a call to WriteWait() occurs and the pending write is buffered
	// in commitlog 1, but not yet flushed. Subsequently, a call to RotateLogs() occurs causing
	// commitlog 1 to be (asynchronously) reset and commitlog 2 to become the new primary. Once
	// the asynchronous Close and flush of commitlog 1 completes, only pending flushFns associated
	// with commitlog 1 should be called as the writer associated with commitlog 2 may not have been
	// flushed at all yet.
	pendingFlushFns []callbackFn
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
	files persist.CommitLogFiles
}

type rotateLogsResult struct {
	file persist.CommitLogFile
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
		writerState: writerState{
			primaryWriter: asyncResettableWriter{
				WaitGroup: &sync.WaitGroup{},
			},
			secondaryWriter: asyncResettableWriter{
				WaitGroup: &sync.WaitGroup{},
			},
		},
		maxQueueSize: int64(opts.BacklogQueueSize()),
		closeErr:     make(chan error),
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
	if _, err := l.openWriters(); err != nil {
		return err
	}

	// Sync the info header to ensure we can write to disk and make sure that we can at least
	// read the info about the commitlog file later.
	if err := l.writerState.primaryWriter.writer.Flush(true); err != nil {
		return err
	}

	l.commitLogFailFn = func(err error) {
		l.log.Fatal("fatal commit log error", zap.Error(err))
	}

	// Asynchronously write
	go l.write()

	if flushInterval := l.opts.FlushInterval(); flushInterval > 0 {
		// Continually flush the commit log at given interval if set
		go l.flushEvery(flushInterval)
	}

	return nil
}

func (l *commitLog) ActiveLogs() (persist.CommitLogFiles, error) {
	l.closedState.RLock()
	defer l.closedState.RUnlock()

	if l.closedState.closed {
		return nil, errCommitLogClosed
	}

	var (
		err   error
		files []persist.CommitLogFile
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

			files = result.files
		},
	}

	wg.Wait()

	if err != nil {
		return nil, err
	}

	return files, nil
}

func (l *commitLog) RotateLogs() (persist.CommitLogFile, error) {
	l.closedState.RLock()
	defer l.closedState.RUnlock()

	if l.closedState.closed {
		return persist.CommitLogFile{}, errCommitLogClosed
	}

	var (
		err  error
		file persist.CommitLogFile
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
		return persist.CommitLogFile{}, err
	}

	return file, nil
}

func (l *commitLog) QueueLength() int64 {
	return atomic.LoadInt64(&l.numWritesInQueue)
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
			l.writerState.primaryWriter.writer.Flush(false)
			continue
		}

		if write.eventType == activeLogsEventType {
			write.callbackFn(callbackResult{
				eventType: write.eventType,
				err:       nil,
				activeLogs: activeLogsCallbackResult{
					files: l.writerState.activeFiles,
				},
			})
			continue
		}

		// For writes requiring acks add to pending acks
		if write.eventType == writeEventType && write.callbackFn != nil {
			l.writerState.primaryWriter.pendingFlushFns = append(
				l.writerState.primaryWriter.pendingFlushFns, write.callbackFn)
		}

		isRotateLogsEvent := write.eventType == rotateLogsEventType
		if isRotateLogsEvent {
			files, err := l.openWriters()
			if err != nil {
				l.metrics.errors.Inc(1)
				l.metrics.openErrors.Inc(1)
				l.log.Error("failed to open commit log", zap.Error(err))

				if l.commitLogFailFn != nil {
					l.commitLogFailFn(err)
				}
			}

			var file persist.CommitLogFile
			if err == nil {
				// The contract for the RotateLogs() API is that it will return the
				// commitlog file that became the primary one as a direct result of
				// the method call. The new active file corresponds to index zero and
				// the new secondary file corresponds to index 1.
				file = files[0]
			}

			write.callbackFn(callbackResult{
				eventType: write.eventType,
				err:       err,
				rotateLogs: rotateLogsResult{
					file: file,
				},
			})

			continue
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

			if writeBatch.SkipWrite {
				// This entry should not be written to the commitlog as it is a duplicate
				// datapoint.
				continue
			}

			write := writeBatch.Write
			err := l.writerState.primaryWriter.writer.Write(write.Series,
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

	// Ensure that there is no active background goroutine in the middle of reseting
	// the secondary writer / modifying its state.
	l.waitForSecondaryWriterAsyncResetComplete()
	// Can be nil in the case where the background goroutine spawned in openWriters
	// encountered an error trying to re-open it.
	if l.writerState.secondaryWriter.writer != nil {
		// Don't care about errors closing the secondary writer because it doesn't
		// have any data.
		l.writerState.secondaryWriter.writer.Close()
		l.writerState.secondaryWriter.writer = nil
	}

	writer := l.writerState.primaryWriter.writer
	l.writerState.primaryWriter.writer = nil

	l.closeErr <- writer.Close()
}

// newOnFlushFn is used to create new flushFns because each one needs to know which
// slide of pendingFlushFns it should modify.
func (l *commitLog) newOnFlushFn(writer *asyncResettableWriter) flushFn {
	return func(err error) {
		l.flushState.setLastFlushAt(l.nowFn())

		if err != nil {
			l.metrics.errors.Inc(1)
			l.metrics.flushErrors.Inc(1)
			l.log.Error("failed to flush commit log", zap.Error(err))

			if l.commitLogFailFn != nil {
				l.commitLogFailFn(err)
			}
		}

		// onFlush will never be called concurrently. The flushFn for the primaryWriter
		// will only ever be called synchronously by the single-threaded writer goroutine
		// and the flushFn for the secondaryWriter will only be called by the asynchronous
		// goroutine (created by the single-threaded writer) when it calls Close() on the
		// secondary (previously primary due to a hot-swap) writer during the reset.
		//
		// Note that both the primary and secondar's flushFn may be called during calls to
		// Open() on the commitlog, but this takes place before the single-threaded writer
		// is spawned which precludes it from occurring concurrently with either of the
		// scenarios described above.
		if len(writer.pendingFlushFns) == 0 {
			l.metrics.flushDone.Inc(1)
			return
		}

		for i := range writer.pendingFlushFns {
			writer.pendingFlushFns[i](callbackResult{
				eventType: flushEventType,
				err:       err,
			})
			writer.pendingFlushFns[i] = nil
		}
		writer.pendingFlushFns = writer.pendingFlushFns[:0]
		l.metrics.flushDone.Inc(1)
	}
}

// writerState lock must be held for the duration of this function call.
func (l *commitLog) openWriters() (persist.CommitLogFiles, error) {
	// Ensure that the previous asynchronous reset of the secondary writer (if any)
	// has completed before attempting to start a new one and/or modify the writerState
	// in any way.
	l.waitForSecondaryWriterAsyncResetComplete()

	if l.writerState.primaryWriter.writer == nil || l.writerState.secondaryWriter.writer == nil {
		// If either of the commitlog writers is nil then open both of them synchronously. Under
		// normal circumstances this will only occur when the commitlog is first opened.
		primaryWriterFlushFn := l.newOnFlushFn(&l.writerState.primaryWriter)
		secondaryWriterFlushFn := l.newOnFlushFn(&l.writerState.secondaryWriter)
		l.writerState.primaryWriter.writer = l.newCommitLogWriterFn(primaryWriterFlushFn, l.opts)
		l.writerState.secondaryWriter.writer = l.newCommitLogWriterFn(secondaryWriterFlushFn, l.opts)

		primaryFile, err := l.writerState.primaryWriter.writer.Open()
		if err != nil {
			return nil, err
		}

		secondaryFile, err := l.writerState.secondaryWriter.writer.Open()
		if err != nil {
			return nil, err
		}

		l.writerState.activeFiles = persist.CommitLogFiles{primaryFile, secondaryFile}
		return l.writerState.activeFiles, nil
	}

	// Swap the primary and secondary writers so that the secondary becomes primary and vice versa.
	// This consumes the standby secondary writer, but a new one will be prepared asynchronously by
	// resetting the formerly primary writer.
	prevPrimary := l.writerState.primaryWriter
	l.writerState.primaryWriter = l.writerState.secondaryWriter
	l.writerState.secondaryWriter = prevPrimary
	l.startSecondaryWriterAsyncReset()

	var (
		// Determine the persist.CommitLogFile for the not-yet-created secondary file so that the
		// ActiveLogs() API returns the correct values even before the asynchronous reset completes.
		primaryFile   = l.writerState.activeFiles[1]
		fsPrefix      = l.opts.FilesystemOptions().FilePathPrefix()
		nextIndex     = primaryFile.Index + 1
		secondaryFile = persist.CommitLogFile{
			FilePath: fs.CommitLogFilePath(fsPrefix, int(nextIndex)),
			Index:    nextIndex,
		}
	)
	files := persist.CommitLogFiles{primaryFile, secondaryFile}
	l.writerState.activeFiles = files

	return files, nil
}

func (l *commitLog) startSecondaryWriterAsyncReset() {
	l.writerState.secondaryWriter.Add(1)

	go func() {
		var err error
		defer func() {
			if err != nil {
				// Set to nil so that the next call to openWriters() will attempt
				// to try and create a new writer.
				l.writerState.secondaryWriter.writer = nil

				l.metrics.errors.Inc(1)
				l.metrics.openErrors.Inc(1)
			}

			l.writerState.secondaryWriter.Done()
		}()

		if err = l.writerState.secondaryWriter.writer.Close(); err != nil {
			l.commitLogFailFn(err)
			return
		}

		_, err = l.writerState.secondaryWriter.writer.Open()
		if err != nil {
			l.commitLogFailFn(err)
			return
		}
	}()
}

func (l *commitLog) waitForSecondaryWriterAsyncResetComplete() {
	l.writerState.secondaryWriter.Wait()
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
	numEnqueued := atomic.AddInt64(&l.numWritesInQueue, numToEnqueue)

	// If we exceeded the limit, decrement the number of enqueued writes and bail.
	if numEnqueued > l.maxQueueSize {
		atomic.AddInt64(&l.numWritesInQueue, -numToEnqueue)
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
	numEnqueued := atomic.AddInt64(&l.numWritesInQueue, numToEnqueue)

	// If we exceeded the limit, decrement the number of enqueued writes and bail.
	if numEnqueued > l.maxQueueSize {
		atomic.AddInt64(&l.numWritesInQueue, -numToEnqueue)
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
	l.log.Error("failed to write to commit log", zap.Error(err))

	if l.commitLogFailFn != nil {
		l.commitLogFailFn(err)
	}
}
