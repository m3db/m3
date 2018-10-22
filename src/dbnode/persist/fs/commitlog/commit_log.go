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
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
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
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error

type commitLogFailFn func(err error)

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

	opts  Options
	nowFn clock.NowFn
	log   xlog.Logger

	newCommitLogWriterFn newCommitLogWriterFn
	writeFn              writeCommitLogFn
	commitLogFailFn      commitLogFailFn

	metrics commitLogMetrics
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
	activeFile     *File
}

type closedState struct {
	sync.RWMutex
	closed bool
}

type commitLogMetrics struct {
	queued        tally.Gauge
	queueCapacity tally.Gauge
	success       tally.Counter
	errors        tally.Counter
	openErrors    tally.Counter
	closeErrors   tally.Counter
	flushErrors   tally.Counter
	flushDone     tally.Counter
}

type eventType int

// nolint: varcheck, unused
const (
	writeEventType eventType = iota
	flushEventType
	activeLogsEventType
)

type callbackFn func(callbackResult)

type callbackResult struct {
	eventType  eventType
	err        error
	activeLogs activeLogsCallbackResult
}

type activeLogsCallbackResult struct {
	file *File
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

type commitLogWrite struct {
	eventType eventType

	series     Series
	datapoint  ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
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
		writes:               make(chan commitLogWrite, opts.BacklogQueueSize()),
		closeErr:             make(chan error),
		metrics: commitLogMetrics{
			queued:        scope.Gauge("writes.queued"),
			queueCapacity: scope.Gauge("writes.queue-capacity"),
			success:       scope.Counter("writes.success"),
			errors:        scope.Counter("writes.errors"),
			openErrors:    scope.Counter("writes.open-errors"),
			closeErrors:   scope.Counter("writes.close-errors"),
			flushErrors:   scope.Counter("writes.flush-errors"),
			flushDone:     scope.Counter("writes.flush-done"),
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
	if err := l.openWriter(l.nowFn()); err != nil {
		return err
	}

	// Sync the info header to ensure we can write to disk and make sure that we can at least
	// read the info about the commitlog file later.
	if err := l.writerState.writer.Sync(); err != nil {
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

func (l *commitLog) ActiveLogs() ([]File, error) {
	l.closedState.RLock()
	defer l.closedState.RUnlock()

	if l.closedState.closed {
		return nil, errCommitLogClosed
	}

	var (
		err   error
		files []File
		wg    = sync.WaitGroup{}
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

func (l *commitLog) flushEvery(interval time.Duration) {
	// Periodically flush the underlying commit log writer to cover
	// the case when writes stall for a considerable time
	var sleepForOverride time.Duration

	for {
		l.metrics.queued.Update(float64(len(l.writes)))
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
	for write := range l.writes {
		if write.eventType == flushEventType {
			// TODO(rartoul): This should probably be replaced with a call to Sync() as the expectation
			// is that the commitlog will actually FSync the data at regular intervals, whereas Flush
			// just ensures that the writers buffer flushes to the chunkWriter (creating a new chunk), but
			// does not guarantee that the O.S isn't still buffering the data. Leaving as is for now as making
			// this change will require extensive benchmarking in production clusters.
			l.writerState.writer.Flush()
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

		if now := l.nowFn(); !now.Before(l.writerState.writerExpireAt) {
			err := l.openWriter(now)
			if err != nil {
				l.metrics.errors.Inc(1)
				l.metrics.openErrors.Inc(1)
				l.log.Errorf("failed to open commit log: %v", err)

				if l.commitLogFailFn != nil {
					l.commitLogFailFn(err)
				}

				continue
			}
		}

		err := l.writerState.writer.Write(write.series,
			write.datapoint, write.unit, write.annotation)

		if err != nil {
			l.metrics.errors.Inc(1)
			l.log.Errorf("failed to write to commit log: %v", err)

			if l.commitLogFailFn != nil {
				l.commitLogFailFn(err)
			}

			continue
		}
		l.metrics.success.Inc(1)
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
func (l *commitLog) openWriter(now time.Time) error {
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
		return err
	}

	l.writerState.activeFile = &file
	l.writerState.writerExpireAt = start.Add(blockSize)

	return nil
}

func (l *commitLog) Write(
	ctx context.Context,
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error {
	return l.writeFn(ctx, series, datapoint, unit, annotation)
}

func (l *commitLog) writeWait(
	ctx context.Context,
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
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

	write := commitLogWrite{
		series:     series,
		datapoint:  datapoint,
		unit:       unit,
		annotation: annotation,
		callbackFn: completion,
	}

	enqueued := false

	select {
	case l.writes <- write:
		enqueued = true
	default:
	}

	l.closedState.RUnlock()

	if !enqueued {
		return ErrCommitLogQueueFull
	}

	wg.Wait()

	return result
}

func (l *commitLog) writeBehind(
	ctx context.Context,
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error {
	l.closedState.RLock()
	if l.closedState.closed {
		l.closedState.RUnlock()
		return errCommitLogClosed
	}

	write := commitLogWrite{
		series:     series,
		datapoint:  datapoint,
		unit:       unit,
		annotation: annotation,
	}

	enqueued := false

	select {
	case l.writes <- write:
		enqueued = true
	default:
	}

	l.closedState.RUnlock()

	if !enqueued {
		return ErrCommitLogQueueFull
	}

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
