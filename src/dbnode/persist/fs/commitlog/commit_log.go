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
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	// ErrCommitLogQueueFull is raised when trying to write to the commit log
	// when the queue is full
	ErrCommitLogQueueFull = errors.New("commit log queue is full")

	errCommitLogClosed        = errors.New("commit log is closed")
	errRotationAlreadyPending = fmt.Errorf(
		"%s rotation: rotation already pending", instrument.InvariantViolatedMetricName)

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

type completionFn func(err error)

type rotationResult struct {
	file File
	err  error
}

type commitLog struct {
	sync.RWMutex
	opts  Options
	nowFn clock.NowFn

	log xlog.Logger

	newCommitLogWriterFn newCommitLogWriterFn
	writeFn              writeCommitLogFn
	commitLogFailFn      commitLogFailFn
	writer               commitLogWriter

	// TODO(r): replace buffered channel with concurrent striped
	// circular buffer to avoid central write lock contention
	writes chan commitLogWrite

	flushMutex      sync.RWMutex
	lastFlushAt     time.Time
	pendingFlushFns []completionFn

	writerExpireAt time.Time
	closed         bool
	closeErr       chan error

	activeFile *File

	metrics commitLogMetrics

	rotationPending  int64
	rotationComplete chan rotationResult
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

type valueType int

// nolint: varcheck, unused
const (
	writeValueType valueType = iota
	flushValueType
)

type commitLogWrite struct {
	valueType valueType

	series       Series
	datapoint    ts.Datapoint
	unit         xtime.Unit
	annotation   ts.Annotation
	completionFn completionFn
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

		rotationComplete: make(chan rotationResult),
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
	l.Lock()
	defer l.Unlock()

	// Open the buffered commit log writer
	if _, err := l.openWriterWithLock(l.nowFn()); err != nil {
		return err
	}

	// Flush the info header to ensure we can write to disk
	if err := l.writer.Flush(); err != nil {
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

func (l *commitLog) Rotate() (File, error) {
	l.Lock()
	defer l.Unlock()

	if l.isRotationPending() {
		// Should never happen
		return File{}, errRotationAlreadyPending
	}

	l.setRotationPending()
	result := <-l.rotationComplete

	// Goroutine that performs the rotation will take care of clearing the rotationPending
	// status.
	return result.file, result.err
}

func (l *commitLog) ActiveLogs() ([]File, error) {
	l.Lock()
	defer l.Unlock()

	if l.closed {
		return nil, errCommitLogClosed
	}

	if l.writer == nil || l.activeFile == nil {
		return nil, nil
	}

	return []File{*l.activeFile}, nil
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

		l.flushMutex.RLock()
		lastFlushAt := l.lastFlushAt
		l.flushMutex.RUnlock()

		if sinceFlush := l.nowFn().Sub(lastFlushAt); sinceFlush < interval {
			// Flushed already recently, sleep until we would next consider flushing
			sleepForOverride = interval - sinceFlush
			continue
		}

		// Request a flush
		l.RLock()
		if l.closed {
			l.RUnlock()
			return
		}

		l.writes <- commitLogWrite{valueType: flushValueType}
		l.RUnlock()
	}
}

func (l *commitLog) write() {
	for write := range l.writes {
		// For writes requiring acks add to pending acks
		if write.completionFn != nil {
			l.pendingFlushFns = append(l.pendingFlushFns, write.completionFn)
		}

		if write.valueType == flushValueType {
			l.writer.Flush()
			continue
		}

		var (
			isRotationPending           = l.isRotationPending()
			now                         = l.nowFn()
			isWriteForNextCommitLogFile = !now.Before(l.writerExpireAt)
		)
		if isRotationPending || isWriteForNextCommitLogFile {
			l.Lock()
			file, err := l.openWriterWithLock(now)
			l.Unlock()
			if err != nil {
				l.metrics.errors.Inc(1)
				l.metrics.openErrors.Inc(1)
				l.log.Errorf("failed to open commit log: %v", err)

				if l.commitLogFailFn != nil {
					l.commitLogFailFn(err)
				}

				continue
			}

			if isRotationPending {
				// TODO: Done?
				l.rotationComplete <- rotationResult{file: file}
			}
		}

		err := l.writer.Write(write.series,
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

	l.Lock()
	defer l.Unlock()

	writer := l.writer
	l.writer = nil
	l.closeErr <- writer.Close()
}

func (l *commitLog) onFlush(err error) {
	l.flushMutex.Lock()
	l.lastFlushAt = l.nowFn()
	l.flushMutex.Unlock()

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
		l.pendingFlushFns[i](err)
		l.pendingFlushFns[i] = nil
	}
	l.pendingFlushFns = l.pendingFlushFns[:0]
	l.metrics.flushDone.Inc(1)
}

func (l *commitLog) openWriterWithLock(now time.Time) (File, error) {
	if l.writer != nil {
		if err := l.writer.Close(); err != nil {
			l.metrics.closeErrors.Inc(1)
			l.log.Errorf("failed to close commit log: %v", err)

			// If we failed to close then create a new commit log writer
			l.writer = nil
		}
	}

	if l.writer == nil {
		l.writer = l.newCommitLogWriterFn(l.onFlush, l.opts)
	}

	blockSize := l.opts.BlockSize()
	start := now.Truncate(blockSize)

	file, err := l.writer.Open(start, blockSize)
	if err != nil {
		return File{}, err
	}

	l.activeFile = &file
	l.writerExpireAt = start.Add(blockSize)

	return file, nil
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
	l.RLock()
	if l.closed {
		l.RUnlock()
		return errCommitLogClosed
	}

	var (
		wg     sync.WaitGroup
		result error
	)

	wg.Add(1)

	completion := func(err error) {
		result = err
		wg.Done()
	}

	write := commitLogWrite{
		series:       series,
		datapoint:    datapoint,
		unit:         unit,
		annotation:   annotation,
		completionFn: completion,
	}

	enqueued := false

	select {
	case l.writes <- write:
		enqueued = true
	default:
	}

	l.RUnlock()

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
	l.RLock()
	if l.closed {
		l.RUnlock()
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

	l.RUnlock()

	if !enqueued {
		return ErrCommitLogQueueFull
	}

	return nil
}

func (l *commitLog) Close() error {
	l.Lock()
	if l.closed {
		l.Unlock()
		return nil
	}

	l.closed = true
	close(l.writes)
	l.Unlock()

	// Receive the result of closing the writer from asynchronous writer
	return <-l.closeErr
}

func (l *commitLog) setRotationPending() {
	atomic.StoreInt64(&l.rotationPending, 1)
}
func (l *commitLog) clearRotationPending() {
	atomic.StoreInt64(&l.rotationPending, 0)
}
func (l *commitLog) isRotationPending() bool {
	return atomic.LoadInt64(&l.rotationPending) == 1
}
