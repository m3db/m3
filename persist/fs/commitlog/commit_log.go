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
	"sync"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3x/metrics"
	"github.com/m3db/m3x/time"
)

const (
	commitLogWriteQueue = 32768 // at 1MM write/s allow to fall behind by ~6.5% our deadline
)

var (
	errCommitLogClosed = errors.New("commit log is closed")

	writeRetries    = 2
	writeRetryDelay = time.Millisecond
	timeNow         = time.Now
	timeZero        = time.Time{}
)

type completionFn func(err error)

type commitLog struct {
	sync.RWMutex
	opts  m3db.DatabaseOptions
	nowFn m3db.NowFn
	// TODO(r): replace buffered channel with concurrent striped
	// circular buffer to avoid central write lock contention
	writes         chan commitLogWrite
	writer         commitLogWriter
	pendingAcks    []completionFn
	bitset         bitset
	writerExpireAt time.Time
	closed         bool
	metrics        xmetrics.Scope
}

type commitLogWrite struct {
	series       m3db.CommitLogSeries
	datapoint    m3db.Datapoint
	unit         xtime.Unit
	annotation   []byte
	attempt      int
	completionFn completionFn
}

// NewCommitLog creates a new commit log
func NewCommitLog(opts m3db.DatabaseOptions) (m3db.CommitLog, error) {
	l := &commitLog{
		opts:    opts,
		nowFn:   opts.GetNowFn(),
		writes:  make(chan commitLogWrite, commitLogWriteQueue),
		metrics: opts.GetMetricsScope().SubScope("commitlog"),
	}
	if err := l.openWriter(l.nowFn()); err != nil {
		return nil, err
	}
	// Flush the info header to ensure we can write to disk
	if err := l.writer.Flush(); err != nil {
		return nil, err
	}
	go l.write()
	return l, nil
}

func (l *commitLog) write() {
	// TODO(r): also periodically flush the underlying commit log writer
	// in case writes stall and commit log not written
	var (
		write commitLogWrite
		retry = commitLogWrite{attempt: writeRetries}
		open  bool
	)
	for {
		if retry.attempt < writeRetries {
			retry.attempt++
			write = retry
			time.Sleep(writeRetryDelay)
		} else {
			write, open = <-l.writes
			if !open {
				break
			}
		}

		now := l.nowFn()
		if !now.Before(l.writerExpireAt) {
			if err := l.openWriter(now); err != nil {
				l.metrics.IncCounter("writes.errors", 1)
				l.metrics.IncCounter("writes.openerrors", 1)
				// Re-attempt next write
				retry = write
				continue
			}
		}

		// For writes requiring acks add to pending acks
		if write.completionFn != nil {
			l.pendingAcks = append(l.pendingAcks, write.completionFn)
		}

		err := l.writer.Write(write.series, write.datapoint, write.unit, write.annotation)
		if err != nil {
			l.metrics.IncCounter("writes.errors", 1)
			// Log error and expire writer
			l.writeError(err)
			// Re-attempt next write
			retry = write
			continue
		}

		l.metrics.IncCounter("writes", 1)
	}

	if l.writer != nil {
		l.writer.Close()
	}
}

func (l *commitLog) onFlush(err error) {
	// Taking the pending acks is safe here as is called by the goroutine
	// enqueuing the pending acks in write method from the writer as a
	// result of a write
	if len(l.pendingAcks) == 0 {
		return
	}

	// Safe to callback acks as all they do is record a result and unblock
	// another goroutine
	for i := range l.pendingAcks {
		l.pendingAcks[i](err)
	}
	l.pendingAcks = l.pendingAcks[:0]
}

func (l *commitLog) openWriter(now time.Time) error {
	log := l.opts.GetLogger()
	if l.writer != nil {
		if err := l.writer.Close(); err != nil {
			log.Errorf("failed to close commit log: %v", err)
			// If we failed to close then create a new commit log writer
			l.writer = nil
		}
	}
	if l.writer == nil {
		l.writer = newCommitLogWriter(l.onFlush, l.opts)
	}

	blockSize := l.opts.GetBlockSize()
	start := now.Truncate(blockSize)
	if err := l.writer.Open(start, blockSize); err != nil {
		log.Errorf("failed to open new commit log: %v", err)
		return err
	}

	l.writerExpireAt = start.Add(blockSize)
	return nil
}

func (l *commitLog) writeError(err error) {
	log := l.opts.GetLogger()
	log.Errorf("failed to write commit log entry: %v", err)
	// Explicitly expire the writer so we reopen a new commit log
	l.writerExpireAt = timeZero
}

func (l *commitLog) Write(
	series m3db.CommitLogSeries,
	datapoint m3db.Datapoint,
	unit xtime.Unit,
	annotation []byte,
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

	l.writes <- commitLogWrite{
		series:       series,
		datapoint:    datapoint,
		unit:         unit,
		annotation:   annotation,
		completionFn: completion,
	}

	l.RUnlock()

	wg.Wait()

	return result
}

func (l *commitLog) WriteBehind(
	series m3db.CommitLogSeries,
	datapoint m3db.Datapoint,
	unit xtime.Unit,
	annotation []byte,
) error {
	l.RLock()
	if l.closed {
		l.RUnlock()
		return errCommitLogClosed
	}

	l.writes <- commitLogWrite{
		series:     series,
		datapoint:  datapoint,
		unit:       unit,
		annotation: annotation,
	}

	l.RUnlock()
	return nil
}

func (l *commitLog) Iter() (m3db.CommitLogIterator, error) {
	return NewCommitLogIterator(l.opts)
}

func (l *commitLog) Close() error {
	var err error
	l.Lock()
	if l.closed {
		l.Unlock()
		return nil
	}
	l.closed = true
	close(l.writes)
	l.Unlock()
	return err
}
