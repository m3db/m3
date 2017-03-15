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
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/time"
	"github.com/uber-go/tally"

	mclock "github.com/facebookgo/clock"
	"github.com/stretchr/testify/assert"
)

type overrides struct {
	clock            *mclock.Mock
	flushInterval    *time.Duration
	backlogQueueSize *int
}

func newTestOptions(
	t *testing.T,
	overrides overrides,
) (
	Options,
	tally.TestScope,
) {
	dir, err := ioutil.TempDir("", "foo")
	assert.NoError(t, err)

	var c mclock.Clock
	if overrides.clock != nil {
		c = overrides.clock
	} else {
		c = mclock.New()
	}

	scope := tally.NewTestScope("", nil)

	opts := NewOptions().
		SetClockOptions(clock.NewOptions().SetNowFn(c.Now)).
		SetInstrumentOptions(instrument.NewOptions().SetMetricsScope(scope)).
		SetFilesystemOptions(fs.NewOptions().SetFilePathPrefix(dir)).
		SetRetentionOptions(retention.NewOptions().SetBlockSize(2 * time.Hour)).
		SetFlushSize(4096).
		SetFlushInterval(100 * time.Millisecond).
		SetBacklogQueueSize(1024)

	if overrides.flushInterval != nil {
		opts = opts.SetFlushInterval(*overrides.flushInterval)
	}

	if overrides.backlogQueueSize != nil {
		opts = opts.SetBacklogQueueSize(*overrides.backlogQueueSize)
	}

	return opts, scope
}

func cleanup(t *testing.T, opts Options) {
	filePathPrefix := opts.FilesystemOptions().FilePathPrefix()
	assert.NoError(t, os.RemoveAll(filePathPrefix))
}

type testWrite struct {
	series      Series
	t           time.Time
	v           float64
	u           xtime.Unit
	a           []byte
	expectedErr error
}

func testSeries(
	uniqueIndex uint64,
	id string,
	shard uint32,
) Series {
	return Series{
		UniqueIndex: uniqueIndex,
		Namespace:   ts.StringID("testNS"),
		ID:          ts.StringID(id),
		Shard:       shard,
	}
}

func (w testWrite) assert(
	t *testing.T,
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation []byte,
) {
	assert.Equal(t, w.series.UniqueIndex, series.UniqueIndex)
	assert.True(t, w.series.ID.Equal(series.ID), fmt.Sprintf("write ID '%s' does not match actual ID '%s'", w.series.ID.String(), series.ID.String()))
	assert.Equal(t, w.series.Shard, series.Shard)
	assert.True(t, w.t.Equal(datapoint.Timestamp))
	assert.Equal(t, datapoint.Value, datapoint.Value)
	assert.Equal(t, w.u, unit)
	assert.Equal(t, w.a, annotation)
}

type mockCommitLogWriter struct {
	openFn  func(start time.Time, duration time.Duration) error
	writeFn func(Series, ts.Datapoint, xtime.Unit, ts.Annotation) error
	flushFn func() error
	closeFn func() error
}

func newMockCommitLogWriter() *mockCommitLogWriter {
	return &mockCommitLogWriter{
		openFn: func(start time.Time, duration time.Duration) error {
			return nil
		},
		writeFn: func(Series, ts.Datapoint, xtime.Unit, ts.Annotation) error {
			return nil
		},
		flushFn: func() error {
			return nil
		},
		closeFn: func() error {
			return nil
		},
	}
}

func (w *mockCommitLogWriter) Open(start time.Time, duration time.Duration) error {
	return w.openFn(start, duration)
}

func (w *mockCommitLogWriter) Write(
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error {
	return w.writeFn(series, datapoint, unit, annotation)
}

func (w *mockCommitLogWriter) Flush() error {
	return w.flushFn()
}

func (w *mockCommitLogWriter) Close() error {
	return w.closeFn()
}

func newTestCommitLog(t *testing.T, opts Options) *commitLog {
	commitLog := NewCommitLog(opts).(*commitLog)
	assert.NoError(t, commitLog.Open())

	// Ensure files present
	fsopts := opts.FilesystemOptions()
	files, err := fs.CommitLogFiles(fs.CommitLogsDirPath(fsopts.FilePathPrefix()))
	assert.NoError(t, err)
	assert.True(t, len(files) == 1)

	return commitLog
}

type writeCommitLogFn func(
	ctx context.Context,
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error

func writeCommitLogs(
	t *testing.T,
	scope tally.TestScope,
	writeFn writeCommitLogFn,
	writes []testWrite,
) *sync.WaitGroup {
	wg := sync.WaitGroup{}

	getAllWrites := func() int {
		snapshot := scope.Snapshot()
		counters := snapshot.Counters()
		result := counters["commitlog.writes.success"].Value() +
			counters["commitlog.writes.errors"].Value()
		return int(result)
	}

	ctx := context.NewContext()
	defer ctx.Close()

	preWrites := getAllWrites()

	for i, write := range writes {
		i := i
		write := write

		// Wait for previous writes to enqueue
		for getAllWrites() != preWrites+i {
			time.Sleep(10 * time.Millisecond)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			series := write.series
			datapoint := ts.Datapoint{Timestamp: write.t, Value: write.v}
			err := writeFn(ctx, series, datapoint, write.u, write.a)

			if write.expectedErr != nil {
				assert.True(t, strings.Contains(fmt.Sprintf("%v", err), fmt.Sprintf("%v", write.expectedErr)))
			} else {
				assert.NoError(t, err)
			}
		}()
	}

	// Wait for all writes to enqueue
	for getAllWrites() != preWrites+len(writes) {
		time.Sleep(10 * time.Millisecond)
	}

	return &wg
}

func flushUntilDone(l *commitLog, wg *sync.WaitGroup) {
	done := uint64(0)
	blockWg := sync.WaitGroup{}
	blockWg.Add(1)
	go func() {
		for atomic.LoadUint64(&done) == 0 {
			l.writes <- commitLogWrite{valueType: flushValueType}
			time.Sleep(time.Millisecond)
		}
		blockWg.Done()
	}()

	go func() {
		wg.Wait()
		atomic.StoreUint64(&done, 1)
	}()

	blockWg.Wait()
}

func assertCommitLogWritesByIterating(t *testing.T, l *commitLog, writes []testWrite) {
	iter, err := l.Iter()
	assert.NoError(t, err)
	defer iter.Close()

	for _, write := range writes {
		assert.True(t, iter.Next())
		series, datapoint, unit, annotation := iter.Current()
		write.assert(t, series, datapoint, unit, annotation)
	}

	assert.NoError(t, iter.Err())
}

func setupCloseOnFail(t *testing.T, l *commitLog) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	wg.Add(1)
	l.commitLogFailFn = func(err error) {
		go func() { l.closeErr <- nil }()
		assert.NoError(t, l.Close())
		wg.Done()
	}
	return &wg
}

func TestCommitLogWrite(t *testing.T) {
	opts, scope := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	writes := []testWrite{
		{testSeries(0, "foo.bar", 127), time.Now(), 123.456, xtime.Second, []byte{1, 2, 3}, nil},
		{testSeries(1, "foo.baz", 150), time.Now(), 456.789, xtime.Second, nil, nil},
	}

	// Call write sync
	writeCommitLogs(t, scope, commitLog.Write, writes).Wait()

	// Close the commit log and consequently flush
	assert.NoError(t, commitLog.Close())

	// Assert writes occurred by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)
}

func TestCommitLogWriteBehind(t *testing.T) {
	opts, scope := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	writes := []testWrite{
		{testSeries(0, "foo.bar", 127), time.Now(), 123.456, xtime.Millisecond, nil, nil},
		{testSeries(1, "foo.baz", 150), time.Now(), 456.789, xtime.Millisecond, nil, nil},
		{testSeries(2, "foo.qux", 291), time.Now(), 789.123, xtime.Millisecond, nil, nil},
	}

	// Call write behind
	writeCommitLogs(t, scope, commitLog.WriteBehind, writes)

	// Close the commit log and consequently flush
	assert.NoError(t, commitLog.Close())

	// Assert writes occurred by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)
}

func TestCommitLogWriteErrorOnClosed(t *testing.T) {
	opts, _ := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)
	assert.NoError(t, commitLog.Close())

	series := testSeries(0, "foo.bar", 127)
	datapoint := ts.Datapoint{Timestamp: time.Now(), Value: 123.456}

	ctx := context.NewContext()
	defer ctx.Close()

	err := commitLog.Write(ctx, series, datapoint, xtime.Millisecond, nil)
	assert.Error(t, err)
	assert.Equal(t, errCommitLogClosed, err)

	err = commitLog.WriteBehind(ctx, series, datapoint, xtime.Millisecond, nil)
	assert.Error(t, err)
	assert.Equal(t, errCommitLogClosed, err)
}

func TestCommitLogWriteErrorOnFull(t *testing.T) {
	// Set backlog of size one and don't automatically flush
	backlogQueueSize := 1
	flushInterval := time.Duration(0)
	opts, _ := newTestOptions(t, overrides{
		backlogQueueSize: &backlogQueueSize,
		flushInterval:    &flushInterval,
	})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	// Test filling queue
	var writes []testWrite
	series := testSeries(0, "foo.bar", 127)
	dp := ts.Datapoint{Timestamp: time.Now(), Value: 123.456}
	unit := xtime.Millisecond

	ctx := context.NewContext()
	defer ctx.Close()

	for {
		if err := commitLog.WriteBehind(ctx, series, dp, unit, nil); err != nil {
			// Ensure queue full error
			assert.Equal(t, ErrCommitLogQueueFull, err)
			break
		}
		writes = append(writes, testWrite{series, dp.Timestamp, dp.Value, unit, nil, nil})

		// Increment timestamp and value for next write
		dp.Timestamp = dp.Timestamp.Add(time.Second)
		dp.Value += 1.0
	}

	// Close and consequently flush
	assert.NoError(t, commitLog.Close())

	// Assert write flushed by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)
}

func TestCommitLogExpiresWriter(t *testing.T) {
	clock := mclock.NewMock()
	opts, scope := newTestOptions(t, overrides{clock: clock})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	blockSize := opts.RetentionOptions().BlockSize()
	alignedStart := clock.Now().Truncate(blockSize)

	// Writes spaced apart by block size
	writes := []testWrite{
		{testSeries(0, "foo.bar", 127), alignedStart, 123.456, xtime.Millisecond, nil, nil},
		{testSeries(1, "foo.baz", 150), alignedStart.Add(1 * blockSize), 456.789, xtime.Millisecond, nil, nil},
		{testSeries(2, "foo.qux", 291), alignedStart.Add(2 * blockSize), 789.123, xtime.Millisecond, nil, nil},
	}

	for _, write := range writes {
		// Set clock to align with the write
		clock.Add(write.t.Sub(clock.Now()))

		// Write entry
		wg := writeCommitLogs(t, scope, commitLog.Write, []testWrite{write})

		// Flush until finished, this is required as timed flusher not active when clock is mocked
		flushUntilDone(commitLog, wg)
	}

	// Ensure files present for each block size time window
	fsopts := opts.FilesystemOptions()
	files, err := fs.CommitLogFiles(fs.CommitLogsDirPath(fsopts.FilePathPrefix()))
	assert.NoError(t, err)
	assert.True(t, len(files) == len(writes))

	// Close and consequently flush
	assert.NoError(t, commitLog.Close())

	// Assert write flushed by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)
}

func TestCommitLogFailOnWriteError(t *testing.T) {
	opts, scope := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := NewCommitLog(opts).(*commitLog)
	writer := newMockCommitLogWriter()

	writer.writeFn = func(Series, ts.Datapoint, xtime.Unit, ts.Annotation) error {
		return fmt.Errorf("an error")
	}

	var opens int64
	writer.openFn = func(start time.Time, duration time.Duration) error {
		if atomic.AddInt64(&opens, 1) >= 2 {
			return fmt.Errorf("an error")
		}
		return nil
	}

	writer.flushFn = func() error {
		commitLog.onFlush(nil)
		return nil
	}

	commitLog.newCommitLogWriterFn = func(
		flushFn flushFn,
		opts Options,
	) commitLogWriter {
		return writer
	}

	assert.NoError(t, commitLog.Open())

	wg := setupCloseOnFail(t, commitLog)

	writes := []testWrite{
		{testSeries(0, "foo.bar", 127), time.Now(), 123.456, xtime.Millisecond, nil, nil},
	}

	writeCommitLogs(t, scope, commitLog.WriteBehind, writes)

	wg.Wait()

	// Check stats
	assert.Equal(t, int64(1), scope.Snapshot().Counters()["commitlog.writes.errors"].Value())
}

func TestCommitLogFailOnOpenError(t *testing.T) {
	opts, scope := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := NewCommitLog(opts).(*commitLog)
	writer := newMockCommitLogWriter()

	var opens int64
	writer.openFn = func(start time.Time, duration time.Duration) error {
		if atomic.AddInt64(&opens, 1) >= 2 {
			return fmt.Errorf("an error")
		}
		return nil
	}

	writer.flushFn = func() error {
		commitLog.onFlush(nil)
		return nil
	}

	commitLog.newCommitLogWriterFn = func(
		flushFn flushFn,
		opts Options,
	) commitLogWriter {
		return writer
	}

	assert.NoError(t, commitLog.Open())

	wg := setupCloseOnFail(t, commitLog)

	func() {
		commitLog.RLock()
		defer commitLog.RUnlock()
		// Expire the writer so it requires a new open
		commitLog.writerExpireAt = timeZero
	}()

	writes := []testWrite{
		{testSeries(0, "foo.bar", 127), time.Now(), 123.456, xtime.Millisecond, nil, nil},
	}

	writeCommitLogs(t, scope, commitLog.WriteBehind, writes)

	wg.Wait()

	// Check stats
	counters := scope.Snapshot().Counters()
	assert.Equal(t, int64(1), counters["commitlog.writes.errors"].Value())
	assert.Equal(t, int64(1), counters["commitlog.writes.open-errors"].Value())
}

func TestCommitLogFailOnFlushError(t *testing.T) {
	opts, scope := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := NewCommitLog(opts).(*commitLog)
	writer := newMockCommitLogWriter()

	var flushes int64
	writer.flushFn = func() error {
		if atomic.AddInt64(&flushes, 1) >= 2 {
			commitLog.onFlush(fmt.Errorf("an error"))
		} else {
			commitLog.onFlush(nil)
		}
		return nil
	}

	commitLog.newCommitLogWriterFn = func(
		flushFn flushFn,
		opts Options,
	) commitLogWriter {
		return writer
	}

	assert.NoError(t, commitLog.Open())

	wg := setupCloseOnFail(t, commitLog)

	writes := []testWrite{
		{testSeries(0, "foo.bar", 127), time.Now(), 123.456, xtime.Millisecond, nil, nil},
	}

	writeCommitLogs(t, scope, commitLog.WriteBehind, writes)

	wg.Wait()

	// Check stats
	counters := scope.Snapshot().Counters()
	assert.Equal(t, int64(1), counters["commitlog.writes.errors"].Value())
	assert.Equal(t, int64(1), counters["commitlog.writes.flush-errors"].Value())
}
