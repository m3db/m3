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
	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/ts"
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

// testStatsReporter should probably be moved to the tally project for better testing
type testStatsReporter struct {
	m        sync.Mutex
	counters map[string]int64
	gauges   map[string]int64
	timers   map[string]time.Duration
}

func (r *testStatsReporter) ReportCounter(name string, tags map[string]string, value int64) {
	r.m.Lock()
	r.counters[name] += value
	r.m.Unlock()
}

func (r *testStatsReporter) ReportGauge(name string, tags map[string]string, value int64) {
	r.m.Lock()
	r.gauges[name] = value
	r.m.Unlock()
}

func (r *testStatsReporter) ReportTimer(name string, tags map[string]string, interval time.Duration) {
	r.m.Lock()
	r.timers[name] = interval
	r.m.Unlock()
}

func (r *testStatsReporter) Flush() {}

// newTestStatsReporter returns a new TestStatsReporter
func newTestStatsReporter() *testStatsReporter {
	return &testStatsReporter{
		counters: make(map[string]int64),
		gauges:   make(map[string]int64),
		timers:   make(map[string]time.Duration),
	}
}

func newTestOptions(
	t *testing.T,
	overrides overrides,
) (
	Options,
	*testStatsReporter,
) {
	dir, err := ioutil.TempDir("", "foo")
	assert.NoError(t, err)

	var c mclock.Clock
	if overrides.clock != nil {
		c = overrides.clock
	} else {
		c = mclock.New()
	}

	stats := newTestStatsReporter()

	opts := NewOptions().
		ClockOptions(clock.NewOptions().NowFn(c.Now)).
		InstrumentOptions(instrument.NewOptions().MetricsScope(tally.NewRootScope("", nil, stats, 0))).
		FilesystemOptions(fs.NewOptions().FilePathPrefix(dir)).
		RetentionOptions(retention.NewOptions().BlockSize(2 * time.Hour)).
		FlushSize(4096).
		FlushInterval(100 * time.Millisecond).
		BacklogQueueSize(1024)

	if overrides.flushInterval != nil {
		opts = opts.FlushInterval(*overrides.flushInterval)
	}

	if overrides.backlogQueueSize != nil {
		opts = opts.BacklogQueueSize(*overrides.backlogQueueSize)
	}

	return opts, stats
}

func cleanup(t *testing.T, opts Options) {
	filePathPrefix := opts.GetFilesystemOptions().GetFilePathPrefix()
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
		ID:          id,
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
	assert.Equal(t, w.series.ID, series.ID)
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
	fsopts := opts.GetFilesystemOptions()
	files, err := fs.CommitLogFiles(fs.CommitLogsDirPath(fsopts.GetFilePathPrefix()))
	assert.NoError(t, err)
	assert.True(t, len(files) == 1)

	return commitLog
}

type writeCommitLogFn func(
	series Series,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation ts.Annotation,
) error

func writeCommitLogs(
	t *testing.T,
	scope tally.Scope,
	stats *testStatsReporter,
	writeFn writeCommitLogFn,
	writes []testWrite,
) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	getAllWrites := func() int {
		scope.Report(stats)
		stats.m.Lock()
		result := stats.counters["commitlog.writes.success"] +
			stats.counters["commitlog.writes.errors"]
		stats.m.Unlock()
		return int(result)
	}

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
			datapoint := ts.Datapoint{Timestamp: write.t, Value: write.v}

			err := writeFn(write.series, datapoint, write.u, write.a)
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

func waitForFlush(l *commitLog) {
	var currFlushAt, lastFlushAt time.Time
	l.flushMutex.RLock()
	lastFlushAt = l.lastFlushAt
	l.flushMutex.RUnlock()
	matches := 0
	for {
		l.flushMutex.RLock()
		currFlushAt = l.lastFlushAt
		l.flushMutex.RUnlock()
		if currFlushAt.Equal(lastFlushAt) {
			matches++
		} else {
			matches = 0
		}
		if matches >= 10 {
			break
		}
		lastFlushAt = currFlushAt
		time.Sleep(10 * time.Millisecond)
	}
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
	opts, stats := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)
	scope := commitLog.scope

	writes := []testWrite{
		{testSeries(0, "foo.bar", 127), time.Now(), 123.456, xtime.Second, []byte{1, 2, 3}, nil},
		{testSeries(1, "foo.baz", 150), time.Now(), 456.789, xtime.Second, nil, nil},
	}

	// Call write sync
	writeCommitLogs(t, scope, stats, commitLog.Write, writes).Wait()

	// Wait for flush
	waitForFlush(commitLog)

	// Assert writes occurred by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)

	// Close the commit log
	assert.NoError(t, commitLog.Close())
}

func TestCommitLogWriteBehind(t *testing.T) {
	opts, stats := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)
	scope := commitLog.scope

	writes := []testWrite{
		{testSeries(0, "foo.bar", 127), time.Now(), 123.456, xtime.Millisecond, nil, nil},
		{testSeries(1, "foo.baz", 150), time.Now(), 456.789, xtime.Millisecond, nil, nil},
		{testSeries(2, "foo.qux", 291), time.Now(), 789.123, xtime.Millisecond, nil, nil},
	}

	// Call write behind
	writeCommitLogs(t, scope, stats, commitLog.WriteBehind, writes)

	// Wait for flush
	waitForFlush(commitLog)

	// Assert writes occurred by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)

	// Close the commit log
	assert.NoError(t, commitLog.Close())
}

func TestCommitLogWriteErrorOnClosed(t *testing.T) {
	opts, _ := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)
	assert.NoError(t, commitLog.Close())

	series := testSeries(0, "foo.bar", 127)
	datapoint := ts.Datapoint{Timestamp: time.Now(), Value: 123.456}

	err := commitLog.Write(series, datapoint, xtime.Millisecond, nil)
	assert.Error(t, err)
	assert.Equal(t, errCommitLogClosed, err)

	err = commitLog.WriteBehind(series, datapoint, xtime.Millisecond, nil)
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
	for {
		if err := commitLog.WriteBehind(series, dp, unit, nil); err != nil {
			// Ensure queue full error
			assert.Equal(t, errCommitLogQueueFull, err)
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
	opts, stats := newTestOptions(t, overrides{clock: clock})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)
	scope := commitLog.scope

	blockSize := opts.GetRetentionOptions().GetBlockSize()
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
		wg := writeCommitLogs(t, scope, stats, commitLog.Write, []testWrite{write})

		// Flush until finished, this is required as timed flusher not active when clock is mocked
		flushUntilDone(commitLog, wg)
	}

	// Ensure files present for each block size time window
	fsopts := opts.GetFilesystemOptions()
	files, err := fs.CommitLogFiles(fs.CommitLogsDirPath(fsopts.GetFilePathPrefix()))
	assert.NoError(t, err)
	assert.True(t, len(files) == len(writes))

	// Close and consequently flush
	assert.NoError(t, commitLog.Close())

	// Assert write flushed by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)
}

func TestCommitLogFailOnWriteError(t *testing.T) {
	opts, stats := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := NewCommitLog(opts).(*commitLog)
	scope := commitLog.scope
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
	writeCommitLogs(t, scope, stats, commitLog.WriteBehind, writes)

	wg.Wait()

	// Check stats
	scope.Report(stats)
	stats.m.Lock()
	assert.Equal(t, int64(1), stats.counters["commitlog.writes.errors"])
	stats.m.Unlock()
}

func TestCommitLogFailOnOpenError(t *testing.T) {
	opts, stats := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := NewCommitLog(opts).(*commitLog)
	scope := commitLog.scope
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
	writeCommitLogs(t, scope, stats, commitLog.WriteBehind, writes)

	wg.Wait()

	// Check stats
	scope.Report(stats)
	stats.m.Lock()
	assert.Equal(t, int64(1), stats.counters["commitlog.writes.errors"])
	assert.Equal(t, int64(1), stats.counters["commitlog.writes.open-errors"])
	stats.m.Unlock()
}

func TestCommitLogFailOnFlushError(t *testing.T) {
	opts, stats := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := NewCommitLog(opts).(*commitLog)
	scope := commitLog.scope
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
	writeCommitLogs(t, scope, stats, commitLog.WriteBehind, writes)

	wg.Wait()

	// Check stats
	scope.Report(stats)
	stats.m.Lock()
	assert.Equal(t, int64(1), stats.counters["commitlog.writes.errors"])
	assert.Equal(t, int64(1), stats.counters["commitlog.writes.flush-errors"])
	stats.m.Unlock()
}
