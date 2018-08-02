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
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/bitset"
	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"

	mclock "github.com/facebookgo/clock"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

type overrides struct {
	clock            *mclock.Mock
	flushInterval    *time.Duration
	backlogQueueSize *int
	strategy         Strategy
}

func newTestOptions(
	t *testing.T,
	overrides overrides,
) (
	Options,
	tally.TestScope,
) {
	dir, err := ioutil.TempDir("", "foo")
	require.NoError(t, err)

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
		SetBlockSize(2 * time.Hour).
		SetFlushSize(4096).
		SetFlushInterval(100 * time.Millisecond).
		SetBacklogQueueSize(1024)

	if overrides.flushInterval != nil {
		opts = opts.SetFlushInterval(*overrides.flushInterval)
	}

	if overrides.backlogQueueSize != nil {
		opts = opts.SetBacklogQueueSize(*overrides.backlogQueueSize)
	}

	opts = opts.SetStrategy(overrides.strategy)

	return opts, scope
}

func cleanup(t *testing.T, opts Options) {
	filePathPrefix := opts.FilesystemOptions().FilePathPrefix()
	require.NoError(t, os.RemoveAll(filePathPrefix))
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
	tags ident.Tags,
	shard uint32,
) Series {
	return Series{
		UniqueIndex: uniqueIndex,
		Namespace:   ident.StringID("testNS"),
		ID:          ident.StringID(id),
		Tags:        tags,
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
	require.Equal(t, w.series.UniqueIndex, series.UniqueIndex)
	require.True(t, w.series.ID.Equal(series.ID), fmt.Sprintf("write ID '%s' does not match actual ID '%s'", w.series.ID.String(), series.ID.String()))
	require.Equal(t, w.series.Shard, series.Shard)

	// ident.Tags.Equal will compare length
	require.True(t, w.series.Tags.Equal(series.Tags))

	require.True(t, w.t.Equal(datapoint.Timestamp))
	require.Equal(t, datapoint.Value, datapoint.Value)
	require.Equal(t, w.u, unit)
	require.Equal(t, w.a, annotation)
}

func snapshotCounterValue(
	scope tally.TestScope,
	counter string,
) (tally.CounterSnapshot, bool) {
	counters := scope.Snapshot().Counters()
	c, ok := counters[tally.KeyForPrefixedStringMap(counter, nil)]
	return c, ok
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
	commitLogI, err := NewCommitLog(opts)
	require.NoError(t, err)
	commitLog := commitLogI.(*commitLog)
	require.NoError(t, commitLog.Open())

	// Ensure files present
	fsopts := opts.FilesystemOptions()
	files, err := fs.SortedCommitLogFiles(fs.CommitLogsDirPath(fsopts.FilePathPrefix()))
	require.NoError(t, err)
	require.True(t, len(files) == 1)

	return commitLog
}

func writeCommitLogs(
	t *testing.T,
	scope tally.TestScope,
	commitLog CommitLog,
	writes []testWrite,
) *sync.WaitGroup {
	wg := sync.WaitGroup{}

	getAllWrites := func() int {
		result := int64(0)
		success, ok := snapshotCounterValue(scope, "commitlog.writes.success")
		if ok {
			result += success.Value()
		}
		errors, ok := snapshotCounterValue(scope, "commitlog.writes.errors")
		if ok {
			result += errors.Value()
		}
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
			time.Sleep(time.Microsecond)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			series := write.series
			datapoint := ts.Datapoint{Timestamp: write.t, Value: write.v}
			err := commitLog.Write(ctx, series, datapoint, write.u, write.a)

			if write.expectedErr != nil {
				require.True(t, strings.Contains(fmt.Sprintf("%v", err), fmt.Sprintf("%v", write.expectedErr)))
			} else {
				require.NoError(t, err)
			}
		}()
	}

	// Wait for all writes to enqueue
	for getAllWrites() != preWrites+len(writes) {
		time.Sleep(time.Microsecond)
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

type seriesTestWritesAndReadPosition struct {
	writes       []testWrite
	readPosition int
}

func assertCommitLogWritesByIterating(t *testing.T, l *commitLog, writes []testWrite) {
	iterOpts := IteratorOpts{
		CommitLogOptions:      l.opts,
		FileFilterPredicate:   ReadAllPredicate(),
		SeriesFilterPredicate: ReadAllSeriesPredicate(),
	}
	iter, err := NewIterator(iterOpts)
	require.NoError(t, err)
	defer iter.Close()

	// Convert the writes to be in-order, but keyed by series ID because the
	// commitlog reader only guarantees the same order on disk within a
	// given series
	writesBySeries := map[string]seriesTestWritesAndReadPosition{}
	for _, write := range writes {
		seriesWrites := writesBySeries[write.series.ID.String()]
		if seriesWrites.writes == nil {
			seriesWrites.writes = []testWrite{}
		}
		seriesWrites.writes = append(seriesWrites.writes, write)
		writesBySeries[write.series.ID.String()] = seriesWrites
	}

	for iter.Next() {
		series, datapoint, unit, annotation := iter.Current()

		seriesWrites := writesBySeries[series.ID.String()]
		write := seriesWrites.writes[seriesWrites.readPosition]

		write.assert(t, series, datapoint, unit, annotation)

		seriesWrites.readPosition++
		writesBySeries[series.ID.String()] = seriesWrites
	}

	require.NoError(t, iter.Err())
}

func setupCloseOnFail(t *testing.T, l *commitLog) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	wg.Add(1)
	l.commitLogFailFn = func(err error) {
		go func() { l.closeErr <- nil }()
		require.NoError(t, l.Close())
		wg.Done()
	}
	return &wg
}

func TestCommitLogWrite(t *testing.T) {
	opts, scope := newTestOptions(t, overrides{
		strategy: StrategyWriteWait,
	})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	writes := []testWrite{
		{testSeries(0, "foo.bar", ident.NewTags(ident.StringTag("name1", "val1")), 127), time.Now(), 123.456, xtime.Second, []byte{1, 2, 3}, nil},
		{testSeries(1, "foo.baz", ident.NewTags(ident.StringTag("name2", "val2")), 150), time.Now(), 456.789, xtime.Second, nil, nil},
	}

	// Call write sync
	writeCommitLogs(t, scope, commitLog, writes).Wait()

	// Close the commit log and consequently flush
	require.NoError(t, commitLog.Close())

	// Assert writes occurred by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)
}

func TestReadCommitLogMissingMetadata(t *testing.T) {
	readConc := 4
	// Make sure we're not leaking goroutines
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	opts, scope := newTestOptions(t, overrides{
		strategy: StrategyWriteWait,
	})
	// Set read concurrency so that the parallel path is definitely tested
	opts.SetReadConcurrency(readConc)
	defer cleanup(t, opts)

	// Replace bitset in writer with one that configurably returns true or false
	// depending on the series
	commitLog := newTestCommitLog(t, opts)
	writer := commitLog.writer.(*writer)

	bitSet := bitset.NewBitSet(0)

	// Generate fake series, where approximately half will be missing metadata.
	// This works because the commitlog writer uses the bitset to determine if
	// the metadata for a particular series had already been written to disk.
	allSeries := []Series{}
	for i := 0; i < 200; i++ {
		willNotHaveMetadata := !(i%2 == 0)
		allSeries = append(allSeries, testSeries(
			uint64(i),
			"hax",
			ident.NewTags(ident.StringTag("name", "val")),
			uint32(i%100),
		))
		if willNotHaveMetadata {
			bitSet.Set(uint(i))
		}
	}
	writer.seen = bitSet

	// Generate fake writes for each of the series
	writes := []testWrite{}
	for _, series := range allSeries {
		for i := 0; i < 10; i++ {
			writes = append(writes, testWrite{series, time.Now(), rand.Float64(), xtime.Second, []byte{1, 2, 3}, nil})
		}
	}

	// Call write sync
	writeCommitLogs(t, scope, commitLog, writes).Wait()

	// Close the commit log and consequently flush
	require.NoError(t, commitLog.Close())

	// Make sure we don't panic / deadlock
	iterOpts := IteratorOpts{
		CommitLogOptions:      opts,
		FileFilterPredicate:   ReadAllPredicate(),
		SeriesFilterPredicate: ReadAllSeriesPredicate(),
	}
	iter, err := NewIterator(iterOpts)
	require.NoError(t, err)
	for iter.Next() {
		require.NoError(t, iter.Err())
	}
	require.Equal(t, errCommitLogReaderMissingMetadata, iter.Err())
	iter.Close()
	require.NoError(t, commitLog.Close())
}

func TestCommitLogReaderIsNotReusable(t *testing.T) {
	// Make sure we're not leaking goroutines
	defer leaktest.CheckTimeout(t, time.Second)()

	overrideFlushInterval := 10 * time.Millisecond
	opts, scope := newTestOptions(t, overrides{
		strategy:      StrategyWriteWait,
		flushInterval: &overrideFlushInterval,
	})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	writes := []testWrite{
		{testSeries(0, "foo.bar", testTags1, 127), time.Now(), 123.456, xtime.Second, []byte{1, 2, 3}, nil},
		{testSeries(1, "foo.baz", testTags2, 150), time.Now(), 456.789, xtime.Second, nil, nil},
	}

	// Call write sync
	writeCommitLogs(t, scope, commitLog, writes).Wait()

	// Close the commit log and consequently flush
	require.NoError(t, commitLog.Close())

	// Assert writes occurred by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)

	// Assert commitlog file exists and retrieve path
	fsopts := opts.FilesystemOptions()
	files, err := fs.SortedCommitLogFiles(fs.CommitLogsDirPath(fsopts.FilePathPrefix()))
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	// Assert commitlog cannot be opened more than once
	reader := newCommitLogReader(opts, ReadAllSeriesPredicate())
	_, _, _, err = reader.Open(files[0])
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	_, _, _, err = reader.Open(files[0])
	require.Equal(t, errCommitLogReaderIsNotReusable, err)
	reader.Close()
}

func TestCommitLogIteratorUsesPredicateFilter(t *testing.T) {
	clock := mclock.NewMock()
	opts, scope := newTestOptions(t, overrides{
		clock:    clock,
		strategy: StrategyWriteWait,
	})

	blockSize := opts.BlockSize()
	alignedStart := clock.Now().Truncate(blockSize)

	// Writes spaced apart by block size
	writes := []testWrite{
		{testSeries(0, "foo.bar", testTags1, 127), alignedStart, 123.456, xtime.Millisecond, nil, nil},
		{testSeries(1, "foo.baz", testTags2, 150), alignedStart.Add(1 * blockSize), 456.789, xtime.Millisecond, nil, nil},
		{testSeries(2, "foo.qux", testTags3, 291), alignedStart.Add(2 * blockSize), 789.123, xtime.Millisecond, nil, nil},
	}
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	// Write, making sure that the clock is set properly for each write
	for _, write := range writes {
		clock.Add(write.t.Sub(clock.Now()))
		wg := writeCommitLogs(t, scope, commitLog, []testWrite{write})
		// Flush until finished, this is required as timed flusher not active when clock is mocked
		flushUntilDone(commitLog, wg)
	}

	// Close the commit log and consequently flush
	require.NoError(t, commitLog.Close())

	// Make sure multiple commitlog files were generated
	fsopts := opts.FilesystemOptions()
	files, err := fs.SortedCommitLogFiles(fs.CommitLogsDirPath(fsopts.FilePathPrefix()))
	require.NoError(t, err)
	require.True(t, len(files) == 3)

	// This predicate should eliminate the first commitlog file
	commitLogPredicate := func(f File) bool {
		return f.Start.After(alignedStart)
	}

	// Assert that the commitlog iterator honors the predicate and only uses
	// 2 of the 3 files
	iterOpts := IteratorOpts{
		CommitLogOptions:      opts,
		FileFilterPredicate:   commitLogPredicate,
		SeriesFilterPredicate: ReadAllSeriesPredicate(),
	}
	iter, err := NewIterator(iterOpts)
	require.NoError(t, err)
	iterStruct := iter.(*iterator)
	require.True(t, len(iterStruct.files) == 2)
}

func TestCommitLogWriteBehind(t *testing.T) {
	opts, scope := newTestOptions(t, overrides{
		strategy: StrategyWriteBehind,
	})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	writes := []testWrite{
		{testSeries(0, "foo.bar", testTags1, 127), time.Now(), 123.456, xtime.Millisecond, nil, nil},
		{testSeries(1, "foo.baz", testTags2, 150), time.Now(), 456.789, xtime.Millisecond, nil, nil},
		{testSeries(2, "foo.qux", testTags3, 291), time.Now(), 789.123, xtime.Millisecond, nil, nil},
	}

	// Call write behind
	writeCommitLogs(t, scope, commitLog, writes)

	// Close the commit log and consequently flush
	require.NoError(t, commitLog.Close())

	// Assert writes occurred by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)
}

func TestCommitLogWriteErrorOnClosed(t *testing.T) {
	opts, _ := newTestOptions(t, overrides{})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)
	require.NoError(t, commitLog.Close())

	series := testSeries(0, "foo.bar", testTags1, 127)
	datapoint := ts.Datapoint{Timestamp: time.Now(), Value: 123.456}

	ctx := context.NewContext()
	defer ctx.Close()

	err := commitLog.Write(ctx, series, datapoint, xtime.Millisecond, nil)
	require.Error(t, err)
	require.Equal(t, errCommitLogClosed, err)
}

func TestCommitLogWriteErrorOnFull(t *testing.T) {
	// Set backlog of size one and don't automatically flush
	backlogQueueSize := 1
	flushInterval := time.Duration(0)
	opts, _ := newTestOptions(t, overrides{
		backlogQueueSize: &backlogQueueSize,
		flushInterval:    &flushInterval,
		strategy:         StrategyWriteBehind,
	})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	// Test filling queue
	var writes []testWrite
	series := testSeries(0, "foo.bar", testTags1, 127)
	dp := ts.Datapoint{Timestamp: time.Now(), Value: 123.456}
	unit := xtime.Millisecond

	ctx := context.NewContext()
	defer ctx.Close()

	for {
		if err := commitLog.Write(ctx, series, dp, unit, nil); err != nil {
			// Ensure queue full error
			require.Equal(t, ErrCommitLogQueueFull, err)
			break
		}
		writes = append(writes, testWrite{series, dp.Timestamp, dp.Value, unit, nil, nil})

		// Increment timestamp and value for next write
		dp.Timestamp = dp.Timestamp.Add(time.Second)
		dp.Value += 1.0
	}

	// Close and consequently flush
	require.NoError(t, commitLog.Close())

	// Assert write flushed by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)
}

func TestCommitLogExpiresWriter(t *testing.T) {
	clock := mclock.NewMock()
	opts, scope := newTestOptions(t, overrides{
		clock:    clock,
		strategy: StrategyWriteWait,
	})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	blockSize := opts.BlockSize()
	alignedStart := clock.Now().Truncate(blockSize)

	// Writes spaced apart by block size
	writes := []testWrite{
		{testSeries(0, "foo.bar", testTags1, 127), alignedStart, 123.456, xtime.Millisecond, nil, nil},
		{testSeries(1, "foo.baz", testTags2, 150), alignedStart.Add(1 * blockSize), 456.789, xtime.Millisecond, nil, nil},
		{testSeries(2, "foo.qux", testTags3, 291), alignedStart.Add(2 * blockSize), 789.123, xtime.Millisecond, nil, nil},
	}

	for _, write := range writes {
		// Set clock to align with the write
		clock.Add(write.t.Sub(clock.Now()))

		// Write entry
		wg := writeCommitLogs(t, scope, commitLog, []testWrite{write})

		// Flush until finished, this is required as timed flusher not active when clock is mocked
		flushUntilDone(commitLog, wg)
	}

	// Ensure files present for each block size time window
	fsopts := opts.FilesystemOptions()
	files, err := fs.SortedCommitLogFiles(fs.CommitLogsDirPath(fsopts.FilePathPrefix()))
	require.NoError(t, err)
	require.True(t, len(files) == len(writes))

	// Close and consequently flush
	require.NoError(t, commitLog.Close())

	// Assert write flushed by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)
}

func TestCommitLogFailOnWriteError(t *testing.T) {
	opts, scope := newTestOptions(t, overrides{
		strategy: StrategyWriteBehind,
	})
	defer cleanup(t, opts)

	commitLogI, err := NewCommitLog(opts)
	require.NoError(t, err)
	commitLog := commitLogI.(*commitLog)
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
		_ flushFn,
		_ Options,
	) commitLogWriter {
		return writer
	}

	require.NoError(t, commitLog.Open())

	wg := setupCloseOnFail(t, commitLog)

	writes := []testWrite{
		{testSeries(0, "foo.bar", testTags1, 127), time.Now(), 123.456, xtime.Millisecond, nil, nil},
	}

	writeCommitLogs(t, scope, commitLog, writes)

	wg.Wait()

	// Check stats
	errors, ok := snapshotCounterValue(scope, "commitlog.writes.errors")
	require.True(t, ok)
	require.Equal(t, int64(1), errors.Value())
}

func TestCommitLogFailOnOpenError(t *testing.T) {
	opts, scope := newTestOptions(t, overrides{
		strategy: StrategyWriteBehind,
	})
	defer cleanup(t, opts)

	commitLogI, err := NewCommitLog(opts)
	require.NoError(t, err)
	commitLog := commitLogI.(*commitLog)
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
		_ flushFn,
		_ Options,
	) commitLogWriter {
		return writer
	}

	require.NoError(t, commitLog.Open())

	wg := setupCloseOnFail(t, commitLog)

	func() {
		commitLog.RLock()
		defer commitLog.RUnlock()
		// Expire the writer so it requires a new open
		commitLog.writerExpireAt = timeZero
	}()

	writes := []testWrite{
		{testSeries(0, "foo.bar", testTags1, 127), time.Now(), 123.456, xtime.Millisecond, nil, nil},
	}

	writeCommitLogs(t, scope, commitLog, writes)

	wg.Wait()

	// Check stats
	errors, ok := snapshotCounterValue(scope, "commitlog.writes.errors")
	require.True(t, ok)
	require.Equal(t, int64(1), errors.Value())

	openErrors, ok := snapshotCounterValue(scope, "commitlog.writes.open-errors")
	require.True(t, ok)
	require.Equal(t, int64(1), openErrors.Value())
}

func TestCommitLogFailOnFlushError(t *testing.T) {
	opts, scope := newTestOptions(t, overrides{
		strategy: StrategyWriteBehind,
	})
	defer cleanup(t, opts)

	commitLogI, err := NewCommitLog(opts)
	require.NoError(t, err)
	commitLog := commitLogI.(*commitLog)
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
		_ flushFn,
		_ Options,
	) commitLogWriter {
		return writer
	}

	require.NoError(t, commitLog.Open())

	wg := setupCloseOnFail(t, commitLog)

	writes := []testWrite{
		{testSeries(0, "foo.bar", testTags1, 127), time.Now(), 123.456, xtime.Millisecond, nil, nil},
	}

	writeCommitLogs(t, scope, commitLog, writes)

	wg.Wait()

	// Check stats
	errors, ok := snapshotCounterValue(scope, "commitlog.writes.errors")
	require.True(t, ok)
	require.Equal(t, int64(1), errors.Value())

	flushErrors, ok := snapshotCounterValue(scope, "commitlog.writes.flush-errors")
	require.True(t, ok)
	require.Equal(t, int64(1), flushErrors.Value())
}

var (
	testTag1 = ident.StringTag("name1", "val1")
	testTag2 = ident.StringTag("name2", "val2")
	testTag3 = ident.StringTag("name3", "val3")

	testTags1 = ident.NewTags(testTag1)
	testTags2 = ident.NewTags(testTag2)
	testTags3 = ident.NewTags(testTag3)
)
