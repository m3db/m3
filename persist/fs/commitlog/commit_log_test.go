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
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3db/generated/mocks/mocks"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/metrics"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type overrides struct {
	backlogQueueSize *int
	flushInterval    *time.Duration
}

func newTestOptions(
	t *testing.T,
	ctrl *gomock.Controller,
	overrides overrides,
) (
	*mocks.MockDatabaseOptions,
	xmetrics.TestStatsReporter,
) {
	dir, err := ioutil.TempDir("", "foo")
	assert.NoError(t, err)

	stats := xmetrics.NewTestStatsReporter()

	opts := mocks.NewMockDatabaseOptions(ctrl)
	opts.EXPECT().GetNowFn().Return(time.Now).AnyTimes()
	opts.EXPECT().GetLogger().Return(xlog.SimpleLogger).AnyTimes()
	opts.EXPECT().GetMetricsScope().Return(xmetrics.NewScope("", stats)).AnyTimes()
	opts.EXPECT().GetFilePathPrefix().Return(dir).AnyTimes()
	opts.EXPECT().GetFileWriterOptions().Return(fs.NewFileWriterOptions()).AnyTimes()
	opts.EXPECT().GetBlockSize().Return(2 * time.Hour).AnyTimes()
	if overrides.backlogQueueSize != nil {
		opts.EXPECT().GetCommitLogBacklogQueueSize().Return(*overrides.backlogQueueSize).AnyTimes()
	} else {
		opts.EXPECT().GetCommitLogBacklogQueueSize().Return(1024).AnyTimes()
	}
	opts.EXPECT().GetCommitLogFlushSize().Return(4096).AnyTimes()
	if overrides.flushInterval != nil {
		opts.EXPECT().GetCommitLogFlushInterval().Return(*overrides.flushInterval).AnyTimes()
	} else {
		opts.EXPECT().GetCommitLogFlushInterval().Return(100 * time.Millisecond).AnyTimes()
	}

	return opts, stats
}

func cleanup(t *testing.T, opts m3db.DatabaseOptions) {
	filePathPrefix := opts.GetFilePathPrefix()
	assert.NoError(t, os.RemoveAll(filePathPrefix))
}

type testWrite struct {
	series m3db.CommitLogSeries
	t      time.Time
	v      float64
	u      xtime.Unit
	a      []byte
}

func testSeries(
	uniqueIndex uint64,
	id string,
	shard uint32,
) m3db.CommitLogSeries {
	return m3db.CommitLogSeries{
		UniqueIndex: uniqueIndex,
		ID:          id,
		Shard:       shard,
	}
}

func (w testWrite) assert(
	t *testing.T,
	series m3db.CommitLogSeries,
	datapoint m3db.Datapoint,
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

func newTestCommitLog(t *testing.T, opts m3db.DatabaseOptions) *commitLog {
	log, err := NewCommitLog(opts)
	assert.NoError(t, err)
	commitLog := log.(*commitLog)

	// Ensure files present
	files, err := fs.CommitLogFiles(fs.CommitLogsDirPath(opts.GetFilePathPrefix()))
	assert.NoError(t, err)
	assert.True(t, len(files) == 1)

	return commitLog
}

type writeCommitLogFn func(
	series m3db.CommitLogSeries,
	datapoint m3db.Datapoint,
	unit xtime.Unit,
	annotation m3db.Annotation,
) error

func writeCommitLogs(
	t *testing.T,
	stats xmetrics.TestStatsReporter,
	writeFn writeCommitLogFn,
	writes []testWrite,
) {
	completedWrites := stats.Counter("commitlog.writes.success", nil)
	for i, write := range writes {
		i := i
		write := write

		// Wait for previous writes to enqueue
		for stats.Counter("commitlog.writes.success", nil) != completedWrites+int64(+i) {
			time.Sleep(10 * time.Millisecond)
		}

		go func() {
			datapoint := m3db.Datapoint{Timestamp: write.t, Value: write.v}
			assert.NoError(t, writeFn(write.series, datapoint, write.u, write.a))
		}()
	}

	// Wait for all writes to enqueue
	for stats.Counter("commitlog.writes.success", nil) != completedWrites+int64(len(writes)) {
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForFlush(l *commitLog) {
	for {
		l.RLock()
		buffered := l.writer.(*writer).buffer.Buffered()
		l.RUnlock()
		if buffered == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
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

func TestCommitLogWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts, stats := newTestOptions(t, ctrl, overrides{})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	writes := []testWrite{
		{testSeries(0, "foo.bar", 127), time.Now(), 123.456, xtime.Second, []byte{1, 2, 3}},
		{testSeries(1, "foo.baz", 150), time.Now(), 456.789, xtime.Second, nil},
	}

	// Call write sync
	writeCommitLogs(t, stats, commitLog.Write, writes)

	// Wait for flush
	waitForFlush(commitLog)

	// Assert writes occurred by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)

	// Close the commit log
	assert.NoError(t, commitLog.Close())
}

func TestCommitLogWriteBehind(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts, stats := newTestOptions(t, ctrl, overrides{})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	writes := []testWrite{
		{testSeries(0, "foo.bar", 127), time.Now(), 123.456, xtime.Millisecond, nil},
		{testSeries(1, "foo.baz", 150), time.Now(), 456.789, xtime.Millisecond, nil},
		{testSeries(2, "foo.qux", 291), time.Now(), 789.123, xtime.Millisecond, nil},
	}

	// Call write behind
	writeCommitLogs(t, stats, commitLog.WriteBehind, writes)

	// Wait for flush
	waitForFlush(commitLog)

	// Assert writes occurred by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)

	// Close the commit log
	assert.NoError(t, commitLog.Close())
}

func TestCommitLogWriteErrorOnClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts, _ := newTestOptions(t, ctrl, overrides{})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)
	assert.NoError(t, commitLog.Close())

	series := testSeries(0, "foo.bar", 127)
	datapoint := m3db.Datapoint{Timestamp: time.Now(), Value: 123.456}

	err := commitLog.Write(series, datapoint, xtime.Millisecond, nil)
	assert.Error(t, err)
	assert.Equal(t, errCommitLogClosed, err)

	err = commitLog.WriteBehind(series, datapoint, xtime.Millisecond, nil)
	assert.Error(t, err)
	assert.Equal(t, errCommitLogClosed, err)
}

func TestCommitLogWriteErrorOnFull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Set backlog of size one and don't automatically flush
	backlogQueueSize := 1
	flushInterval := time.Duration(0)
	opts, _ := newTestOptions(t, ctrl, overrides{
		backlogQueueSize: &backlogQueueSize,
		flushInterval:    &flushInterval,
	})
	defer cleanup(t, opts)

	commitLog := newTestCommitLog(t, opts)

	// Test filling queue
	var writes []testWrite
	series := testSeries(0, "foo.bar", 127)
	dp := m3db.Datapoint{Timestamp: time.Now(), Value: 123.456}
	unit := xtime.Millisecond
	for {
		if err := commitLog.WriteBehind(series, dp, unit, nil); err != nil {
			// Ensure queue full error
			assert.Equal(t, errCommitLogQueueFull, err)
			break
		}
		writes = append(writes, testWrite{series, dp.Timestamp, dp.Value, unit, nil})

		// Increment timestamp and value for next write
		dp.Timestamp = dp.Timestamp.Add(time.Second)
		dp.Value += 1.0
	}

	// Close and consequently flush
	assert.NoError(t, commitLog.Close())

	// Assert write flushed by reading the commit log
	assertCommitLogWritesByIterating(t, commitLog, writes)
}
