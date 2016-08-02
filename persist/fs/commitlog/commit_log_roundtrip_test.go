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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3x/metrics"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
)

func newTestOptions(t *testing.T) (m3db.DatabaseOptions, xmetrics.TestStatsReporter) {
	dir, err := ioutil.TempDir("", "foo")
	assert.NoError(t, err)
	stats := xmetrics.NewTestStatsReporter()
	return storage.NewDatabaseOptions().
			FilePathPrefix(dir).
			MetricsScope(xmetrics.NewScope("", stats)).
			BlockSize(2 * time.Hour),
		stats
}

func cleanup(t *testing.T, opts m3db.DatabaseOptions) {
	filePathPrefix := opts.GetFilePathPrefix()
	assert.NoError(t, os.RemoveAll(filePathPrefix))
}

type testSeries struct {
	idx   uint64
	id    string
	shard uint32
}

func (s testSeries) UniqueIndex() uint64 {
	return s.idx
}

func (s testSeries) ID() string {
	return s.id
}

func (s testSeries) Shard() uint32 {
	return s.shard
}

type testWrite struct {
	series testSeries
	t      time.Time
	v      float64
	u      xtime.Unit
	a      []byte
}

func (w testWrite) assert(
	t *testing.T,
	series m3db.CommitLogSeries,
	datapoint m3db.Datapoint,
	unit xtime.Unit,
	annotation []byte,
) {
	assert.Equal(t, w.series.idx, series.UniqueIndex())
	assert.Equal(t, w.series.id, series.ID())
	assert.Equal(t, w.series.shard, series.Shard())
	assert.True(t, w.t.Equal(datapoint.Timestamp))
	assert.Equal(t, datapoint.Value, datapoint.Value)
	assert.Equal(t, w.u, unit)
	assert.Equal(t, w.a, annotation)
}

func TestCommitLogRoundtrip(t *testing.T) {
	opts, stats := newTestOptions(t)
	defer cleanup(t, opts)

	log, err := NewCommitLog(opts)
	assert.NoError(t, err)
	commitLog := log.(*commitLog)
	defer func() { assert.NoError(t, commitLog.Close()) }()

	writes := []testWrite{
		{testSeries{0, "foo.bar", 127}, time.Now(), 123.456, xtime.Second, []byte{1, 2, 3}},
		{testSeries{1, "foo.baz", 150}, time.Now(), 456.789, xtime.Second, nil},
	}

	var wg sync.WaitGroup
	for i, write := range writes {
		i := i
		write := write

		// Wait for previous writes to enqueue
		for stats.Counter("commitlog.writes", nil) != int64(i) {
			time.Sleep(10 * time.Millisecond)
		}

		// Enqueue write
		wg.Add(1)
		go func() {
			defer wg.Done()
			datapoint := m3db.Datapoint{Timestamp: write.t, Value: write.v}
			assert.NoError(t, commitLog.Write(write.series, datapoint, write.u, write.a))
		}()
	}

	// Wait for all writes to enqueue
	for stats.Counter("commitlog.writes", nil) != int64(len(writes)) {
		time.Sleep(10 * time.Millisecond)
	}

	// Ensure files present
	files, err := fs.CommitLogFiles(fs.CommitLogsDirPath(opts.GetFilePathPrefix()))
	assert.NoError(t, err)
	assert.True(t, len(files) >= 1)

	// Flush the writes
	lastFile := files[len(files)-1]
	stat, err := os.Stat(lastFile)
	assert.NoError(t, err)
	assert.NoError(t, commitLog.writer.Flush())
	statAfterFlush, err := os.Stat(lastFile)
	assert.True(t, statAfterFlush.Size() > stat.Size())

	// Wait for write callbacks to fire
	wg.Wait()

	// Iterate the writes
	iter, err := commitLog.Iter()
	assert.NoError(t, err)
	defer iter.Close()

	for _, write := range writes {
		assert.True(t, iter.Next())
		series, datapoint, unit, annotation := iter.Current()
		write.assert(t, series, datapoint, unit, annotation)
	}

	assert.NoError(t, iter.Err())
}
