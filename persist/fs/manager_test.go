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

package fs

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/ts"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func createShardDir(t *testing.T, prefix string, namespace ts.ID, shard uint32) string {
	shardDirPath := ShardDirPath(prefix, namespace, shard)
	err := os.MkdirAll(shardDirPath, os.ModeDir|os.FileMode(0755))
	require.Nil(t, err)
	return shardDirPath
}

func testManager(t *testing.T, ctrl *gomock.Controller) (*persistManager, *MockFileSetWriter) {
	dir := createTempDir(t)

	opts := NewOptions().
		SetFilePathPrefix(dir).
		SetWriterBufferSize(10).
		SetRetentionOptions(
			retention.NewOptions().
				SetBlockSize(2 * time.Hour))

	writer := NewMockFileSetWriter(ctrl)

	manager := NewPersistManager(opts).(*persistManager)
	manager.writer = writer

	return manager, writer
}

func TestPersistenceManagerPrepareFileExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, _ := testManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	shard := uint32(0)
	blockStart := time.Unix(1000, 0)
	shardDir := createShardDir(t, pm.filePathPrefix, testNamespaceID, shard)
	checkpointFilePath := filesetPathFromTime(shardDir, blockStart, checkpointFileSuffix)
	f, err := os.Create(checkpointFilePath)
	require.NoError(t, err)
	f.Close()

	prepared, err := pm.Prepare(testNamespaceID, shard, blockStart)
	require.NoError(t, err)
	require.Nil(t, prepared.Persist)
	require.Nil(t, prepared.Close)
}

func TestPersistenceManagerPrepareOpenError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer := testManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	shard := uint32(0)
	blockStart := time.Unix(1000, 0)
	expectedErr := errors.New("foo")
	writer.EXPECT().Open(testNamespaceID, shard, blockStart).Return(expectedErr)

	prepared, err := pm.Prepare(testNamespaceID, shard, blockStart)
	require.Equal(t, expectedErr, err)
	require.Nil(t, prepared.Persist)
	require.Nil(t, prepared.Close)
}

func TestPersistenceManagerPrepareSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer := testManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	shard := uint32(0)
	blockStart := time.Unix(1000, 0)
	writer.EXPECT().Open(testNamespaceID, shard, blockStart).Return(nil)

	id := ts.StringID("foo")
	segment := ts.Segment{Head: []byte{0x1, 0x2}, Tail: []byte{0x3, 0x4}}
	writer.EXPECT().WriteAll(id, gomock.Any()).Return(nil)
	writer.EXPECT().Close()

	prepared, err := pm.Prepare(testNamespaceID, shard, blockStart)
	require.Nil(t, err)

	defer prepared.Close()
	require.Nil(t, prepared.Persist(id, segment))
}

func TestPersistenceManagerClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer := testManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	now := time.Now()
	pm.start = now
	pm.lastCheck = now
	pm.bytesWritten = 100

	writer.EXPECT().Close()
	pm.close()

	require.True(t, pm.start.IsZero())
	require.True(t, pm.lastCheck.IsZero())
	require.Equal(t, int64(0), pm.bytesWritten)
}

func TestPersistenceManagerNoThroughputLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer := testManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	var (
		now     time.Time
		slept   time.Duration
		id      = ts.StringID("foo")
		segment = ts.Segment{Head: []byte{0x1, 0x2}, Tail: []byte{0x3}}
	)

	pm.nowFn = func() time.Time { return now }
	pm.sleepFn = func(d time.Duration) { slept += d }
	writer.EXPECT().WriteAll(id, pm.segmentHolder).Return(nil).Times(2)

	// Disable throughput limiting
	pm.throughputLimitOpts = pm.throughputLimitOpts.SetThroughputLimitEnabled(false)

	// Start persistence
	now = time.Now()
	require.NoError(t, pm.persist(id, segment))

	// Advance time and write again
	now = now.Add(time.Millisecond)
	require.NoError(t, pm.persist(id, segment))

	// Check there is no rate limiting
	require.Equal(t, time.Duration(0), slept)
	require.Equal(t, int64(6), pm.bytesWritten)
}

func TestPersistenceManagerWithThroughputLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer := testManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	var (
		now     time.Time
		slept   time.Duration
		iter    = 2
		id      = ts.StringID("foo")
		segment = ts.Segment{Head: []byte{0x1, 0x2}, Tail: []byte{0x3}}
	)

	pm.nowFn = func() time.Time { return now }
	pm.sleepFn = func(d time.Duration) { slept += d }
	writer.EXPECT().WriteAll(id, pm.segmentHolder).Return(nil).AnyTimes()
	writer.EXPECT().Close().Times(iter)

	// Enable throughput limiting
	pm.throughputLimitOpts = pm.throughputLimitOpts.
		SetThroughputLimitEnabled(true).
		SetThroughputCheckInterval(time.Microsecond).
		SetThroughputLimitMbps(16.0)

	for i := 0; i < iter; i++ {
		// Start persistence
		now = time.Now()
		require.NoError(t, pm.persist(id, segment))

		// Advance time and check we don't rate limit if it's not check interval yet
		now = now.Add(time.Nanosecond)
		require.NoError(t, pm.persist(id, segment))
		require.Equal(t, time.Duration(0), slept)

		// Advance time and check we rate limit if the disk throughput exceeds the limit
		now = now.Add(time.Microsecond - time.Nanosecond)
		require.NoError(t, pm.persist(id, segment))
		require.Equal(t, time.Duration(1861), slept)

		// Advance time and check we don't rate limit if the disk throughput is below the limit
		now = now.Add(time.Second - time.Microsecond)
		require.NoError(t, pm.persist(id, segment))
		require.Equal(t, time.Duration(1861), slept)

		require.Equal(t, int64(12), pm.bytesWritten)

		// Reset
		slept = time.Duration(0)
		pm.close()
	}
}
