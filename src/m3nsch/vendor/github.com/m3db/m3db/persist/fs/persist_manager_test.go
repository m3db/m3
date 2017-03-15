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

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
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

	flush, err := pm.StartFlush()
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.Done())
	}()

	prepared, err := flush.Prepare(testNamespaceID, shard, blockStart)
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

	flush, err := pm.StartFlush()
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.Done())
	}()

	prepared, err := flush.Prepare(testNamespaceID, shard, blockStart)
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

	var (
		id       = ts.StringID("foo")
		head     = checked.NewBytes([]byte{0x1, 0x2}, nil)
		tail     = checked.NewBytes([]byte{0x3, 0x4}, nil)
		segment  = ts.NewSegment(head, tail, ts.FinalizeNone)
		checksum = digest.SegmentChecksum(segment)
	)
	writer.EXPECT().WriteAll(id, gomock.Any(), checksum).Return(nil)
	writer.EXPECT().Close()

	flush, err := pm.StartFlush()
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.Done())
	}()

	now := time.Now()
	pm.start = now
	pm.count = 123
	pm.bytesWritten = 100

	prepared, err := flush.Prepare(testNamespaceID, shard, blockStart)
	defer prepared.Close()

	require.Nil(t, err)

	require.Nil(t, prepared.Persist(id, segment, checksum))

	require.True(t, pm.start.Equal(now))
	require.Equal(t, 124, pm.count)
	require.Equal(t, int64(104), pm.bytesWritten)
}

func TestPersistenceManagerClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer := testManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	writer.EXPECT().Close()
	pm.close()
}

func TestPersistenceManagerNoRateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer := testManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	var (
		now      time.Time
		slept    time.Duration
		id       = ts.StringID("foo")
		head     = checked.NewBytes([]byte{0x1, 0x2}, nil)
		tail     = checked.NewBytes([]byte{0x3}, nil)
		segment  = ts.NewSegment(head, tail, ts.FinalizeNone)
		checksum = digest.SegmentChecksum(segment)
	)

	pm.nowFn = func() time.Time { return now }
	pm.sleepFn = func(d time.Duration) { slept += d }

	writer.EXPECT().WriteAll(id, pm.segmentHolder, checksum).Return(nil).Times(2)

	// Disable rate limiting
	pm.SetRateLimitOptions(pm.RateLimitOptions().SetLimitEnabled(false))

	flush, err := pm.StartFlush()
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.Done())
	}()

	// Start persistence
	now = time.Now()
	require.NoError(t, pm.persist(id, segment, checksum))

	// Advance time and write again
	now = now.Add(time.Millisecond)
	require.NoError(t, pm.persist(id, segment, checksum))

	// Check there is no rate limiting
	require.Equal(t, time.Duration(0), slept)
	require.Equal(t, int64(6), pm.bytesWritten)
}

func TestPersistenceManagerWithRateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer := testManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	var (
		now      time.Time
		slept    time.Duration
		iter     = 2
		id       = ts.StringID("foo")
		head     = checked.NewBytes([]byte{0x1, 0x2}, nil)
		tail     = checked.NewBytes([]byte{0x3}, nil)
		segment  = ts.NewSegment(head, tail, ts.FinalizeNone)
		checksum = digest.SegmentChecksum(segment)
	)

	pm.nowFn = func() time.Time { return now }
	pm.sleepFn = func(d time.Duration) { slept += d }

	writer.EXPECT().WriteAll(id, pm.segmentHolder, checksum).Return(nil).AnyTimes()
	writer.EXPECT().Close().Times(iter)

	// Enable rate limiting
	pm.SetRateLimitOptions(pm.RateLimitOptions().
		SetLimitEnabled(true).
		SetLimitCheckEvery(2).
		SetLimitMbps(16.0))

	for i := 0; i < iter; i++ {
		// Reset
		slept = time.Duration(0)

		flush, err := pm.StartFlush()
		require.NoError(t, err)

		// Start persistence
		now = time.Now()
		require.NoError(t, pm.persist(id, segment, checksum))

		// Assert we don't rate limit if the count is not enough yet
		require.NoError(t, pm.persist(id, segment, checksum))
		require.Equal(t, time.Duration(0), slept)

		// Advance time and check we rate limit if the disk throughput exceeds the limit
		now = now.Add(time.Microsecond)
		require.NoError(t, pm.persist(id, segment, checksum))
		require.Equal(t, time.Duration(1861), slept)

		// Advance time and check we don't rate limit if the disk throughput is below the limit
		require.NoError(t, pm.persist(id, segment, checksum))
		now = now.Add(time.Second - time.Microsecond)
		require.NoError(t, pm.persist(id, segment, checksum))
		require.Equal(t, time.Duration(1861), slept)

		require.Equal(t, int64(15), pm.bytesWritten)

		pm.close()

		assert.NoError(t, flush.Done())
	}
}
