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

package storage

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func testCleanupManager(t *testing.T, ctrl *gomock.Controller) (*mockDatabase, *MockdatabaseFlushManager, *cleanupManager) {
	db := newMockDatabase(t)
	fm := NewMockdatabaseFlushManager(ctrl)
	return db, fm, newCleanupManager(db, fm, tally.NoopScope).(*cleanupManager)
}

func TestCleanupManagerCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ts := tf(36000)
	rOpts := retention.NewOptions().
		SetRetentionPeriod(21600 * time.Second).
		SetBufferPast(0 * time.Second).
		SetBlockSize(7200 * time.Second)
	nsOpts := namespace.NewOptions().SetRetentionOptions(rOpts)
	db, fm, mgr := testCleanupManager(t, ctrl)
	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().SetRetentionOptions(rOpts))

	inputs := []struct {
		name string
		err  error
	}{
		{"foo", errors.New("some error")},
		{"bar", errors.New("some other error")},
		{"baz", nil},
	}

	start := tf(14400)
	namespaces := make(map[string]databaseNamespace)
	for _, input := range inputs {
		ns := NewMockdatabaseNamespace(ctrl)
		ns.EXPECT().Options().Return(nsOpts).AnyTimes()
		ns.EXPECT().CleanupFileset(start).Return(input.err)
		namespaces[input.name] = ns
	}
	db.namespaces = namespaces

	mgr.commitLogFilesBeforeFn = func(_ string, t time.Time) ([]string, error) {
		return []string{"foo", "bar"}, errors.New("error1")
	}
	mgr.commitLogFilesForTimeFn = func(_ string, t time.Time) ([]string, error) {
		if t == tf(14400) {
			return []string{"baz"}, nil
		}
		return nil, errors.New("error" + strconv.Itoa(int(t.Unix())))
	}
	var deletedFiles []string
	mgr.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}

	gomock.InOrder(
		fm.EXPECT().NeedsFlush(tf(14400), tf(28800)).Return(false),
		fm.EXPECT().NeedsFlush(tf(7200), tf(21600)).Return(false),
		fm.EXPECT().NeedsFlush(tf(0), tf(14400)).Return(false),
	)
	require.Error(t, mgr.Cleanup(ts))
	require.Equal(t, []string{"foo", "bar", "baz"}, deletedFiles)
}

func TestCleanupManagerCommitLogTimeRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		ts    = time.Unix(25200, 0)
		rOpts = retention.NewOptions().
			SetRetentionPeriod(18000 * time.Second).
			SetBufferPast(0 * time.Second).
			SetBlockSize(7200 * time.Second)
		_, _, mgr = testCleanupManager(t, ctrl)
	)

	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().SetRetentionOptions(rOpts))
	cs, ce := mgr.commitLogTimeRange(ts)
	require.Equal(t, time.Unix(0, 0), cs)
	require.Equal(t, time.Unix(7200, 0), ce)
}

// The following tests exercise commitLogTimes(). Consider the following situation:
//
// Commit Log Retention Options:
// - Retention Period: 30 seconds
// - BlockSize: 10 seconds
// - BufferPast: 0 seconds
//
// - Current Time: 50 seconds
//
// name:    a    b    c    d    e
//       |    |xxxx|xxxx|xxxx|    |
//    t: 0    10   20   30   40   50
//
// we can potentially flush blocks starting [10, 30], i.e. b, c, d
// so for each, we check the surround two blocks in the fs manager to see
// if any namespace still requires to be flushed for that period. If so,
// we cannot remove the data for it. We should get back all the times
// we can delete data for.
func newCleanupManagerCommitLogTimesTest(t *testing.T, ctrl *gomock.Controller) (
	*MockdatabaseFlushManager,
	*cleanupManager,
) {
	var (
		rOpts = retention.NewOptions().
			SetRetentionPeriod(30 * time.Second).
			SetBufferPast(0 * time.Second).
			SetBlockSize(10 * time.Second)
		_, fm, mgr = testCleanupManager(t, ctrl)
	)
	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().SetRetentionOptions(rOpts))

	return fm, mgr
}

func TestCleanupManagerCommitLogTimesAllFlushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fm, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := tf(50)

	gomock.InOrder(
		fm.EXPECT().NeedsFlush(tf(20), tf(40)).Return(false),
		fm.EXPECT().NeedsFlush(tf(10), tf(30)).Return(false),
		fm.EXPECT().NeedsFlush(tf(0), tf(20)).Return(false),
	)

	earliest, times := mgr.commitLogTimes(currentTime)
	require.Equal(t, tf(10), earliest)
	require.Equal(t, 3, len(times))
	require.True(t, contains(times, tf(10)))
	require.True(t, contains(times, tf(20)))
	require.True(t, contains(times, tf(30)))
}

func TestCleanupManagerCommitLogTimesMiddlePendingFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fm, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := tf(50)

	gomock.InOrder(
		fm.EXPECT().NeedsFlush(tf(20), tf(40)).Return(false),
		fm.EXPECT().NeedsFlush(tf(10), tf(30)).Return(true),
		fm.EXPECT().NeedsFlush(tf(0), tf(20)).Return(false),
	)

	earliest, times := mgr.commitLogTimes(currentTime)
	require.Equal(t, tf(10), earliest)
	require.Equal(t, 2, len(times))
	require.True(t, contains(times, tf(10)))
	require.True(t, contains(times, tf(30)))
}

func TestCleanupManagerCommitLogTimesStartPendingFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fm, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := tf(50)

	gomock.InOrder(
		fm.EXPECT().NeedsFlush(tf(20), tf(40)).Return(true),
		fm.EXPECT().NeedsFlush(tf(10), tf(30)).Return(false),
		fm.EXPECT().NeedsFlush(tf(0), tf(20)).Return(false),
	)

	earliest, times := mgr.commitLogTimes(currentTime)
	require.Equal(t, tf(10), earliest)
	require.Equal(t, 2, len(times))
	require.True(t, contains(times, tf(20)))
	require.True(t, contains(times, tf(10)))
}

func TestCleanupManagerCommitLogTimesAllPendingFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fm, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := tf(50)

	gomock.InOrder(
		fm.EXPECT().NeedsFlush(tf(20), tf(40)).Return(true),
		fm.EXPECT().NeedsFlush(tf(10), tf(30)).Return(true),
		fm.EXPECT().NeedsFlush(tf(0), tf(20)).Return(true),
	)

	earliest, times := mgr.commitLogTimes(currentTime)
	require.Equal(t, tf(10), earliest)
	require.Equal(t, 0, len(times))
}

func tf(s int64) time.Time {
	return time.Unix(s, 0)
}

func contains(arr []time.Time, t time.Time) bool {
	for _, at := range arr {
		if at.Equal(t) {
			return true
		}
	}
	return false
}
