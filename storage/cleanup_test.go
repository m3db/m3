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

func testCleanupManager(ctrl *gomock.Controller) (*Mockdatabase, *cleanupManager) {
	db := newMockdatabase(ctrl)
	return db, newCleanupManager(db, tally.NoopScope).(*cleanupManager)
}

func TestCleanupManagerCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ts := tf(36000)
	rOpts := retention.NewOptions().
		SetRetentionPeriod(21600 * time.Second).
		SetBufferPast(0).
		SetBufferFuture(0).
		SetBlockSize(7200 * time.Second)
	nsOpts := namespace.NewOptions().SetRetentionOptions(rOpts)

	inputs := []struct {
		name string
		err  error
	}{
		{"foo", errors.New("some error")},
		{"bar", errors.New("some other error")},
		{"baz", nil},
	}

	start := tf(14400)
	namespaces := make([]databaseNamespace, 0, len(inputs))
	for _, input := range inputs {
		ns := NewMockdatabaseNamespace(ctrl)
		ns.EXPECT().Options().Return(nsOpts).AnyTimes()
		ns.EXPECT().CleanupFileset(start).Return(input.err)
		ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
		namespaces = append(namespaces, ns)
	}
	db := newMockdatabase(ctrl, namespaces...)
	mgr := newCleanupManager(db, tally.NoopScope).(*cleanupManager)
	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().SetRetentionOptions(rOpts))

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
		_, mgr = testCleanupManager(ctrl)
	)

	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().SetRetentionOptions(rOpts))
	cs, ce := mgr.commitLogTimeRange(ts)
	require.Equal(t, time.Unix(0, 0), cs)
	require.Equal(t, time.Unix(7200, 0), ce)
}

func newTestRetentionOptions(
	blockSizeSecs int,
	bufferPastSecs int,
	bufferFutureSecs int,
) retention.Options {
	return retention.NewOptions().
		SetBufferPast(time.Duration(bufferPastSecs) * time.Second).
		SetBufferFuture(time.Duration(bufferFutureSecs) * time.Second).
		SetBlockSize(time.Duration(blockSizeSecs) * time.Second)
}

type testCaseCleanupMgrNsBlocks struct {
	// input
	id          string
	nsRetention retention.Options
	clBlockSize time.Duration
	blockStart  time.Time

	// output
	expectedStart time.Time
	expectedEnd   time.Time
}

func TestCleanupManagerCommitLogNamespaceBlocks(t *testing.T) {
	tcs := []testCaseCleanupMgrNsBlocks{
		{
			id:            "test-case-0",
			nsRetention:   newTestRetentionOptions(30, 0, 0),
			clBlockSize:   15 * time.Second,
			blockStart:    tf(15),
			expectedStart: tf(0),
			expectedEnd:   tf(30),
		},
		{
			id:            "test-case-1",
			nsRetention:   newTestRetentionOptions(30, 0, 0),
			clBlockSize:   15 * time.Second,
			blockStart:    tf(30),
			expectedStart: tf(30),
			expectedEnd:   tf(60),
		},
		{
			id:            "test-case-2",
			nsRetention:   newTestRetentionOptions(10, 0, 0),
			clBlockSize:   15 * time.Second,
			blockStart:    tf(15),
			expectedStart: tf(10),
			expectedEnd:   tf(30),
		},
		{
			id:            "test-case-3",
			nsRetention:   newTestRetentionOptions(15, 0, 0),
			clBlockSize:   12 * time.Second,
			blockStart:    tf(24),
			expectedStart: tf(15),
			expectedEnd:   tf(45),
		},
		{
			id:            "test-case-4",
			nsRetention:   newTestRetentionOptions(20, 0, 0),
			clBlockSize:   22 * time.Second,
			blockStart:    tf(22),
			expectedStart: tf(20),
			expectedEnd:   tf(60),
		},
		{
			id:            "test-case-5",
			nsRetention:   newTestRetentionOptions(20, 5, 0),
			clBlockSize:   10 * time.Second,
			blockStart:    tf(30),
			expectedStart: tf(20),
			expectedEnd:   tf(40),
		},
		{
			id:            "test-case-6",
			nsRetention:   newTestRetentionOptions(20, 0, 5),
			clBlockSize:   10 * time.Second,
			blockStart:    tf(40),
			expectedStart: tf(40),
			expectedEnd:   tf(60),
		},
		{
			id:            "test-case-7",
			nsRetention:   newTestRetentionOptions(25, 10, 5),
			clBlockSize:   20 * time.Second,
			blockStart:    tf(30),
			expectedStart: tf(0),
			expectedEnd:   tf(75),
		},
		{
			id:            "test-case-8",
			nsRetention:   newTestRetentionOptions(720, 720, 60),
			clBlockSize:   15 * time.Second,
			blockStart:    tf(1410),
			expectedStart: tf(0),
			expectedEnd:   tf(2160),
		},
	}
	for _, tc := range tcs {
		start, end := commitLogNamespaceBlockTimes(tc.blockStart, tc.clBlockSize, tc.nsRetention)
		require.Equal(t, tc.expectedStart.Unix(), start.Unix(), tc.id)
		require.Equal(t, tc.expectedEnd.Unix(), end.Unix(), tc.id)
	}
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
func newCleanupManagerCommitLogTimesTest(t *testing.T, ctrl *gomock.Controller) (*MockdatabaseNamespace, *cleanupManager) {
	var (
		rOpts = retention.NewOptions().
			SetRetentionPeriod(30 * time.Second).
			SetBufferPast(0 * time.Second).
			SetBufferFuture(0 * time.Second).
			SetBlockSize(10 * time.Second)
	)
	no := namespace.NewMockOptions(ctrl)
	no.EXPECT().RetentionOptions().Return(rOpts).AnyTimes()

	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(no).AnyTimes()

	db := newMockdatabase(ctrl, ns)
	mgr := newCleanupManager(db, tally.NoopScope).(*cleanupManager)

	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().SetRetentionOptions(rOpts))

	return ns, mgr
}

func TestCleanupManagerCommitLogTimesAllFlushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := tf(50)

	gomock.InOrder(
		ns.EXPECT().NeedsFlush(tf(30), tf(40)).Return(false),
		ns.EXPECT().NeedsFlush(tf(20), tf(30)).Return(false),
		ns.EXPECT().NeedsFlush(tf(10), tf(20)).Return(false),
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

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := tf(50)

	gomock.InOrder(
		ns.EXPECT().NeedsFlush(tf(30), tf(40)).Return(false),
		ns.EXPECT().NeedsFlush(tf(20), tf(30)).Return(true),
		ns.EXPECT().NeedsFlush(tf(10), tf(20)).Return(false),
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

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := tf(50)

	gomock.InOrder(
		ns.EXPECT().NeedsFlush(tf(30), tf(40)).Return(true),
		ns.EXPECT().NeedsFlush(tf(20), tf(30)).Return(false),
		ns.EXPECT().NeedsFlush(tf(10), tf(20)).Return(false),
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

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := tf(50)

	gomock.InOrder(
		ns.EXPECT().NeedsFlush(tf(30), tf(40)).Return(true),
		ns.EXPECT().NeedsFlush(tf(20), tf(30)).Return(true),
		ns.EXPECT().NeedsFlush(tf(10), tf(20)).Return(true),
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
