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

	ts := timeFor(36000)
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

	start := timeFor(14400)
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
		if t == timeFor(14400) {
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

type testCaseCleanupMgrNsBlocks struct {
	// input
	id                     string
	nsRetention            testRetentionOptions
	commitlogBlockSizeSecs int64
	blockStartSecs         int64
	// output
	expectedStartSecs int64
	expectedEndSecs   int64
}

type testRetentionOptions struct {
	blockSizeSecs    int64
	bufferPastSecs   int64
	bufferFutureSecs int64
}

func (t *testRetentionOptions) newRetentionOptions() retention.Options {
	return retention.NewOptions().
		SetBufferPast(time.Duration(t.bufferPastSecs) * time.Second).
		SetBufferFuture(time.Duration(t.bufferFutureSecs) * time.Second).
		SetBlockSize(time.Duration(t.blockSizeSecs) * time.Second)
}

func TestCleanupManagerCommitLogNamespaceBlocks(t *testing.T) {
	tcs := []testCaseCleanupMgrNsBlocks{
		{
			id: "test-case-0",
			nsRetention: testRetentionOptions{
				blockSizeSecs:    30,
				bufferPastSecs:   0,
				bufferFutureSecs: 0,
			},
			commitlogBlockSizeSecs: 15,
			blockStartSecs:         15,
			expectedStartSecs:      0,
			expectedEndSecs:        30,
		},
		{
			id: "test-case-1",
			nsRetention: testRetentionOptions{
				blockSizeSecs:    30,
				bufferPastSecs:   0,
				bufferFutureSecs: 0,
			},
			commitlogBlockSizeSecs: 15,
			blockStartSecs:         30,
			expectedStartSecs:      30,
			expectedEndSecs:        30,
		},
		{
			id: "test-case-2",
			nsRetention: testRetentionOptions{
				blockSizeSecs:    10,
				bufferPastSecs:   0,
				bufferFutureSecs: 0,
			},
			commitlogBlockSizeSecs: 15,
			blockStartSecs:         15,
			expectedStartSecs:      10,
			expectedEndSecs:        30,
		},
		{
			id: "test-case-3",
			nsRetention: testRetentionOptions{
				blockSizeSecs:    15,
				bufferPastSecs:   0,
				bufferFutureSecs: 0,
			},
			commitlogBlockSizeSecs: 12,
			blockStartSecs:         24,
			expectedStartSecs:      15,
			expectedEndSecs:        30,
		},
		{
			id: "test-case-4",
			nsRetention: testRetentionOptions{
				blockSizeSecs:    20,
				bufferPastSecs:   5,
				bufferFutureSecs: 0,
			},
			commitlogBlockSizeSecs: 10,
			blockStartSecs:         30,
			expectedStartSecs:      20,
			expectedEndSecs:        40,
		},
		{
			id: "test-case-5",
			nsRetention: testRetentionOptions{
				blockSizeSecs:    20,
				bufferPastSecs:   0,
				bufferFutureSecs: 15,
			},
			commitlogBlockSizeSecs: 10,
			blockStartSecs:         40,
			expectedStartSecs:      40,
			expectedEndSecs:        60,
		},
		{
			id: "test-case-6",
			nsRetention: testRetentionOptions{
				blockSizeSecs:    25,
				bufferPastSecs:   20,
				bufferFutureSecs: 15,
			},
			commitlogBlockSizeSecs: 20,
			blockStartSecs:         40,
			expectedStartSecs:      0,
			expectedEndSecs:        75,
		},
		{
			id: "test-case-7",
			nsRetention: testRetentionOptions{
				blockSizeSecs:    720,
				bufferPastSecs:   720,
				bufferFutureSecs: 60,
			},
			commitlogBlockSizeSecs: 15,
			blockStartSecs:         1410,
			expectedStartSecs:      0,
			expectedEndSecs:        1440,
		},
	}
	for _, tc := range tcs {
		var (
			blockStart         = time.Unix(tc.blockStartSecs, 0)
			commitLogBlockSize = time.Duration(tc.commitlogBlockSizeSecs) * time.Second
			nsRetention        = tc.nsRetention.newRetentionOptions()
			expectedStart      = time.Unix(tc.expectedStartSecs, 0)
			expectedEnd        = time.Unix(tc.expectedEndSecs, 0)
		)
		// blockStart needs to be commitlogBlockSize aligned
		require.Equal(t, blockStart, blockStart.Truncate(commitLogBlockSize), tc.id)
		start, end := commitLogNamespaceBlockTimes(blockStart, commitLogBlockSize, nsRetention)
		require.Equal(t, expectedStart.Unix(), start.Unix(), tc.id)
		require.Equal(t, expectedEnd.Unix(), end.Unix(), tc.id)
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
	currentTime := timeFor(50)

	gomock.InOrder(
		ns.EXPECT().NeedsFlush(timeFor(30), timeFor(40)).Return(false),
		ns.EXPECT().NeedsFlush(timeFor(20), timeFor(30)).Return(false),
		ns.EXPECT().NeedsFlush(timeFor(10), timeFor(20)).Return(false),
	)

	earliest, times := mgr.commitLogTimes(currentTime)
	require.Equal(t, timeFor(10), earliest)
	require.Equal(t, 3, len(times))
	require.True(t, contains(times, timeFor(10)))
	require.True(t, contains(times, timeFor(20)))
	require.True(t, contains(times, timeFor(30)))
}

func TestCleanupManagerCommitLogTimesMiddlePendingFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := timeFor(50)

	gomock.InOrder(
		ns.EXPECT().NeedsFlush(timeFor(30), timeFor(40)).Return(false),
		ns.EXPECT().NeedsFlush(timeFor(20), timeFor(30)).Return(true),
		ns.EXPECT().NeedsFlush(timeFor(10), timeFor(20)).Return(false),
	)

	earliest, times := mgr.commitLogTimes(currentTime)
	require.Equal(t, timeFor(10), earliest)
	require.Equal(t, 2, len(times))
	require.True(t, contains(times, timeFor(10)))
	require.True(t, contains(times, timeFor(30)))
}

func TestCleanupManagerCommitLogTimesStartPendingFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := timeFor(50)

	gomock.InOrder(
		ns.EXPECT().NeedsFlush(timeFor(30), timeFor(40)).Return(true),
		ns.EXPECT().NeedsFlush(timeFor(20), timeFor(30)).Return(false),
		ns.EXPECT().NeedsFlush(timeFor(10), timeFor(20)).Return(false),
	)

	earliest, times := mgr.commitLogTimes(currentTime)
	require.Equal(t, timeFor(10), earliest)
	require.Equal(t, 2, len(times))
	require.True(t, contains(times, timeFor(20)))
	require.True(t, contains(times, timeFor(10)))
}

func TestCleanupManagerCommitLogTimesAllPendingFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	currentTime := timeFor(50)

	gomock.InOrder(
		ns.EXPECT().NeedsFlush(timeFor(30), timeFor(40)).Return(true),
		ns.EXPECT().NeedsFlush(timeFor(20), timeFor(30)).Return(true),
		ns.EXPECT().NeedsFlush(timeFor(10), timeFor(20)).Return(true),
	)

	earliest, times := mgr.commitLogTimes(currentTime)
	require.Equal(t, timeFor(10), earliest)
	require.Equal(t, 0, len(times))
}

func timeFor(s int64) time.Time {
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
