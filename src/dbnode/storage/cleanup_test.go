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
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/ident"

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
		SetBlockSize(7200 * time.Second)
	nsOpts := namespace.NewOptions().SetRetentionOptions(rOpts)

	namespaces := make([]databaseNamespace, 0, 3)
	for range namespaces {
		ns := NewMockdatabaseNamespace(ctrl)
		ns.EXPECT().Options().Return(nsOpts).AnyTimes()
		ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
		namespaces = append(namespaces, ns)
	}
	db := newMockdatabase(ctrl, namespaces...)
	db.EXPECT().GetOwnedNamespaces().Return(namespaces, nil).AnyTimes()
	mgr := newCleanupManager(db, tally.NoopScope).(*cleanupManager)
	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().
			SetRetentionPeriod(rOpts.RetentionPeriod()).
			SetBlockSize(rOpts.BlockSize()))

	mgr.commitLogFilesBeforeFn = func(_ string, t time.Time) ([]string, error) {
		return []string{"foo", "bar"}, errors.New("error1")
	}
	mgr.commitLogFilesForTimeFn = func(_ string, t time.Time) ([]string, error) {
		if t.Equal(timeFor(14400)) {
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

// Test NS doesn't cleanup when flag is present
func TestCleanupManagerDoesntNeedCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ts := timeFor(36000)
	rOpts := retention.NewOptions().
		SetRetentionPeriod(21600 * time.Second).
		SetBlockSize(7200 * time.Second)
	nsOpts := namespace.NewOptions().SetRetentionOptions(rOpts).SetCleanupEnabled(false)

	namespaces := make([]databaseNamespace, 0, 3)
	for range namespaces {
		ns := NewMockdatabaseNamespace(ctrl)
		ns.EXPECT().Options().Return(nsOpts).AnyTimes()
		ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
		namespaces = append(namespaces, ns)
	}
	db := newMockdatabase(ctrl, namespaces...)
	db.EXPECT().GetOwnedNamespaces().Return(namespaces, nil).AnyTimes()
	mgr := newCleanupManager(db, tally.NoopScope).(*cleanupManager)
	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().
			SetRetentionPeriod(rOpts.RetentionPeriod()).
			SetBlockSize(rOpts.BlockSize()))

	mgr.commitLogFilesBeforeFn = func(_ string, t time.Time) ([]string, error) {
		return []string{"foo", "bar"}, nil
	}
	mgr.commitLogFilesForTimeFn = func(_ string, t time.Time) ([]string, error) {
		return nil, nil
	}
	var deletedFiles []string
	mgr.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}

	require.NoError(t, mgr.Cleanup(ts))
}

func TestCleanupDataAndSnapshotFileSetFiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ts := timeFor(36000)

	nsOpts := namespace.NewOptions()
	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()

	shard := NewMockdatabaseShard(ctrl)
	expectedEarliestToRetain := retention.FlushTimeStart(ns.Options().RetentionOptions(), ts)
	shard.EXPECT().CleanupFileSet(expectedEarliestToRetain).Return(nil)
	shard.EXPECT().CleanupSnapshots(expectedEarliestToRetain)
	shard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	ns.EXPECT().GetOwnedShards().Return([]databaseShard{shard}).AnyTimes()
	ns.EXPECT().ID().Return(ident.StringID("nsID")).AnyTimes()
	ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
	namespaces := []databaseNamespace{ns}

	db := newMockdatabase(ctrl, namespaces...)
	db.EXPECT().GetOwnedNamespaces().Return(namespaces, nil).AnyTimes()
	mgr := newCleanupManager(db, tally.NoopScope).(*cleanupManager)

	require.NoError(t, mgr.Cleanup(ts))
}

type deleteInactiveDirectoriesCall struct {
	parentDirPath  string
	activeDirNames []string
}

func TestDeleteInactiveDataAndSnapshotFileSetFiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ts := timeFor(36000)

	nsOpts := namespace.NewOptions().
		SetCleanupEnabled(false)
	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()

	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	ns.EXPECT().GetOwnedShards().Return([]databaseShard{shard}).AnyTimes()
	ns.EXPECT().ID().Return(ident.StringID("nsID")).AnyTimes()
	ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
	namespaces := []databaseNamespace{ns}

	db := newMockdatabase(ctrl, namespaces...)
	db.EXPECT().GetOwnedNamespaces().Return(namespaces, nil).AnyTimes()
	mgr := newCleanupManager(db, tally.NoopScope).(*cleanupManager)

	deleteInactiveDirectoriesCalls := []deleteInactiveDirectoriesCall{}
	deleteInactiveDirectoriesFn := func(parentDirPath string, activeDirNames []string) error {
		deleteInactiveDirectoriesCalls = append(deleteInactiveDirectoriesCalls, deleteInactiveDirectoriesCall{
			parentDirPath:  parentDirPath,
			activeDirNames: activeDirNames,
		})
		return nil
	}
	mgr.deleteInactiveDirectoriesFn = deleteInactiveDirectoriesFn

	require.NoError(t, mgr.Cleanup(ts))

	expectedCalls := []deleteInactiveDirectoriesCall{
		deleteInactiveDirectoriesCall{
			parentDirPath:  "data/nsID",
			activeDirNames: []string{"0"},
		},
		deleteInactiveDirectoriesCall{
			parentDirPath:  "snapshots/nsID",
			activeDirNames: []string{"0"},
		},
		deleteInactiveDirectoriesCall{
			parentDirPath:  "data",
			activeDirNames: []string{"nsID"},
		},
	}

	for _, expectedCall := range expectedCalls {
		found := false
		for _, call := range deleteInactiveDirectoriesCalls {
			if strings.Contains(call.parentDirPath, expectedCall.parentDirPath) &&
				expectedCall.activeDirNames[0] == call.activeDirNames[0] {
				found = true
			}
		}
		require.Equal(t, true, found)
	}
}

func TestCleanupManagerPropagatesGetOwnedNamespacesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ts := timeFor(36000)

	db := NewMockdatabase(ctrl)
	db.EXPECT().Options().Return(testDatabaseOptions()).AnyTimes()
	db.EXPECT().Open().Return(nil)
	db.EXPECT().Terminate().Return(nil)
	db.EXPECT().GetOwnedNamespaces().Return(nil, errDatabaseIsClosed).AnyTimes()

	mgr := newCleanupManager(db, tally.NoopScope).(*cleanupManager)
	require.NoError(t, db.Open())
	require.NoError(t, db.Terminate())

	require.Error(t, mgr.Cleanup(ts))
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
		mgr.opts.CommitLogOptions().
			SetRetentionPeriod(rOpts.RetentionPeriod()).
			SetBlockSize(rOpts.BlockSize()))
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
		mgr.opts.CommitLogOptions().
			SetRetentionPeriod(rOpts.RetentionPeriod()).
			SetBlockSize(rOpts.BlockSize()))
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

	earliest, times, err := mgr.commitLogTimes(currentTime)
	require.NoError(t, err)
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

	earliest, times, err := mgr.commitLogTimes(currentTime)
	require.NoError(t, err)
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

	earliest, times, err := mgr.commitLogTimes(currentTime)
	require.NoError(t, err)
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

	earliest, times, err := mgr.commitLogTimes(currentTime)
	require.NoError(t, err)
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
