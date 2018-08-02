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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	currentTime        = timeFor(50)
	time10             = timeFor(10)
	time20             = timeFor(20)
	time30             = timeFor(30)
	time40             = timeFor(40)
	commitLogBlockSize = 10 * time.Second
)

func TestCleanupManagerCleanup(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ts := timeFor(36000)
	rOpts := retention.NewOptions().
		SetRetentionPeriod(21600 * time.Second).
		SetBlockSize(7200 * time.Second)
	nsOpts := namespace.NewOptions().SetRetentionOptions(rOpts)

	namespaces := make([]databaseNamespace, 0, 3)
	for i := 0; i < 3; i++ {
		ns := NewMockdatabaseNamespace(ctrl)
		ns.EXPECT().ID().Return(ident.StringID(fmt.Sprintf("ns%d", i))).AnyTimes()
		ns.EXPECT().Options().Return(nsOpts).AnyTimes()
		ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
		ns.EXPECT().GetOwnedShards().Return(nil).AnyTimes()
		namespaces = append(namespaces, ns)
	}
	db := newMockdatabase(ctrl, namespaces...)
	db.EXPECT().GetOwnedNamespaces().Return(namespaces, nil).AnyTimes()
	mgr := newCleanupManager(db, tally.NoopScope).(*cleanupManager)
	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().
			SetBlockSize(rOpts.BlockSize()))

	mgr.commitLogFilesFn = func(_ commitlog.Options) ([]commitlog.File, error) {
		return []commitlog.File{
			commitlog.File{FilePath: "foo", Start: timeFor(14400)},
		}, nil
	}
	var deletedFiles []string
	mgr.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}

	require.NoError(t, mgr.Cleanup(ts))
	require.Equal(t, []string{"foo"}, deletedFiles)
}

func TestCleanupManagerNamespaceCleanup(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ts := timeFor(36000)
	rOpts := retention.NewOptions().
		SetRetentionPeriod(21600 * time.Second).
		SetBlockSize(3600 * time.Second)
	nsOpts := namespace.NewOptions().
		SetRetentionOptions(rOpts).
		SetCleanupEnabled(true).
		SetIndexOptions(namespace.NewIndexOptions().
			SetEnabled(true).
			SetBlockSize(7200 * time.Second))

	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().ID().Return(ident.StringID("ns")).AnyTimes()
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()
	ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
	ns.EXPECT().GetOwnedShards().Return(nil).AnyTimes()

	idx := NewMocknamespaceIndex(ctrl)
	ns.EXPECT().GetIndex().Return(idx, nil)

	nses := []databaseNamespace{ns}
	db := newMockdatabase(ctrl, ns)
	db.EXPECT().GetOwnedNamespaces().Return(nses, nil).AnyTimes()

	mgr := newCleanupManager(db, tally.NoopScope).(*cleanupManager)
	idx.EXPECT().CleanupExpiredFileSets(ts).Return(nil)
	require.NoError(t, mgr.Cleanup(ts))
}

// Test NS doesn't cleanup when flag is present
func TestCleanupManagerDoesntNeedCleanup(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
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
			SetBlockSize(rOpts.BlockSize()))

	var deletedFiles []string
	mgr.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}

	require.NoError(t, mgr.Cleanup(ts))
}

func TestCleanupDataAndSnapshotFileSetFiles(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()
	ts := timeFor(36000)

	nsOpts := namespace.NewOptions()
	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()

	shard := NewMockdatabaseShard(ctrl)
	expectedEarliestToRetain := retention.FlushTimeStart(ns.Options().RetentionOptions(), ts)
	shard.EXPECT().CleanupExpiredFileSets(expectedEarliestToRetain).Return(nil)
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
	ctrl := gomock.NewController(xtest.Reporter{t})
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
	ctrl := gomock.NewController(xtest.Reporter{t})
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
			SetBlockSize(rOpts.BlockSize()))
	return ns, mgr
}

func newCleanupManagerCommitLogTimesTestMultiNS(
	t *testing.T,
	ctrl *gomock.Controller,
) (*MockdatabaseNamespace, *MockdatabaseNamespace, *cleanupManager) {
	var (
		rOpts = retention.NewOptions().
			SetRetentionPeriod(30 * time.Second).
			SetBufferPast(0 * time.Second).
			SetBufferFuture(0 * time.Second).
			SetBlockSize(10 * time.Second)
	)
	no := namespace.NewMockOptions(ctrl)
	no.EXPECT().RetentionOptions().Return(rOpts).AnyTimes()

	ns1 := NewMockdatabaseNamespace(ctrl)
	ns1.EXPECT().Options().Return(no).AnyTimes()

	ns2 := NewMockdatabaseNamespace(ctrl)
	ns2.EXPECT().Options().Return(no).AnyTimes()

	db := newMockdatabase(ctrl, ns1, ns2)
	mgr := newCleanupManager(db, tally.NoopScope).(*cleanupManager)

	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().
			SetBlockSize(rOpts.BlockSize()))
	return ns1, ns2, mgr
}

func TestCleanupManagerCommitLogTimesAllFlushed(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	mgr.commitLogFilesFn = func(_ commitlog.Options) ([]commitlog.File, error) {
		return []commitlog.File{
			commitlog.File{Start: time10, Duration: commitLogBlockSize},
			commitlog.File{Start: time20, Duration: commitLogBlockSize},
			commitlog.File{Start: time30, Duration: commitLogBlockSize},
		}, nil
	}

	gomock.InOrder(
		ns.EXPECT().NeedsFlush(time10, time20).Return(false),
		ns.EXPECT().NeedsFlush(time20, time30).Return(false),
		ns.EXPECT().NeedsFlush(time30, time40).Return(false),
	)

	filesToCleanup, err := mgr.commitLogTimes(currentTime)
	require.NoError(t, err)
	require.Equal(t, 3, len(filesToCleanup))
	require.True(t, contains(filesToCleanup, time10))
	require.True(t, contains(filesToCleanup, time20))
	require.True(t, contains(filesToCleanup, time30))
}

func TestCleanupManagerCommitLogTimesMiddlePendingFlush(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	mgr.commitLogFilesFn = func(_ commitlog.Options) ([]commitlog.File, error) {
		return []commitlog.File{
			commitlog.File{Start: time10, Duration: commitLogBlockSize},
			commitlog.File{Start: time20, Duration: commitLogBlockSize},
			commitlog.File{Start: time30, Duration: commitLogBlockSize},
		}, nil
	}

	ns.EXPECT().IsCapturedBySnapshot(
		gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	gomock.InOrder(
		ns.EXPECT().NeedsFlush(time10, time20).Return(false),
		ns.EXPECT().NeedsFlush(time20, time30).Return(true),
		ns.EXPECT().NeedsFlush(time30, time40).Return(false),
	)

	filesToCleanup, err := mgr.commitLogTimes(currentTime)
	require.NoError(t, err)
	require.Equal(t, 2, len(filesToCleanup))
	require.True(t, contains(filesToCleanup, time10))
	require.True(t, contains(filesToCleanup, time30))
}

func TestCleanupManagerCommitLogTimesStartPendingFlush(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	mgr.commitLogFilesFn = func(_ commitlog.Options) ([]commitlog.File, error) {
		return []commitlog.File{
			commitlog.File{Start: time10, Duration: commitLogBlockSize},
			commitlog.File{Start: time20, Duration: commitLogBlockSize},
			commitlog.File{Start: time30, Duration: commitLogBlockSize},
		}, nil
	}

	ns.EXPECT().IsCapturedBySnapshot(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(false, nil).AnyTimes()
	gomock.InOrder(
		ns.EXPECT().NeedsFlush(time10, time20).Return(false),
		ns.EXPECT().NeedsFlush(time20, time30).Return(false),
		ns.EXPECT().NeedsFlush(time30, time40).Return(true),
	)

	filesToCleanup, err := mgr.commitLogTimes(currentTime)
	require.NoError(t, err)
	require.Equal(t, 2, len(filesToCleanup))
	require.True(t, contains(filesToCleanup, time20))
	require.True(t, contains(filesToCleanup, time10))
}

func TestCleanupManagerCommitLogTimesAllPendingFlush(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	mgr.commitLogFilesFn = func(_ commitlog.Options) ([]commitlog.File, error) {
		return []commitlog.File{
			commitlog.File{Start: time10, Duration: commitLogBlockSize},
			commitlog.File{Start: time20, Duration: commitLogBlockSize},
			commitlog.File{Start: time30, Duration: commitLogBlockSize},
		}, nil
	}

	ns.EXPECT().IsCapturedBySnapshot(
		gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	gomock.InOrder(
		ns.EXPECT().NeedsFlush(time10, time20).Return(true),
		ns.EXPECT().NeedsFlush(time20, time30).Return(true),
		ns.EXPECT().NeedsFlush(time30, time40).Return(true),
	)

	filesToCleanup, err := mgr.commitLogTimes(currentTime)
	require.NoError(t, err)
	require.Equal(t, 0, len(filesToCleanup))
}

func timeFor(s int64) time.Time {
	return time.Unix(s, 0)
}

func contains(arr []commitlog.File, t time.Time) bool {
	for _, at := range arr {
		if at.Start.Equal(t) {
			return true
		}
	}
	return false
}

func TestCleanupManagerCommitLogTimesAllPendingFlushButHaveSnapshot(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	var (
		ns, mgr            = newCleanupManagerCommitLogTimesTest(t, ctrl)
		currentTime        = timeFor(50)
		commitLogBlockSize = 10 * time.Second
	)
	mgr.commitLogFilesFn = func(_ commitlog.Options) ([]commitlog.File, error) {
		return []commitlog.File{
			commitlog.File{Start: time10, Duration: commitLogBlockSize},
			commitlog.File{Start: time20, Duration: commitLogBlockSize},
			commitlog.File{Start: time30, Duration: commitLogBlockSize},
		}, nil
	}

	gomock.InOrder(
		// Commit log with start time10 captured by snapshot,
		// should be able to delete.
		ns.EXPECT().NeedsFlush(time10, time20).Return(true),
		ns.EXPECT().IsCapturedBySnapshot(
			gomock.Any(), gomock.Any(), time20).Return(true, nil),
		// Commit log with start time20 captured by snapshot,
		// should be able to delete.
		ns.EXPECT().NeedsFlush(time20, time30).Return(true),
		ns.EXPECT().IsCapturedBySnapshot(
			gomock.Any(), gomock.Any(), time30).Return(true, nil),
		// Commit log with start time30 not captured by snapshot,
		// will need to retain.
		ns.EXPECT().NeedsFlush(time30, time40).Return(true),
		ns.EXPECT().IsCapturedBySnapshot(
			gomock.Any(), gomock.Any(), time40).Return(false, nil),
	)

	filesToCleanup, err := mgr.commitLogTimes(currentTime)
	require.NoError(t, err)

	// Only commit log files with starts time10 and time20 were
	// captured by snapshot files, so those are the only ones
	// we can delete.
	require.True(t, contains(filesToCleanup, time10))
	require.True(t, contains(filesToCleanup, time20))
}

func TestCleanupManagerCommitLogTimesHandlesIsCapturedBySnapshotError(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ns, mgr := newCleanupManagerCommitLogTimesTest(t, ctrl)
	mgr.commitLogFilesFn = func(_ commitlog.Options) ([]commitlog.File, error) {
		return []commitlog.File{
			commitlog.File{Start: time30, Duration: commitLogBlockSize},
		}, nil
	}

	gomock.InOrder(
		ns.EXPECT().NeedsFlush(time30, time40).Return(true),
		ns.EXPECT().IsCapturedBySnapshot(
			gomock.Any(), gomock.Any(), time40).Return(false, errors.New("err")),
	)

	_, err := mgr.commitLogTimes(currentTime)
	require.Error(t, err)
}

func TestCleanupManagerCommitLogTimesMultiNS(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ns1, ns2, mgr := newCleanupManagerCommitLogTimesTestMultiNS(t, ctrl)
	mgr.commitLogFilesFn = func(_ commitlog.Options) ([]commitlog.File, error) {
		return []commitlog.File{
			commitlog.File{Start: time10, Duration: commitLogBlockSize},
			commitlog.File{Start: time20, Duration: commitLogBlockSize},
			commitlog.File{Start: time30, Duration: commitLogBlockSize},
		}, nil
	}

	// ns1 is flushed for time10->time20 and time20->time30.
	// It is not flushed for time30->time40, but it doe have
	// a snapshot that covers that range.
	//
	// ns2 is flushed for time10->time20. It is not flushed for
	// time20->time30 but it does have a snapshot that covers
	// that range. It does not have a flush or snapshot for
	// time30->time40.
	gomock.InOrder(
		ns1.EXPECT().NeedsFlush(time10, time20).Return(false),
		ns2.EXPECT().NeedsFlush(time10, time20).Return(false),

		ns1.EXPECT().NeedsFlush(time20, time30).Return(false),
		ns2.EXPECT().NeedsFlush(time20, time30).Return(true),
		ns2.EXPECT().IsCapturedBySnapshot(
			gomock.Any(), gomock.Any(), time30).Return(true, nil),

		ns1.EXPECT().NeedsFlush(time30, time40).Return(true),
		ns1.EXPECT().IsCapturedBySnapshot(
			gomock.Any(), gomock.Any(), time40).Return(true, nil),
		ns2.EXPECT().NeedsFlush(time30, time40).Return(true),
		ns2.EXPECT().IsCapturedBySnapshot(
			gomock.Any(), gomock.Any(), time40).Return(false, nil),
	)

	filesToCleanup, err := mgr.commitLogTimes(currentTime)
	require.NoError(t, err)

	// time10 and time20 were covered by either a flush or snapshot
	// for both namespaces, but time30 was only covered for ns1 by
	// a snapshot, and ns2 didn't have a snapshot or flush for that
	// time so the file needs to be retained.
	require.True(t, contains(filesToCleanup, time10))
	require.True(t, contains(filesToCleanup, time20))
}
