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

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"
	"github.com/pborman/uuid"

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

func TestCleanupManagerCleanupCommitlogsAndSnapshots(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testBlockStart := time.Now().Truncate(2 * time.Hour)
	testSnapshotUUID0 := uuid.Parse("a6367b49-9c83-4706-bd5c-400a4a9ec77c")
	require.NotNil(t, testSnapshotUUID0)

	testSnapshotUUID1 := uuid.Parse("bed2156f-182a-47ea-83ff-0a55d34c8a82")
	require.NotNil(t, testSnapshotUUID1)

	testCommitlogFileIdentifier := persist.CommitlogFile{
		FilePath: "commitlog-filepath-1",
		Start:    time.Now().Truncate(10 * time.Minute),
		Duration: 10 * time.Minute,
		Index:    1,
	}
	testSnapshotMetadataIdentifier1 := fs.SnapshotMetadataIdentifier{
		Index: 0,
		UUID:  testSnapshotUUID0,
	}
	testSnapshotMetadataIdentifier2 := fs.SnapshotMetadataIdentifier{
		Index: 0,
		UUID:  testSnapshotUUID1,
	}
	testSnapshotMetadata0 := fs.SnapshotMetadata{
		ID:                  testSnapshotMetadataIdentifier1,
		CommitlogIdentifier: testCommitlogFileIdentifier,
		MetadataFilePath:    "metadata-filepath-0",
		CheckpointFilePath:  "checkpoint-filepath-0",
	}
	testSnapshotMetadata1 := fs.SnapshotMetadata{
		ID:                  testSnapshotMetadataIdentifier2,
		CommitlogIdentifier: testCommitlogFileIdentifier,
		MetadataFilePath:    "metadata-filepath-1",
		CheckpointFilePath:  "checkpoint-filepath-1",
	}

	testCases := []struct {
		title                string
		snapshotMetadata     sortedSnapshotMetadataFilesFn
		commitlogs           commitLogFilesFn
		snapshots            snapshotFilesFn
		expectedDeletedFiles []string
	}{
		{
			title: "Does nothing if no snapshot metadata files",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return nil, nil, nil
			},
		},
		{
			title: "Does not delete snapshots associated with the most recent snapshot metadata file",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return fs.FileSetFilesSlice{
					{
						ID: fs.FileSetFileIdentifier{
							Namespace:   namespace,
							BlockStart:  testBlockStart,
							Shard:       shard,
							VolumeIndex: 0,
						},
						AbsoluteFilepaths:  []string{fmt.Sprintf("/snapshots/%s/snapshot-filepath-%d", namespace, shard)},
						CachedSnapshotTime: testBlockStart,
						CachedSnapshotID:   testSnapshotUUID0,
					},
				}, nil
			},
			commitlogs: func(commitlog.Options) ([]persist.CommitlogFile, []commitlog.ErrorWithPath, error) {
				return nil, nil, nil
			},
		},
		{
			title: "Deletes snapshots and metadata not associated with the most recent snapshot metadata file",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0, testSnapshotMetadata1}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return fs.FileSetFilesSlice{
					{
						ID: fs.FileSetFileIdentifier{
							Namespace:   namespace,
							BlockStart:  testBlockStart,
							Shard:       shard,
							VolumeIndex: 0,
						},
						AbsoluteFilepaths:  []string{fmt.Sprintf("/snapshots/%s/snapshot-filepath-%d", namespace, shard)},
						CachedSnapshotTime: testBlockStart,
						CachedSnapshotID:   testSnapshotUUID0,
					},
				}, nil
			},
			commitlogs: func(commitlog.Options) ([]persist.CommitlogFile, []commitlog.ErrorWithPath, error) {
				return nil, nil, nil
			},
			expectedDeletedFiles: []string{
				"/snapshots/ns0/snapshot-filepath-0",
				"/snapshots/ns0/snapshot-filepath-1",
				"/snapshots/ns0/snapshot-filepath-2",
				"/snapshots/ns1/snapshot-filepath-0",
				"/snapshots/ns1/snapshot-filepath-1",
				"/snapshots/ns1/snapshot-filepath-2",
				"/snapshots/ns2/snapshot-filepath-0",
				"/snapshots/ns2/snapshot-filepath-1",
				"/snapshots/ns2/snapshot-filepath-2",
				"metadata-filepath-0",
				"checkpoint-filepath-0",
			},
		},
		{
			title: "Deletes corrupt snapshot metadata",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata1}, []fs.SnapshotMetadataErrorWithPaths{
					{
						Error:              errors.New("some-error"),
						MetadataFilePath:   "metadata-filepath-0",
						CheckpointFilePath: "checkpoint-filepath-0",
					},
				}, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return nil, nil
			},
			commitlogs: func(commitlog.Options) ([]persist.CommitlogFile, []commitlog.ErrorWithPath, error) {
				return nil, nil, nil
			},
			expectedDeletedFiles: []string{
				"metadata-filepath-0",
				"checkpoint-filepath-0",
			},
		},
		{
			title: "Deletes corrupt snapshot files",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return fs.FileSetFilesSlice{
					{
						ID: fs.FileSetFileIdentifier{
							Namespace:   namespace,
							BlockStart:  testBlockStart,
							Shard:       shard,
							VolumeIndex: 0,
						},
						AbsoluteFilepaths: []string{fmt.Sprintf("/snapshots/%s/snapshot-filepath-%d", namespace, shard)},
						// Zero these out so it will try to look them up and return an error, indicating the files
						// are corrupt.
						CachedSnapshotTime: time.Time{},
						CachedSnapshotID:   nil,
					},
				}, nil
			},
			commitlogs: func(commitlog.Options) ([]persist.CommitlogFile, []commitlog.ErrorWithPath, error) {
				return nil, nil, nil
			},
			expectedDeletedFiles: []string{
				"/snapshots/ns0/snapshot-filepath-0",
				"/snapshots/ns0/snapshot-filepath-1",
				"/snapshots/ns0/snapshot-filepath-2",
				"/snapshots/ns1/snapshot-filepath-0",
				"/snapshots/ns1/snapshot-filepath-1",
				"/snapshots/ns1/snapshot-filepath-2",
				"/snapshots/ns2/snapshot-filepath-0",
				"/snapshots/ns2/snapshot-filepath-1",
				"/snapshots/ns2/snapshot-filepath-2",
			},
		},
		{
			title: "Does not delete the commitlog identified in the most recent snapshot metadata file, or any with a higher index",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return nil, nil
			},
			commitlogs: func(commitlog.Options) ([]persist.CommitlogFile, []commitlog.ErrorWithPath, error) {
				return []persist.CommitlogFile{
					{FilePath: "commitlog-file-0", Index: 0},
					// Index 1, the one pointed to bby testSnapshotMetdata1
					testCommitlogFileIdentifier,
					{FilePath: "commitlog-file-2", Index: 2},
				}, nil, nil
			},
			// Should only delete anything with an index lower than 1.
			expectedDeletedFiles: []string{"commitlog-file-0"},
		},
		{
			title: "Deletes all corrupt commitlog files",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return nil, nil
			},
			commitlogs: func(commitlog.Options) ([]persist.CommitlogFile, []commitlog.ErrorWithPath, error) {
				return nil, []commitlog.ErrorWithPath{
					commitlog.NewErrorWithPath(errors.New("some-error-0"), "corrupt-commitlog-file-0"),
					commitlog.NewErrorWithPath(errors.New("some-error-1"), "corrupt-commitlog-file-1"),
				}, nil
			},
			// Should only delete anything with an index lower than 1.
			expectedDeletedFiles: []string{"corrupt-commitlog-file-0", "corrupt-commitlog-file-1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			ts := timeFor(36000)
			rOpts := retention.NewOptions().
				SetRetentionPeriod(21600 * time.Second).
				SetBlockSize(7200 * time.Second)
			nsOpts := namespace.NewOptions().SetRetentionOptions(rOpts)

			namespaces := make([]databaseNamespace, 0, 3)
			shards := make([]databaseShard, 0, 3)
			for i := 0; i < 3; i++ {
				shard := NewMockdatabaseShard(ctrl)
				shard.EXPECT().ID().Return(uint32(i)).AnyTimes()
				shard.EXPECT().CleanupExpiredFileSets(gomock.Any()).Return(nil).AnyTimes()
				shards = append(shards, shard)
			}

			for i := 0; i < 3; i++ {
				ns := NewMockdatabaseNamespace(ctrl)
				ns.EXPECT().ID().Return(ident.StringID(fmt.Sprintf("ns%d", i))).AnyTimes()
				ns.EXPECT().Options().Return(nsOpts).AnyTimes()
				ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
				ns.EXPECT().GetOwnedShards().Return(shards).AnyTimes()
				namespaces = append(namespaces, ns)
			}

			db := newMockdatabase(ctrl, namespaces...)
			db.EXPECT().GetOwnedNamespaces().Return(namespaces, nil).AnyTimes()
			mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)
			mgr.opts = mgr.opts.SetCommitLogOptions(
				mgr.opts.CommitLogOptions().
					SetBlockSize(rOpts.BlockSize()))

			mgr.sortedSnapshotMetadataFilesFn = tc.snapshotMetadata
			mgr.commitLogFilesFn = tc.commitlogs
			mgr.snapshotFilesFn = tc.snapshots

			var deletedFiles []string
			mgr.deleteFilesFn = func(files []string) error {
				deletedFiles = append(deletedFiles, files...)
				return nil
			}

			require.NoError(t, mgr.Cleanup(ts))
			require.Equal(t, tc.expectedDeletedFiles, deletedFiles)
		})
	}
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

	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)
	idx.EXPECT().CleanupExpiredFileSets(ts).Return(nil)
	require.NoError(t, mgr.Cleanup(ts))
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
	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ts := timeFor(36000)

	nsOpts := namespace.NewOptions()
	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()

	shard := NewMockdatabaseShard(ctrl)
	expectedEarliestToRetain := retention.FlushTimeStart(ns.Options().RetentionOptions(), ts)
	shard.EXPECT().CleanupExpiredFileSets(expectedEarliestToRetain).Return(nil)
	shard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	ns.EXPECT().GetOwnedShards().Return([]databaseShard{shard}).AnyTimes()
	ns.EXPECT().ID().Return(ident.StringID("nsID")).AnyTimes()
	ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false).AnyTimes()
	namespaces := []databaseNamespace{ns}

	db := newMockdatabase(ctrl, namespaces...)
	db.EXPECT().GetOwnedNamespaces().Return(namespaces, nil).AnyTimes()
	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)

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
	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)

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

	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)
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

func timeFor(s int64) time.Time {
	return time.Unix(s, 0)
}

// TODO: Delete if unused
type fakeActiveLogs struct {
	activeLogs []persist.CommitlogFile
}

func (f fakeActiveLogs) ActiveLogs() ([]persist.CommitlogFile, error) {
	return f.activeLogs, nil
}

func newNoopFakeActiveLogs() fakeActiveLogs {
	return newFakeActiveLogs(nil)
}

func newFakeActiveLogs(activeLogs []persist.CommitlogFile) fakeActiveLogs {
	return fakeActiveLogs{
		activeLogs: activeLogs,
	}
}
