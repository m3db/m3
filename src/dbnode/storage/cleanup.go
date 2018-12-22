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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
)

type commitLogFilesFn func(commitlog.Options) (persist.CommitlogFiles, []commitlog.ErrorWithPath, error)
type sortedSnapshotMetadataFilesFn func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error)

type snapshotFilesFn func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error)

type deleteFilesFn func(files []string) error

type deleteInactiveDirectoriesFn func(parentDirPath string, activeDirNames []string) error

// Narrow interface so as not to expose all the functionality of the commitlog
// to the cleanup manager.
type activeCommitlogs interface {
	ActiveLogs() (persist.CommitlogFiles, error)
}

type cleanupManager struct {
	sync.RWMutex

	database         database
	activeCommitlogs activeCommitlogs

	opts                          Options
	nowFn                         clock.NowFn
	filePathPrefix                string
	commitLogsDir                 string
	commitLogFilesFn              commitLogFilesFn
	sortedSnapshotMetadataFilesFn sortedSnapshotMetadataFilesFn
	snapshotFilesFn               snapshotFilesFn

	deleteFilesFn               deleteFilesFn
	deleteInactiveDirectoriesFn deleteInactiveDirectoriesFn
	cleanupInProgress           bool
	metrics                     cleanupManagerMetrics
}

type cleanupManagerMetrics struct {
	status                      tally.Gauge
	corruptCommitlogFile        tally.Counter
	corruptSnapshotMetadataFile tally.Counter
	deletedCommitlogFile        tally.Counter
	deletedSnapshotMetadataFile tally.Counter
}

func newCleanupManagerMetrics(scope tally.Scope) cleanupManagerMetrics {
	clScope := scope.SubScope("commitlog")
	smScope := scope.SubScope("snapshot-metadata")
	return cleanupManagerMetrics{
		status:                      scope.Gauge("cleanup"),
		corruptCommitlogFile:        clScope.Counter("corrupt"),
		corruptSnapshotMetadataFile: smScope.Counter("corrupt"),
		deletedCommitlogFile:        clScope.Counter("deleted"),
		deletedSnapshotMetadataFile: smScope.Counter("deleted"),
	}
}

func newCleanupManager(
	database database, activeLogs activeCommitlogs, scope tally.Scope) databaseCleanupManager {
	opts := database.Options()
	filePathPrefix := opts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
	commitLogsDir := fs.CommitLogsDirPath(filePathPrefix)

	return &cleanupManager{
		database:         database,
		activeCommitlogs: activeLogs,

		opts:                          opts,
		nowFn:                         opts.ClockOptions().NowFn(),
		filePathPrefix:                filePathPrefix,
		commitLogsDir:                 commitLogsDir,
		commitLogFilesFn:              commitlog.Files,
		sortedSnapshotMetadataFilesFn: fs.SortedSnapshotMetadataFiles,
		snapshotFilesFn:               fs.SnapshotFiles,
		deleteFilesFn:                 fs.DeleteFiles,
		deleteInactiveDirectoriesFn:   fs.DeleteInactiveDirectories,
		metrics:                       newCleanupManagerMetrics(scope),
	}
}

func (m *cleanupManager) Cleanup(t time.Time) error {
	m.Lock()
	m.cleanupInProgress = true
	m.Unlock()

	defer func() {
		m.Lock()
		m.cleanupInProgress = false
		m.Unlock()
	}()

	multiErr := xerrors.NewMultiError()
	if err := m.cleanupExpiredDataFiles(t); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when cleaning up data files for %v: %v", t, err))
	}

	if err := m.cleanupExpiredIndexFiles(t); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when cleaning up index files for %v: %v", t, err))
	}

	if err := m.deleteInactiveDataFiles(); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when deleting inactive data files for %v: %v", t, err))
	}

	if err := m.deleteInactiveDataSnapshotFiles(); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when deleting inactive snapshot files for %v: %v", t, err))
	}

	if err := m.deleteInactiveNamespaceFiles(); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when deleting inactive namespace files for %v: %v", t, err))
	}

	if err := m.cleanupSnapshotsAndCommitlogs(); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when cleaning up snapshot and commitlog files: %v", err))
	}

	return multiErr.FinalError()
}

func (m *cleanupManager) Report() {
	m.RLock()
	cleanupInProgress := m.cleanupInProgress
	m.RUnlock()

	if cleanupInProgress {
		m.metrics.status.Update(1)
	} else {
		m.metrics.status.Update(0)
	}
}

func (m *cleanupManager) deleteInactiveNamespaceFiles() error {
	var namespaceDirNames []string
	filePathPrefix := m.database.Options().CommitLogOptions().FilesystemOptions().FilePathPrefix()
	dataDirPath := fs.DataDirPath(filePathPrefix)
	namespaces, err := m.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}

	for _, n := range namespaces {
		namespaceDirNames = append(namespaceDirNames, n.ID().String())
	}

	return m.deleteInactiveDirectoriesFn(dataDirPath, namespaceDirNames)
}

// deleteInactiveDataFiles will delete data files for shards that the node no longer owns
// which can occur in the case of topology changes
func (m *cleanupManager) deleteInactiveDataFiles() error {
	return m.deleteInactiveDataFileSetFiles(fs.NamespaceDataDirPath)
}

// deleteInactiveDataSnapshotFiles will delete snapshot files for shards that the node no longer owns
// which can occur in the case of topology changes
func (m *cleanupManager) deleteInactiveDataSnapshotFiles() error {
	return m.deleteInactiveDataFileSetFiles(fs.NamespaceSnapshotsDirPath)
}

func (m *cleanupManager) deleteInactiveDataFileSetFiles(filesetFilesDirPathFn func(string, ident.ID) string) error {
	multiErr := xerrors.NewMultiError()
	filePathPrefix := m.database.Options().CommitLogOptions().FilesystemOptions().FilePathPrefix()
	namespaces, err := m.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}
	for _, n := range namespaces {
		var activeShards []string
		namespaceDirPath := filesetFilesDirPathFn(filePathPrefix, n.ID())
		for _, s := range n.GetOwnedShards() {
			shard := fmt.Sprintf("%d", s.ID())
			activeShards = append(activeShards, shard)
		}
		multiErr = multiErr.Add(m.deleteInactiveDirectoriesFn(namespaceDirPath, activeShards))
	}

	return multiErr.FinalError()
}

func (m *cleanupManager) cleanupExpiredDataFiles(t time.Time) error {
	multiErr := xerrors.NewMultiError()
	namespaces, err := m.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}
	for _, n := range namespaces {
		if !n.Options().CleanupEnabled() {
			continue
		}
		earliestToRetain := retention.FlushTimeStart(n.Options().RetentionOptions(), t)
		shards := n.GetOwnedShards()
		multiErr = multiErr.Add(m.cleanupExpiredNamespaceDataFiles(earliestToRetain, shards))
	}
	return multiErr.FinalError()
}

func (m *cleanupManager) cleanupExpiredIndexFiles(t time.Time) error {
	namespaces, err := m.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}
	multiErr := xerrors.NewMultiError()
	for _, n := range namespaces {
		if !n.Options().CleanupEnabled() || !n.Options().IndexOptions().Enabled() {
			continue
		}
		idx, err := n.GetIndex()
		if err != nil {
			multiErr = multiErr.Add(err)
			continue
		}
		multiErr = multiErr.Add(idx.CleanupExpiredFileSets(t))
	}
	return multiErr.FinalError()
}

func (m *cleanupManager) cleanupExpiredNamespaceDataFiles(earliestToRetain time.Time, shards []databaseShard) error {
	multiErr := xerrors.NewMultiError()
	for _, shard := range shards {
		if err := shard.CleanupExpiredFileSets(earliestToRetain); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

// The goal of the cleanupSnapshotsAndCommitlogs function is to delete all snapshots files, snapshot metadata
// files, and commitlog files except for those that are currently required for recovery from a node failure.
// According to the snapshotting / commitlog rotation logic, the files that are required for a complete
// recovery are:
//
//     1. The most recent (highest index) snapshot metadata files.
//     2. All snapshot files whose associated snapshot ID matches the snapshot ID of the most recent snapshot
//        metadata file.
//     3. All commitlog files whose index is larger than or equal to the index of the commitlog identifier stored
//        in the most recent snapshot metadata file. This is because the snapshotting and commitlog rotation process
//        guarantees that the most recent snapshot contains all data stored in commitlogs that were created before
//        the rotation / snapshot process began.
//
// cleanupSnapshotsAndCommitlogs accomplishes this goal by performing the following steps:
//
//     1. List all the snapshot metadata files on disk.
//     2. Identify the most recent one (highest index).
//     3. For every namespace/shard/block combination, delete all snapshot files that match one of the following criteria:
//         1. Snapshot files whose associated snapshot ID does not match the snapshot ID of the most recent
//            snapshot metadata file.
//         2. Snapshot files that are corrupt.
//     4. Delete all snapshot metadata files prior to the most recent once.
//     5. Delete corrupt snapshot metadata files.
//     6. List all the commitlog files on disk.
//     7. List all the commitlog files that are being actively written to.
//     8. Delete all commitlog files whose index is lower than the index of the commitlog file referenced in the
//        most recent snapshot metadata file (ignoring any commitlog files being actively written to.)
//     9. Delete all corrupt commitlog files (ignoring any commitlog files being actively written to.)
func (m *cleanupManager) cleanupSnapshotsAndCommitlogs() (finalErr error) {
	namespaces, err := m.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}

	fsOpts := m.opts.CommitLogOptions().FilesystemOptions()
	sortedSnapshotMetadatas, snapshotMetadataErrorsWithPaths, err := m.sortedSnapshotMetadataFilesFn(fsOpts)
	if err != nil {
		return err
	}

	// Assert that the snapshot metadata files are indeed sorted.
	lastMetadataIndex := int64(-1)
	for _, snapshotMetadata := range sortedSnapshotMetadatas {
		currIndex := snapshotMetadata.ID.Index
		if !(currIndex > lastMetadataIndex) {
			// Should never happen.
			return fmt.Errorf(
				"snapshot metadata files are not sorted, previous index: %d, current index: %d",
				lastMetadataIndex, currIndex)
		}
		lastMetadataIndex = currIndex
	}

	if len(sortedSnapshotMetadatas) == 0 {
		// No cleanup can be performed until we have at least one complete snapshot.
		return nil
	}

	var (
		multiErr           = xerrors.NewMultiError()
		filesToDelete      = []string{}
		mostRecentSnapshot = sortedSnapshotMetadatas[len(sortedSnapshotMetadatas)-1]
	)
	defer func() {
		// Use a defer to perform the final file deletion so that we can attempt to cleanup *some* files
		// when we encounter partial errors on a best effort basis.
		multiErr = multiErr.Add(finalErr)
		multiErr = multiErr.Add(m.deleteFilesFn(filesToDelete))
		finalErr = multiErr.FinalError()
	}()

	for _, ns := range namespaces {
		for _, s := range ns.GetOwnedShards() {
			shardSnapshots, err := m.snapshotFilesFn(fsOpts.FilePathPrefix(), ns.ID(), s.ID())
			if err != nil {
				multiErr = multiErr.Add(fmt.Errorf("err reading snapshot files for ns: %s and shard: %d, err: %v", ns.ID(), s.ID(), err))
				continue
			}

			for _, snapshot := range shardSnapshots {
				_, snapshotID, err := snapshot.SnapshotTimeAndID()
				if err != nil {
					// If we can't parse the snapshotID, assume the snapshot is corrupt and delete it. This could be caused
					// by a variety of situations, like a node crashing while writing out a set of snapshot files and should
					// have no impact on correctness as the snapshot files from previous (successful) snapshot will still be
					// retained.
					m.opts.InstrumentOptions().Logger().WithFields(
						xlog.NewField("err", err),
						xlog.NewField("files", snapshot.AbsoluteFilepaths),
					).Errorf(
						"encountered corrupt snapshot file during cleanup, marking files for deletion")
					filesToDelete = append(filesToDelete, snapshot.AbsoluteFilepaths...)
					continue
				}

				if !uuid.Equal(snapshotID, mostRecentSnapshot.ID.UUID) {
					// If the UUID of the snapshot files doesn't match the most recent snapshot
					// then its safe to delete because it means we have a more recently complete set.
					filesToDelete = append(filesToDelete, snapshot.AbsoluteFilepaths...)
				}
			}
		}
	}

	// Delete all snapshot metadatas prior to the most recent one.
	for _, snapshot := range sortedSnapshotMetadatas[:len(sortedSnapshotMetadatas)-1] {
		m.metrics.deletedSnapshotMetadataFile.Inc(1)
		filesToDelete = append(filesToDelete, snapshot.AbsoluteFilepaths()...)
	}

	// Delete corrupt snapshot metadata files.
	for _, errorWithPath := range snapshotMetadataErrorsWithPaths {
		m.metrics.corruptSnapshotMetadataFile.Inc(1)
		m.opts.InstrumentOptions().Logger().WithFields(
			xlog.NewField("err", errorWithPath.Error),
			xlog.NewField("metadataFilePath", errorWithPath.MetadataFilePath),
			xlog.NewField("checkpointFilePath", errorWithPath.CheckpointFilePath),
		).Errorf(
			"encountered corrupt snapshot metadata file during cleanup, marking files for deletion")
		filesToDelete = append(filesToDelete, errorWithPath.MetadataFilePath)
		filesToDelete = append(filesToDelete, errorWithPath.CheckpointFilePath)
	}

	// Figure out which commitlog files exist on disk.
	files, commitlogErrorsWithPaths, err := m.commitLogFilesFn(m.opts.CommitLogOptions())
	if err != nil {
		// Hard failure here because the remaining cleanup logic relies on this data
		// being available.
		return err
	}

	// Figure out which commitlog files are being actively written to.
	activeCommitlogs, err := m.activeCommitlogs.ActiveLogs()
	if err != nil {
		// Hard failure here because the remaining cleanup logic relies on this data
		// being available.
		return err
	}

	// Delete all commitlog files prior to the one captured by the most recent snapshot.
	for _, file := range files {
		if activeCommitlogs.Contains(file.FilePath) {
			// Skip over any commitlog files that are being actively written to.
			continue
		}

		if file.Index < mostRecentSnapshot.CommitlogIdentifier.Index {
			m.metrics.deletedCommitlogFile.Inc(1)
			filesToDelete = append(filesToDelete, file.FilePath)
		}
	}

	// Delete corrupt commitlog files.
	for _, errorWithPath := range commitlogErrorsWithPaths {
		if activeCommitlogs.Contains(errorWithPath.Path()) {
			// Skip over any commitlog files that are being actively written to. Note that is
			// is common for an active commitlog to appear corrupt because the info header has
			// not been flushed yet.
			continue
		}

		m.metrics.corruptCommitlogFile.Inc(1)
		// If we were unable to read the commit log files info header, then we're forced to assume
		// that the file is corrupt and remove it. This can happen in situations where M3DB experiences
		// sudden shutdown.
		m.opts.InstrumentOptions().Logger().WithFields(
			xlog.NewField("err", errorWithPath.Error()),
			xlog.NewField("path", errorWithPath.Path()),
		).Errorf(
			"encountered corrupt commitlog file during cleanup, marking file for deletion: %s",
			errorWithPath.Error())
		filesToDelete = append(filesToDelete, errorWithPath.Path())
	}

	return nil
}
