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
	"sort"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	commitLogFilesFn        func(commitlog.Options) (persist.CommitLogFiles, []commitlog.ErrorWithPath, error)
	snapshotMetadataFilesFn func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error)
)

type snapshotFilesFn func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error)

type deleteFilesFn func(files []string) error

type deleteInactiveDirectoriesFn func(parentDirPath string, activeDirNames []string) error

// Narrow interface so as not to expose all the functionality of the commitlog
// to the cleanup manager.
type activeCommitlogs interface {
	ActiveLogs() (persist.CommitLogFiles, error)
}

type cleanupManager struct {
	sync.RWMutex

	database         database
	activeCommitlogs activeCommitlogs

	opts                    Options
	nowFn                   clock.NowFn
	filePathPrefix          string
	commitLogsDir           string
	commitLogFilesFn        commitLogFilesFn
	snapshotMetadataFilesFn snapshotMetadataFilesFn
	snapshotFilesFn         snapshotFilesFn

	deleteFilesFn               deleteFilesFn
	deleteInactiveDirectoriesFn deleteInactiveDirectoriesFn
	warmFlushCleanupInProgress  bool
	coldFlushCleanupInProgress  bool
	metrics                     cleanupManagerMetrics
	logger                      *zap.Logger
}

type cleanupManagerMetrics struct {
	warmFlushCleanupStatus      tally.Gauge
	coldFlushCleanupStatus      tally.Gauge
	corruptCommitlogFile        tally.Counter
	corruptSnapshotFile         tally.Counter
	corruptSnapshotMetadataFile tally.Counter
	deletedCommitlogFile        tally.Counter
	deletedSnapshotFile         tally.Counter
	deletedSnapshotMetadataFile tally.Counter
}

func newCleanupManagerMetrics(scope tally.Scope) cleanupManagerMetrics {
	clScope := scope.SubScope("commitlog")
	sScope := scope.SubScope("snapshot")
	smScope := scope.SubScope("snapshot-metadata")
	return cleanupManagerMetrics{
		warmFlushCleanupStatus:      scope.Gauge("warm-flush-cleanup"),
		coldFlushCleanupStatus:      scope.Gauge("cold-flush-cleanup"),
		corruptCommitlogFile:        clScope.Counter("corrupt"),
		corruptSnapshotFile:         sScope.Counter("corrupt"),
		corruptSnapshotMetadataFile: smScope.Counter("corrupt"),
		deletedCommitlogFile:        clScope.Counter("deleted"),
		deletedSnapshotFile:         sScope.Counter("deleted"),
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

		opts:                        opts,
		nowFn:                       opts.ClockOptions().NowFn(),
		filePathPrefix:              filePathPrefix,
		commitLogsDir:               commitLogsDir,
		commitLogFilesFn:            commitlog.Files,
		snapshotMetadataFilesFn:     fs.SortedSnapshotMetadataFiles,
		snapshotFilesFn:             fs.SnapshotFiles,
		deleteFilesFn:               fs.DeleteFiles,
		deleteInactiveDirectoriesFn: fs.DeleteInactiveDirectories,
		metrics:                     newCleanupManagerMetrics(scope),
		logger:                      opts.InstrumentOptions().Logger(),
	}
}

func (m *cleanupManager) WarmFlushCleanup(t time.Time) error {
	m.Lock()
	m.warmFlushCleanupInProgress = true
	m.Unlock()

	defer func() {
		m.Lock()
		m.warmFlushCleanupInProgress = false
		m.Unlock()
	}()

	namespaces, err := m.database.OwnedNamespaces()
	if err != nil {
		return err
	}

	multiErr := xerrors.NewMultiError()
	if err := m.cleanupExpiredIndexFiles(t, namespaces); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when cleaning up index files for %v: %w", t, err))
	}

	if err := m.cleanupCorruptedIndexFiles(namespaces); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when cleaning up corrupted files for %v: %w", t, err))
	}

	if err := m.cleanupDuplicateIndexFiles(namespaces); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when cleaning up index files for %v: %w", t, err))
	}

	if err := m.deleteInactiveDataSnapshotFiles(namespaces); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when deleting inactive snapshot files for %v: %w", t, err))
	}

	if err := m.deleteInactiveNamespaceFiles(namespaces); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when deleting inactive namespace files for %v: %w", t, err))
	}

	if err := m.cleanupSnapshotsAndCommitlogs(namespaces); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when cleaning up snapshot and commitlog files: %w", err))
	}

	return multiErr.FinalError()
}

func (m *cleanupManager) ColdFlushCleanup(t time.Time) error {
	m.Lock()
	m.coldFlushCleanupInProgress = true
	m.Unlock()

	defer func() {
		m.Lock()
		m.coldFlushCleanupInProgress = false
		m.Unlock()
	}()

	namespaces, err := m.database.OwnedNamespaces()
	if err != nil {
		return err
	}

	multiErr := xerrors.NewMultiError()
	if err := m.cleanupDataFiles(t, namespaces); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when cleaning up data files for %v: %v", t, err))
	}

	if err := m.deleteInactiveDataFiles(namespaces); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when deleting inactive data files for %v: %v", t, err))
	}

	return multiErr.FinalError()
}

func (m *cleanupManager) Report() {
	m.RLock()
	coldFlushCleanupInProgress := m.coldFlushCleanupInProgress
	warmFlushCleanupInProgress := m.warmFlushCleanupInProgress
	m.RUnlock()

	if coldFlushCleanupInProgress {
		m.metrics.coldFlushCleanupStatus.Update(1)
	} else {
		m.metrics.coldFlushCleanupStatus.Update(0)
	}

	if warmFlushCleanupInProgress {
		m.metrics.warmFlushCleanupStatus.Update(1)
	} else {
		m.metrics.warmFlushCleanupStatus.Update(0)
	}
}

func (m *cleanupManager) deleteInactiveNamespaceFiles(namespaces []databaseNamespace) error {
	var namespaceDirNames []string
	filePathPrefix := m.database.Options().CommitLogOptions().FilesystemOptions().FilePathPrefix()
	dataDirPath := fs.DataDirPath(filePathPrefix)

	for _, n := range namespaces {
		namespaceDirNames = append(namespaceDirNames, n.ID().String())
	}

	return m.deleteInactiveDirectoriesFn(dataDirPath, namespaceDirNames)
}

// deleteInactiveDataFiles will delete data files for shards that the node no longer owns
// which can occur in the case of topology changes
func (m *cleanupManager) deleteInactiveDataFiles(namespaces []databaseNamespace) error {
	return m.deleteInactiveDataFileSetFiles(fs.NamespaceDataDirPath, namespaces)
}

// deleteInactiveDataSnapshotFiles will delete snapshot files for shards that the node no longer owns
// which can occur in the case of topology changes
func (m *cleanupManager) deleteInactiveDataSnapshotFiles(namespaces []databaseNamespace) error {
	return m.deleteInactiveDataFileSetFiles(fs.NamespaceSnapshotsDirPath, namespaces)
}

func (m *cleanupManager) deleteInactiveDataFileSetFiles(filesetFilesDirPathFn func(string, ident.ID) string, namespaces []databaseNamespace) error {
	multiErr := xerrors.NewMultiError()
	filePathPrefix := m.database.Options().CommitLogOptions().FilesystemOptions().FilePathPrefix()
	for _, n := range namespaces {
		var activeShards []string
		namespaceDirPath := filesetFilesDirPathFn(filePathPrefix, n.ID())
		// NB(linasn) This should list ALL shards because it will delete dirs for the shards NOT LISTED below.
		for _, s := range n.OwnedShards() {
			shard := fmt.Sprintf("%d", s.ID())
			activeShards = append(activeShards, shard)
		}
		multiErr = multiErr.Add(m.deleteInactiveDirectoriesFn(namespaceDirPath, activeShards))
	}

	return multiErr.FinalError()
}

func (m *cleanupManager) cleanupDataFiles(t time.Time, namespaces []databaseNamespace) error {
	multiErr := xerrors.NewMultiError()
	for _, n := range namespaces {
		if !n.Options().CleanupEnabled() {
			continue
		}
		earliestToRetain := retention.FlushTimeStart(n.Options().RetentionOptions(), t)
		shards := n.OwnedShards()
		multiErr = multiErr.Add(m.cleanupExpiredNamespaceDataFiles(earliestToRetain, shards))
		multiErr = multiErr.Add(m.cleanupCompactedNamespaceDataFiles(shards))
	}
	return multiErr.FinalError()
}

func (m *cleanupManager) cleanupExpiredIndexFiles(t time.Time, namespaces []databaseNamespace) error {
	multiErr := xerrors.NewMultiError()
	for _, n := range namespaces {
		if !n.Options().CleanupEnabled() || !n.Options().IndexOptions().Enabled() {
			continue
		}
		idx, err := n.Index()
		if err != nil {
			multiErr = multiErr.Add(err)
			continue
		}
		multiErr = multiErr.Add(idx.CleanupExpiredFileSets(t))
	}
	return multiErr.FinalError()
}

func (m *cleanupManager) cleanupCorruptedIndexFiles(namespaces []databaseNamespace) error {
	multiErr := xerrors.NewMultiError()
	for _, n := range namespaces {
		if !n.Options().CleanupEnabled() || !n.Options().IndexOptions().Enabled() {
			continue
		}
		idx, err := n.Index()
		if err != nil {
			multiErr = multiErr.Add(err)
			continue
		}
		multiErr = multiErr.Add(idx.CleanupCorruptedFileSets())
	}
	return multiErr.FinalError()
}

func (m *cleanupManager) cleanupDuplicateIndexFiles(namespaces []databaseNamespace) error {
	multiErr := xerrors.NewMultiError()
	for _, n := range namespaces {
		if !n.Options().CleanupEnabled() || !n.Options().IndexOptions().Enabled() {
			continue
		}
		idx, err := n.Index()
		if err != nil {
			multiErr = multiErr.Add(err)
			continue
		}
		activeShards := make([]uint32, 0)
		for _, s := range n.OwnedShards() {
			activeShards = append(activeShards, s.ID())
		}
		multiErr = multiErr.Add(idx.CleanupDuplicateFileSets(activeShards))
	}
	return multiErr.FinalError()
}

func (m *cleanupManager) cleanupExpiredNamespaceDataFiles(earliestToRetain time.Time, shards []databaseShard) error {
	multiErr := xerrors.NewMultiError()
	for _, shard := range shards {
		if !shard.IsBootstrapped() {
			continue
		}
		if err := shard.CleanupExpiredFileSets(earliestToRetain); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (m *cleanupManager) cleanupCompactedNamespaceDataFiles(shards []databaseShard) error {
	multiErr := xerrors.NewMultiError()
	for _, shard := range shards {
		if !shard.IsBootstrapped() {
			continue
		}
		if err := shard.CleanupCompactedFileSets(); err != nil {
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
//
// This process is also modeled formally in TLA+ in the file `SnapshotsSpec.tla`.
func (m *cleanupManager) cleanupSnapshotsAndCommitlogs(namespaces []databaseNamespace) (finalErr error) {
	logger := m.opts.InstrumentOptions().Logger().With(
		zap.String("comment",
			"partial/corrupt files are expected as result of a restart (this is ok)"),
	)

	fsOpts := m.opts.CommitLogOptions().FilesystemOptions()
	snapshotMetadatas, snapshotMetadataErrorsWithPaths, err := m.snapshotMetadataFilesFn(fsOpts)
	if err != nil {
		return err
	}

	if len(snapshotMetadatas) == 0 {
		// No cleanup can be performed until we have at least one complete snapshot.
		return nil
	}

	// They should technically already be sorted, but better to be safe.
	sort.Slice(snapshotMetadatas, func(i, j int) bool {
		return snapshotMetadatas[i].ID.Index < snapshotMetadatas[j].ID.Index
	})
	sortedSnapshotMetadatas := snapshotMetadatas

	// Sanity check.
	lastMetadataIndex := int64(-1)
	for _, snapshotMetadata := range sortedSnapshotMetadatas {
		currIndex := snapshotMetadata.ID.Index
		if currIndex == lastMetadataIndex {
			// Should never happen.
			return fmt.Errorf(
				"found two snapshot metadata files with duplicate index: %d", currIndex)
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
		for _, s := range ns.OwnedShards() {
			if !s.IsBootstrapped() {
				continue
			}
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
					m.metrics.corruptSnapshotFile.Inc(1)
					logger.With(
						zap.Error(err),
						zap.Strings("files", snapshot.AbsoluteFilePaths),
					).Warn("corrupt snapshot file during cleanup, marking files for deletion")
					filesToDelete = append(filesToDelete, snapshot.AbsoluteFilePaths...)
					continue
				}

				if !uuid.Equal(snapshotID, mostRecentSnapshot.ID.UUID) {
					// If the UUID of the snapshot files doesn't match the most recent snapshot
					// then its safe to delete because it means we have a more recently complete set.
					m.metrics.deletedSnapshotFile.Inc(1)
					filesToDelete = append(filesToDelete, snapshot.AbsoluteFilePaths...)
				}
			}
		}
	}

	// Delete all snapshot metadatas prior to the most recent one.
	for _, snapshot := range sortedSnapshotMetadatas[:len(sortedSnapshotMetadatas)-1] {
		m.metrics.deletedSnapshotMetadataFile.Inc(1)
		filesToDelete = append(filesToDelete, snapshot.AbsoluteFilePaths()...)
	}

	// Delete corrupt snapshot metadata files.
	for _, errorWithPath := range snapshotMetadataErrorsWithPaths {
		m.metrics.corruptSnapshotMetadataFile.Inc(1)
		logger.With(
			zap.Error(errorWithPath.Error),
			zap.String("metadataFilePath", errorWithPath.MetadataFilePath),
			zap.String("checkpointFilePath", errorWithPath.CheckpointFilePath),
		).Warn("corrupt snapshot metadata file during cleanup, marking files for deletion")
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
		logger.With(
			zap.Error(errorWithPath),
			zap.String("path", errorWithPath.Path()),
		).Warn("corrupt commitlog file during cleanup, marking file for deletion")
		filesToDelete = append(filesToDelete, errorWithPath.Path())
	}

	return finalErr
}
