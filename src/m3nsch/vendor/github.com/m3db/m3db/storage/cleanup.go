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

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3x/errors"

	"github.com/uber-go/tally"
)

type commitLogFilesBeforeFn func(commitLogsDir string, t time.Time) ([]string, error)

type commitLogFilesForTimeFn func(commitLogsDir string, t time.Time) ([]string, error)

type deleteFilesFn func(files []string) error

type cleanupManager struct {
	sync.RWMutex

	database                database
	opts                    Options
	nowFn                   clock.NowFn
	blockSize               time.Duration
	filePathPrefix          string
	commitLogsDir           string
	fm                      databaseFlushManager
	commitLogFilesBeforeFn  commitLogFilesBeforeFn
	commitLogFilesForTimeFn commitLogFilesForTimeFn
	deleteFilesFn           deleteFilesFn
	cleanupInProgress       bool
	status                  tally.Gauge
}

func newCleanupManager(database database, fm databaseFlushManager, scope tally.Scope) databaseCleanupManager {
	opts := database.Options()
	blockSize := opts.RetentionOptions().BlockSize()
	filePathPrefix := opts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
	commitLogsDir := fs.CommitLogsDirPath(filePathPrefix)

	return &cleanupManager{
		database:       database,
		opts:           opts,
		nowFn:          opts.ClockOptions().NowFn(),
		blockSize:      blockSize,
		filePathPrefix: filePathPrefix,
		commitLogsDir:  commitLogsDir,
		fm:             fm,
		commitLogFilesBeforeFn:  fs.CommitLogFilesBefore,
		commitLogFilesForTimeFn: fs.CommitLogFilesForTime,
		deleteFilesFn:           fs.DeleteFiles,
		status:                  scope.Gauge("cleanup"),
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
	filesetFilesStart := m.fm.FlushTimeStart(t)
	if err := m.cleanupFilesetFiles(filesetFilesStart); err != nil {
		detailedErr := fmt.Errorf("encountered errors when cleaning up fileset files for %v: %v", filesetFilesStart, err)
		multiErr = multiErr.Add(detailedErr)
	}
	commitLogStart, commitLogTimes := m.commitLogTimes(t)
	if err := m.cleanupCommitLogs(commitLogStart, commitLogTimes); err != nil {
		detailedErr := fmt.Errorf("encountered errors when cleaning up commit logs for commitLogStart %v commitLogTimes %v: %v", commitLogStart, commitLogTimes, err)
		multiErr = multiErr.Add(detailedErr)
	}

	return multiErr.FinalError()
}

func (m *cleanupManager) Report() {
	m.RLock()
	cleanupInProgress := m.cleanupInProgress
	m.RUnlock()

	if cleanupInProgress {
		m.status.Update(1)
	} else {
		m.status.Update(0)
	}
}

func (m *cleanupManager) cleanupFilesetFiles(earliestToRetain time.Time) error {
	multiErr := xerrors.NewMultiError()

	namespaces := m.database.getOwnedNamespaces()
	for _, n := range namespaces {
		if err := n.CleanupFileset(earliestToRetain); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

// NB(xichen): since each commit log contains data needed for bootstrapping not only
// its own block size period but also its left and right block neighbors due to past
// writes and future writes, we need to shift flush time range by block size as the
// time range for commit log files we need to check.
func (m *cleanupManager) commitLogTimeRange(t time.Time) (time.Time, time.Time) {
	flushStart, flushEnd := m.fm.FlushTimeStart(t), m.fm.FlushTimeEnd(t)
	return flushStart.Add(-m.blockSize), flushEnd.Add(-m.blockSize)
}

// commitLogTimes returns the earliest time before which the commit logs are expired,
// as well as a list of times we need to clean up commit log files for.
func (m *cleanupManager) commitLogTimes(t time.Time) (time.Time, []time.Time) {
	earliest, latest := m.commitLogTimeRange(t)

	// TODO(xichen): preallocate the slice here
	var commitLogTimes []time.Time
	for commitLogTime := latest; !commitLogTime.Before(earliest); commitLogTime = commitLogTime.Add(-m.blockSize) {
		hasFlushedAll := true
		leftBlockStart := commitLogTime.Add(-m.blockSize)
		rightBlockStart := commitLogTime.Add(m.blockSize)
		for blockStart := leftBlockStart; !blockStart.After(rightBlockStart); blockStart = blockStart.Add(m.blockSize) {
			if m.fm.NeedsFlush(blockStart) {
				hasFlushedAll = false
				break
			}
		}
		if hasFlushedAll {
			commitLogTimes = append(commitLogTimes, commitLogTime)
		}
	}

	return earliest, commitLogTimes
}

func (m *cleanupManager) cleanupCommitLogs(earliestToRetain time.Time, cleanupTimes []time.Time) error {
	multiErr := xerrors.NewMultiError()
	toCleanup, err := m.commitLogFilesBeforeFn(m.commitLogsDir, earliestToRetain)
	if err != nil {
		multiErr = multiErr.Add(err)
	}

	for _, t := range cleanupTimes {
		files, err := m.commitLogFilesForTimeFn(m.commitLogsDir, t)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
		toCleanup = append(toCleanup, files...)
	}

	if err := m.deleteFilesFn(toCleanup); err != nil {
		multiErr = multiErr.Add(err)
	}

	return multiErr.FinalError()
}
