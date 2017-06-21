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
	"github.com/m3db/m3db/retention"
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
	filePathPrefix := opts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
	commitLogsDir := fs.CommitLogsDirPath(filePathPrefix)

	return &cleanupManager{
		database:       database,
		opts:           opts,
		nowFn:          opts.ClockOptions().NowFn(),
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
	if err := m.cleanupFilesetFiles(t); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when cleaning up fileset files for %v: %v", t, err))
	}

	commitLogStart, commitLogTimes := m.commitLogTimes(t)
	if err := m.cleanupCommitLogs(commitLogStart, commitLogTimes); err != nil {
		multiErr = multiErr.Add(fmt.Errorf(
			"encountered errors when cleaning up commit logs for commitLogStart %v commitLogTimes %v: %v",
			commitLogStart, commitLogTimes, err))
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

func (m *cleanupManager) cleanupFilesetFiles(t time.Time) error {
	var (
		multiErr   = xerrors.NewMultiError()
		namespaces = m.database.getOwnedNamespaces()
	)
	for _, n := range namespaces {
		earliestToRetain := retention.FlushTimeStart(n.Options().RetentionOptions(), t)
		multiErr = multiErr.Add(n.CleanupFileset(earliestToRetain))
	}

	return multiErr.FinalError()
}

// NB(xichen): since each commit log contains data needed for bootstrapping not only
// its own block size period but also its left and right block neighbors due to past
// writes and future writes, we need to shift flush time range by block size as the
// time range for commit log files we need to check.
func (m *cleanupManager) commitLogTimeRange(t time.Time) (time.Time, time.Time) {
	var (
		ropts      = m.opts.CommitLogOptions().RetentionOptions()
		blockSize  = ropts.BlockSize()
		flushStart = retention.FlushTimeStart(ropts, t)
		flushEnd   = retention.FlushTimeEnd(ropts, t)
	)
	return flushStart.Add(-blockSize), flushEnd.Add(-blockSize)
}

// commitLogTimes returns the earliest time before which the commit logs are expired,
// as well as a list of times we need to clean up commit log files for.
// TODO(prateek): need to rework to better handle commitLogTimes
// Consider a database running with two namespaces, and the following retention opts:
//
//           | RetentionPeriod | BlockSize
// ns1       |      30d        |   6h
// commitLog |      24h        |   2h
// ns2       |      8h         |   30m
//
// blocks for each depictied over time (flowing left --> right):
// time-label: t0  t1  t2  t3
//  ns1        *           *       [each * is 6h]
//  commitlog  .   .   .   .   .   [each . is 2h]
//  ns2        ,,,,,,,,,,,,,,,,,   [each , is 30min]
//
//  we cannot remove commitlog blocks at t0, t1, and t2 until ns1 block at t0
// is written safely. The same applies to all of the ns2 blocks between period
// [t0,t3).
// TODO(prateek): databaseNamespace.NeedsFlush will need to take a [start, end] or
// [start, duration] to answer the question accurately.
func (m *cleanupManager) commitLogTimes(t time.Time) (time.Time, []time.Time) {
	var (
		ropts            = m.opts.CommitLogOptions().RetentionOptions()
		blockSize        = ropts.BlockSize()
		earliest, latest = m.commitLogTimeRange(t)
	)

	// TODO(xichen): preallocate the slice here
	var commitLogTimes []time.Time
	for commitLogTime := latest; !commitLogTime.Before(earliest); commitLogTime = commitLogTime.Add(-blockSize) {
		hasFlushedAll := true
		leftBlockStart := commitLogTime.Add(-blockSize)
		rightBlockStart := commitLogTime.Add(blockSize)
		for blockStart := leftBlockStart; !blockStart.After(rightBlockStart); blockStart = blockStart.Add(blockSize) {
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
