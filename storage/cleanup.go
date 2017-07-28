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
	commitLogFilesBeforeFn  commitLogFilesBeforeFn
	commitLogFilesForTimeFn commitLogFilesForTimeFn
	deleteFilesFn           deleteFilesFn
	cleanupInProgress       bool
	status                  tally.Gauge
}

func newCleanupManager(database database, scope tally.Scope) databaseCleanupManager {
	opts := database.Options()
	filePathPrefix := opts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
	commitLogsDir := fs.CommitLogsDirPath(filePathPrefix)

	return &cleanupManager{
		database:                database,
		opts:                    opts,
		nowFn:                   opts.ClockOptions().NowFn(),
		filePathPrefix:          filePathPrefix,
		commitLogsDir:           commitLogsDir,
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
		namespaces = m.database.GetOwnedNamespaces()
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
func (m *cleanupManager) commitLogTimes(t time.Time) (time.Time, []time.Time) {
	var (
		ropts            = m.opts.CommitLogOptions().RetentionOptions()
		blockSize        = ropts.BlockSize()
		earliest, latest = m.commitLogTimeRange(t)
	)
	// NB(prateek): this logic of polling the namespaces across the commit log's entire
	// retention history could get expensive if commit logs are retained for long periods.
	// e.g. if we retain them for 40 days, with a block 2 hours; then every time
	// we try to flush we are going to be polling each namespace, for each shard, for 480
	// distinct blockstarts. Say we have 2 namespaces, each with 8192 shards, that's ~10M map lookups.
	// If we cared about 100% correctness, we would optimize this by retaining a smarter data
	// structure (e.g. interval tree), but for our use-case, it's safe to assume that commit logs
	// are only retained for a period of 1-2 days (at most), after we which we'd live we with the
	// data loss.

	candidateTimes := timesInRange(earliest, latest, blockSize)
	namespaces := m.database.GetOwnedNamespaces()
	cleanupTimes := filterTimes(candidateTimes, func(t time.Time) bool {
		for _, ns := range namespaces {
			ropts := ns.Options().RetentionOptions()
			start, end := commitLogNamespaceBlockTimes(t, blockSize, ropts)
			if ns.NeedsFlush(start, end) {
				return false
			}
		}
		return true
	})

	return earliest, cleanupTimes
}

// commitLogNamespaceBlockTimes returns the range of namespace block starts which for which the
// given commit log block may contain data for.
//
// consider the situation where we have a single namespace, and a commit log with the following
// retention options:
//             buffer past | buffer future | block size
// namespace:    ns_bp     |     ns_bf     |    ns_bs
// commit log:     _       |      _        |    cl_bs
//
// for the commit log block with start time `t`, we can receive data for a range of namespace
// blocks depending on the namespace retention options. The range is given by these relationships:
//	- earliest ns block start = t.Add(-ns_bp).Truncate(ns_bs)
//  - latest ns block start   = t.Add(cl_bs).Add(ns_bf).Truncate(ns_bs)
// NB:
// - blockStart assumed to be aligned to commit log block size
func commitLogNamespaceBlockTimes(
	blockStart time.Time,
	commitlogBlockSize time.Duration,
	nsRetention retention.Options,
) (time.Time, time.Time) {
	earliest := blockStart.
		Add(-nsRetention.BufferPast()).
		Truncate(nsRetention.BlockSize())
	latest := blockStart.
		Add(commitlogBlockSize).
		Add(nsRetention.BufferFuture()).
		Truncate(nsRetention.BlockSize())
	return earliest, latest
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
