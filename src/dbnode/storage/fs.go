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
	"sync"

	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

type fileOpStatus int

const (
	fileOpNotStarted fileOpStatus = iota
	fileOpInProgress
	fileOpSuccess
	fileOpFailed
)

type fileOpState struct {
	// WarmStatus is the status of data persistence for WarmWrites only.
	// Each block will only be warm-flushed once, so not keeping track of a
	// version here is okay. This is used in the buffer Tick to determine when
	// a warm bucket is evictable from memory.
	WarmStatus fileOpStatus
	// ColdVersionRetrievable keeps track of data persistence for ColdWrites only.
	// Each block can be cold-flushed multiple times, so this tracks which
	// version of the flush completed successfully. This is ultimately used in
	// the buffer Tick to determine which buckets are evictable. Note the distinction
	// between ColdVersionRetrievable and ColdVersionFlushed described below.
	ColdVersionRetrievable int
	// ColdVersionFlushed is the same as ColdVersionRetrievable except in some cases it will be
	// higher than ColdVersionRetrievable. ColdVersionFlushed will be higher than
	// ColdVersionRetrievable in the situation that a cold flush has completed successfully but
	// signaling the update to the BlockLeaseManager hasn't completed yet. As a result, the
	// ColdVersionRetrievable can't be incremented yet because a concurrent tick could evict the
	// block before it becomes queryable via the block retriever / seeker manager, however, the
	// BlockLeaseVerifier needs to know that a higher cold flush version exists on disk so that
	// it can approve the SeekerManager's request to open a lease on the latest version.
	//
	// In other words ColdVersionRetrievable is used to keep track of the latest cold version that has
	// been successfully flushed and can be queried via the block retriever / seeker manager and
	// as a result is safe to evict, while ColdVersionFlushed is used to keep track of the latest
	// cold version that has been flushed and to validate lease requests from the SeekerManager when it
	// receives a signal to open a new lease.
	ColdVersionFlushed int
	NumFailures        int
}

type forceType int

const (
	noForce forceType = iota
	force
)

type fileSystemManager struct {
	databaseFlushManager
	databaseCleanupManager
	sync.RWMutex

	log      *zap.Logger
	database database
	opts     Options
	status   fileOpStatus
	enabled  bool
}

func newFileSystemManager(
	database database,
	commitLog commitlog.CommitLog,
	opts Options,
) databaseFileSystemManager {
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope().SubScope("fs")
	fm := newFlushManager(database, commitLog, scope)
	cm := newCleanupManager(database, commitLog, scope)

	return &fileSystemManager{
		databaseFlushManager:   fm,
		databaseCleanupManager: cm,
		log:                    instrumentOpts.Logger(),
		database:               database,
		opts:                   opts,
		status:                 fileOpNotStarted,
		enabled:                true,
	}
}

func (m *fileSystemManager) Disable() fileOpStatus {
	m.Lock()
	status := m.status
	m.enabled = false
	m.Unlock()
	return status
}

func (m *fileSystemManager) Enable() fileOpStatus {
	m.Lock()
	status := m.status
	m.enabled = true
	m.Unlock()
	return status
}

func (m *fileSystemManager) Status() fileOpStatus {
	m.RLock()
	status := m.status
	m.RUnlock()
	return status
}

func (m *fileSystemManager) Run(t xtime.UnixNano) bool {
	m.Lock()
	if !m.shouldRunWithLock() {
		m.Unlock()
		return false
	}
	m.status = fileOpInProgress
	m.Unlock()

	defer func() {
		m.Lock()
		m.status = fileOpNotStarted
		m.Unlock()
	}()

	m.log.Debug("starting warm flush", zap.Time("time", t.ToTime()))

	// NB(xichen): perform data cleanup and flushing sequentially to minimize the impact of disk seeks.
	if err := m.WarmFlushCleanup(t); err != nil {
		// NB(r): Use invariant here since flush errors were introduced
		// and not caught in CI or integration tests.
		// When an invariant occurs in CI tests it panics so as to fail
		// the build.
		instrument.EmitAndLogInvariantViolation(m.opts.InstrumentOptions(),
			func(l *zap.Logger) {
				l.Error("error when cleaning up data", zap.Time("time", t.ToTime()), zap.Error(err))
			})
	}
	if err := m.Flush(t); err != nil {
		instrument.EmitAndLogInvariantViolation(m.opts.InstrumentOptions(),
			func(l *zap.Logger) {
				l.Error("error when flushing data", zap.Time("time", t.ToTime()), zap.Error(err))
			})
	}
	m.log.Debug("completed warm flush", zap.Time("time", t.ToTime()))

	return true
}

func (m *fileSystemManager) Report() {
	m.databaseCleanupManager.Report()
	m.databaseFlushManager.Report()
}

func (m *fileSystemManager) shouldRunWithLock() bool {
	return m.enabled && m.status != fileOpInProgress && m.database.IsBootstrapped()
}
