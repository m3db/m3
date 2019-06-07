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
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"

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
	// ColdVersion keeps track of data persistence for ColdWrites only.
	// Each block can be cold-flushed multiple times, so this tracks which
	// version of the flush completed successfully. This is ultimately used in
	// the buffer Tick to determine which buckets are evictable.
	ColdVersion int
	NumFailures int
}

type runType int

const (
	syncRun runType = iota
	asyncRun
)

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
		log:      instrumentOpts.Logger(),
		database: database,
		opts:     opts,
		status:   fileOpNotStarted,
		enabled:  true,
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

func (m *fileSystemManager) Run(
	t time.Time,
	dbBootstrapStates DatabaseBootstrapState,
	runType runType,
	forceType forceType,
) bool {
	m.Lock()
	if forceType == noForce && !m.shouldRunWithLock() {
		m.Unlock()
		return false
	}
	m.status = fileOpInProgress
	m.Unlock()

	// NB(xichen): perform data cleanup and flushing sequentially to minimize the impact of disk seeks.
	flushFn := func() {
		if err := m.Cleanup(t); err != nil {
			m.log.Error("error when cleaning up data", zap.Time("time", t), zap.Error(err))
		}
		if err := m.Flush(t, dbBootstrapStates); err != nil {
			m.log.Error("error when flushing data", zap.Time("time", t), zap.Error(err))
		}
		m.Lock()
		m.status = fileOpNotStarted
		m.Unlock()
	}

	if runType == syncRun {
		flushFn()
	} else {
		go flushFn()
	}
	return true
}

func (m *fileSystemManager) Report() {
	m.databaseCleanupManager.Report()
	m.databaseFlushManager.Report()
}

func (m *fileSystemManager) shouldRunWithLock() bool {
	return m.enabled && m.status != fileOpInProgress && m.database.IsBootstrapped()
}
