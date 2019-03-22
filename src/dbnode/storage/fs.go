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
	xlog "github.com/m3db/m3x/log"
)

type fileOpStatus int

const (
	fileOpNotStarted fileOpStatus = iota
	fileOpInProgress
	fileOpSuccess
	fileOpFailed
)

type fileOpState struct {
	Status      fileOpStatus
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

	log      xlog.Logger
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
			m.log.Errorf("error when cleaning up data for time %v: %v", t, err)
		}
		if err := m.Flush(t, dbBootstrapStates); err != nil {
			m.log.Errorf("error when flushing data for time %v: %v", t, err)
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
