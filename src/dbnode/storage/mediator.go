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
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type mediatorState int

const (
	fileOpCheckInterval = time.Second
	tickCheckInterval   = 5 * time.Second

	mediatorNotOpen mediatorState = iota
	mediatorOpen
	mediatorClosed
)

var (
	errMediatorAlreadyOpen   = errors.New("mediator is already open")
	errMediatorNotOpen       = errors.New("mediator is not open")
	errMediatorAlreadyClosed = errors.New("mediator is already closed")
)

type mediatorMetrics struct {
	bootstrapStatus tally.Gauge
	cleanupStatus   tally.Gauge
	flushStatus     tally.Gauge
	repairStatus    tally.Gauge
}

func newMediatorMetrics(scope tally.Scope) mediatorMetrics {
	return mediatorMetrics{
		bootstrapStatus: scope.Gauge("bootstrapped"),
		cleanupStatus:   scope.Gauge("cleanup"),
		flushStatus:     scope.Gauge("flush"),
		repairStatus:    scope.Gauge("repair"),
	}
}

type mediator struct {
	sync.RWMutex
	database database
	databaseBootstrapManager
	databaseFileSystemManager
	databaseTickManager
	databaseRepairer

	opts                       Options
	nowFn                      clock.NowFn
	sleepFn                    sleepFn
	metrics                    mediatorMetrics
	state                      mediatorState
	mediatorTime               time.Time
	filesystemProcessesRunning bool
	filesystemProcessesBarrier chan struct{}
	closedCh                   chan struct{}
}

func newMediator(database database, commitlog commitlog.CommitLog, opts Options) (databaseMediator, error) {
	scope := opts.InstrumentOptions().MetricsScope()
	d := &mediator{
		database:                   database,
		opts:                       opts,
		nowFn:                      opts.ClockOptions().NowFn(),
		sleepFn:                    time.Sleep,
		metrics:                    newMediatorMetrics(scope),
		state:                      mediatorNotOpen,
		filesystemProcessesBarrier: make(chan struct{}),
		closedCh:                   make(chan struct{}),
	}

	fsm := newFileSystemManager(database, commitlog, opts)
	d.databaseFileSystemManager = fsm

	d.databaseRepairer = newNoopDatabaseRepairer()
	if opts.RepairEnabled() {
		var err error
		d.databaseRepairer, err = newDatabaseRepairer(database, opts)
		if err != nil {
			return nil, err
		}
	}

	d.databaseTickManager = newTickManager(database, opts)
	d.databaseBootstrapManager = newBootstrapManager(database, d, opts)
	return d, nil
}

func (m *mediator) Open() error {
	m.Lock()
	defer m.Unlock()
	if m.state != mediatorNotOpen {
		return errMediatorAlreadyOpen
	}
	m.state = mediatorOpen
	go m.reportLoop()
	go m.ongoingFilesystemProcesses()
	go m.ongoingTick()
	m.databaseRepairer.Start()
	return nil
}

func (m *mediator) DisableFileOps() {
	status := m.databaseFileSystemManager.Disable()
	for status == fileOpInProgress {
		m.sleepFn(fileOpCheckInterval)
		status = m.databaseFileSystemManager.Status()
	}
}

func (m *mediator) EnableFileOps() {
	m.databaseFileSystemManager.Enable()
}

// The mediator mediates the relationship between ticks and flushes(warm and cold)/snapshots/cleanups.
//
// For example, the requirements to perform a flush are:
// 		1) currentTime > blockStart.Add(blockSize).Add(bufferPast)
// 		2) node is not bootstrapping (technically shard is not bootstrapping)
//
// For example, there is logic in the Tick flow for removing shard flush states from a map so that it doesn't
// grow infinitely for nodes that are not restarted. If the Tick path measured the current time when it made that
// decision instead of using the same measurement that is shared with the flush logic, it might end up removing
// a shard flush state (due to it expiring), but since the flush logic is using a slightly more stale timestamp it
// will think that the old block hasn't been flushed (even thought it has) and try to flush it even though the data
// is potentially still on disk (if it hasn't been cleaned up yet).
func (m *mediator) RunFilesystemProcesses(forceType forceType, lastCompletedTickStartTime time.Time) error {
	dbBootstrapStateAtTickStart := m.database.BootstrapState()
	m.databaseFileSystemManager.Run(lastCompletedTickStartTime, dbBootstrapStateAtTickStart, syncRun, forceType)
	return nil
}

func (m *mediator) Report() {
	m.databaseBootstrapManager.Report()
	m.databaseRepairer.Report()
	m.databaseFileSystemManager.Report()
}

func (m *mediator) Close() error {
	m.Lock()
	defer m.Unlock()
	if m.state == mediatorNotOpen {
		return errMediatorNotOpen
	}
	if m.state == mediatorClosed {
		return errMediatorAlreadyClosed
	}
	m.state = mediatorClosed
	close(m.closedCh)
	m.databaseRepairer.Stop()
	return nil
}

func (m *mediator) ongoingFilesystemProcesses() {
	var prevMediatorTime time.Time
	for {
		select {
		case <-m.closedCh:
			return
		default:
			m.sleepFn(tickCheckInterval)

			// Wait for next time.
			// NB(xichen): if we attempt to tick while another tick
			// is in progress, throttle a little to avoid constantly
			// checking whether the ongoing tick is finished
			// lastCompletedTickStartTime, anyTicksHaveCompleted := m.databaseTickManager.LastCompletedTickStartTime()
			// if !anyTicksHaveCompleted {
			// 	// Can't proceed until at least one tick has completed successfully.
			// 	continue
			// }
			mediatorTime := m.getMediatorTime()
			if mediatorTime.Equal(prevMediatorTime) {
				// Wait for a full tick to elapse between running filesystem processes.
				continue
			}

			m.setFSProcessesRunning(true)
			err := m.RunFilesystemProcesses(noForce, lastCompletedTickStartTime)
			m.setFSProcessesRunning(false)
			if err != nil {
				log := m.opts.InstrumentOptions().Logger()
				log.Error("error within ongoingFilesystemProcesses", zap.Error(err))
				continue
			}

			// Only update if no errors to allow retries.
			prevLastCompletedTickStartTime = lastCompletedTickStartTime
		}
	}
}

func (m *mediator) ongoingTick() {
	for {
		select {
		case <-m.closedCh:
			return
		default:
			// If flush in progress, tick
			// else, set new time.
			m.sleepFn(tickCheckInterval)
			fsProcessRunning := m.getFSProcessesRunning()
			if !fsProcessRunning {
				m.setMediatorTime(m.nowFn())
			}
			// TODO: pass time
			if err := m.Tick(force); err != nil {
				log := m.opts.InstrumentOptions().Logger()
				log.Error("error within tick", zap.Error(err))
			}

			// fsProcessesBarrier
			// fsProcessRunning := m.getFSProcessesRunning()
			// if !fsProcessRunning {
			// 	m.setMediatorTime(m.nowFn())
			// }
			m.file
		}
	}
}

func (m *mediator) reportLoop() {
	interval := m.opts.InstrumentOptions().ReportInterval()
	t := time.NewTicker(interval)

	for {
		select {
		case <-t.C:
			m.Report()
		case <-m.closedCh:
			t.Stop()
			return
		}
	}
}

func (m *mediator) setMediatorTime(t time.Time) {
	m.Lock()
	m.mediatorTime = t
	m.Unlock()
}

func (m *mediator) getMediatorTime() time.Time {
	m.RLock()
	t := m.mediatorTime
	m.RUnlock()
	return t
}

func (m *mediator) setFSProcessesRunning(running bool) {
	m.Lock()
	m.filesystemProcessesRunning = running
	m.Unlock()
}

func (m *mediator) getFSProcessesRunning() bool {
	m.RLock()
	running := m.filesystemProcessesRunning
	m.RUnlock()
	return running
}

type timeBarrier struct {
	sync.Mutex
	isWaiting bool
	doneCh    chan time.Time
}

func (b *timeBarrier) wait() time.Time {
	b.Lock()
	b.isWaiting = true
	b.Unlock()

	t := <-b.doneCh

	b.Lock()
	b.isWaiting = false
	b.Unlock()
	return t
}

func (b *timeBarrier) release(t time.Time) {
	b.doneCh <- t
}

func (b *timeBarrier) hasWaiter() bool {
	b.Lock()
	isWaiting := b.isWaiting
	b.Unlock()
	return isWaiting
}
