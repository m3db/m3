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
	errMediatorAlreadyOpen               = errors.New("mediator is already open")
	errMediatorNotOpen                   = errors.New("mediator is not open")
	errMediatorAlreadyClosed             = errors.New("mediator is already closed")
	errMediatorTimeBarrierAlreadyWaiting = errors.New("mediator time barrier already has a waiter")
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
	mediatorTimeBarrier        mediatorTimeBarrier
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
//
// See comment over mediatorTimeBarrier for more details on how this is implemented.
func (m *mediator) RunFilesystemProcesses(forceType forceType, startTime time.Time) error {
	m.databaseFileSystemManager.Run(startTime, syncRun, forceType)
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
	log := m.opts.InstrumentOptions().Logger()
	for {
		select {
		case <-m.closedCh:
			return
		default:
			m.sleepFn(tickCheckInterval)
			// See comment over mediatorTimeBarrier for an explanation of this logic.
			mediatorTime, err := m.mediatorTimeBarrier.wait()
			if err != nil {
				log.Error("error within ongoingFilesystemProcesses", zap.Error(err))
			}

			if err := m.RunFilesystemProcesses(noForce, mediatorTime); err != nil {
				log.Error("error within ongoingFilesystemProcesses", zap.Error(err))
				continue
			}
		}
	}
}

func (m *mediator) ongoingTick() {
	var (
		log          = m.opts.InstrumentOptions().Logger()
		mediatorTime = m.nowFn()
	)
	for {
		select {
		case <-m.closedCh:
			return
		default:
			m.sleepFn(tickCheckInterval)
			// See comment over mediatorTimeBarrier for an explanation of this logic.
			fsProcessWaiting := m.mediatorTimeBarrier.hasWaiter()
			if fsProcessWaiting {
				mediatorTime := m.nowFn()
				m.mediatorTimeBarrier.release(mediatorTime)
			}
			if err := m.Tick(force, mediatorTime); err != nil {
				log.Error("error within tick", zap.Error(err))
			}
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

// mediatorTimeBarrier is used to prevent the tick process and the filesystem processes from ever running
// concurrently with an inconsistent view of time. Each time the filesystem processes want to run they first
// register for the next barrier by calling wait(). Once a tick completes it will detect that the filesystem
// processes are waiting for the next barrier by calling hasWaiter() at which point it will update the mediator
// time and propagate that information to the filesystem processes via a call to release(). If the filesystem
// processes are still running when the tick completes, it will just tick again but use the same startTime as
// the previous run.
//
// This cooperation ensures that multiple ticks can run during a single run of filesystem processes (although
// each tick will run with the same startTime), but that if a tick and run of filesystem processes are executing
// concurrently they will always have the same value for startTime.
//
// One implication of this scheme is that once a run of filesystem processes completes it will always have to wait
// until the currently executing tick completes before performing the next run, but in practice this should not be
// much of an issue.
//
//  ____________       ___________
// | Flush (t0) |     | Tick (t0) |
// |            |     |           |
// |            |     |___________|
// |            |      ___________
// |            |     | Tick (t0) |
// |            |     |           |
// |            |     |___________|
// |            |      ___________
// |____________|     | Tick (t0) |
//  barrier.wait()    |           |
//                    |___________|
//                    barrier.release()
// ------------------------------------
//  ____________       ___________
// | Flush (t1) |     | Tick (t1) |
// |            |     |           |
// |            |     |___________|
// |            |      ___________
// |            |     | Tick (t1) |
// |            |     |           |
// |            |     |___________|
// |            |      ___________
// |____________|     | Tick (t1) |
//  barrier.wait()    |           |
//                    |___________|
//                    barrier.release()
// ------------------------------------
type mediatorTimeBarrier struct {
	sync.Mutex
	isWaiting bool
	doneCh    chan time.Time
}

func (b *mediatorTimeBarrier) wait() (time.Time, error) {
	b.Lock()
	if b.isWaiting {
		b.Unlock()
		return time.Time{}, errMediatorTimeBarrierAlreadyWaiting
	}
	b.isWaiting = true
	b.Unlock()

	t := <-b.doneCh

	b.Lock()
	b.isWaiting = false
	b.Unlock()
	return t, nil
}

func (b *mediatorTimeBarrier) release(t time.Time) {
	b.doneCh <- t
}

func (b *mediatorTimeBarrier) hasWaiter() bool {
	b.Lock()
	isWaiting := b.isWaiting
	b.Unlock()
	return isWaiting
}
