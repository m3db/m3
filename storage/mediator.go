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

	"github.com/m3db/m3db/clock"

	"github.com/uber-go/tally"
)

type mediatorState int

const (
	fileOpCheckInterval = time.Second
	numOngoingTasks     = 2

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
	databaseBootstrapManager
	databaseFileSystemManager
	databaseTickManager
	databaseRepairer

	opts     Options
	nowFn    clock.NowFn
	sleepFn  sleepFn
	metrics  mediatorMetrics
	state    mediatorState
	closedCh chan struct{}
	wg       sync.WaitGroup
}

func newMediator(database database, opts Options) (databaseMediator, error) {
	scope := opts.InstrumentOptions().MetricsScope()
	fsm, err := newFileSystemManager(database, scope)
	if err != nil {
		return nil, err
	}

	bsm := newBootstrapManager(database)
	tm := newTickManager(database)

	var repairer databaseRepairer
	if opts.RepairEnabled() {
		var err error
		repairer, err = newDatabaseRepairer(database)
		if err != nil {
			return nil, err
		}
	} else {
		repairer = newNoopDatabaseRepairer()
	}

	return &mediator{
		databaseBootstrapManager:  bsm,
		databaseFileSystemManager: fsm,
		databaseTickManager:       tm,
		databaseRepairer:          repairer,
		opts:                      opts,
		nowFn:                     opts.ClockOptions().NowFn(),
		sleepFn:                   time.Sleep,
		metrics:                   newMediatorMetrics(scope),
		state:                     mediatorNotOpen,
		closedCh:                  make(chan struct{}),
	}, nil
}

func (m *mediator) Open() error {
	m.Lock()
	defer m.Unlock()
	if m.state != mediatorNotOpen {
		return errMediatorAlreadyOpen
	}
	m.state = mediatorOpen
	m.wg.Add(numOngoingTasks)
	go m.reportLoop()
	go m.ongoingTick()
	m.databaseRepairer.Start()
	return nil
}

func (m *mediator) IsBootstrapped() bool {
	return m.databaseBootstrapManager.IsBootstrapped()
}

func (m *mediator) Bootstrap() error {
	// NB(xichen): disable filesystem manager before we bootstrap to minimize
	// the impact of file operations on node performance
	fileOpInProgess := m.databaseFileSystemManager.Disable()
	defer m.databaseFileSystemManager.Enable()

	// Wait for in-flight file operations to finish before we proceed
	// with bootstrapping
	for fileOpInProgess {
		m.sleepFn(fileOpCheckInterval)
		fileOpInProgess = m.databaseFileSystemManager.IsRunning()
	}

	// Perform file operations after bootstrapping
	err := m.databaseBootstrapManager.Bootstrap()
	m.tickWithFileOp(0, false, true)
	return err
}

func (m *mediator) Repair() error {
	return m.databaseRepairer.Repair()
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
	m.wg.Wait()

	return nil
}

func (m *mediator) ongoingTick() {
	defer m.wg.Done()

	for {
		select {
		case <-m.closedCh:
			return
		default:
			m.tickWithFileOp(m.opts.RetentionOptions().BufferDrain(), true, false)
		}
	}
}

func (m *mediator) tickWithFileOp(
	softDeadline time.Duration,
	asyncFileOp bool,
	force bool,
) {
	start := m.nowFn()
	if !m.databaseTickManager.Tick(softDeadline, force) {
		return
	}
	// NB(r): Cleanup and/or flush if required to cleanup files and/or
	// flush blocks to disk. Note this has to run after the tick as
	// blocks may only have just become available during a tick beginning
	// from the tick begin marker.
	m.databaseFileSystemManager.Run(start, asyncFileOp, force)
}

func (m *mediator) reportLoop() {
	defer m.wg.Done()

	interval := m.opts.InstrumentOptions().ReportInterval()
	t := time.Tick(interval)

	for {
		select {
		case <-t:
			if m.databaseBootstrapManager.IsBootstrapped() {
				m.metrics.bootstrapStatus.Update(1)
			} else {
				m.metrics.bootstrapStatus.Update(0)
			}
			if m.databaseRepairer.IsRepairing() {
				m.metrics.repairStatus.Update(1)
			} else {
				m.metrics.repairStatus.Update(0)
			}
			if m.databaseFileSystemManager.IsCleaningUp() {
				m.metrics.cleanupStatus.Update(1)
			} else {
				m.metrics.cleanupStatus.Update(0)
			}
			if m.databaseFileSystemManager.IsFlushing() {
				m.metrics.flushStatus.Update(1)
			} else {
				m.metrics.flushStatus.Update(0)
			}
		case <-m.closedCh:
			return
		}
	}
}
