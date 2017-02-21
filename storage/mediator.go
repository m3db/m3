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
}

func newMediator(database database, opts Options) (databaseMediator, error) {
	scope := opts.InstrumentOptions().MetricsScope()
	d := &mediator{
		opts:     opts,
		nowFn:    opts.ClockOptions().NowFn(),
		sleepFn:  time.Sleep,
		metrics:  newMediatorMetrics(scope),
		state:    mediatorNotOpen,
		closedCh: make(chan struct{}),
	}

	fsm, err := newFileSystemManager(database, opts)
	if err != nil {
		return nil, err
	}
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

func (m *mediator) Tick(softDeadline time.Duration, runType runType, forceType forceType) error {
	start := m.nowFn()
	if err := m.databaseTickManager.Tick(softDeadline, forceType); err != nil {
		return err
	}
	// NB(r): Cleanup and/or flush if required to cleanup files and/or
	// flush blocks to disk. Note this has to run after the tick as
	// blocks may only have just become available during a tick beginning
	// from the tick begin marker.
	m.databaseFileSystemManager.Run(start, runType, forceType)
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

func (m *mediator) ongoingTick() {
	for {
		select {
		case <-m.closedCh:
			return
		default:
			// NB(xichen): if we attempt to tick while another tick
			// is in progress, throttle a little to avoid constantly
			// checking whether the ongoing tick is finished
			err := m.Tick(m.opts.RetentionOptions().BufferDrain(), asyncRun, noForce)
			if err == errTickInProgress {
				m.sleepFn(tickCheckInterval)
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
