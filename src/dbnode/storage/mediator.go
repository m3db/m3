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
	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	mediatorState            int
	fileSystemProcessesState int
)

const (
	fileOpCheckInterval              = time.Second
	tickCheckInterval                = 5 * time.Second
	fileSystemProcessesCheckInterval = 100 * time.Millisecond

	mediatorNotOpen mediatorState = iota
	mediatorOpen
	mediatorClosed

	fileSystemProcessesIdle fileSystemProcessesState = iota
	fileSystemProcessesBusy
)

var (
	errMediatorAlreadyOpen                  = errors.New("mediator is already open")
	errMediatorNotOpen                      = errors.New("mediator is not open")
	errMediatorAlreadyClosed                = errors.New("mediator is already closed")
	errMediatorTimeBarrierAlreadyWaiting    = errors.New("mediator time barrier already has a waiter")
	errMediatorTimeTriedToProgressBackwards = errors.New("mediator time tried to progress backwards")
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

	opts                     Options
	nowFn                    clock.NowFn
	sleepFn                  sleepFn
	metrics                  mediatorMetrics
	state                    mediatorState
	fileSystemProcessesState fileSystemProcessesState
	mediatorTimeBarrier      mediatorTimeBarrier
	closedCh                 chan struct{}
}

// TODO(r): Consider renaming "databaseMediator" to "databaseCoordinator"
// when we have time (now is not that time).
func newMediator(database database, commitlog commitlog.CommitLog, opts Options) (databaseMediator, error) {
	var (
		iOpts = opts.InstrumentOptions()
		scope = iOpts.MetricsScope()
		nowFn = opts.ClockOptions().NowFn()
	)
	d := &mediator{
		database:                 database,
		opts:                     opts,
		nowFn:                    opts.ClockOptions().NowFn(),
		sleepFn:                  time.Sleep,
		metrics:                  newMediatorMetrics(scope),
		state:                    mediatorNotOpen,
		fileSystemProcessesState: fileSystemProcessesIdle,
		mediatorTimeBarrier:      newMediatorTimeBarrier(nowFn, iOpts),
		closedCh:                 make(chan struct{}),
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
	go m.ongoingFileSystemProcesses()
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

func (m *mediator) Report() {
	m.databaseBootstrapManager.Report()
	m.databaseRepairer.Report()
	m.databaseFileSystemManager.Report()
}

func (m *mediator) WaitForFileSystemProcesses() {
	m.RLock()
	fileSystemProcessesState := m.fileSystemProcessesState
	m.RUnlock()
	for fileSystemProcessesState == fileSystemProcessesBusy {
		m.sleepFn(fileSystemProcessesCheckInterval)
		m.RLock()
		fileSystemProcessesState = m.fileSystemProcessesState
		m.RUnlock()
	}
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

// The mediator mediates the relationship between ticks and flushes(warm and cold)/snapshots/cleanups.
//
// For example, the requirements to perform a flush are:
// 		1) currentTime > blockStart.Add(blockSize).Add(bufferPast)
// 		2) node is not bootstrapping (technically shard is not bootstrapping)
//
// Similarly, there is logic in the Tick flow for removing shard flush states from a map so that it doesn't
// grow infinitely for nodes that are not restarted. If the Tick path measured the current time when it made that
// decision instead of using the same measurement that is shared with the flush logic, it might end up removing
// a shard flush state (due to it expiring), but since the flush logic is using a slightly more stale timestamp it
// will think that the old block hasn't been flushed (even thought it has) and try to flush it even though the data
// is potentially still on disk (if it hasn't been cleaned up yet).
//
// See comment over mediatorTimeBarrier for more details on how this is implemented.
func (m *mediator) ongoingFileSystemProcesses() {
	for {
		select {
		case <-m.closedCh:
			return
		default:
			m.sleepFn(tickCheckInterval)

			// Check if the mediator is already closed.
			if !m.isOpen() {
				return
			}

			m.runFileSystemProcesses()
		}
	}
}

func (m *mediator) ongoingTick() {
	var (
		log          = m.opts.InstrumentOptions().Logger()
		mediatorTime = m.mediatorTimeBarrier.initialMediatorTime()
	)
	for {
		select {
		case <-m.closedCh:
			return
		default:
			m.sleepFn(tickCheckInterval)

			// Check if the mediator is already closed.
			if !m.isOpen() {
				return
			}

			// See comment over mediatorTimeBarrier for an explanation of this logic.
			newMediatorTime, err := m.mediatorTimeBarrier.maybeRelease()
			if err != nil {
				log.Error("ongoing tick was unable to release time barrier", zap.Error(err))
				continue
			}
			mediatorTime = newMediatorTime

			// NB(bodu): We may still hit a db closed error here since the db does not wait upon
			// completion of ticks.
			if err := m.Tick(force, mediatorTime); err != nil && err != errDatabaseIsClosed {
				log.Error("error within tick", zap.Error(err))
			}
		}
	}
}

func (m *mediator) runFileSystemProcesses() {
	m.Lock()
	m.fileSystemProcessesState = fileSystemProcessesBusy
	m.Unlock()
	defer func() {
		m.Lock()
		m.fileSystemProcessesState = fileSystemProcessesIdle
		m.Unlock()
	}()

	// See comment over mediatorTimeBarrier for an explanation of this logic.
	log := m.opts.InstrumentOptions().Logger()
	mediatorTime, err := m.mediatorTimeBarrier.fsProcessesWait()
	if err != nil {
		log.Error("error within ongoingFileSystemProcesses waiting for next mediatorTime", zap.Error(err))
		return
	}

	m.databaseFileSystemManager.Run(mediatorTime, syncRun, noForce)
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

func (m *mediator) isOpen() bool {
	m.RLock()
	defer m.RUnlock()
	return m.state == mediatorOpen
}

// mediatorTimeBarrier is used to prevent the tick process and the filesystem processes from ever running
// concurrently with an inconsistent view of time. Each time the filesystem processes want to run they first
// register for the next barrier by calling fsProcessesWait(). Once a tick completes it will call maybeRelease()
// which will detect that the filesystem processes are waiting for the next barrier at which point it will update
// the mediator time and propagate that information to the filesystem processes via the releaseCh. If the filesystem
// processes are still running when the tick completes, the call to maybeRelease() will just return the same time
// as the previous run and another tick will run with the same timestamp as the previous one.
//
// This cooperation ensures that multiple ticks can run during a single run of filesystem processes (although
// each tick will run with the same startTime), but that if a tick and run of filesystem processes are executing
// concurrently they will always have the same value for startTime.
//
// Note that this scheme (specifically the tick process calling maybeRelease() and the fs processes waiting instead
// of vice versa) is specifically designed such that the ticking process is never blocked and is constantly running.
// This means that once a run of filesystem processes completes it will always have to wait until the currently
// executing tick completes before performing the next run, but in practice this should not be much of an issue.
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
//                    mediatorTime = t1
//                    barrier.release()
// -------------------------------------
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
	mediatorTime       time.Time
	nowFn              func() time.Time
	iOpts              instrument.Options
	fsProcessesWaiting bool
	releaseCh          chan time.Time
}

// initialMediatorTime should only be used to obtain the initial time for
// the ongoing tick loop. All subsequent updates should come from the
// release method.
func (b *mediatorTimeBarrier) initialMediatorTime() time.Time {
	b.Lock()
	defer b.Unlock()
	return b.mediatorTime
}

func (b *mediatorTimeBarrier) fsProcessesWait() (time.Time, error) {
	b.Lock()
	if b.fsProcessesWaiting {
		b.Unlock()
		return time.Time{}, errMediatorTimeBarrierAlreadyWaiting
	}
	b.fsProcessesWaiting = true
	b.Unlock()

	t := <-b.releaseCh

	b.Lock()
	b.fsProcessesWaiting = false
	b.Unlock()
	return t, nil
}

func (b *mediatorTimeBarrier) maybeRelease() (time.Time, error) {
	b.Lock()
	hasWaiter := b.fsProcessesWaiting
	mediatorTime := b.mediatorTime
	b.Unlock()

	if !hasWaiter {
		// If there isn't a waiter yet then the filesystem processes may still
		// be ongoing in which case we don't want to release the barrier / update
		// the current time yet. Allow the tick to run again with the same time
		// as before.
		return mediatorTime, nil
	}

	// If the filesystem processes are waiting then update the time and allow
	// both the filesystem processes and the tick to proceed with the new time.
	newMediatorTime := b.nowFn()
	if newMediatorTime.Before(b.mediatorTime) {
		instrument.EmitAndLogInvariantViolation(b.iOpts, func(l *zap.Logger) {
			l.Error(
				"mediator time attempted to move backwards in time",
				zap.Time("prevTime", b.mediatorTime), zap.Time("newTime", newMediatorTime))
		})
		return time.Time{}, errMediatorTimeTriedToProgressBackwards
	}

	b.mediatorTime = newMediatorTime
	b.releaseCh <- b.mediatorTime
	return b.mediatorTime, nil
}

func newMediatorTimeBarrier(nowFn func() time.Time, iOpts instrument.Options) mediatorTimeBarrier {
	return mediatorTimeBarrier{
		mediatorTime: nowFn(),
		nowFn:        nowFn,
		iOpts:        iOpts,
		releaseCh:    make(chan time.Time, 0),
	}
}
