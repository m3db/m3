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

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	mediatorState int
)

const (
	fileOpCheckInterval        = time.Second
	defaultExternalChannelSize = 8

	mediatorNotOpen mediatorState = iota
	mediatorOpen
	mediatorClosed
)

var (
	errMediatorAlreadyOpen                  = errors.New("mediator is already open")
	errMediatorNotOpen                      = errors.New("mediator is not open")
	errMediatorAlreadyClosed                = errors.New("mediator is already closed")
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
	databaseColdFlushManager
	databaseTickManager

	opts                Options
	nowFn               clock.NowFn
	sleepFn             sleepFn
	metrics             mediatorMetrics
	state               mediatorState
	mediatorTimeBarrier mediatorTimeBarrier
	closedCh            chan struct{}
	tickInterval        time.Duration
	fileOpsProcesses    []FileOpsProcess
	backgroundProcesses []BackgroundProcess
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
		database:     database,
		opts:         opts,
		nowFn:        opts.ClockOptions().NowFn(),
		sleepFn:      time.Sleep,
		metrics:      newMediatorMetrics(scope),
		state:        mediatorNotOpen,
		closedCh:     make(chan struct{}),
		tickInterval: opts.MediatorTickInterval(),
	}
	fsm := newFileSystemManager(database, commitlog, opts)
	d.databaseFileSystemManager = fsm
	d.fileOpsProcesses = []FileOpsProcess{
		FileOpsProcessFn(d.ongoingFileSystemProcesses),
		FileOpsProcessFn(d.ongoingColdFlushProcesses),
	}
	d.mediatorTimeBarrier = newMediatorTimeBarrier(nowFn, iOpts, len(d.fileOpsProcesses))

	// NB(bodu): Cold flush needs its own persist manager now
	// that its running in its own thread.
	fsOpts := opts.CommitLogOptions().FilesystemOptions()
	pm, err := fs.NewPersistManager(fsOpts)
	if err != nil {
		return nil, err
	}
	cfm := newColdFlushManager(database, pm, opts)
	d.databaseColdFlushManager = cfm

	d.databaseTickManager = newTickManager(database, opts)
	d.databaseBootstrapManager = newBootstrapManager(database, d, opts)
	return d, nil
}

func (m *mediator) RegisterBackgroundProcess(process BackgroundProcess) error {
	m.Lock()
	defer m.Unlock()

	if m.state != mediatorNotOpen {
		return errMediatorAlreadyOpen
	}

	m.backgroundProcesses = append(m.backgroundProcesses, process)
	return nil
}

func (m *mediator) EnqueueMutuallyExclusiveFn(fn func()) error {
	m.RLock()
	if m.state != mediatorOpen {
		m.RUnlock()
		return errMediatorNotOpen
	}
	m.RUnlock()

	m.mediatorTimeBarrier.externalFnCh <- fn
	return nil
}

func (m *mediator) Open() error {
	m.Lock()
	defer m.Unlock()
	if m.state != mediatorNotOpen {
		return errMediatorAlreadyOpen
	}
	m.state = mediatorOpen

	go m.reportLoop()
	for _, fileOpsProcess := range m.fileOpsProcesses {
		go fileOpsProcess.Start()
	}
	go m.ongoingTick()

	for _, process := range m.backgroundProcesses {
		process.Start()
	}

	return nil
}

func (m *mediator) DisableFileOpsAndWait() {
	fsStatus := m.databaseFileSystemManager.Disable()
	// Even though the cold flush runs separately, its still
	// considered a fs process.
	cfStatus := m.databaseColdFlushManager.Disable()
	for fsStatus == fileOpInProgress {
		m.sleepFn(fileOpCheckInterval)
		fsStatus = m.databaseFileSystemManager.Status()
	}
	for cfStatus == fileOpInProgress {
		m.sleepFn(fileOpCheckInterval)
		cfStatus = m.databaseColdFlushManager.Status()
	}
}

func (m *mediator) EnableFileOps() {
	m.databaseFileSystemManager.Enable()
	// Even though the cold flush runs separately, its still
	// considered a fs process.
	m.databaseColdFlushManager.Enable()
}

func (m *mediator) Report() {
	m.databaseBootstrapManager.Report()
	m.databaseFileSystemManager.Report()
	m.databaseColdFlushManager.Report()

	for _, process := range m.backgroundProcesses {
		process.Report()
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

	for _, process := range m.backgroundProcesses {
		process.Stop()
	}

	return nil
}

// The mediator mediates the relationship between ticks and warm flushes/snapshots.
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
			m.sleepFn(m.tickInterval)

			// Check if the mediator is already closed.
			if !m.IsOpen() {
				return
			}

			m.runFileSystemProcesses()
		}
	}
}

// The mediator mediates the relationship between ticks and cold flushes/cleanup the same way it does for warm flushes/snapshots.
// We want to begin each cold/warm flush with an in sync view of time as a tick.
// NB(bodu): Cold flushes and cleanup have been separated out into it's own thread to avoid blocking snapshots.
func (m *mediator) ongoingColdFlushProcesses() {
	for {
		select {
		case <-m.closedCh:
			return
		default:
			m.sleepFn(m.tickInterval)

			// Check if the mediator is already closed.
			if !m.IsOpen() {
				return
			}

			m.runColdFlushProcesses()
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
			m.sleepFn(m.tickInterval)

			// Check if the mediator is already closed.
			if !m.IsOpen() {
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
	// See comment over mediatorTimeBarrier for an explanation of this logic.
	mediatorTime := m.mediatorTimeBarrier.fsProcessesWait()
	m.databaseFileSystemManager.Run(mediatorTime)
}

func (m *mediator) runColdFlushProcesses() {
	// See comment over mediatorTimeBarrier for an explanation of this logic.
	mediatorTime := m.mediatorTimeBarrier.fsProcessesWait()
	m.databaseColdFlushManager.Run(mediatorTime)
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

func (m *mediator) IsOpen() bool {
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
// Additionally, an independent cold flush process complicates this a bit more in that we have more than one filesystem
// process waiting on the mediator barrier. The invariant here is that both warm and cold flushes always start on a tick
// with a consistent view of time as the tick it is on. They don't necessarily need to start on the same tick. See the
// diagram below for an example case.
//
//  ____________       ___________          _________________
// | Flush (t0) |     | Tick (t0) |        | Cold Flush (t0) |
// |            |     |           |        |                 |
// |            |     |___________|        |                 |
// |            |      ___________         |                 |
// |            |     | Tick (t0) |        |                 |
// |            |     |           |        |                 |
// |            |     |___________|        |                 |
// |            |      ___________         |                 |
// |____________|     | Tick (t0) |        |                 |
//  barrier.wait()    |           |        |                 |
//                    |___________|        |                 |
//                    mediatorTime = t1    |                 |
//                    barrier.release()    |                 |
//  ____________       ___________         |                 |
// | Flush (t1) |     | Tick (t1) |        |_________________|
// |            |     |           |         barrier.wait()
// |            |     |___________|
// |            |      mediatorTime = t2
// |            |      barrier.release()
// |            |       ___________         _________________
// |            |      | Tick (t2) |       | Cold Flush (t2) |
// |____________|      |           |       |                 |
//  barrier.wait()     |___________|       |                 |
//                     mediatorTime = t3   |                 |
//                     barrier.release()   |                 |
//   ____________       ___________        |                 |
//  | Flush (t3) |     | Tick (t3) |       |                 |
//  |            |     |           |       |                 |
//  |            |     |___________|       |                 |
//  |            |      ___________        |                 |
//  |            |     | Tick (t3) |       |                 |
//  |            |     |           |       |                 |
//  |            |     |___________|       |                 |
//  |            |      ___________        |                 |
//  |____________|     | Tick (t3) |       |_________________|
//   barrier.wait()    |           |        barrier.wait()
//                     |___________|
//                     mediatorTime = t4
//                     barrier.release()
//   ____________       ___________         _________________
//  | Flush (t4) |     | Tick (t4) |       | Cold Flush (t4) |
//  |            |     |           |       |                 |
// ------------------------------------------------------------
type mediatorTimeBarrier struct {
	sync.Mutex
	// Both mediatorTime and numFsProcessesWaiting are protected
	// by the mutex.
	mediatorTime          xtime.UnixNano
	numFsProcessesWaiting int
	numMaxWaiters         int

	nowFn        func() time.Time
	iOpts        instrument.Options
	releaseCh    chan xtime.UnixNano
	externalFnCh chan func()
}

// initialMediatorTime should only be used to obtain the initial time for
// the ongoing tick loop. All subsequent updates should come from the
// release method.
func (b *mediatorTimeBarrier) initialMediatorTime() xtime.UnixNano {
	b.Lock()
	defer b.Unlock()
	return b.mediatorTime
}

func (b *mediatorTimeBarrier) fsProcessesWait() xtime.UnixNano {
	b.Lock()
	b.numFsProcessesWaiting++
	b.Unlock()

	t := <-b.releaseCh

	b.Lock()
	b.numFsProcessesWaiting--
	b.Unlock()
	return t
}

func (b *mediatorTimeBarrier) maybeRelease() (xtime.UnixNano, error) {
	b.Lock()
	numWaiters := b.numFsProcessesWaiting
	mediatorTime := b.mediatorTime
	b.Unlock()

	if numWaiters == 0 {
		// If there isn't a waiter yet then the filesystem processes may still
		// be ongoing in which case we don't want to release the barrier / update
		// the current time yet. Allow the tick to run again with the same time
		// as before.
		return mediatorTime, nil
	}

	// If the filesystem processes are waiting then update the time and allow
	// both the filesystem processes and the tick to proceed with the new time.
	newMediatorTime := xtime.ToUnixNano(b.nowFn())
	if newMediatorTime.Before(b.mediatorTime) {
		instrument.EmitAndLogInvariantViolation(b.iOpts, func(l *zap.Logger) {
			l.Error(
				"mediator time attempted to move backwards in time",
				zap.Time("prevTime", b.mediatorTime.ToTime()),
				zap.Time("newTime", newMediatorTime.ToTime()))
		})
		return 0, errMediatorTimeTriedToProgressBackwards
	}

	b.mediatorTime = newMediatorTime

	// If all waiters are waiting, we can safely call mutually exclusive external functions.
	if numWaiters == b.numMaxWaiters {
		// Drain the channel.
	Loop:
		for {
			select {
			case fn := <-b.externalFnCh:
				fn()
			default:
				break Loop
			}
		}
	}

	for i := 0; i < numWaiters; i++ {
		b.releaseCh <- b.mediatorTime
	}
	return b.mediatorTime, nil
}

func newMediatorTimeBarrier(nowFn func() time.Time, iOpts instrument.Options, maxWaiters int) mediatorTimeBarrier {
	return mediatorTimeBarrier{
		mediatorTime:  xtime.ToUnixNano(nowFn()),
		nowFn:         nowFn,
		iOpts:         iOpts,
		numMaxWaiters: maxWaiters,
		releaseCh:     make(chan xtime.UnixNano),
		externalFnCh:  make(chan func(), defaultExternalChannelSize),
	}
}
