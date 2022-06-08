// Copyright (c) 2020 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/dbnode/persist"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type coldFlushManager struct {
	databaseCleanupManager
	sync.RWMutex

	log      *zap.Logger
	database database
	pm       persist.Manager
	opts     Options
	// Retain using fileOpStatus here to be consistent w/ the
	// filesystem manager since both are filesystem processes.
	status         fileOpStatus
	isColdFlushing tally.Gauge
	enabled        bool
}

func newColdFlushManager(
	database database,
	pm persist.Manager,
	opts Options,
) databaseColdFlushManager {
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope().SubScope("fs")
	// NB(bodu): cold flush cleanup doesn't require commit logs.
	cm := newCleanupManager(database, nil, scope)

	return &coldFlushManager{
		databaseCleanupManager: cm,
		log:                    instrumentOpts.Logger(),
		database:               database,
		pm:                     pm,
		opts:                   opts,
		status:                 fileOpNotStarted,
		isColdFlushing:         scope.Gauge("cold-flush"),
		enabled:                true,
	}
}

func (m *coldFlushManager) Disable() fileOpStatus {
	m.Lock()
	status := m.status
	m.enabled = false
	m.Unlock()
	return status
}

func (m *coldFlushManager) Enable() fileOpStatus {
	m.Lock()
	status := m.status
	m.enabled = true
	m.Unlock()
	return status
}

func (m *coldFlushManager) Status() fileOpStatus {
	m.RLock()
	status := m.status
	m.RUnlock()
	return status
}

func (m *coldFlushManager) Run(t xtime.UnixNano) bool {
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

	if log := m.log.Check(zapcore.DebugLevel, "cold flush run start"); log != nil {
		log.Write(zap.Time("time", t.ToTime()))
	}

	// NB(xichen): perform data cleanup and flushing sequentially to minimize the impact of disk seeks.
	// NB(r): Use invariant here since flush errors were introduced
	// and not caught in CI or integration tests.
	// When an invariant occurs in CI tests it panics so as to fail
	// the build.
	if err := m.ColdFlushCleanup(t); err != nil {
		instrument.EmitAndLogInvariantViolation(m.opts.InstrumentOptions(),
			func(l *zap.Logger) {
				l.Error("error when cleaning up cold flush data",
					zap.Time("time", t.ToTime()), zap.Error(err))
			})
	}
	if err := m.trackedColdFlush(); err != nil {
		instrument.EmitAndLogInvariantViolation(m.opts.InstrumentOptions(),
			func(l *zap.Logger) {
				l.Error("error when cold flushing data",
					zap.Time("time", t.ToTime()), zap.Error(err))
			})
	}

	if log := m.log.Check(zapcore.DebugLevel, "cold flush run complete"); log != nil {
		log.Write(zap.Time("time", t.ToTime()))
	}

	return true
}

func (m *coldFlushManager) trackedColdFlush() error {
	// The cold flush process will persist any data that has been "loaded" into memory via
	// the Load() API but has not yet been persisted durably. As a result, if the cold flush
	// process completes without error, then we want to "decrement" the number of tracked bytes
	// by however many were outstanding right before the cold flush began.
	//
	// For example:
	// t0: Load 100 bytes --> (numLoadedBytes == 100, numPendingLoadedBytes == 0)
	// t1: memTracker.MarkLoadedAsPending() --> (numLoadedBytes == 100, numPendingLoadedBytes == 100)
	// t2: Load 200 bytes --> (numLoadedBytes == 300, numPendingLoadedBytes == 100)
	// t3: ColdFlushStart()
	// t4: Load 300 bytes --> (numLoadedBytes == 600, numPendingLoadedBytes == 100)
	// t5: ColdFlushEnd()
	// t6: memTracker.DecPendingLoadedBytes() --> (numLoadedBytes == 500, numPendingLoadedBytes == 0)
	// t7: memTracker.MarkLoadedAsPending() --> (numLoadedBytes == 500, numPendingLoadedBytes == 500)
	// t8: ColdFlushStart()
	// t9: ColdFlushError()
	// t10: memTracker.MarkLoadedAsPending() --> (numLoadedBytes == 500, numPendingLoadedBytes == 500)
	// t11: ColdFlushStart()
	// t12: ColdFlushEnd()
	// t13: memTracker.DecPendingLoadedBytes() --> (numLoadedBytes == 0, numPendingLoadedBytes == 0)
	memTracker := m.opts.MemoryTracker()
	memTracker.MarkLoadedAsPending()

	if err := m.coldFlush(); err != nil {
		return err
	}

	// Only decrement if the cold flush was a success. In this case, the decrement will reduce the
	// value by however many bytes had been tracked when the cold flush began.
	memTracker.DecPendingLoadedBytes()
	return nil
}

func (m *coldFlushManager) coldFlush() error {
	namespaces, err := m.database.OwnedNamespaces()
	if err != nil {
		return err
	}

	flushPersist, err := m.pm.StartFlushPersist()
	if err != nil {
		return err
	}

	multiErr := xerrors.NewMultiError()
	for _, ns := range namespaces {
		if err = ns.ColdFlush(flushPersist); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	multiErr = multiErr.Add(flushPersist.DoneFlush())
	err = multiErr.FinalError()
	return err
}

func (m *coldFlushManager) Report() {
	m.databaseCleanupManager.Report()

	m.RLock()
	status := m.status
	m.RUnlock()
	if status == fileOpInProgress {
		m.isColdFlushing.Update(1)
	} else {
		m.isColdFlushing.Update(0)
	}
}

func (m *coldFlushManager) shouldRunWithLock() bool {
	return m.enabled && m.status != fileOpInProgress && m.database.IsBootstrapped()
}

type coldFlushNsOpts struct {
	reuseResources bool
}

// NewColdFlushNsOpts returns a new ColdFlushNsOpts.
func NewColdFlushNsOpts(reuseResources bool) ColdFlushNsOpts {
	return &coldFlushNsOpts{reuseResources: reuseResources}
}

func (o *coldFlushNsOpts) ReuseResources() bool {
	return o.reuseResources
}
