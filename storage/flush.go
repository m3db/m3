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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/retention"
	xerrors "github.com/m3db/m3x/errors"

	"github.com/uber-go/tally"
)

var (
	errFlushAlreadyInProgress = errors.New("flush already in progress")
)

type flushManager struct {
	sync.RWMutex

	database        database
	opts            Options
	nowFn           clock.NowFn
	pm              persist.Manager
	flushInProgress bool
	status          tally.Gauge
}

func newFlushManager(database database, scope tally.Scope) databaseFlushManager {
	opts := database.Options()
	return &flushManager{
		database: database,
		opts:     opts,
		nowFn:    opts.ClockOptions().NowFn(),
		pm:       opts.PersistManager(),
		status:   scope.Gauge("flush"),
	}
}

func (m *flushManager) Flush(curr time.Time) error {
	// ensure only a single flush is happening at a time
	m.Lock()
	if m.flushInProgress {
		m.Unlock()
		return errFlushAlreadyInProgress
	}
	m.flushInProgress = true
	m.Unlock()

	// create flush-er
	flush, err := m.pm.StartFlush()
	if err != nil {
		return err
	}

	defer func() {
		m.Lock()
		m.flushInProgress = false
		m.Unlock()
	}()

	namespaces, err := m.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}

	multiErr := xerrors.NewMultiError()
	for _, ns := range namespaces {
		flushTimes := m.namespaceFlushTimes(ns, curr)
		multiErr = multiErr.Add(m.flushNamespaceWithTimes(ns, flushTimes, flush))
	}

	// mark flush finished
	multiErr = multiErr.Add(flush.Done())
	return multiErr.FinalError()
}

func (m *flushManager) Report() {
	m.RLock()
	flushInProgress := m.flushInProgress
	m.RUnlock()

	if flushInProgress {
		m.status.Update(1)
	} else {
		m.status.Update(0)
	}
}

func (m *flushManager) flushRange(ropts retention.Options, t time.Time) (time.Time, time.Time) {
	return retention.FlushTimeStart(ropts, t), retention.FlushTimeEnd(ropts, t)
}

func (m *flushManager) namespaceFlushTimes(ns databaseNamespace, curr time.Time) []time.Time {
	var (
		rOpts            = ns.Options().RetentionOptions()
		blockSize        = rOpts.BlockSize()
		earliest, latest = m.flushRange(rOpts, curr)
	)

	candidateTimes := timesInRange(earliest, latest, blockSize)
	return filterTimes(candidateTimes, func(t time.Time) bool {
		return ns.NeedsFlush(t, t)
	})
}

// flushWithTime flushes in-memory data for a given namespace, at a given
// time, returning any error encountered during flushing
func (m *flushManager) flushNamespaceWithTimes(ns databaseNamespace, times []time.Time, flush persist.Flush) error {
	multiErr := xerrors.NewMultiError()
	for _, t := range times {
		// NB(xichen): we still want to proceed if a namespace fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := ns.Flush(t, flush); err != nil {
			detailedErr := fmt.Errorf("namespace %s failed to flush data: %v",
				ns.ID().String(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}
