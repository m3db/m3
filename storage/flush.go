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
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3x/errors"

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
	blockSize       time.Duration
	pm              persist.Manager
	flushInProgress bool
	status          tally.Gauge
}

func newFlushManager(database database, scope tally.Scope) databaseFlushManager {
	opts := database.Options()
	return &flushManager{
		database:  database,
		opts:      opts,
		nowFn:     opts.ClockOptions().NowFn(),
		blockSize: opts.RetentionOptions().BlockSize(),
		pm:        opts.PersistManager(),
		status:    scope.Gauge("flush"),
	}
}

func (m *flushManager) NeedsFlush(t time.Time) bool {
	namespaces := m.database.getOwnedNamespaces()
	for _, n := range namespaces {
		if n.NeedsFlush(t) {
			return true
		}
	}
	return false
}

func (m *flushManager) FlushTimeStart(t time.Time) time.Time {
	return retention.FlushTimeStart(m.opts.RetentionOptions(), t)
}

func (m *flushManager) FlushTimeEnd(t time.Time) time.Time {
	return retention.FlushTimeEnd(m.opts.RetentionOptions(), t)
}

func (m *flushManager) Flush(curr time.Time) error {
	timesToFlush := m.flushTimes(curr)
	if len(timesToFlush) == 0 {
		return nil
	}

	flush, err := m.pm.StartFlush()
	if err != nil {
		return err
	}

	defer flush.Done()

	m.Lock()
	if m.flushInProgress {
		m.Unlock()
		return errFlushAlreadyInProgress
	}
	m.flushInProgress = true
	m.Unlock()

	defer func() {
		m.Lock()
		m.flushInProgress = false
		m.Unlock()
	}()

	multiErr := xerrors.NewMultiError()
	for _, flushTime := range timesToFlush {
		if err := m.flushWithTime(flushTime, flush); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (m *flushManager) SetRateLimitOptions(value ratelimit.Options) {
	m.pm.SetRateLimitOptions(value)
}

func (m *flushManager) RateLimitOptions() ratelimit.Options {
	return m.pm.RateLimitOptions()
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

func (m *flushManager) flushTimes(curr time.Time) []time.Time {
	earliest, latest := m.FlushTimeStart(curr), m.FlushTimeEnd(curr)

	// NB(xichen): could preallocate slice here.
	var flushTimes []time.Time
	for t := latest; !t.Before(earliest); t = t.Add(-m.blockSize) {
		if m.NeedsFlush(t) {
			flushTimes = append(flushTimes, t)
		}
	}

	return flushTimes
}

// flushWithTime flushes in-memory data across all namespaces for a given
// time, returning any error encountered during flushing
func (m *flushManager) flushWithTime(t time.Time, flush persist.Flush) error {
	multiErr := xerrors.NewMultiError()
	namespaces := m.database.getOwnedNamespaces()
	for _, n := range namespaces {
		// NB(xichen): we still want to proceed if a namespace fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := n.Flush(t, flush); err != nil {
			detailedErr := fmt.Errorf("namespace %s failed to flush data: %v",
				n.ID().String(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}
