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
	"github.com/m3db/m3db/context"

	"github.com/uber-go/tally"
)

const (
	tokenCheckInterval        = time.Second
	cancellationCheckInterval = time.Second
)

var (
	errEmptyNamespaces = errors.New("empty namespaces")
	errTickInProgress  = errors.New("another tick is in progress")
	errTickCancelled   = errors.New("tick is cancelled")
)

type tickManagerMetrics struct {
	tickDuration       tally.Timer
	tickCancelled      tally.Counter
	tickDeadlineMissed tally.Counter
	tickDeadlineMet    tally.Counter
}

func newTickManagerMetrics(scope tally.Scope) tickManagerMetrics {
	return tickManagerMetrics{
		tickDuration:       scope.Timer("duration"),
		tickCancelled:      scope.Counter("cancelled"),
		tickDeadlineMissed: scope.Counter("deadline.missed"),
		tickDeadlineMet:    scope.Counter("deadline.met"),
	}
}

type tickManager struct {
	sync.RWMutex

	database database
	opts     Options
	nowFn    clock.NowFn
	sleepFn  sleepFn

	metrics tickManagerMetrics
	c       context.Cancellable
	tokenCh chan struct{}
}

func newTickManager(database database, opts Options) databaseTickManager {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("tick")
	tokenCh := make(chan struct{}, 1)
	tokenCh <- struct{}{}

	return &tickManager{
		database: database,
		opts:     opts,
		nowFn:    opts.ClockOptions().NowFn(),
		sleepFn:  time.Sleep,
		metrics:  newTickManagerMetrics(scope),
		c:        context.NewCancellable(),
		tokenCh:  tokenCh,
	}
}

func (mgr *tickManager) Tick(softDeadline time.Duration, forceType forceType) error {
	if forceType == force {
		acquired := false
		waiter := time.NewTicker(tokenCheckInterval)
		// NB(xichen): cancellation is done in a loop so if there are multiple
		// forced ticks, their cancellations don't get reset when token is acquired.
		for !acquired {
			select {
			case <-mgr.tokenCh:
				acquired = true
			case <-waiter.C:
				mgr.c.Cancel()
			}
		}
		waiter.Stop()
	} else {
		select {
		case <-mgr.tokenCh:
		default:
			return errTickInProgress
		}
	}

	// Release the token
	defer func() { mgr.tokenCh <- struct{}{} }()

	// Now we acquired the token, reset the cancellable
	mgr.c.Reset()
	namespaces := mgr.database.getOwnedNamespaces()
	if len(namespaces) == 0 {
		return errEmptyNamespaces
	}

	// Begin ticking
	start := mgr.nowFn()
	sizes := make([]int64, 0, len(namespaces))
	totalSize := int64(0)
	for _, n := range namespaces {
		size := n.NumSeries()
		sizes = append(sizes, size)
		totalSize += size
	}
	for i, n := range namespaces {
		deadline := float64(softDeadline) * (float64(sizes[i]) / float64(totalSize))
		n.Tick(mgr.c, time.Duration(deadline))
	}

	end := mgr.nowFn()
	duration := end.Sub(start)
	mgr.metrics.tickDuration.Record(duration)

	if duration > softDeadline {
		mgr.metrics.tickDeadlineMissed.Inc(1)
	} else {
		mgr.metrics.tickDeadlineMet.Inc(1)

		// Throttle to reduce locking overhead during ticking
		for d := time.Duration(0); d < softDeadline-duration; d += cancellationCheckInterval {
			if mgr.c.IsCancelled() {
				break
			}
			mgr.sleepFn(cancellationCheckInterval)
		}
	}

	if mgr.c.IsCancelled() {
		mgr.metrics.tickCancelled.Inc(1)
		return errTickCancelled
	}
	return nil
}
