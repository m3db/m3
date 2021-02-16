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

	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/uber-go/tally"
)

const (
	tokenCheckInterval = time.Second
)

var (
	errEmptyNamespaces = errors.New("empty namespaces")
	errTickInProgress  = errors.New("another tick is in progress")
	errTickCancelled   = errors.New("tick is cancelled")
)

type tickManagerMetrics struct {
	tickDuration       tally.Timer
	tickWorkDuration   tally.Timer
	tickCancelled      tally.Counter
	tickDeadlineMissed tally.Counter
	tickDeadlineMet    tally.Counter
}

func newTickManagerMetrics(scope tally.Scope) tickManagerMetrics {
	return tickManagerMetrics{
		tickDuration:       scope.Timer("duration"),
		tickWorkDuration:   scope.Timer("work-duration"),
		tickCancelled:      scope.Counter("cancelled"),
		tickDeadlineMissed: scope.Counter("deadline.missed"),
		tickDeadlineMet:    scope.Counter("deadline.met"),
	}
}

type tickManager struct {
	sync.Mutex
	database database
	opts     Options
	nowFn    clock.NowFn
	sleepFn  sleepFn

	metrics tickManagerMetrics
	c       context.Cancellable
	tokenCh chan struct{}

	runtimeOpts tickManagerRuntimeOptions
}

type tickManagerRuntimeOptions struct {
	sync.RWMutex
	vals tickManagerRuntimeOptionsValues
}

func (o *tickManagerRuntimeOptions) set(v tickManagerRuntimeOptionsValues) {
	o.Lock()
	o.vals = v
	o.Unlock()
}

func (o *tickManagerRuntimeOptions) values() tickManagerRuntimeOptionsValues {
	o.RLock()
	v := o.vals
	o.RUnlock()
	return v
}

type tickManagerRuntimeOptionsValues struct {
	tickMinInterval               time.Duration
	tickCancellationCheckInterval time.Duration
}

func newTickManager(database database, opts Options) databaseTickManager {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("tick")
	tokenCh := make(chan struct{}, 1)
	tokenCh <- struct{}{}

	mgr := &tickManager{
		database: database,
		opts:     opts,
		nowFn:    opts.ClockOptions().NowFn(),
		sleepFn:  time.Sleep,
		metrics:  newTickManagerMetrics(scope),
		c:        context.NewCancellable(),
		tokenCh:  tokenCh,
	}

	runtimeOptsMgr := opts.RuntimeOptionsManager()
	runtimeOptsMgr.RegisterListener(mgr)
	return mgr
}

func (mgr *tickManager) SetRuntimeOptions(opts runtime.Options) {
	mgr.runtimeOpts.set(tickManagerRuntimeOptionsValues{
		tickMinInterval:               opts.TickMinimumInterval(),
		tickCancellationCheckInterval: opts.TickCancellationCheckInterval(),
	})
}

func (mgr *tickManager) Tick(forceType forceType, startTime time.Time) error {
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
	namespaces, err := mgr.database.OwnedNamespaces()
	if err != nil {
		return err
	}
	if len(namespaces) == 0 {
		return errEmptyNamespaces
	}

	// Begin ticking
	var (
		start    = mgr.nowFn()
		multiErr xerrors.MultiError
	)
	for _, n := range namespaces {
		multiErr = multiErr.Add(n.Tick(mgr.c, startTime))
	}

	// NB(r): Always sleep for some constant period since ticking
	// is variable with num series. With a really small amount of series
	// the per shard amount of constant sleeping is only:
	// = num shards * sleep per series (100 microseconds default)
	// This number can be quite small if configuring to have small amount of
	// shards (say 10 shards) with 32 series per shard (total of 320 series):
	// = 10 num shards * ~32 series per shard * 100 microseconds default sleep per series
	// = 10*32*0.1 milliseconds
	// = 32ms
	// Because of this we always sleep at least some fixed constant amount.
	took := mgr.nowFn().Sub(start)
	mgr.metrics.tickWorkDuration.Record(took)

	vals := mgr.runtimeOpts.values()
	min := vals.tickMinInterval

	// Sleep in a loop so that cancellations propagate if need to
	// wait to fulfill the tick min interval
	interval := vals.tickCancellationCheckInterval
	for d := time.Duration(0); d < min-took; d += interval {
		if mgr.c.IsCancelled() {
			break
		}
		mgr.sleepFn(interval)
		// Check again at the end of each sleep to see if it
		// has changed. Particularly useful for integration tests.
		min = mgr.runtimeOpts.values().tickMinInterval
	}

	end := mgr.nowFn()
	duration := end.Sub(start)
	mgr.metrics.tickDuration.Record(duration)

	if mgr.c.IsCancelled() {
		mgr.metrics.tickCancelled.Inc(1)
		return errTickCancelled
	}

	return multiErr.FinalError()
}
