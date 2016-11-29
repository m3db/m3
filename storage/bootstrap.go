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

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
)

type bootstrapState int

const (
	bootstrapNotStarted bootstrapState = iota
	bootstrapping
	bootstrapped
)

var (
	// errDatabaseNotBootstrapped raised when trying to query a database that's not yet bootstrapped.
	errDatabaseNotBootstrapped = errors.New("database is not yet bootstrapped")

	// errNamespaceIsBootstrapping raised when trying to bootstrap a namespace that's being bootstrapped.
	errNamespaceIsBootstrapping = errors.New("namespace is bootstrapping")

	// errNamespaceNotBootstrapped raised when trying to flush data for a namespace that's not yet bootstrapped.
	errNamespaceNotBootstrapped = errors.New("namespace is not yet bootstrapped")

	// errShardIsBootstrapping raised when trying to bootstrap a shard that's being bootstrapped.
	errShardIsBootstrapping = errors.New("shard is bootstrapping")

	// errShardNotBootstrapped raised when trying to flush data for a shard that's not yet bootstrapped.
	errShardNotBootstrapped = errors.New("shard is not yet bootstrapped")

	// errBootstrapEnqueued raised when trying to bootstrap and bootstrap becomes enqueued.
	errBootstrapEnqueued = errors.New("database bootstrapping enqueued bootstrap")
)

type bootstrapManager struct {
	sync.RWMutex

	database       database
	opts           Options
	log            xlog.Logger
	nowFn          clock.NowFn
	newBootstrapFn NewBootstrapFn
	state          bootstrapState
	pending        bool
	fsManager      databaseFileSystemManager
}

func newBootstrapManager(
	database database,
	fsManager databaseFileSystemManager,
) databaseBootstrapManager {
	opts := database.Options()
	return &bootstrapManager{
		database:       database,
		opts:           opts,
		log:            opts.InstrumentOptions().Logger(),
		nowFn:          opts.ClockOptions().NowFn(),
		newBootstrapFn: opts.NewBootstrapFn(),
		fsManager:      fsManager,
	}
}

func (m *bootstrapManager) IsBootstrapping() bool {
	m.RLock()
	state := m.state
	m.RUnlock()
	return state == bootstrapping
}

func (m *bootstrapManager) IsBootstrapped() bool {
	m.RLock()
	state := m.state
	m.RUnlock()
	return state == bootstrapped
}

func (m *bootstrapManager) targetRanges() xtime.Ranges {
	now := m.nowFn()
	ropts := m.opts.RetentionOptions()
	start := now.Add(-ropts.RetentionPeriod())
	midPoint := now.
		Add(-ropts.BlockSize()).
		Add(-ropts.BufferPast()).
		Truncate(ropts.BlockSize())
	cutover := now.Add(ropts.BufferFuture())

	return xtime.NewRanges().
		AddRange(xtime.Range{Start: start, End: midPoint}).
		AddRange(xtime.Range{Start: midPoint, End: cutover})
}

func (m *bootstrapManager) Bootstrap() error {
	var enqueued bool
	m.Lock()
	switch m.state {
	case bootstrapping:
		// NB(r): Already bootstrapping, now a consequent bootstrap
		// request comes in - we queue this up to bootstrap again
		// once the current bootstrap has completed.
		// This is an edge case that can occur if during either an
		// initial bootstrap or a resharding bootstrap if a new
		// reshard occurs and we need to bootstrap more shards.
		m.pending = true
		enqueued = true
	default:
		m.state = bootstrapping
	}
	m.Unlock()

	if enqueued {
		return errBootstrapEnqueued
	}

	// Keep performing bootstraps until none pending
	multiErr := xerrors.NewMultiError()
	for {
		err := m.bootstrap()
		if err != nil {
			multiErr = multiErr.Add(err)
		}

		m.Lock()
		currPending := m.pending
		if currPending {
			// New bootstrap calls should now enqueue another pending bootstrap
			m.pending = false
		} else {
			m.state = bootstrapped
		}
		m.Unlock()

		if !currPending {
			break
		}
	}

	return multiErr.FinalError()
}

func (m *bootstrapManager) bootstrap() error {
	targetRanges := m.targetRanges()

	// NB(xichen): each bootstrapper should be responsible for choosing the most
	// efficient way of bootstrapping database shards, be it sequential or parallel.
	multiErr := xerrors.NewMultiError()

	bs := m.newBootstrapFn()
	for _, namespace := range m.database.getOwnedNamespaces() {
		start := m.nowFn()
		if err := namespace.Bootstrap(bs, targetRanges); err != nil {
			multiErr = multiErr.Add(err)
		}
		end := m.nowFn()
		m.log.WithFields(
			xlog.NewLogField("namespace", namespace.ID().String()),
			xlog.NewLogField("duration", end.Sub(start).String()),
		).Info("bootstrap finished")
	}

	// At this point we have bootstrapped everything between now - retentionPeriod
	// and now, so we should run the filesystem manager to clean up files and flush
	// all the data we bootstrapped.
	rateLimitOpts := m.fsManager.RateLimitOptions()
	m.fsManager.SetRateLimitOptions(rateLimitOpts.SetLimitEnabled(false))
	m.fsManager.Run(m.nowFn(), false)
	m.fsManager.SetRateLimitOptions(rateLimitOpts)

	return multiErr.FinalError()
}
