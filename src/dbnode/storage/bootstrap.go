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

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	xerrors "github.com/m3db/m3x/errors"
	xlog "github.com/m3db/m3x/log"

	"github.com/uber-go/tally"
)

var (
	// errNamespaceIsBootstrapping raised when trying to bootstrap a namespace that's being bootstrapped.
	errNamespaceIsBootstrapping = errors.New("namespace is bootstrapping")

	// errNamespaceNotBootstrapped raised when trying to flush/snapshot data for a namespace that's not yet bootstrapped.
	errNamespaceNotBootstrapped = errors.New("namespace is not yet bootstrapped")

	// errShardIsBootstrapping raised when trying to bootstrap a shard that's being bootstrapped.
	errShardIsBootstrapping = errors.New("shard is bootstrapping")

	// errShardNotBootstrappedToFlush raised when trying to flush data for a shard that's not yet bootstrapped.
	errShardNotBootstrappedToFlush = errors.New("shard is not yet bootstrapped to flush")

	// errShardNotBootstrappedToSnapshot raised when trying to snapshot data for a shard that's not yet bootstrapped.
	errShardNotBootstrappedToSnapshot = errors.New("shard is not yet bootstrapped to snapshot")

	// errShardNotBootstrappedToRead raised when trying to read data for a shard that's not yet bootstrapped.
	errShardNotBootstrappedToRead = errors.New("shard is not yet bootstrapped to read")

	// errIndexNotBootstrappedToRead raised when trying to read the index before being bootstrapped.
	errIndexNotBootstrappedToRead = errors.New("index is not yet bootstrapped to read")

	// errBootstrapEnqueued raised when trying to bootstrap and bootstrap becomes enqueued.
	errBootstrapEnqueued = errors.New("database bootstrapping enqueued bootstrap")
)

type bootstrapManager struct {
	sync.RWMutex

	database        database
	mediator        databaseMediator
	opts            Options
	log             xlog.Logger
	nowFn           clock.NowFn
	processProvider bootstrap.ProcessProvider
	state           BootstrapState
	hasPending      bool
	status          tally.Gauge
}

func newBootstrapManager(
	database database,
	mediator databaseMediator,
	opts Options,
) databaseBootstrapManager {
	scope := opts.InstrumentOptions().MetricsScope()
	return &bootstrapManager{
		database:        database,
		mediator:        mediator,
		opts:            opts,
		log:             opts.InstrumentOptions().Logger(),
		nowFn:           opts.ClockOptions().NowFn(),
		processProvider: opts.BootstrapProcessProvider(),
		status:          scope.Gauge("bootstrapped"),
	}
}

func (m *bootstrapManager) IsBootstrapped() bool {
	m.RLock()
	state := m.state
	m.RUnlock()
	return state == Bootstrapped
}

func (m *bootstrapManager) Bootstrap() error {
	m.Lock()
	switch m.state {
	case Bootstrapping:
		// NB(r): Already bootstrapping, now a consequent bootstrap
		// request comes in - we queue this up to bootstrap again
		// once the current bootstrap has completed.
		// This is an edge case that can occur if during either an
		// initial bootstrap or a resharding bootstrap if a new
		// reshard occurs and we need to bootstrap more shards.
		m.hasPending = true
		m.Unlock()
		return errBootstrapEnqueued
	default:
		m.state = Bootstrapping
	}
	m.Unlock()

	// NB(xichen): disable filesystem manager before we bootstrap to minimize
	// the impact of file operations on bootstrapping performance
	m.mediator.DisableFileOps()
	defer m.mediator.EnableFileOps()

	// Keep performing bootstraps until none pending
	multiErr := xerrors.NewMultiError()
	for {
		err := m.bootstrap()
		if err != nil {
			multiErr = multiErr.Add(err)
		}

		m.Lock()
		currPending := m.hasPending
		if currPending {
			// New bootstrap calls should now enqueue another pending bootstrap
			m.hasPending = false
		} else {
			m.state = Bootstrapped
		}
		m.Unlock()

		if !currPending {
			break
		}
	}

	// NB(xichen): in order for bootstrapped data to be flushed to disk, a tick
	// needs to happen to drain the in-memory buffers and a consequent flush will
	// flush all the data onto disk. However, this has shown to be too intensive
	// to do immediately after bootstrap due to bootstrapping nodes simultaneously
	// attempting to tick through their series and flushing data, adding significant
	// load to the cluster. It turns out to be better to let ticking happen naturally
	// on its own course so that the load of ticking and flushing is more spread out
	// across the cluster.

	return multiErr.FinalError()
}

func (m *bootstrapManager) Report() {
	if m.IsBootstrapped() {
		m.status.Update(1)
	} else {
		m.status.Update(0)
	}
}

func (m *bootstrapManager) bootstrap() error {
	// NB(r): construct new instance of the bootstrap process to avoid
	// state being kept around by bootstrappers.
	process, err := m.processProvider.Provide()
	if err != nil {
		return err
	}

	// NB(xichen): each bootstrapper should be responsible for choosing the most
	// efficient way of bootstrapping database shards, be it sequential or parallel.
	multiErr := xerrors.NewMultiError()

	namespaces, err := m.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}

	startBootstrap := m.nowFn()
	for _, namespace := range namespaces {
		startNamespaceBootstrap := m.nowFn()
		if err := namespace.Bootstrap(startBootstrap, process); err != nil {
			multiErr = multiErr.Add(err)
		}
		took := m.nowFn().Sub(startNamespaceBootstrap)
		m.log.WithFields(
			xlog.NewField("namespace", namespace.ID().String()),
			xlog.NewField("duration", took.String()),
		).Info("bootstrap finished")
	}

	return multiErr.FinalError()
}
