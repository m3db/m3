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

package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/topology"
	xlog "github.com/m3db/m3x/log"

	"github.com/uber-go/tally"
)

var (
	// newStorageDatabase is the injected constructor to construct a database,
	// useful for replacing which database constructor is called in tests
	newStorageDatabase = storage.NewDatabase

	errAlreadyWatchingTopology = errors.New("cluster database is already watching topology")
	errNotWatchingTopology     = errors.New("cluster database is not watching topology")
)

type newStorageDatabaseFn func(
	shardSet sharding.ShardSet,
	opts storage.Options,
) (storage.Database, error)

type databaseMetrics struct {
	initializing tally.Gauge
	leaving      tally.Gauge
	available    tally.Gauge
}

func newDatabaseMetrics(scope tally.Scope) databaseMetrics {
	return databaseMetrics{
		initializing: scope.Gauge("shards.initializing"),
		leaving:      scope.Gauge("shards.leaving"),
		available:    scope.Gauge("shards.available"),
	}
}

type clusterDB struct {
	storage.Database

	log     xlog.Logger
	metrics databaseMetrics
	hostID  string
	topo    topology.Topology
	watch   topology.MapWatch

	watchMutex sync.Mutex
	watching   bool
	doneCh     chan struct{}
	closedCh   chan struct{}

	initializing   map[uint32]shard.Shard
	bootstrapCount map[uint32]int
}

// NewDatabase creates a new clustered time series database
func NewDatabase(
	hostID string,
	topo topology.Topology,
	topoWatch topology.MapWatch,
	opts storage.Options,
) (Database, error) {
	instrumentOpts := opts.InstrumentOptions()
	log := instrumentOpts.Logger()
	m := newDatabaseMetrics(instrumentOpts.MetricsScope().SubScope("cluster"))

	log.Info("cluster database initializing topology")

	// Wait for the topology to be available
	log.Info("cluster database resolving topology")
	<-topoWatch.C()
	log.Info("cluster database resolved topology")

	d := &clusterDB{
		log:            log,
		metrics:        m,
		hostID:         hostID,
		topo:           topo,
		watch:          topoWatch,
		initializing:   make(map[uint32]shard.Shard),
		bootstrapCount: make(map[uint32]int),
	}

	shardSet := d.hostOrEmptyShardSet(topoWatch.Get())
	db, err := newStorageDatabase(shardSet, opts)
	if err != nil {
		return nil, err
	}

	d.Database = db
	return d, nil
}

func (d *clusterDB) Topology() topology.Topology {
	return d.topo
}

func (d *clusterDB) TopologyMap() (topology.Map, error) {
	return d.topo.Get(), nil
}

func (d *clusterDB) Open() error {
	select {
	case <-d.watch.C():
		shardSet := d.hostOrEmptyShardSet(d.watch.Get())
		d.Database.AssignShardSet(shardSet)
	default:
		// No updates to the topology since cluster DB created
	}
	if err := d.Database.Open(); err != nil {
		return err
	}
	return d.startActiveTopologyWatch()
}

func (d *clusterDB) Close() error {
	if err := d.Database.Close(); err != nil {
		return err
	}
	return d.stopActiveTopologyWatch()
}

func (d *clusterDB) startActiveTopologyWatch() error {
	d.watchMutex.Lock()
	defer d.watchMutex.Unlock()

	if d.watching {
		return errAlreadyWatchingTopology
	}

	d.watching = true

	d.doneCh = make(chan struct{}, 1)
	d.closedCh = make(chan struct{}, 1)

	go d.activeTopologyWatch()

	return nil
}

func (d *clusterDB) stopActiveTopologyWatch() error {
	d.watchMutex.Lock()
	defer d.watchMutex.Unlock()

	if !d.watching {
		return errNotWatchingTopology
	}

	d.watching = false

	close(d.doneCh)
	<-d.closedCh

	return nil
}

func (d *clusterDB) activeTopologyWatch() {
	reportClosingCh := make(chan struct{}, 1)
	reportClosedCh := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				d.analyzeAndReportShardStates()
			case <-reportClosingCh:
				ticker.Stop()
				close(reportClosedCh)
				return
			}
		}
	}()

	defer func() {
		// Issue closing signal to report channel
		close(reportClosingCh)
		// Wait for report channel to close
		<-reportClosedCh
		// Signal all closed
		close(d.closedCh)
	}()

	for {
		select {
		case <-d.doneCh:
			return
		case _, ok := <-d.watch.C():
			// NB(prateek): cluster/Database shares the topology with client/Session, so we
			// explicitly check if the watch channel has been closed
			if !ok {
				return
			}
			d.log.Info("received update from kv topology watch")
			shardSet := d.hostOrEmptyShardSet(d.watch.Get())
			d.Database.AssignShardSet(shardSet)
		}
	}
}

func (d *clusterDB) analyzeAndReportShardStates() {
	entry, ok := d.watch.Get().LookupHostShardSet(d.hostID)
	if !ok {
		return
	}

	reportStats := func() {
		var (
			initializing int64
			leaving      int64
			available    int64
		)
		for _, s := range entry.ShardSet().All() {
			switch s.State() {
			case shard.Initializing:
				initializing++
			case shard.Leaving:
				leaving++
			case shard.Available:
				available++
			}
		}
		d.metrics.initializing.Update(float64(initializing))
		d.metrics.leaving.Update(float64(leaving))
		d.metrics.available.Update(float64(available))
	}

	defer reportStats()

	// Manage the reuseable vars
	d.resetReuseable()
	defer d.resetReuseable()

	for _, s := range entry.ShardSet().All() {
		if s.State() == shard.Initializing {
			d.initializing[s.ID()] = s
		}
	}

	if len(d.initializing) == 0 {
		// No initializing shards
		return
	}

	// To mark any initializing shards as available we need a
	// dynamic topology, check if we have one and if not we will report
	// that shards are initialzing and that we do not have a dynamic
	// topology to mark them as available
	topo, ok := d.topo.(topology.DynamicTopology)
	if !ok {
		err := fmt.Errorf("topology constructed is not a dynamic topology")
		d.log.Errorf("cluster db cannot mark shard available: %v", err)
		return
	}

	// Count if initializing shards have bootstrapped in all namespaces. This
	// check is redundant with the database check below, but we do it for
	// posterity.
	namespaces := d.Database.Namespaces()
	for _, n := range namespaces {
		for _, s := range n.Shards() {
			if _, ok := d.initializing[s.ID()]; !ok {
				continue
			}
			if !s.IsBootstrapped() {
				continue
			}
			d.bootstrapCount[s.ID()]++
		}
	}

	if !d.IsBootstrappedAndDurable() {
		return
	}

	var markAvailable []uint32
	for id := range d.initializing {
		count := d.bootstrapCount[id]
		if count != len(namespaces) {
			continue
		}

		// Mark this shard as available
		if markAvailable == nil {
			// Defer allocation until needed, alloc as much as could be required
			markAvailable = make([]uint32, 0, len(d.initializing))
		}
		markAvailable = append(markAvailable, id)
	}

	if len(markAvailable) == 0 {
		return
	}

	if err := topo.MarkShardsAvailable(d.hostID, markAvailable...); err != nil {
		d.log.Errorf("cluster db failed marking shards %v available: %v",
			markAvailable, err)
	}
}

func (d *clusterDB) resetReuseable() {
	d.resetInitializing()
	d.resetBootstrapCount()
}

func (d *clusterDB) resetInitializing() {
	for id := range d.initializing {
		delete(d.initializing, id)
	}
}

func (d *clusterDB) resetBootstrapCount() {
	for id := range d.bootstrapCount {
		delete(d.bootstrapCount, id)
	}
}

// hostOrEmptyShardSet returns a shard set for the given host ID from a
// topology map and if none exists then an empty shard set. If successfully
// found the shard set for the host the second parameter returns true,
// otherwise false.
func (d *clusterDB) hostOrEmptyShardSet(m topology.Map) sharding.ShardSet {
	if hostShardSet, ok := m.LookupHostShardSet(d.hostID); ok {
		return hostShardSet.ShardSet()
	}
	d.log.Warnf("topology has no shard set for host ID: %s", d.hostID)
	return sharding.NewEmptyShardSet(m.ShardSet().HashFn())
}
