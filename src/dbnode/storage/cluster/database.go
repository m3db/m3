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
	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
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
	initializing          tally.Gauge
	leaving               tally.Gauge
	available             tally.Gauge
	shardsClusterTotal    tally.Gauge
	shardsClusterReplicas tally.Gauge
}

func newDatabaseMetrics(scope tally.Scope) databaseMetrics {
	shardsScope := scope.SubScope("shards")
	shardsClusterScope := scope.SubScope("shards-cluster")
	return databaseMetrics{
		initializing:          shardsScope.Gauge("initializing"),
		leaving:               shardsScope.Gauge("leaving"),
		available:             shardsScope.Gauge("available"),
		shardsClusterTotal:    shardsClusterScope.Gauge("total"),
		shardsClusterReplicas: shardsClusterScope.Gauge("replicas"),
	}
}

type clusterDB struct {
	storage.Database

	opts    storage.Options
	log     *zap.Logger
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
		opts:           opts,
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

// IsBootstrappedAndDurable determines whether the database is bootstrapped
// and durable, meaning that it could recover all data in memory using only
// the local disk.
//
// The logic here is a little tricky because there are two levels of
// IsBootstrappedAndDurable():
//
// The first level is this method which exists on the clustered database. It is
// used by our health check endpoints and tooling in general to determine when
// it is safe to continue a deploy or performing topology changes. In that case,
// we only need to determine two things:
//
//     1. Is the node bootstrapped?
//     2. Are all of its shards available?
//
// If so, then the node has finished bootstrapping and will be able to recover
// all of its data (assuming the default bootstrapper configuration of
// [filesystem, commitlog, peers]) if it goes down and its safe to continue
// operations. The reason this is true is because a node will ONLY mark its shards
// as available once it reaches a point where it is durable for the new shards it
// has received, and M3DB is architected in such a way (again, assuming the default
// bootstrapping configuration) that once a node reaches the AVAILABLE state it will
// always remain durable for that shard.
//
// The implications of only checking those two conditions means that when we're
// deploying a cluster, we only have to wait for the node to finish bootstrapping
// because all the shards will already be in the AVAILABLE state. When performing
// topology changes (like adding nodes) we'll have to wait until the node finishes
// bootstrapping AND that it marks its newly acquired shards as available. This is
// also desired because it means that the operations won't proceed until this node
// is capable of restoring all of the data it is responsible for from its own disk
// without relying on its peers.
//
// The second level of IsBootstrappedAndDurable exists on the storage database (see
// the comment on that method for a high-level overview of the conditions it checks
// for) and we only use that method when we're trying to determine if it is safe to
// mark newly acquired shards as AVAILABLE. That method is responsible for determining
// that all the shards it has been assigned are durable. The storage database method
// is very precautious so we want to avoid checking it if we don't have to (I.E when our
// shards are already in the AVAILABLE state) because it would significantly slow down
// our deployments and topology changes operations as every step would require the nodes
// to wait for a complete snapshot to take place before proceeding, when in fact that is
// often not required for correctness.
func (d *clusterDB) IsBootstrappedAndDurable() bool {
	if !d.Database.IsBootstrapped() {
		return false
	}

	_, ok := d.topo.(topology.DynamicTopology)
	if !ok {
		// If the topology is not dynamic, then the only thing we care
		// about is whether the node is bootstrapped or not because the
		// concept of durability as it relates to shard state doesn't
		// make sense if we're using a static topology.
		//
		// In other words, we don't need to check the shards states because
		// they don't change, and we don't need to check if the storage
		// database IsBootstrappedAndDurable() because that is only important
		// when we're trying to figure out if the storage database has become
		// durable since we made a topology change which is not possible with
		// a static topology.
		return true
	}

	entry, ok := d.watch.Get().LookupHostShardSet(d.hostID)
	if !ok {
		// If we're bootstrapped, but not in the placement, then we
		// are durable because we don't have any data we need to store
		// anyways.
		return true
	}

	for _, s := range entry.ShardSet().All() {
		switch s.State() {
		case shard.Leaving:
			continue
		case shard.Available:
			continue
		}

		return false
	}

	// If all of the shards we own are either LEAVING or AVAILABLE then we know
	// we are durable because we will only mark shards as AVAILABLE once we become
	// durable for them, and then once a shard has reached the AVAILABLE state we
	// are responsible for always remaining in a durable state.
	return true
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
	placement := d.watch.Get()
	entry, ok := placement.LookupHostShardSet(d.hostID)
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
		shardsClusterTotal := len(placement.ShardSet().All())
		d.metrics.shardsClusterTotal.Update(float64(shardsClusterTotal))
		d.metrics.shardsClusterReplicas.Update(float64(placement.Replicas()))
	}

	defer reportStats()

	// Manage the reusable vars
	d.resetReusable()
	defer d.resetReusable()

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
	// topology to mark them as available.
	topo, ok := d.topo.(topology.DynamicTopology)
	if !ok {
		err := fmt.Errorf("topology constructed is not a dynamic topology")
		d.log.Error("cluster db cannot mark shard available", zap.Error(err))
		return
	}

	// Call IsBootstrappedAndDurable on storage database, not cluster.
	if !d.Database.IsBootstrappedAndDurable() {
		return
	}

	// Count if initializing shards have bootstrapped in all namespaces. This
	// check is redundant with the database check above, but we do it for
	// posterity just to make sure everything is in the correct state.
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

	var markAvailable []uint32
	for id := range d.initializing {
		count := d.bootstrapCount[id]
		if count != len(namespaces) {
			// Should never happen if bootstrapped and durable.
			instrument.EmitAndLogInvariantViolation(d.opts.InstrumentOptions(), func(l *zap.Logger) {
				l.With(
					zap.Uint32("shard", id),
					zap.Int("count", count),
					zap.Int("numNamespaces", len(namespaces)),
				).Error("database indicated that it was bootstrapped and durable, but number of bootstrapped shards did not match number of namespaces")
			})
			continue
		}

		// Mark this shard as available
		if markAvailable == nil {
			// Defer allocation until needed, alloc as much as could be required.
			markAvailable = make([]uint32, 0, len(d.initializing))
		}
		markAvailable = append(markAvailable, id)
	}

	if len(markAvailable) == 0 {
		return
	}

	if err := topo.MarkShardsAvailable(d.hostID, markAvailable...); err != nil {
		d.log.Error("cluster db failed marking shards available",
			zap.Uint32s("shards", markAvailable), zap.Error(err))
		return
	}

	d.log.Info("cluster db successfully marked shards as available",
		zap.Uint32s("shards", markAvailable))
}

func (d *clusterDB) resetReusable() {
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
	d.log.Warn("topology has no shard set for host ID", zap.String("hostID", d.hostID))
	return sharding.NewEmptyShardSet(m.ShardSet().HashFn())
}
