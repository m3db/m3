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
	"sync"

	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"

	"github.com/m3db/m3x/log"
)

var (
	// newStorageDatabase is the injected constructor to construct a database,
	// useful for replacing which database constructor is called in tests
	newStorageDatabase newStorageDatabaseFn = storage.NewDatabase

	errAlreadyWatchingTopology = errors.New("cluster database is already watching topology")
	errNotWatchingTopology     = errors.New("cluster database is already watching topology")
)

type newStorageDatabaseFn func(
	namespaces []namespace.Metadata,
	shardSet sharding.ShardSet,
	opts storage.Options,
) (storage.Database, error)

type clusterDB struct {
	storage.Database

	log    xlog.Logger
	hostID string
	watch  topology.MapWatch

	watchMutex sync.Mutex
	watching   bool
	doneCh     chan struct{}
	closedCh   chan struct{}
}

// NewDatabase creates a new clustered time series database
func NewDatabase(
	namespaces []namespace.Metadata,
	hostID string,
	topoInit topology.Initializer,
	opts storage.Options,
) (Database, error) {
	log := opts.InstrumentOptions().Logger()
	topo, err := topoInit.Init()
	if err != nil {
		return nil, err
	}

	watch, err := topo.Watch()
	if err != nil {
		return nil, err
	}

	// Wait for the topology to be available
	<-watch.C()

	shardSet, ok := hostOrEmptyShardSet(hostID, watch.Get())
	if !ok {
		log.Warnf("topology has no shard set for host ID: %s", hostID)
	}

	db, err := newStorageDatabase(namespaces, shardSet, opts)
	if err != nil {
		return nil, err
	}

	return &clusterDB{
		Database: db,
		log:      log,
		hostID:   hostID,
		watch:    watch,
	}, nil
}

func (d *clusterDB) Open() error {
	select {
	case <-d.watch.C():
		shardSet, ok := hostOrEmptyShardSet(d.hostID, d.watch.Get())
		if !ok {
			d.log.Warnf("topology has no shard set for host ID: %s", d.hostID)
		}
		d.Database.AssignShardSet(shardSet)
	default:
		// No updates to the topology since cluster DB created
	}

	if err := d.Database.Open(); err != nil {
		return err
	}
	if err := d.startActiveTopologyWatch(); err != nil {
		return err
	}

	return nil
}

func (d *clusterDB) Close() error {
	if err := d.Database.Close(); err != nil {
		return err
	}
	if err := d.stopActiveTopologyWatch(); err != nil {
		return err
	}
	return nil
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

	d.doneCh <- struct{}{}
	close(d.doneCh)
	<-d.closedCh
	close(d.closedCh)

	return nil
}

func (d *clusterDB) activeTopologyWatch() {
	for {
		select {
		case <-d.doneCh:
			d.closedCh <- struct{}{}
			return
		case <-d.watch.C():
			shardSet, ok := hostOrEmptyShardSet(d.hostID, d.watch.Get())
			if !ok {
			}
			d.Database.AssignShardSet(shardSet)
		}
	}
}

// hostOrEmptyShardSet returns a shard set for the given host ID from a
// topology map and if none exists then an empty shard set. If successfully
// found the shard set for the host the second parameter returns true,
// otherwise false.
func hostOrEmptyShardSet(
	hostID string,
	m topology.Map,
) (sharding.ShardSet, bool) {
	if hostShardSet, ok := m.LookupHostShardSet(hostID); ok {
		return hostShardSet.ShardSet(), true
	}
	allShardSet := m.ShardSet()
	shardSet, _ := sharding.NewShardSet(nil, allShardSet.HashFn())
	return shardSet, false
}
