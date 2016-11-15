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
	"fmt"

	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
)

type clusterDB struct {
	storage.Database
}

// NewDatabase creates a new clustered time series database
func NewDatabase(
	namespaces []namespace.Metadata,
	hostID string,
	topoInit topology.Initializer,
	opts storage.Options,
) (Database, error) {
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

	m := watch.Get()

	hostShardSet, ok := m.LookupHostShardSet(hostID)
	if !ok {
		return nil, fmt.Errorf("topology missing host shard set for host ID: %s",
			hostID)
	}

	shardSet := hostShardSet.ShardSet()
	db, err := storage.NewDatabase(namespaces, shardSet, opts)
	if err != nil {
		return nil, err
	}

	return &clusterDB{Database: db}, nil
}

func (d *clusterDB) Open() error {
	// todo: read any updates from the watch and reshard before open

	if err := d.Database.Open(); err != nil {
		return err
	}

	// todo: spawn background goroutine to listen for reshards and act upon them

	return nil
}

func (d *clusterDB) Close() error {
	if err := d.Database.Close(); err != nil {
		return err
	}
	// todo: stop listening for reshards
}
