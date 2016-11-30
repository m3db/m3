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

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
)

var (
	testNamespace = namespace.NewMetadata(ts.StringID("foo"),
		namespace.NewOptions())
	testNamespaces = []namespace.Metadata{testNamespace}
)

type restoreFn func()

func mockNewStorageDatabase(
	ctrl *gomock.Controller,
) (*storage.MockDatabase, restoreFn) {
	var mock *storage.MockDatabase
	if ctrl != nil {
		mock = storage.NewMockDatabase(ctrl)
	}
	restore := setNewStorageDatabase(func(
		namespaces []namespace.Metadata,
		shardSet sharding.ShardSet,
		opts storage.Options,
	) (storage.Database, error) {
		if mock == nil {
			return nil, fmt.Errorf("no injected storage database")
		}
		return mock, nil
	})
	return mock, restore
}

func setNewStorageDatabase(fn newStorageDatabaseFn) restoreFn {
	prevNewStorageDatabase := newStorageDatabase
	newStorageDatabase = fn
	return func() {
		newStorageDatabase = prevNewStorageDatabase
	}
}

type topoView struct {
	hashFn     sharding.HashFn
	assignment map[string][]uint32
	replicas   int
}

func newTopoView(
	replicas int,
	assignment map[string][]uint32,
) topoView {
	total := 0
	for _, shards := range assignment {
		total += len(shards)
	}

	return topoView{
		hashFn:     sharding.DefaultHashGen(total / replicas),
		assignment: assignment,
		replicas:   replicas,
	}
}

func (v topoView) newStaticMap() topology.Map {
	var (
		hostShardSets []topology.HostShardSet
		shards        []uint32
		unique        = make(map[uint32]struct{})
	)

	for hostID, hostShards := range v.assignment {
		shardSet, _ := sharding.NewShardSet(hostShards, v.hashFn)
		host := topology.NewHost(hostID, fmt.Sprintf("%s:9000", hostID))
		hostShardSet := topology.NewHostShardSet(host, shardSet)
		hostShardSets = append(hostShardSets, hostShardSet)
		for _, shard := range hostShards {
			if _, ok := unique[shard]; !ok {
				unique[shard] = struct{}{}
				shards = append(shards, shard)
			}
		}
	}

	shardSet, _ := sharding.NewShardSet(shards, v.hashFn)

	opts := topology.NewStaticOptions().
		SetHostShardSets(hostShardSets).
		SetReplicas(v.replicas).
		SetShardSet(shardSet)

	return topology.NewStaticMap(opts)
}

func newMockTopoInit(
	ctrl *gomock.Controller,
	viewsCh <-chan topoView,
) (*topology.MockInitializer, <-chan struct{}) {
	init := topology.NewMockInitializer(ctrl)

	watch := topology.NewMockMapWatch(ctrl)

	ch := make(chan struct{}, 1)
	// Make the propogate views channel large so it never blocks
	propogateViewsCh := make(chan struct{}, 128)
	go func() {
		for {
			v, ok := <-viewsCh
			if !ok {
				break
			}

			watch.EXPECT().Get().Return(v.newStaticMap())

			ch <- struct{}{}
			propogateViewsCh <- struct{}{}
		}
		close(ch)
	}()

	watch.EXPECT().C().Return(ch).AnyTimes()

	topo := topology.NewMockTopology(ctrl)
	topo.EXPECT().Watch().Return(watch, nil)

	init.EXPECT().Init().Return(topo, nil)

	return init, propogateViewsCh
}
