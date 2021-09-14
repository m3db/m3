// Copyright (c) 2018 Uber Technologies, Inc.
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

package integration

import (
	"math"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
)

type placementInstanceConfig struct {
	instanceID          string
	shardSetID          uint32
	shardStartInclusive uint32
	shardEndExclusive   uint32
}

func (c *placementInstanceConfig) newPlacementInstance() placement.Instance {
	numShards := c.shardEndExclusive - c.shardStartInclusive
	shardSet := make([]shard.Shard, 0, numShards)
	for shardID := c.shardStartInclusive; shardID < c.shardEndExclusive; shardID++ {
		shard := shard.NewShard(shardID).
			SetState(shard.Available).
			SetCutoverNanos(0).
			SetCutoffNanos(math.MaxInt64)
		shardSet = append(shardSet, shard)
	}
	shards := shard.NewShards(shardSet)
	return placement.NewInstance().
		SetID(c.instanceID).
		SetShards(shards).
		SetShardSetID(c.shardSetID).
		SetEndpoint(c.instanceID).
		SetWeight(1)
}

func newPlacement(numShards int, instances []placement.Instance) placement.Placement {
	shards := make([]uint32, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = uint32(i)
	}
	var maxShardSetID uint32
	for _, inst := range instances {
		if maxShardSetID < inst.ShardSetID() {
			maxShardSetID = inst.ShardSetID()
		}
	}
	return placement.NewPlacement().
		SetInstances(instances).
		SetShards(shards).
		SetIsSharded(true).
		SetMaxShardSetID(maxShardSetID).
		SetReplicaFactor(1)
}

func setPlacement(
	key string,
	store kv.Store,
	pl placement.Placement,
) error {
	stagedPlacement, err := placement.NewPlacementsFromLatest(pl)
	if err != nil {
		return err
	}
	stagedPlacementProto, err := stagedPlacement.Proto()
	if err != nil {
		return err
	}
	_, err = store.Set(key, stagedPlacementProto)
	return err
}

func setPlacementWithClusterClient(
	key string,
	clusterClient clusterclient.Client,
	pl placement.Placement,
) error {
	svcs, err := clusterClient.Services(nil)
	if err != nil {
		return err
	}
	ps, err := svcs.PlacementService(services.NewServiceID().SetName(defaultServiceName), placement.NewOptions())
	if err != nil {
		return err
	}
	_, err = ps.Set(pl)
	if err != nil {
		return err
	}

	store, err := clusterClient.KV()
	if err != nil {
		return err
	}
	return setPlacement(key, store, pl)
}
