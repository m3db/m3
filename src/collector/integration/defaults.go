// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"hash/adler32"
	"time"

	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/id/m3"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/x/pool"
)

const (
	defaultNumShards     = 4096
	defaultInstanceID    = "localhost:6789"
	defaultPlacementKey  = "placement"
	defaultNamespace     = "integration"
	defaultNamespacesKey = "namespaces"
	defaultRuleSetKeyFmt = "rulesets/%s"
	// nolint: varcheck
	defaultRuleSetKey   = "rulesets/integration"
	defaultNamespaceTag = "namespace"
	defaultNameTag      = "name"
)

func defaultStagedPlacementProto() (*placementpb.PlacementSnapshots, error) {
	shardSet := make([]shard.Shard, defaultNumShards)
	for i := 0; i < defaultNumShards; i++ {
		shardSet[i] = shard.NewShard(uint32(i)).SetState(shard.Available)
	}
	shards := shard.NewShards(shardSet)
	instance := placement.NewInstance().
		SetID(defaultInstanceID).
		SetEndpoint(defaultInstanceID).
		SetShards(shards)
	testPlacement := placement.NewPlacement().
		SetInstances([]placement.Instance{instance}).
		SetShards(shards.AllIDs())
	stagedPlacement, err := placement.NewPlacementsFromLatest(testPlacement)
	if err != nil {
		return nil, err
	}
	return stagedPlacement.Proto()
}

func defaultNamespaces() *rulepb.Namespaces {
	return &rulepb.Namespaces{
		Namespaces: []*rulepb.Namespace{
			{
				Name: defaultNamespace,
				Snapshots: []*rulepb.NamespaceSnapshot{
					{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
				},
			},
		},
	}
}

func defaultMappingRulesConfig() []*rulepb.MappingRule {
	return []*rulepb.MappingRule{
		{
			Uuid: "mappingRule1",
			Snapshots: []*rulepb.MappingRuleSnapshot{
				{
					Name:         "mappingRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 1000,
					Filter:       "mtagName1:mtagValue1",
					StoragePolicies: []*policypb.StoragePolicy{
						{
							Resolution: policypb.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: policypb.Retention{
								Period: int64(24 * time.Hour),
							},
						},
					},
				},
			},
		},
	}
}

func defaultRollupRulesConfig() []*rulepb.RollupRule {
	return []*rulepb.RollupRule{
		{
			Uuid: "rollupRule1",
			Snapshots: []*rulepb.RollupRuleSnapshot{
				{
					Name:         "rollupRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 500,
					Filter:       "rtagName1:rtagValue1",
					KeepOriginal: true,
					TargetsV2: []*rulepb.RollupTargetV2{
						{
							Pipeline: &pipelinepb.Pipeline{
								Ops: []pipelinepb.PipelineOp{
									{
										Type: pipelinepb.PipelineOp_ROLLUP,
										Rollup: &pipelinepb.RollupOp{
											NewName: "newRollupName1",
											Tags:    []string{"namespace", "rtagName1"},
										},
									},
								},
							},
							StoragePolicies: []*policypb.StoragePolicy{
								{
									Resolution: policypb.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: policypb.Retention{
										Period: int64(48 * time.Hour),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func defaultRuleSet() *rulepb.RuleSet {
	return &rulepb.RuleSet{
		Uuid:         "07592642-a105-40a5-a5c5-7c416ccb56c5",
		Namespace:    defaultNamespace,
		Tombstoned:   false,
		CutoverNanos: 1000,
		MappingRules: defaultMappingRulesConfig(),
		RollupRules:  defaultRollupRulesConfig(),
	}
}

func defaultSortedTagIteratorPool() id.SortedTagIteratorPool {
	poolOpts := pool.NewObjectPoolOptions()
	sortedTagIteratorPool := id.NewSortedTagIteratorPool(poolOpts)
	sortedTagIteratorPool.Init(func() id.SortedTagIterator {
		return m3.NewPooledSortedTagIterator(nil, sortedTagIteratorPool)
	})
	return sortedTagIteratorPool
}

func defaultMatcherOptions(
	store kv.Store,
	iterPool id.SortedTagIteratorPool,
) matcher.Options {
	sortedTagIteratorFn := func(tagPairs []byte) id.SortedTagIterator {
		it := iterPool.Get()
		it.Reset(tagPairs)
		return it
	}
	tagsFilterOpts := filters.TagsFilterOptions{
		NameTagKey:          []byte(defaultNameTag),
		NameAndTagsFn:       m3.NameAndTags,
		SortedTagIteratorFn: sortedTagIteratorFn,
	}
	ruleSetOpts := rules.NewOptions().
		SetTagsFilterOptions(tagsFilterOpts).
		SetNewRollupIDFn(m3.NewRollupID)
	return matcher.NewOptions().
		SetKVStore(store).
		SetNamespacesKey(defaultNamespacesKey).
		SetRuleSetKeyFn(func(namespace []byte) string {
			return fmt.Sprintf(defaultRuleSetKeyFmt, namespace)
		}).
		SetNamespaceTag([]byte(defaultNamespaceTag)).
		SetRuleSetOptions(ruleSetOpts)
}

func defaultShardFn(id []byte, numShards uint32) uint32 {
	return adler32.Checksum(id) % numShards
}

func defaultBytesPool() pool.BytesPool {
	return pool.NewBytesPool([]pool.Bucket{
		{Capacity: 128, Count: 1000},
		{Capacity: 256, Count: 1000},
		{Capacity: 4096, Count: 1000},
	}, pool.NewObjectPoolOptions())
}

func defaultAggregatorClientOptions(
	store kv.Store,
) aggclient.Options {
	watcherOpts := placement.NewWatcherOptions().
		SetStagedPlacementKey(defaultPlacementKey).
		SetStagedPlacementStore(store)
	bytesPool := defaultBytesPool()
	bytesPool.Init()
	encoderOpts := protobuf.NewUnaggregatedOptions().SetBytesPool(bytesPool)
	return aggclient.NewOptions().
		SetShardFn(defaultShardFn).
		SetWatcherOptions(watcherOpts).
		SetEncoderOptions(encoderOpts)
}
