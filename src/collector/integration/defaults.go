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

	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	msgpackbackend "github.com/m3db/m3collector/backend/msgpack"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/id/m3"
	msgpackprotocol "github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/pool"
)

const (
	defaultNumShards     = 4096
	defaultInstanceID    = "localhost:6789"
	defaultPlacementKey  = "placement"
	defaultNamespace     = "integration"
	defaultNamespacesKey = "namespaces"
	defaultRuleSetKeyFmt = "rulesets/%s"
	defaultRuleSetKey    = "rulesets/integration"
	defaultNamespaceTag  = "namespace"
	defaultNameTag       = "name"
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
	stagedPlacement := placement.NewStagedPlacement().
		SetPlacements([]placement.Placement{testPlacement})
	return stagedPlacement.Proto()
}

func defaultNamespaces() *schema.Namespaces {
	return &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: defaultNamespace,
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
				},
			},
		},
	}
}

func defaultMappingRulesConfig() []*schema.MappingRule {
	return []*schema.MappingRule{
		&schema.MappingRule{
			Uuid: "mappingRule1",
			Snapshots: []*schema.MappingRuleSnapshot{
				&schema.MappingRuleSnapshot{
					Name:         "mappingRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 1000,
					Filter:       "mtagName1:mtagValue1",
					Policies: []*schema.Policy{
						&schema.Policy{
							StoragePolicy: &schema.StoragePolicy{
								Resolution: &schema.Resolution{
									WindowSize: int64(10 * time.Second),
									Precision:  int64(time.Second),
								},
								Retention: &schema.Retention{
									Period: int64(24 * time.Hour),
								},
							},
						},
					},
				},
			},
		},
	}
}

func defaultRollupRulesConfig() []*schema.RollupRule {
	return []*schema.RollupRule{
		&schema.RollupRule{
			Uuid: "rollupRule1",
			Snapshots: []*schema.RollupRuleSnapshot{
				&schema.RollupRuleSnapshot{
					Name:         "rollupRule1.snapshot1",
					Tombstoned:   false,
					CutoverNanos: 500,
					Filter:       "rtagName1:rtagValue1",
					Targets: []*schema.RollupTarget{
						&schema.RollupTarget{
							Name: "newRollupName1",
							Tags: []string{"namespace", "rtagName1"},
							Policies: []*schema.Policy{
								&schema.Policy{
									StoragePolicy: &schema.StoragePolicy{
										Resolution: &schema.Resolution{
											WindowSize: int64(time.Minute),
											Precision:  int64(time.Minute),
										},
										Retention: &schema.Retention{
											Period: int64(48 * time.Hour),
										},
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

func defaultRuleSet() *schema.RuleSet {
	return &schema.RuleSet{
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

func defaultShardFn(id []byte, numShards int) uint32 {
	return adler32.Checksum(id) % uint32(numShards)
}

func defaultBackendOptions(
	store kv.Store,
) msgpackbackend.ServerOptions {
	watcherOpts := placement.NewStagedPlacementWatcherOptions().
		SetStagedPlacementKey(defaultPlacementKey).
		SetStagedPlacementStore(store)
	poolOpts := pool.NewObjectPoolOptions()
	encoderPoolOpts := msgpackprotocol.NewBufferedEncoderPoolOptions().SetObjectPoolOptions(poolOpts)
	encoderPool := msgpackprotocol.NewBufferedEncoderPool(encoderPoolOpts)
	encoderPool.Init(func() msgpackprotocol.BufferedEncoder {
		return msgpackprotocol.NewPooledBufferedEncoder(encoderPool)
	})
	return msgpackbackend.NewServerOptions().
		SetStagedPlacementWatcherOptions(watcherOpts).
		SetShardFn(defaultShardFn).
		SetBufferedEncoderPool(encoderPool)
}
