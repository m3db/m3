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

package placement

import (
	"errors"
	"fmt"
	"strings"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
)

var (
	errInvalidInstance     = errors.New("invalid shards assigned to an instance")
	errDuplicatedShards    = errors.New("invalid placement, there are duplicated shards in one replica")
	errUnexpectedShards    = errors.New("invalid placement, there are unexpected shard ids on instance")
	errTotalShardsMismatch = errors.New("invalid placement, the total shards in the placement does not match expected number")
	errInvalidShardsCount  = errors.New("invalid placement, the count for a shard does not match replica factor")
)

// NewPlacement returns a ServicePlacement
func NewPlacement(instances []services.PlacementInstance, shards []uint32, rf int) services.ServicePlacement {
	return placement{instances: instances, rf: rf, shards: shards}
}

type placement struct {
	instances []services.PlacementInstance
	rf        int
	shards    []uint32
}

func (p placement) Instances() []services.PlacementInstance {
	result := make([]services.PlacementInstance, p.NumInstances())
	for i, instance := range p.instances {
		result[i] = instance
	}
	return result
}

func (p placement) NumInstances() int {
	return len(p.instances)
}

func (p placement) Instance(id string) services.PlacementInstance {
	for _, instance := range p.Instances() {
		if instance.ID() == id {
			return instance
		}
	}
	return nil
}

func (p placement) ReplicaFactor() int {
	return p.rf
}

func (p placement) Shards() []uint32 {
	return p.shards
}

func (p placement) NumShards() int {
	return len(p.shards)
}

// Validate validates a placement
func Validate(p services.ServicePlacement) error {
	shardCountMap := ConvertShardSliceToMap(p.Shards())
	if len(shardCountMap) != len(p.Shards()) {
		return errDuplicatedShards
	}

	expectedTotal := len(p.Shards()) * p.ReplicaFactor()
	actualTotal := 0
	for _, instance := range p.Instances() {
		for _, id := range instance.Shards().ShardIDs() {
			if count, exist := shardCountMap[id]; exist {
				shardCountMap[id] = count + 1
				continue
			}

			return errUnexpectedShards
		}
		actualTotal += instance.Shards().NumShards()
	}

	if expectedTotal != actualTotal {
		return errTotalShardsMismatch
	}

	for shard, c := range shardCountMap {
		if p.ReplicaFactor() != c {
			return fmt.Errorf("invalid shard count for shard %d: expected %d, actual %d", shard, p.ReplicaFactor(), c)
		}
	}
	return nil
}

// NewInstance returns a new PlacementInstance
func NewInstance() services.PlacementInstance {
	return &instance{shards: shard.NewShards(nil)}
}

// NewEmptyInstance returns a PlacementInstance with some basic properties but no shards assigned
func NewEmptyInstance(id, rack, zone string, weight uint32) services.PlacementInstance {
	return &instance{
		id:     id,
		rack:   rack,
		zone:   zone,
		weight: weight,
		shards: shard.NewShards(nil),
	}
}

type instance struct {
	id       string
	rack     string
	zone     string
	weight   uint32
	endpoint string
	shards   shard.Shards
}

func (i *instance) String() string {
	return fmt.Sprintf("[id:%s, rack:%s, zone:%s, weight:%v]", i.id, i.rack, i.zone, i.weight)
}

func (i *instance) ID() string {
	return i.id
}

func (i *instance) SetID(id string) services.PlacementInstance {
	i.id = id
	return i
}

func (i *instance) Rack() string {
	return i.rack
}

func (i *instance) SetRack(r string) services.PlacementInstance {
	i.rack = r
	return i
}

func (i *instance) Zone() string {
	return i.zone
}

func (i *instance) SetZone(z string) services.PlacementInstance {
	i.zone = z
	return i
}

func (i *instance) Weight() uint32 {
	return i.weight
}

func (i *instance) SetWeight(w uint32) services.PlacementInstance {
	i.weight = w
	return i
}

func (i *instance) Endpoint() string {
	return i.endpoint
}

func (i *instance) SetEndpoint(ip string) services.PlacementInstance {
	i.endpoint = ip
	return i
}

func (i *instance) Shards() shard.Shards {
	return i.shards
}

func (i *instance) SetShards(s shard.Shards) services.PlacementInstance {
	i.shards = s
	return i
}

// ByIDAscending sorts PlacementInstance by ID ascending
type ByIDAscending []services.PlacementInstance

func (s ByIDAscending) Len() int {
	return len(s)
}

func (s ByIDAscending) Less(i, j int) bool {
	return strings.Compare(s[i].ID(), s[j].ID()) < 0
}

func (s ByIDAscending) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// ConvertShardSliceToMap is an util function that converts a slice of shards to a map
func ConvertShardSliceToMap(ids []uint32) map[uint32]int {
	shardCounts := make(map[uint32]int)
	for _, id := range ids {
		shardCounts[id] = 0
	}
	return shardCounts
}
