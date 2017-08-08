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
	"sort"
	"strings"

	placementproto "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
)

var (
	errNilPlacementProto         = errors.New("nil placement proto")
	errNilPlacementInstanceProto = errors.New("nil placement instance proto")
	errInvalidInstance           = errors.New("invalid shards assigned to an instance")
	errDuplicatedShards          = errors.New("invalid placement, there are duplicated shards in one replica")
	errUnexpectedShards          = errors.New("invalid placement, there are unexpected shard ids on instance")
	errInvalidShardsCount        = errors.New("invalid placement, the count for a shard does not match replica factor")
	errMirrorNotSharded          = errors.New("invalid placement, mirrored placement must be sharded")
)

type placement struct {
	instances        map[string]services.PlacementInstance
	instancesByShard map[uint32][]services.PlacementInstance
	rf               int
	shards           []uint32
	isSharded        bool
	isMirrored       bool
	cutoverNanos     int64
	version          int
}

// NewPlacement returns a ServicePlacement
func NewPlacement() services.Placement {
	return &placement{}
}

// NewPlacementFromProto creates a new placement from proto.
func NewPlacementFromProto(p *placementproto.Placement) (services.Placement, error) {
	if p == nil {
		return nil, errNilPlacementProto
	}
	shards := make([]uint32, p.NumShards)
	for i := uint32(0); i < p.NumShards; i++ {
		shards[i] = i
	}
	instances := make([]services.PlacementInstance, 0, len(p.Instances))
	for _, instance := range p.Instances {
		pi, err := NewInstanceFromProto(instance)
		if err != nil {
			return nil, err
		}
		instances = append(instances, pi)
	}

	return NewPlacement().
		SetInstances(instances).
		SetShards(shards).
		SetReplicaFactor(int(p.ReplicaFactor)).
		SetIsSharded(p.IsSharded).
		SetCutoverNanos(p.CutoverTime).
		SetIsMirrored(p.IsMirrored), nil
}

func (p *placement) InstancesForShard(shard uint32) []services.PlacementInstance {
	if len(p.instancesByShard) == 0 {
		return nil
	}
	return p.instancesByShard[shard]
}

func (p *placement) Instances() []services.PlacementInstance {
	result := make([]services.PlacementInstance, 0, p.NumInstances())
	for _, instance := range p.instances {
		result = append(result, instance)
	}
	sort.Sort(ByIDAscending(result))
	return result
}

func (p *placement) SetInstances(instances []services.PlacementInstance) services.Placement {
	instancesMap := make(map[string]services.PlacementInstance, len(instances))
	instancesByShard := make(map[uint32][]services.PlacementInstance)
	for _, instance := range instances {
		instancesMap[instance.ID()] = instance
		for _, shard := range instance.Shards().AllIDs() {
			instancesByShard[shard] = append(instancesByShard[shard], instance)
		}
	}

	// Sort the instances by their ids for deterministic ordering.
	for _, instances := range instancesByShard {
		sort.Sort(ByIDAscending(instances))
	}

	p.instancesByShard = instancesByShard
	p.instances = instancesMap
	return p
}

func (p *placement) NumInstances() int {
	return len(p.instances)
}

func (p *placement) Instance(id string) (services.PlacementInstance, bool) {
	instance, ok := p.instances[id]
	return instance, ok
}

func (p *placement) ReplicaFactor() int {
	return p.rf
}

func (p *placement) SetReplicaFactor(rf int) services.Placement {
	p.rf = rf
	return p
}

func (p *placement) Shards() []uint32 {
	return p.shards
}

func (p *placement) SetShards(shards []uint32) services.Placement {
	p.shards = shards
	return p
}

func (p *placement) NumShards() int {
	return len(p.shards)
}

func (p *placement) IsSharded() bool {
	return p.isSharded
}

func (p *placement) SetIsSharded(v bool) services.Placement {
	p.isSharded = v
	return p
}

func (p *placement) IsMirrored() bool {
	return p.isMirrored
}

func (p *placement) SetIsMirrored(v bool) services.Placement {
	p.isMirrored = v
	return p
}

func (p *placement) CutoverNanos() int64 {
	return p.cutoverNanos
}

func (p *placement) SetCutoverNanos(cutoverNanos int64) services.Placement {
	p.cutoverNanos = cutoverNanos
	return p
}

func (p *placement) GetVersion() int {
	return p.version
}

func (p *placement) SetVersion(v int) services.Placement {
	p.version = v
	return p
}

func (p *placement) String() string {
	return fmt.Sprintf(
		"Placement[Instances=%s, NumShards=%d, ReplicaFactor=%d, IsSharded=%v, IsMirrored=%v]",
		p.Instances(), p.NumShards(), p.ReplicaFactor(), p.IsSharded(), p.IsMirrored(),
	)
}

// CloneShards returns a copy of shards.
func CloneShards(shards shard.Shards) shard.Shards {
	newShards := make([]shard.Shard, shards.NumShards())
	for i, s := range shards.All() {
		newShards[i] = shard.NewShard(s.ID()).SetState(s.State()).SetSourceID(s.SourceID())
	}

	return shard.NewShards(newShards)
}

// CloneInstance returns a copy of an instance.
func CloneInstance(instance services.PlacementInstance) services.PlacementInstance {
	return NewInstance().
		SetID(instance.ID()).
		SetRack(instance.Rack()).
		SetZone(instance.Zone()).
		SetWeight(instance.Weight()).
		SetEndpoint(instance.Endpoint()).
		SetShardSetID(instance.ShardSetID()).
		SetShards(CloneShards(instance.Shards()))
}

// CloneInstances returns a set of cloned instances.
func CloneInstances(instances []services.PlacementInstance) []services.PlacementInstance {
	copied := make([]services.PlacementInstance, len(instances))
	for i, instance := range instances {
		copied[i] = CloneInstance(instance)
	}
	return copied
}

// ClonePlacement creates a copy of a given placment.
func ClonePlacement(p services.Placement) services.Placement {
	return NewPlacement().
		SetInstances(CloneInstances(p.Instances())).
		SetShards(p.Shards()).
		SetReplicaFactor(p.ReplicaFactor()).
		SetIsSharded(p.IsSharded()).
		SetIsMirrored(p.IsMirrored())
}

// Validate validates a placement
func Validate(p services.Placement) error {
	if p.IsMirrored() && !p.IsSharded() {
		return errMirrorNotSharded
	}

	shardCountMap := convertShardSliceToMap(p.Shards())
	if len(shardCountMap) != len(p.Shards()) {
		return errDuplicatedShards
	}

	expectedTotal := len(p.Shards()) * p.ReplicaFactor()
	totalCapacity := 0
	totalLeaving := 0
	totalInit := 0
	totalInitWithSourceID := 0
	for _, instance := range p.Instances() {
		if instance.Endpoint() == "" {
			return fmt.Errorf("instance %s does not contain valid endpoint", instance.String())
		}
		if instance.Shards().NumShards() == 0 && p.IsSharded() {
			return fmt.Errorf("instance %s contains no shard in a sharded placement", instance.String())
		}
		if instance.Shards().NumShards() != 0 && !p.IsSharded() {
			return fmt.Errorf("instance %s contains shards in a non-sharded placement", instance.String())
		}
		for _, s := range instance.Shards().All() {
			count, exist := shardCountMap[s.ID()]
			if !exist {
				return errUnexpectedShards
			}
			switch s.State() {
			case shard.Available:
				shardCountMap[s.ID()] = count + 1
				totalCapacity++
			case shard.Initializing:
				totalInit++
				shardCountMap[s.ID()] = count + 1
				totalCapacity++
				if s.SourceID() != "" {
					totalInitWithSourceID++
				}
			case shard.Leaving:
				totalLeaving++
			default:
				return fmt.Errorf("invalid shard state %v for shard %d", s.State(), s.ID())
			}
		}
	}

	if !p.IsSharded() {
		return nil
	}

	// initializing could be more than leaving for cases like initial placement
	if totalLeaving > totalInit {
		return fmt.Errorf("invalid placement, %d shards in Leaving state, more than %d in Initializing state", totalLeaving, totalInit)
	}

	if totalLeaving != totalInitWithSourceID {
		return fmt.Errorf("invalid placement, %d shards in Leaving state, not equal %d in Initializing state with source id", totalLeaving, totalInitWithSourceID)
	}

	if expectedTotal != totalCapacity {
		return fmt.Errorf("invalid placement, the total available shards in the placement is %d, expecting %d", totalCapacity, expectedTotal)
	}

	for shard, c := range shardCountMap {
		if p.ReplicaFactor() != c {
			return fmt.Errorf("invalid shard count for shard %d: expected %d, actual %d", shard, p.ReplicaFactor(), c)
		}
	}
	return nil
}

func convertShardSliceToMap(ids []uint32) map[uint32]int {
	shardCounts := make(map[uint32]int)
	for _, id := range ids {
		shardCounts[id] = 0
	}
	return shardCounts
}

// NewInstance returns a new PlacementInstance
func NewInstance() services.PlacementInstance {
	return &instance{shards: shard.NewShards(nil)}
}

// NewEmptyInstance returns a PlacementInstance with some basic properties but no shards assigned
func NewEmptyInstance(id, rack, zone, endpoint string, weight uint32) services.PlacementInstance {
	return &instance{
		id:       id,
		rack:     rack,
		zone:     zone,
		weight:   weight,
		endpoint: endpoint,
		shards:   shard.NewShards(nil),
	}
}

// NewInstanceFromProto creates a new placement instance from proto.
func NewInstanceFromProto(instance *placementproto.Instance) (services.PlacementInstance, error) {
	if instance == nil {
		return nil, errNilPlacementInstanceProto
	}
	shards, err := shard.NewShardsFromProto(instance.Shards)
	if err != nil {
		return nil, err
	}

	return NewInstance().
		SetID(instance.Id).
		SetRack(instance.Rack).
		SetWeight(instance.Weight).
		SetZone(instance.Zone).
		SetEndpoint(instance.Endpoint).
		SetShards(shards).
		SetShardSetID(instance.ShardSetId), nil
}

type instance struct {
	id         string
	rack       string
	zone       string
	weight     uint32
	endpoint   string
	shards     shard.Shards
	shardSetID string
}

func (i *instance) String() string {
	return fmt.Sprintf(
		"Instance[ID=%s, Rack=%s, Zone=%s, Weight=%d, Endpoint=%s, ShardSetID=%s, Shards=%s]",
		i.id, i.rack, i.zone, i.weight, i.endpoint, i.shardSetID, i.shards.String(),
	)
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

func (i *instance) ShardSetID() string {
	return i.shardSetID
}

func (i *instance) SetShardSetID(value string) services.PlacementInstance {
	i.shardSetID = value
	return i
}

// IsInstanceLeaving checks if all the shards on the instance is in Leaving state
func IsInstanceLeaving(instance services.PlacementInstance) bool {
	newInstance := true
	for _, s := range instance.Shards().All() {
		if s.State() != shard.Leaving {
			return false
		}
		newInstance = false

	}
	return !newInstance
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

// RemoveInstanceFromList removes a given instance from a list of instances
func RemoveInstanceFromList(list []services.PlacementInstance, instanceID string) []services.PlacementInstance {
	for i, instance := range list {
		if instance.ID() == instanceID {
			last := len(list) - 1
			list[i], list[last] = list[last], list[i]
			return list[:last]
		}
	}
	return list
}

// MarkShardAvailable marks a shard available on a clone of the placement
func MarkShardAvailable(p services.Placement, instanceID string, shardID uint32) (services.Placement, error) {
	p = ClonePlacement(p)
	return markShardAvailable(p, instanceID, shardID)
}

func markShardAvailable(p services.Placement, instanceID string, shardID uint32) (services.Placement, error) {
	instance, exist := p.Instance(instanceID)
	if !exist {
		return nil, fmt.Errorf("instance %s does not exist in placement", instanceID)
	}

	s, exist := instance.Shards().Shard(shardID)
	if !exist {
		return nil, fmt.Errorf("shard %d does not exist in instance %s", shardID, instanceID)
	}

	if s.State() != shard.Initializing {
		return nil, fmt.Errorf("could not mark shard %d as available, it's not in Initializing state", s.ID())
	}

	sourceID := s.SourceID()
	s.SetState(shard.Available).SetSourceID("")

	// there could be no source for cases like initial placement
	if sourceID == "" {
		return p, nil
	}

	sourceInstance, exist := p.Instance(sourceID)
	if !exist {
		return nil, fmt.Errorf("source instance %s for shard %d does not exist in placement", sourceID, shardID)
	}

	sourceShard, exist := sourceInstance.Shards().Shard(shardID)
	if !exist {
		return nil, fmt.Errorf("shard %d does not exist in source instance %s", shardID, sourceID)
	}

	if sourceShard.State() != shard.Leaving {
		return nil, fmt.Errorf("shard %d is not leaving instance %s", shardID, sourceID)
	}

	sourceInstance.Shards().Remove(shardID)
	if sourceInstance.Shards().NumShards() == 0 {
		return NewPlacement().
			SetInstances(RemoveInstanceFromList(p.Instances(), sourceInstance.ID())).
			SetShards(p.Shards()).
			SetReplicaFactor(p.ReplicaFactor()).
			SetIsSharded(p.IsSharded()), nil
	}
	return p, nil
}

// MarkAllShardsAsAvailable marks all shard available
func MarkAllShardsAsAvailable(p services.Placement) (services.Placement, error) {
	var err error
	p = ClonePlacement(p)
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			if s.State() == shard.Initializing {
				p, err = markShardAvailable(p, instance.ID(), s.ID())
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return p, nil
}
