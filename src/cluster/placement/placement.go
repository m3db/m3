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

	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/shard"
)

var (
	errNilPlacementProto          = errors.New("nil placement proto")
	errNilPlacementSnapshotsProto = errors.New("nil placement snapshots proto")
	errNilPlacementInstanceProto  = errors.New("nil placement instance proto")
	errDuplicatedShards           = errors.New("invalid placement, there are duplicated shards in one replica")
	errUnexpectedShards           = errors.New("invalid placement, there are unexpected shard ids on instance")
	errMirrorNotSharded           = errors.New("invalid placement, mirrored placement must be sharded")
)

type placement struct {
	instances        map[string]Instance
	instancesByShard map[uint32][]Instance
	rf               int
	shards           []uint32
	isSharded        bool
	isMirrored       bool
	cutoverNanos     int64
	version          int
}

// NewPlacement returns a ServicePlacement
func NewPlacement() Placement {
	return &placement{}
}

// NewPlacementFromProto creates a new placement from proto.
func NewPlacementFromProto(p *placementpb.Placement) (Placement, error) {
	if p == nil {
		return nil, errNilPlacementProto
	}
	shards := make([]uint32, p.NumShards)
	for i := uint32(0); i < p.NumShards; i++ {
		shards[i] = i
	}
	instances := make([]Instance, 0, len(p.Instances))
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

func (p *placement) InstancesForShard(shard uint32) []Instance {
	if len(p.instancesByShard) == 0 {
		return nil
	}
	return p.instancesByShard[shard]
}

func (p *placement) Instances() []Instance {
	result := make([]Instance, 0, p.NumInstances())
	for _, instance := range p.instances {
		result = append(result, instance)
	}
	sort.Sort(ByIDAscending(result))
	return result
}

func (p *placement) SetInstances(instances []Instance) Placement {
	instancesMap := make(map[string]Instance, len(instances))
	instancesByShard := make(map[uint32][]Instance)
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

func (p *placement) Instance(id string) (Instance, bool) {
	instance, ok := p.instances[id]
	return instance, ok
}

func (p *placement) ReplicaFactor() int {
	return p.rf
}

func (p *placement) SetReplicaFactor(rf int) Placement {
	p.rf = rf
	return p
}

func (p *placement) Shards() []uint32 {
	return p.shards
}

func (p *placement) SetShards(shards []uint32) Placement {
	p.shards = shards
	return p
}

func (p *placement) NumShards() int {
	return len(p.shards)
}

func (p *placement) IsSharded() bool {
	return p.isSharded
}

func (p *placement) SetIsSharded(v bool) Placement {
	p.isSharded = v
	return p
}

func (p *placement) IsMirrored() bool {
	return p.isMirrored
}

func (p *placement) SetIsMirrored(v bool) Placement {
	p.isMirrored = v
	return p
}

func (p *placement) CutoverNanos() int64 {
	return p.cutoverNanos
}

func (p *placement) SetCutoverNanos(cutoverNanos int64) Placement {
	p.cutoverNanos = cutoverNanos
	return p
}

func (p *placement) GetVersion() int {
	return p.version
}

func (p *placement) SetVersion(v int) Placement {
	p.version = v
	return p
}

func (p *placement) String() string {
	return fmt.Sprintf(
		"Placement[Instances=%s, NumShards=%d, ReplicaFactor=%d, IsSharded=%v, IsMirrored=%v]",
		p.Instances(), p.NumShards(), p.ReplicaFactor(), p.IsSharded(), p.IsMirrored(),
	)
}

func (p *placement) Proto() (*placementpb.Placement, error) {
	instances := make(map[string]*placementpb.Instance, p.NumInstances())
	for _, instance := range p.Instances() {
		pi, err := instance.Proto()
		if err != nil {
			return nil, err
		}
		instances[instance.ID()] = pi
	}

	return &placementpb.Placement{
		Instances:     instances,
		ReplicaFactor: uint32(p.ReplicaFactor()),
		NumShards:     uint32(p.NumShards()),
		IsSharded:     p.IsSharded(),
		CutoverTime:   p.CutoverNanos(),
		IsMirrored:    p.IsMirrored(),
	}, nil
}

// Placements represents a list of placements.
type Placements []Placement

// NewPlacementsFromProto creates a list of placements from proto.
func NewPlacementsFromProto(p *placementpb.PlacementSnapshots) (Placements, error) {
	if p == nil {
		return nil, errNilPlacementSnapshotsProto
	}

	placements := make([]Placement, 0, len(p.Snapshots))
	for _, snapshot := range p.Snapshots {
		placement, err := NewPlacementFromProto(snapshot)
		if err != nil {
			return nil, err
		}
		placements = append(placements, placement)
	}
	sort.Sort(placementsByCutoverAsc(placements))
	return placements, nil
}

// Proto converts a list of Placement to a proto.
func (placements Placements) Proto() (*placementpb.PlacementSnapshots, error) {
	snapshots := make([]*placementpb.Placement, 0, len(placements))
	for _, p := range placements {
		placementProto, err := p.Proto()
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, placementProto)
	}
	return &placementpb.PlacementSnapshots{
		Snapshots: snapshots,
	}, nil
}

// CloneShards returns a copy of shards.
func CloneShards(shards shard.Shards) shard.Shards {
	newShards := make([]shard.Shard, shards.NumShards())
	for i, s := range shards.All() {
		newShards[i] = shard.NewShard(s.ID()).
			SetState(s.State()).
			SetSourceID(s.SourceID()).
			SetCutoverNanos(s.CutoverNanos()).
			SetCutoffNanos(s.CutoffNanos())
	}

	return shard.NewShards(newShards)
}

// CloneInstance returns a copy of an instance.
func CloneInstance(instance Instance) Instance {
	return NewInstance().
		SetID(instance.ID()).
		SetRack(instance.Rack()).
		SetZone(instance.Zone()).
		SetWeight(instance.Weight()).
		SetEndpoint(instance.Endpoint()).
		SetHostname(instance.Hostname()).
		SetPort(instance.Port()).
		SetShardSetID(instance.ShardSetID()).
		SetShards(CloneShards(instance.Shards()))
}

// CloneInstances returns a set of cloned instances.
func CloneInstances(instances []Instance) []Instance {
	copied := make([]Instance, len(instances))
	for i, instance := range instances {
		copied[i] = CloneInstance(instance)
	}
	return copied
}

// ClonePlacement creates a copy of a given placment.
func ClonePlacement(p Placement) Placement {
	return NewPlacement().
		SetInstances(CloneInstances(p.Instances())).
		SetShards(p.Shards()).
		SetReplicaFactor(p.ReplicaFactor()).
		SetIsSharded(p.IsSharded()).
		SetIsMirrored(p.IsMirrored()).
		SetCutoverNanos(p.CutoverNanos())
}

// Validate validates a placement
func Validate(p Placement) error {
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

// NewInstance returns a new Instance
func NewInstance() Instance {
	return &instance{shards: shard.NewShards(nil)}
}

// NewEmptyInstance returns a Instance with some basic properties but no shards assigned
func NewEmptyInstance(id, rack, zone, endpoint string, weight uint32) Instance {
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
func NewInstanceFromProto(instance *placementpb.Instance) (Instance, error) {
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
		SetShardSetID(instance.ShardSetId).
		SetHostname(instance.Hostname).
		SetPort(instance.Port), nil
}

type instance struct {
	id         string
	rack       string
	zone       string
	weight     uint32
	endpoint   string
	hostname   string
	port       uint32
	shards     shard.Shards
	shardSetID uint32
}

func (i *instance) String() string {
	return fmt.Sprintf(
		"Instance[ID=%s, Rack=%s, Zone=%s, Weight=%d, Endpoint=%s, Hostname=%s, Port=%d, ShardSetID=%d, Shards=%s]",
		i.id, i.rack, i.zone, i.weight, i.endpoint, i.hostname, i.port, i.shardSetID, i.shards.String(),
	)
}

func (i *instance) ID() string {
	return i.id
}

func (i *instance) SetID(id string) Instance {
	i.id = id
	return i
}

func (i *instance) Rack() string {
	return i.rack
}

func (i *instance) SetRack(r string) Instance {
	i.rack = r
	return i
}

func (i *instance) Zone() string {
	return i.zone
}

func (i *instance) SetZone(z string) Instance {
	i.zone = z
	return i
}

func (i *instance) Weight() uint32 {
	return i.weight
}

func (i *instance) SetWeight(w uint32) Instance {
	i.weight = w
	return i
}

func (i *instance) Endpoint() string {
	return i.endpoint
}

func (i *instance) SetEndpoint(ip string) Instance {
	i.endpoint = ip
	return i
}

func (i *instance) Hostname() string {
	return i.hostname
}

func (i *instance) SetHostname(value string) Instance {
	i.hostname = value
	return i
}

func (i *instance) Port() uint32 {
	return i.port
}

func (i *instance) SetPort(value uint32) Instance {
	i.port = value
	return i
}

func (i *instance) ShardSetID() uint32 {
	return i.shardSetID
}

func (i *instance) SetShardSetID(value uint32) Instance {
	i.shardSetID = value
	return i
}

func (i *instance) Shards() shard.Shards {
	return i.shards
}

func (i *instance) SetShards(s shard.Shards) Instance {
	i.shards = s
	return i
}

func (i *instance) Proto() (*placementpb.Instance, error) {
	ss, err := i.Shards().Proto()
	if err != nil {
		return &placementpb.Instance{}, err
	}

	return &placementpb.Instance{
		Id:         i.ID(),
		Rack:       i.Rack(),
		Zone:       i.Zone(),
		Weight:     i.Weight(),
		Endpoint:   i.Endpoint(),
		Shards:     ss,
		ShardSetId: i.ShardSetID(),
		Hostname:   i.Hostname(),
		Port:       i.Port(),
	}, nil
}

// IsInstanceLeaving checks if all the shards on the instance is in Leaving state
func IsInstanceLeaving(instance Instance) bool {
	newInstance := true
	for _, s := range instance.Shards().All() {
		if s.State() != shard.Leaving {
			return false
		}
		newInstance = false

	}
	return !newInstance
}

// Instances is a slice of instances that can produce a debug string.
type Instances []Instance

func (instances Instances) String() string {
	if len(instances) == 0 {
		return "[]"
	}
	// 256 should be pretty sufficient for the string representation
	// of each instance.
	strs := make([]string, 0, len(instances)*256)
	strs = append(strs, "[\n")
	for _, elem := range instances {
		strs = append(strs, "\t"+elem.String()+",\n")
	}
	strs = append(strs, "]")
	return strings.Join(strs, "")
}

// ByIDAscending sorts Instance by ID ascending
type ByIDAscending []Instance

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
func RemoveInstanceFromList(list []Instance, instanceID string) []Instance {
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
func MarkShardAvailable(p Placement, instanceID string, shardID uint32) (Placement, error) {
	p = ClonePlacement(p)
	return markShardAvailable(p, instanceID, shardID)
}

func markShardAvailable(p Placement, instanceID string, shardID uint32) (Placement, error) {
	instance, exist := p.Instance(instanceID)
	if !exist {
		return nil, fmt.Errorf("instance %s does not exist in placement", instanceID)
	}

	shards := instance.Shards()
	s, exist := shards.Shard(shardID)
	if !exist {
		return nil, fmt.Errorf("shard %d does not exist in instance %s", shardID, instanceID)
	}

	if s.State() != shard.Initializing {
		return nil, fmt.Errorf("could not mark shard %d as available, it's not in Initializing state", s.ID())
	}

	sourceID := s.SourceID()
	shards.Add(shard.NewShard(shardID).SetState(shard.Available))

	// there could be no source for cases like initial placement
	if sourceID == "" {
		return p, nil
	}

	sourceInstance, exist := p.Instance(sourceID)
	if !exist {
		return nil, fmt.Errorf("source instance %s for shard %d does not exist in placement", sourceID, shardID)
	}

	sourceShards := sourceInstance.Shards()
	sourceShard, exist := sourceShards.Shard(shardID)
	if !exist {
		return nil, fmt.Errorf("shard %d does not exist in source instance %s", shardID, sourceID)
	}

	if sourceShard.State() != shard.Leaving {
		return nil, fmt.Errorf("shard %d is not leaving instance %s", shardID, sourceID)
	}

	sourceShards.Remove(shardID)
	if sourceShards.NumShards() == 0 {
		return ClonePlacement(p).
			SetInstances(RemoveInstanceFromList(p.Instances(), sourceInstance.ID())), nil
	}
	return p, nil
}

// MarkAllShardsAsAvailable marks all shard available
func MarkAllShardsAsAvailable(p Placement) (Placement, error) {
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
