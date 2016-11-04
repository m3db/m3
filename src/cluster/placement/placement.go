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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	errInvalidHostShards   = errors.New("invalid shards assigned to a host")
	errDuplicatedShards    = errors.New("invalid placement, there are duplicated shards in one replica")
	errUnexpectedShards    = errors.New("invalid placement, there are unexpected shard ids on host")
	errTotalShardsMismatch = errors.New("invalid placement, the total shards in the placement does not match expected number")
	errInvalidShardsCount  = errors.New("invalid placement, the count for a shard does not match replica factor")
)

// snapshot implements Snapshot
type snapshot struct {
	hostShards []HostShards
	rf         int
	shards     []uint32
}

// NewEmptyPlacementSnapshot returns an empty placement
func NewEmptyPlacementSnapshot(hosts []Host, ids []uint32) Snapshot {
	hostShards := make([]HostShards, len(hosts), len(hosts))
	for i, ph := range hosts {
		hostShards[i] = NewHostShards(ph)
	}

	return snapshot{hostShards: hostShards, shards: ids, rf: 0}
}

// NewPlacementSnapshot returns a placement snapshot
func NewPlacementSnapshot(hss []HostShards, shards []uint32, rf int) Snapshot {
	return snapshot{hostShards: hss, rf: rf, shards: shards}
}

func (ps snapshot) HostShards() []HostShards {
	result := make([]HostShards, ps.HostsLen())
	for i, hs := range ps.hostShards {
		result[i] = hs
	}
	return result
}

func (ps snapshot) HostsLen() int {
	return len(ps.hostShards)
}

func (ps snapshot) Replicas() int {
	return ps.rf
}

func (ps snapshot) ShardsLen() int {
	return len(ps.shards)
}

func (ps snapshot) Shards() []uint32 {
	return ps.shards
}

func (ps snapshot) HostShard(id string) HostShards {
	for _, phs := range ps.HostShards() {
		if phs.Host().ID() == id {
			return phs
		}
	}
	return nil
}

func (ps snapshot) Validate() error {
	shardCountMap := ConvertShardSliceToMap(ps.shards)
	if len(shardCountMap) != len(ps.shards) {
		return errDuplicatedShards
	}

	expectedTotal := len(ps.shards) * ps.rf
	actualTotal := 0
	for _, hs := range ps.hostShards {
		for _, id := range hs.Shards() {
			if count, exist := shardCountMap[id]; exist {
				shardCountMap[id] = count + 1
				continue
			}

			return errUnexpectedShards
		}
		actualTotal += hs.ShardsLen()
	}

	if expectedTotal != actualTotal {
		return errTotalShardsMismatch
	}

	for shard, c := range shardCountMap {
		if ps.rf != c {
			return fmt.Errorf("invalid shard count for shard %d: expected %d, actual %d", shard, ps.rf, c)
		}
	}
	return nil
}

// Copy copies a snapshot
func (ps snapshot) Copy() Snapshot {
	return snapshot{hostShards: copyHostShards(ps.HostShards()), rf: ps.Replicas(), shards: ps.Shards()}
}

func copyHostShards(hss []HostShards) []HostShards {
	copied := make([]HostShards, len(hss))
	for i, hs := range hss {
		copied[i] = newHostShards(hs.Host(), hs.Shards())
	}
	return copied
}

// NewPlacementFromJSON creates a Snapshot from JSON
func NewPlacementFromJSON(data []byte) (Snapshot, error) {
	var ps snapshot
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil, err
	}
	return ps, nil
}

func (ps snapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(ps.placementSnapshotToJSON())
}

func (ps *snapshot) UnmarshalJSON(data []byte) error {
	var m map[string]hostShardsJSON
	var err error
	if err = json.Unmarshal(data, &m); err != nil {
		return err
	}
	if *ps, err = convertJSONtoSnapshot(m); err != nil {
		return err
	}
	return nil
}

func (ps snapshot) placementSnapshotToJSON() map[string]hostShardsJSON {
	m := make(map[string]hostShardsJSON, ps.HostsLen())
	for _, hs := range ps.hostShards {
		m[hs.Host().ID()] = newHostShardsJSON(hs)
	}
	return m
}

func newHostShardsJSON(hs HostShards) hostShardsJSON {
	shards := hs.Shards()
	uintShards := sortableUInt32(shards)
	sort.Sort(uintShards)
	return hostShardsJSON{
		ID:     hs.Host().ID(),
		Rack:   hs.Host().Rack(),
		Zone:   hs.Host().Zone(),
		Weight: hs.Host().Weight(),
		Shards: shards,
	}
}

type sortableUInt32 []uint32

func (su sortableUInt32) Len() int {
	return len(su)
}

func (su sortableUInt32) Less(i, j int) bool {
	return int(su[i]) < int(su[j])
}

func (su sortableUInt32) Swap(i, j int) {
	su[i], su[j] = su[j], su[i]
}

func convertJSONtoSnapshot(m map[string]hostShardsJSON) (snapshot, error) {
	var err error
	hss := make([]HostShards, len(m))
	shardsReplicaMap := make(map[uint32]int)
	i := 0
	for _, hsj := range m {
		if hss[i], err = hostShardsFromJSON(hsj); err != nil {
			return snapshot{}, err
		}
		for _, shard := range hss[i].Shards() {
			shardsReplicaMap[shard] = shardsReplicaMap[shard] + 1
		}
		i++
	}
	shards := make([]uint32, 0, len(shardsReplicaMap))
	snapshotReplica := -1
	for shard, r := range shardsReplicaMap {
		shards = append(shards, shard)
		if snapshotReplica < 0 {
			snapshotReplica = r
			continue
		}
		if snapshotReplica != r {
			return snapshot{}, errInvalidShardsCount
		}
	}
	return snapshot{hostShards: hss, shards: shards, rf: snapshotReplica}, nil
}

type hostShardsJSON struct {
	ID     string   `json:"id"`
	Rack   string   `json:"rack"`
	Zone   string   `json:"zone"`
	Weight uint32   `json:"weight"`
	Shards []uint32 `json:"shards"`
}

func hostShardsFromJSON(hsj hostShardsJSON) (HostShards, error) {
	hs := NewHostShards(NewHost(hsj.ID, hsj.Rack, hsj.Zone, hsj.Weight))
	for _, shard := range hsj.Shards {
		hs.AddShard(shard)
	}
	if len(hsj.Shards) != hs.ShardsLen() {
		return nil, errInvalidHostShards
	}
	return hs, nil
}

// hostShards implements HostShards
type hostShards struct {
	host      Host
	shardsSet map[uint32]struct{}
}

// NewHostShards returns a HostShards with no shards assigned
func NewHostShards(host Host) HostShards {
	return newHostShards(host, nil)
}

// newHostShards returns a HostShards with shards
func newHostShards(host Host, shards []uint32) HostShards {
	m := make(map[uint32]struct{}, len(shards))
	for _, s := range shards {
		m[s] = struct{}{}
	}
	return &hostShards{host: host, shardsSet: m}
}

func (h hostShards) Host() Host {
	return h.host
}

func (h hostShards) Shards() []uint32 {
	s := make([]uint32, 0, len(h.shardsSet))
	for shard := range h.shardsSet {
		s = append(s, shard)
	}
	return s
}

func (h hostShards) AddShard(s uint32) {
	h.shardsSet[s] = struct{}{}
}

func (h hostShards) RemoveShard(shard uint32) {
	delete(h.shardsSet, shard)
}

func (h hostShards) ContainsShard(shard uint32) bool {
	if _, exist := h.shardsSet[shard]; exist {
		return true
	}
	return false
}

func (h hostShards) ShardsLen() int {
	return len(h.shardsSet)
}

// ConvertShardSliceToMap is an util function that converts a slice of shards to a map
func ConvertShardSliceToMap(ids []uint32) map[uint32]int {
	shardCounts := make(map[uint32]int)
	for _, id := range ids {
		shardCounts[id] = 0
	}
	return shardCounts
}

// NewHost returns a Host
func NewHost(id, rack, zone string, weight uint32) Host {
	return host{id: id, rack: rack, zone: zone, weight: weight}
}

type host struct {
	id     string
	rack   string
	zone   string
	weight uint32
}

func (h host) ID() string {
	return h.id
}

func (h host) Rack() string {
	return h.rack
}

func (h host) Zone() string {
	return h.zone
}

func (h host) Weight() uint32 {
	return h.weight
}

func (h host) String() string {
	return fmt.Sprintf("[id:%s, rack:%s, zone:%s, weight:%v]", h.id, h.rack, h.zone, h.weight)
}

// ByIDAscending sorts Hosts by ID
type ByIDAscending []Host

func (s ByIDAscending) Len() int {
	return len(s)
}

func (s ByIDAscending) Less(i, j int) bool {
	return strings.Compare(s[i].ID(), s[j].ID()) < 0
}

func (s ByIDAscending) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
