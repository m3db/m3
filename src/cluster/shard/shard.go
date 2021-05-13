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

package shard

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
)

var (
	errInvalidProtoShardState = errors.New("invalid proto shard state")

	defaultShardState      State
	defaultShardStateProto placementpb.ShardState
)

// NewShardStateFromProto creates new shard state from proto.
func NewShardStateFromProto(state placementpb.ShardState) (State, error) {
	switch state {
	case placementpb.ShardState_INITIALIZING:
		return Initializing, nil
	case placementpb.ShardState_AVAILABLE:
		return Available, nil
	case placementpb.ShardState_LEAVING:
		return Leaving, nil
	default:
		return defaultShardState, errInvalidProtoShardState
	}
}

// Proto returns the proto representation for the shard state.
func (s State) Proto() (placementpb.ShardState, error) {
	switch s {
	case Initializing:
		return placementpb.ShardState_INITIALIZING, nil
	case Available:
		return placementpb.ShardState_AVAILABLE, nil
	case Leaving:
		return placementpb.ShardState_LEAVING, nil
	default:
		return defaultShardStateProto, errInvalidProtoShardState
	}
}

// NewShard returns a new Shard
func NewShard(id uint32) Shard { return &shard{id: id, state: Unknown} }

// NewShardFromProto create a new shard from proto.
func NewShardFromProto(spb *placementpb.Shard) (Shard, error) {
	state, err := NewShardStateFromProto(spb.State)
	if err != nil {
		return nil, err
	}

	var redirectToShardID *uint32
	if spb.RedirectToShardId != nil {
		redirectToShardID = new(uint32)
		*redirectToShardID = spb.RedirectToShardId.Value
	}

	return &shard{
		id:                spb.Id,
		redirectToShardID: redirectToShardID,
		state:             state,
		sourceID:          spb.SourceId,
		cutoverNanos:      spb.CutoverNanos,
		cutoffNanos:       spb.CutoffNanos,
	}, nil
}

type shard struct {
	id                uint32
	redirectToShardID *uint32

	state        State
	sourceID     string
	cutoverNanos int64
	cutoffNanos  int64
}

func (s *shard) ID() uint32                        { return s.id }
func (s *shard) State() State                      { return s.state }
func (s *shard) SetState(state State) Shard        { s.state = state; return s }
func (s *shard) SourceID() string                  { return s.sourceID }
func (s *shard) SetSourceID(sourceID string) Shard { s.sourceID = sourceID; return s }

func (s *shard) CutoverNanos() int64 {
	if s.cutoverNanos != UnInitializedValue {
		return s.cutoverNanos
	}

	// NB(xichen): if the value is not set, we return the default cutover nanos.
	return DefaultShardCutoverNanos
}

func (s *shard) SetCutoverNanos(value int64) Shard {
	// NB(cw): We use UnInitializedValue to represent the DefaultShardCutoverNanos
	// so that we can save some space in the proto representation for the
	// default value of cutover time.
	if value == DefaultShardCutoverNanos {
		value = UnInitializedValue
	}

	s.cutoverNanos = value
	return s
}

func (s *shard) CutoffNanos() int64 {
	if s.cutoffNanos != UnInitializedValue {
		return s.cutoffNanos
	}

	// NB(xichen): if the value is not set, we return the default cutoff nanos.
	return DefaultShardCutoffNanos
}

func (s *shard) SetCutoffNanos(value int64) Shard {
	// NB(cw): We use UnInitializedValue to represent the DefaultShardCutoffNanos
	// so that we can save some space in the proto representation for the
	// default value of cutoff time.
	if value == DefaultShardCutoffNanos {
		value = UnInitializedValue
	}

	s.cutoffNanos = value
	return s
}

func (s *shard) RedirectToShardID() *uint32 {
	return s.redirectToShardID
}

// SetRedirectToShardID sets optional shard to redirect incoming writes to.
func (s *shard) SetRedirectToShardID(id *uint32) Shard {
	s.redirectToShardID = nil
	if id != nil {
		s.redirectToShardID = new(uint32)
		*s.redirectToShardID = *id
	}
	return s
}

func (s *shard) Equals(other Shard) bool {
	return s.ID() == other.ID() &&
		s.State() == other.State() &&
		s.SourceID() == other.SourceID() &&
		s.CutoverNanos() == other.CutoverNanos() &&
		s.CutoffNanos() == other.CutoffNanos() &&
		((s.RedirectToShardID() == nil && other.RedirectToShardID() == nil) ||
			(s.RedirectToShardID() != nil && other.RedirectToShardID() != nil &&
				*s.RedirectToShardID() == *other.RedirectToShardID()))
}

func (s *shard) Proto() (*placementpb.Shard, error) {
	ss, err := s.state.Proto()
	if err != nil {
		return nil, err
	}

	var redirectToShardId *types.UInt32Value
	if s.redirectToShardID != nil {
		redirectToShardId = &types.UInt32Value{Value: *s.redirectToShardID}
	}

	return &placementpb.Shard{
		Id:                s.id,
		RedirectToShardId: redirectToShardId,
		State:             ss,
		SourceId:          s.sourceID,
		CutoverNanos:      s.cutoverNanos,
		CutoffNanos:       s.cutoffNanos,
	}, nil
}

func (s *shard) Clone() Shard {
	if s == nil {
		return nil
	}
	clone := *s
	return &clone
}

// SortableShardsByIDAsc are sortable shards by ID in ascending order
type SortableShardsByIDAsc []Shard

func (s SortableShardsByIDAsc) Len() int      { return len(s) }
func (s SortableShardsByIDAsc) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SortableShardsByIDAsc) Less(i, j int) bool {
	return s[i].ID() < s[j].ID()
}

// SortableIDsAsc are sortable shard IDs in ascending order
type SortableIDsAsc []uint32

func (s SortableIDsAsc) Len() int      { return len(s) }
func (s SortableIDsAsc) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SortableIDsAsc) Less(i, j int) bool {
	return s[i] < s[j]
}

// NewShards creates a new instance of Shards
func NewShards(ss []Shard) Shards {
	// deduplicate first, last one wins
	shardMap := make(map[uint32]Shard, len(ss))
	for _, s := range ss {
		shardMap[s.ID()] = s
	}

	shrds := make([]Shard, 0, len(shardMap))
	for _, s := range shardMap {
		shrds = append(shrds, s)
	}

	sort.Sort(SortableShardsByIDAsc(shrds))

	return &shards{
		shards:   shrds,
		shardMap: shardMap,
	}
}

// NewShardsFromProto creates a new set of shards from proto.
func NewShardsFromProto(shards []*placementpb.Shard) (Shards, error) {
	allShards := make([]Shard, 0, len(shards))
	for _, s := range shards {
		shard, err := NewShardFromProto(s)
		if err != nil {
			return nil, err
		}
		allShards = append(allShards, shard)
	}
	return NewShards(allShards), nil
}

type shards struct {
	shards   []Shard
	shardMap map[uint32]Shard
}

func (ss *shards) All() []Shard {
	shards := make([]Shard, len(ss.shards))
	copy(shards, ss.shards)

	return shards
}

func (ss *shards) AllIDs() []uint32 {
	shardIDs := make([]uint32, 0, len(ss.shards))
	for _, shrd := range ss.shards {
		shardIDs = append(shardIDs, shrd.ID())
	}

	return shardIDs
}

func (ss *shards) NumShards() int {
	return len(ss.shards)
}

func (ss *shards) Shard(id uint32) (Shard, bool) {
	shard, ok := ss.shardMap[id]
	if !ok {
		return nil, false
	}

	return shard, true
}

func (ss *shards) Add(shard Shard) {
	id := shard.ID()
	// we keep a sorted slice of shards, do a binary search to either find the index
	// of an existing shard for replacement, or the target index position
	i := sort.Search(len(ss.shards), func(i int) bool { return ss.shards[i].ID() >= id })
	if i < len(ss.shards) && ss.shards[i].ID() == id {
		ss.shards[i] = shard
		ss.shardMap[id] = shard
		return
	}

	// extend the sorted shard slice by 1
	ss.shards = append(ss.shards, shard)
	ss.shardMap[id] = shard

	// target position was at the end, so extending with the new shard was enough
	if i >= len(ss.shards)-1 {
		return
	}

	// if not, copy over all slice elements shifted by 1 and overwrite data at index
	copy(ss.shards[i+1:], ss.shards[i:])
	ss.shards[i] = shard
}

func (ss *shards) Remove(id uint32) {
	// we keep a sorted slice of shards, do a binary search to find the index
	i := sort.Search(len(ss.shards), func(i int) bool { return ss.shards[i].ID() >= id })
	if i < len(ss.shards) && ss.shards[i].ID() == id {
		delete(ss.shardMap, id)
		// shift all other elements back after removal
		ss.shards = ss.shards[:i+copy(ss.shards[i:], ss.shards[i+1:])]
	}
}

func (ss *shards) Contains(shard uint32) bool {
	_, ok := ss.shardMap[shard]
	return ok
}

func (ss *shards) NumShardsForState(state State) int {
	count := 0
	for _, s := range ss.shards {
		if s.State() == state {
			count++
		}
	}
	return count
}

func (ss *shards) ShardsForState(state State) []Shard {
	r := make([]Shard, 0, len(ss.shards))
	for _, s := range ss.shards {
		if s.State() == state {
			r = append(r, s)
		}
	}
	return r
}

func (ss *shards) Equals(other Shards) bool {
	if len(ss.shards) != other.NumShards() {
		return false
	}

	otherShards := other.All()
	for i, shard := range ss.shards {
		otherShard := otherShards[i]
		if !shard.Equals(otherShard) {
			return false
		}
	}
	return true
}

func (ss *shards) String() string {
	var strs []string
	for _, state := range validStates() {
		ids := NewShards(ss.ShardsForState(state)).AllIDs()
		str := fmt.Sprintf("%s=%v", state.String(), ids)
		strs = append(strs, str)
	}
	return fmt.Sprintf("[%s]", strings.Join(strs, ", "))
}

func (ss *shards) Proto() ([]*placementpb.Shard, error) {
	res := make([]*placementpb.Shard, 0, len(ss.shards))
	for _, shard := range ss.shards {
		sp, err := shard.Proto()
		if err != nil {
			return nil, err
		}
		res = append(res, sp)
	}

	return res, nil
}

func (ss *shards) Clone() Shards {
	shrds := make([]Shard, 0, len(ss.shards))
	shardMap := make(map[uint32]Shard, len(ss.shards))

	for _, shrd := range ss.shards {
		shrds = append(shrds, shrd.Clone())
		shardMap[shrd.ID()] = shrd
	}

	return &shards{
		shards:   shrds,
		shardMap: shardMap,
	}
}

// validStates returns all the valid states.
func validStates() []State {
	return []State{
		Initializing,
		Available,
		Leaving,
	}
}
