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
	"fmt"
	"sort"
	"strings"
)

// State represents the state of a shard
type State int

const (
	// Initializing represents a shard newly assigned to an instance
	Initializing State = iota
	// Available represents a shard bootstraped and ready to serve
	Available
	// Leaving represents a shard that is intending to be removed
	Leaving
)

// States returns all the possible states
func States() []State {
	return []State{
		Initializing,
		Available,
		Leaving,
	}
}

// String returns the string representation of the state
func (s State) String() string {
	switch s {
	case Initializing:
		return "Initializing"
	case Available:
		return "Available"
	case Leaving:
		return "Leaving"
	}
	return "Unknown"
}

// A Shard represents a piece of data owned by the service
type Shard interface {
	// ID returns the ID of the shard
	ID() uint32

	// State returns the state of the shard
	State() State

	// SetState sets the state of the shard
	SetState(s State) Shard

	// Source returns the source of the shard
	SourceID() string

	// SetSource sets the source of the shard
	SetSourceID(sourceID string) Shard
}

// NewShard returns a new Shard
func NewShard(id uint32) Shard { return &shard{id: id, state: Initializing} }

type shard struct {
	id       uint32
	state    State
	sourceID string
}

func (s *shard) ID() uint32                        { return s.id }
func (s *shard) State() State                      { return s.state }
func (s *shard) SetState(state State) Shard        { s.state = state; return s }
func (s *shard) SourceID() string                  { return s.sourceID }
func (s *shard) SetSourceID(sourceID string) Shard { s.sourceID = sourceID; return s }

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

// Shards is a collection of shards owned by one ServiceInstance
type Shards interface {
	// All returns the shards sorted ascending
	All() []Shard

	// AllIDs returns the shard IDs sorted ascending
	AllIDs() []uint32

	// NumShards returns the number of the shards
	NumShards() int

	// ShardsForState returns the shards in a certain state
	ShardsForState(state State) []Shard

	// NumShardsForState returns the number of shards in a certain state
	NumShardsForState(state State) int

	// Add adds a shard
	Add(shard Shard)

	// Remove removes a shard
	Remove(shard uint32)

	// Contains checks if a shard exists
	Contains(shard uint32) bool

	// Shard returns the shard for the id
	Shard(id uint32) (Shard, bool)

	// String returns the string representation of the shards
	String() string
}

// NewShards creates a new instance of Shards
func NewShards(ss []Shard) Shards {
	shardMap := make(map[uint32]Shard, len(ss))
	for _, s := range ss {
		shardMap[s.ID()] = s
	}
	return shards{shardsMap: shardMap}
}

type shards struct {
	shardsMap map[uint32]Shard
}

func (s shards) All() []Shard {
	ss := make([]Shard, 0, len(s.shardsMap))
	for _, shard := range s.shardsMap {
		ss = append(ss, shard)
	}
	sort.Sort(SortableShardsByIDAsc(ss))
	return ss
}

func (s shards) AllIDs() []uint32 {
	ids := make([]uint32, 0, len(s.shardsMap))
	for _, shard := range s.shardsMap {
		ids = append(ids, shard.ID())
	}
	sort.Sort(SortableIDsAsc(ids))
	return ids
}

func (s shards) NumShards() int {
	return len(s.shardsMap)
}

func (s shards) Shard(id uint32) (Shard, bool) {
	shard, ok := s.shardsMap[id]
	return shard, ok
}

func (s shards) Add(shard Shard) {
	s.shardsMap[shard.ID()] = shard
}

func (s shards) Remove(shard uint32) {
	delete(s.shardsMap, shard)
}

func (s shards) Contains(shard uint32) bool {
	_, ok := s.shardsMap[shard]
	return ok
}

func (s shards) NumShardsForState(state State) int {
	count := 0
	for _, s := range s.shardsMap {
		if s.State() == state {
			count++
		}
	}
	return count
}

func (s shards) ShardsForState(state State) []Shard {
	var r []Shard
	for _, s := range s.shardsMap {
		if s.State() == state {
			r = append(r, s)
		}
	}
	return r
}

func (s shards) String() string {
	var strs []string
	for _, state := range States() {
		ids := NewShards(s.ShardsForState(state)).AllIDs()
		str := fmt.Sprintf("%s=%v", state.String(), ids)
		strs = append(strs, str)
	}
	return fmt.Sprintf("[%s]", strings.Join(strs, ", "))
}
