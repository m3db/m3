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

package topology

import "github.com/m3db/m3db/interfaces/m3db"

type shardAssignment struct {
	id    uint32          // shard id
	state m3db.ShardState // shard assignment state
}

func NewShardAssignment(id uint32, state m3db.ShardState) m3db.ShardAssignment {
	return shardAssignment{
		id:    id,
		state: state,
	}
}

func (sa shardAssignment) ShardID() uint32             { return sa.id }
func (sa shardAssignment) ShardState() m3db.ShardState { return sa.state }

type shardAssignments struct {
	assignments map[string][]m3db.ShardAssignment
}

func NewShardAssignments() m3db.ShardAssignments {
	return &shardAssignments{
		assignments: make(map[string][]m3db.ShardAssignment),
	}
}

func (sa *shardAssignments) AddAssignmentsFor(host m3db.Host, assignments []m3db.ShardAssignment) {
	sa.assignments[host.ID()] = assignments
}

func (sa *shardAssignments) GetAssignmentsFor(host m3db.Host) []m3db.ShardAssignment {
	return sa.assignments[host.ID()]
}

// shardAssignmentUpdate contains shards assignment updates for a given host.
type shardAssignmentUpdate struct {
	host   m3db.Host
	shards []m3db.ShardAssignment
}

func NewShardAssignmentUpdate(host m3db.Host, shards []m3db.ShardAssignment) m3db.TopologyUpdate {
	return shardAssignmentUpdate{
		host:   host,
		shards: shards,
	}
}

func (u shardAssignmentUpdate) Type() m3db.TopologyUpdateType { return m3db.ShardAssignmentUpdate }
func (u shardAssignmentUpdate) Data() interface{}             { return u }
