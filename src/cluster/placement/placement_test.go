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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshot(t *testing.T) {
	h1 := NewEmptyHostShards("r1h1", "r1")
	h1.AddShard(1)
	h1.AddShard(2)
	h1.AddShard(3)

	h2 := NewEmptyHostShards("r2h2", "r2")
	h2.AddShard(4)
	h2.AddShard(5)
	h2.AddShard(6)

	h3 := NewEmptyHostShards("r3h3", "r3")
	h3.AddShard(1)
	h3.AddShard(3)
	h3.AddShard(5)

	h4 := NewEmptyHostShards("r4h4", "r4")
	h4.AddShard(2)
	h4.AddShard(4)
	h4.AddShard(6)

	h5 := NewEmptyHostShards("r5h5", "r5")
	h5.AddShard(5)
	h5.AddShard(6)
	h5.AddShard(1)

	h6 := NewEmptyHostShards("r6h6", "r6")
	h6.AddShard(2)
	h6.AddShard(3)
	h6.AddShard(4)

	hss := []HostShards{h1, h2, h3, h4, h5, h6}

	ids := []uint32{1, 2, 3, 4, 5, 6}
	s := NewPlacementSnapshot(hss, ids, 3)
	assert.NoError(t, s.Validate())
	testSnapshotJSONRoundTrip(t, s)

	hs := s.HostShard("r6h6")
	assert.Equal(t, h6, hs)
	hs = s.HostShard("h100")
	assert.Nil(t, hs)

	assert.Equal(t, 6, s.HostsLen())
	assert.Equal(t, 3, s.Replicas())
	assert.Equal(t, ids, s.Shards())
	assert.Equal(t, hss, s.HostShards())

	s = NewEmptyPlacementSnapshot([]Host{NewHost("h1", "r1"), NewHost("h2", "r2")}, ids)
	assert.Equal(t, 0, s.Replicas())
	assert.Equal(t, ids, s.Shards())
	assert.NoError(t, s.Validate())
}

func TestValidate(t *testing.T) {
	ids := []uint32{1, 2, 3, 4, 5, 6}

	h1 := NewEmptyHostShards("r1h1", "r1")
	h1.AddShard(1)
	h1.AddShard(2)
	h1.AddShard(3)

	h2 := NewEmptyHostShards("r2h2", "r2")
	h2.AddShard(4)
	h2.AddShard(5)
	h2.AddShard(6)

	hss := []HostShards{h1, h2}
	s := NewPlacementSnapshot(hss, ids, 1)
	assert.NoError(t, s.Validate())

	// mismatch shards
	s = NewPlacementSnapshot(hss, append(ids, 7), 1)
	assert.Error(t, s.Validate())

	// host missing a shard
	h1 = NewEmptyHostShards("r1h1", "r1")
	h1.AddShard(1)
	h1.AddShard(2)
	h1.AddShard(3)
	h1.AddShard(4)
	h1.AddShard(5)
	h1.AddShard(6)

	h2 = NewEmptyHostShards("r2h2", "r2")
	h2.AddShard(2)
	h2.AddShard(3)
	h2.AddShard(4)
	h2.AddShard(5)
	h2.AddShard(6)

	hss = []HostShards{h1, h2}
	s = NewPlacementSnapshot(hss, ids, 2)
	assert.Error(t, s.Validate())

	// host contains shard that's not supposed to be in snapshot
	h1 = NewEmptyHostShards("r1h1", "r1")
	h1.AddShard(1)
	h1.AddShard(2)
	h1.AddShard(3)
	h1.AddShard(4)
	h1.AddShard(5)
	h1.AddShard(6)
	h1.AddShard(7)

	h2 = NewEmptyHostShards("r2h2", "r2")
	h2.AddShard(2)
	h2.AddShard(3)
	h2.AddShard(4)
	h2.AddShard(5)
	h2.AddShard(6)

	hss = []HostShards{h1, h2}
	s = NewPlacementSnapshot(hss, ids, 2)
	assert.Error(t, s.Validate())

	// duplicated shards
	h1 = NewEmptyHostShards("r1h1", "r1")
	h1.AddShard(2)
	h1.AddShard(3)
	h1.AddShard(4)

	h2 = NewEmptyHostShards("r2h2", "r2")
	h2.AddShard(4)
	h2.AddShard(5)
	h2.AddShard(6)

	hss = []HostShards{h1, h2}
	s = NewPlacementSnapshot(hss, []uint32{2, 3, 4, 4, 5, 6}, 1)
	assert.Error(t, s.Validate())
}

func TestSnapshotMarshalling(t *testing.T) {
	invalidJSON := `[
		{"ID":123,"Rack":"r1","Shards":[0,7,11]}
	]`
	data := []byte(invalidJSON)
	ps, err := NewPlacementFromJSON(data)
	assert.Nil(t, ps)
	assert.Error(t, err)

	validJSON := `[
		{"ID":"r2h4","Rack":"r2","Shards":[6,13,15]},
		{"ID":"r3h5","Rack":"r3","Shards":[2,8,19]},
		{"ID":"r4h6","Rack":"r4","Shards":[3,9,18]},
		{"ID":"r1h1","Rack":"r1","Shards":[0,7,11]},
		{"ID":"r2h3","Rack":"r2","Shards":[1,4,12]},
		{"ID":"r5h7","Rack":"r5","Shards":[10,14]},
		{"ID":"r6h9","Rack":"r6","Shards":[5,16,17]}
	]`
	data = []byte(validJSON)
	ps, err = NewPlacementFromJSON(data)
	assert.NoError(t, err)
	assert.Equal(t, 7, ps.HostsLen())
	assert.Equal(t, 1, ps.Replicas())
	assert.Equal(t, 20, ps.ShardsLen())

	testSnapshotJSONRoundTrip(t, ps)
	// an extra replica for shard 1
	invalidPlacementJSON := `[
		{"ID":"r1h1","Rack":"r1","Shards":[0,1,7,11]},
		{"ID":"r2h3","Rack":"r2","Shards":[1,4,12]},
		{"ID":"r2h4","Rack":"r2","Shards":[6,13,15]},
		{"ID":"r3h5","Rack":"r3","Shards":[2,8,19]},
		{"ID":"r4h6","Rack":"r4","Shards":[3,9,18]},
		{"ID":"r5h7","Rack":"r5","Shards":[10,14]},
		{"ID":"r6h9","Rack":"r6","Shards":[5,16,17]}
	]`
	data = []byte(invalidPlacementJSON)
	ps, err = NewPlacementFromJSON(data)
	assert.Equal(t, err, errShardsWithDifferentReplicas)
	assert.Nil(t, ps)

	// an extra replica for shard 0 on r1h1
	invalidPlacementJSON = `[
		{"ID":"r1h1","Rack":"r1","Shards":[0,0,7,11]},
		{"ID":"r2h3","Rack":"r2","Shards":[1,4,12]},
		{"ID":"r2h4","Rack":"r2","Shards":[6,13,15]},
		{"ID":"r3h5","Rack":"r3","Shards":[2,8,19]},
		{"ID":"r4h6","Rack":"r4","Shards":[3,9,18]},
		{"ID":"r5h7","Rack":"r5","Shards":[10,14]},
		{"ID":"r6h9","Rack":"r6","Shards":[5,16,17]}
	]`
	data = []byte(invalidPlacementJSON)
	ps, err = NewPlacementFromJSON(data)
	assert.Equal(t, err, errInvalidHostShards)
	assert.Nil(t, ps)
}

func TestHostShards(t *testing.T) {
	h1 := NewEmptyHostShards("r1h1", "r1")
	h1.AddShard(1)
	h1.AddShard(2)
	h1.AddShard(3)

	assert.True(t, h1.ContainsShard(1))
	assert.False(t, h1.ContainsShard(100))
	assert.Equal(t, 3, h1.ShardsLen())
	assert.Equal(t, "r1h1", h1.Host().ID())
	assert.Equal(t, "r1", h1.Host().Rack())

	h1.RemoveShard(1)
	assert.False(t, h1.ContainsShard(1))
	assert.False(t, h1.ContainsShard(100))
	assert.Equal(t, 2, h1.ShardsLen())
	assert.Equal(t, "r1h1", h1.Host().ID())
	assert.Equal(t, "r1", h1.Host().Rack())
}

func testSnapshotJSONRoundTrip(t *testing.T, s Snapshot) {
	json1, err := json.Marshal(s)
	assert.NoError(t, err)

	var snapShotFromJSON snapshot
	err = json.Unmarshal(json1, &snapShotFromJSON)
	assert.NoError(t, err)

	json2, err := json.Marshal(s)
	assert.NoError(t, err)
	assert.Equal(t, string(json1), string(json2))
}
