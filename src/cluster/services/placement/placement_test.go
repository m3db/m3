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
	"sort"
	"testing"

	"github.com/m3db/m3cluster/services"
	"github.com/stretchr/testify/assert"
)

func TestPlacement(t *testing.T) {
	h1 := NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().AddShard(1)
	h1.Shards().AddShard(2)
	h1.Shards().AddShard(3)

	h2 := NewEmptyInstance("r2h2", "r2", "z1", 1)
	h2.Shards().AddShard(4)
	h2.Shards().AddShard(5)
	h2.Shards().AddShard(6)

	h3 := NewEmptyInstance("r3h3", "r3", "z1", 1)
	h3.Shards().AddShard(1)
	h3.Shards().AddShard(3)
	h3.Shards().AddShard(5)

	h4 := NewEmptyInstance("r4h4", "r4", "z1", 1)
	h4.Shards().AddShard(2)
	h4.Shards().AddShard(4)
	h4.Shards().AddShard(6)

	h5 := NewEmptyInstance("r5h5", "r5", "z1", 1)
	h5.Shards().AddShard(5)
	h5.Shards().AddShard(6)
	h5.Shards().AddShard(1)

	h6 := NewEmptyInstance("r6h6", "r6", "z1", 1)
	h6.Shards().AddShard(2)
	h6.Shards().AddShard(3)
	h6.Shards().AddShard(4)

	instances := []services.PlacementInstance{h1, h2, h3, h4, h5, h6}

	ids := []uint32{1, 2, 3, 4, 5, 6}
	p := NewPlacement(instances, ids, 3)
	assert.NoError(t, Validate(p))

	i := p.Instance("r6h6")
	assert.Equal(t, h6, i)
	i = p.Instance("h100")
	assert.Nil(t, i)

	assert.Equal(t, 6, p.NumInstances())
	assert.Equal(t, 3, p.ReplicaFactor())
	assert.Equal(t, ids, p.Shards())
	assert.Equal(t, 6, p.NumShards())
	assert.Equal(t, instances, p.Instances())

	p = NewPlacement([]services.PlacementInstance{NewEmptyInstance("h1", "r1", "z1", 1), NewEmptyInstance("h2", "r2", "z1", 1)}, ids, 0)
	assert.Equal(t, 0, p.ReplicaFactor())
	assert.Equal(t, ids, p.Shards())
	assert.NoError(t, Validate(p))
}

func TestValidate(t *testing.T) {
	ids := []uint32{1, 2, 3, 4, 5, 6}

	h1 := NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().AddShard(1)
	h1.Shards().AddShard(2)
	h1.Shards().AddShard(3)

	h2 := NewEmptyInstance("r2h2", "r2", "z1", 1)
	h2.Shards().AddShard(4)
	h2.Shards().AddShard(5)
	h2.Shards().AddShard(6)

	instances := []services.PlacementInstance{h1, h2}
	p := NewPlacement(instances, ids, 1)
	assert.NoError(t, Validate(p))

	// mismatch shards
	p = NewPlacement(instances, append(ids, 7), 1)
	assert.Error(t, Validate(p))
	assert.Error(t, Validate(p))

	// missing a shard
	h1 = NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().AddShard(1)
	h1.Shards().AddShard(2)
	h1.Shards().AddShard(3)
	h1.Shards().AddShard(4)
	h1.Shards().AddShard(5)
	h1.Shards().AddShard(6)

	h2 = NewEmptyInstance("r2h2", "r2", "z1", 1)
	h2.Shards().AddShard(2)
	h2.Shards().AddShard(3)
	h2.Shards().AddShard(4)
	h2.Shards().AddShard(5)
	h2.Shards().AddShard(6)

	instances = []services.PlacementInstance{h1, h2}
	p = NewPlacement(instances, ids, 2)
	assert.Error(t, Validate(p))
	assert.Equal(t, errTotalShardsMismatch, Validate(p))

	// contains shard that's unexpected to be in placement
	h1 = NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().AddShard(1)
	h1.Shards().AddShard(2)
	h1.Shards().AddShard(3)
	h1.Shards().AddShard(4)
	h1.Shards().AddShard(5)
	h1.Shards().AddShard(6)
	h1.Shards().AddShard(7)

	h2 = NewEmptyInstance("r2h2", "r2", "z1", 1)
	h2.Shards().AddShard(2)
	h2.Shards().AddShard(3)
	h2.Shards().AddShard(4)
	h2.Shards().AddShard(5)
	h2.Shards().AddShard(6)

	instances = []services.PlacementInstance{h1, h2}
	p = NewPlacement(instances, ids, 2)
	assert.Error(t, Validate(p))
	assert.Equal(t, errUnexpectedShards, Validate(p))

	// duplicated shards
	h1 = NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().AddShard(2)
	h1.Shards().AddShard(3)
	h1.Shards().AddShard(4)

	h2 = NewEmptyInstance("r2h2", "r2", "z1", 1)
	h2.Shards().AddShard(4)
	h2.Shards().AddShard(5)
	h2.Shards().AddShard(6)

	instances = []services.PlacementInstance{h1, h2}
	p = NewPlacement(instances, []uint32{2, 3, 4, 4, 5, 6}, 1)
	assert.Error(t, Validate(p))
	assert.Equal(t, errDuplicatedShards, Validate(p))

	// three shard 2 and only one shard 4
	h1 = NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().AddShard(1)
	h1.Shards().AddShard(2)
	h1.Shards().AddShard(3)

	h2 = NewEmptyInstance("r2h2", "r2", "z1", 1)
	h2.Shards().AddShard(2)
	h2.Shards().AddShard(3)
	h2.Shards().AddShard(4)

	h3 := NewEmptyInstance("r3h3", "r3", "z1", 1)
	h3.Shards().AddShard(1)
	h3.Shards().AddShard(2)

	instances = []services.PlacementInstance{h1, h2, h3}
	p = NewPlacement(instances, []uint32{1, 2, 3, 4}, 2)
	assert.Error(t, Validate(p))
}

func TestInstance(t *testing.T) {
	h1 := NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().AddShard(1)
	h1.Shards().AddShard(2)
	h1.Shards().AddShard(3)

	assert.Equal(t, "[id:r1h1, rack:r1, zone:z1, weight:1]", h1.String())

	assert.True(t, h1.Shards().ContainsShard(1))
	assert.False(t, h1.Shards().ContainsShard(100))
	assert.Equal(t, 3, h1.Shards().NumShards())
	assert.Equal(t, "r1h1", h1.ID())
	assert.Equal(t, "r1", h1.Rack())

	h1.Shards().RemoveShard(1)
	assert.False(t, h1.Shards().ContainsShard(1))
	assert.False(t, h1.Shards().ContainsShard(100))
	assert.Equal(t, 2, h1.Shards().NumShards())
	assert.Equal(t, "r1h1", h1.ID())
	assert.Equal(t, "r1", h1.Rack())
}

func TestSortInstanceByID(t *testing.T) {
	h1 := NewEmptyInstance("h1", "", "", 1)
	h2 := NewEmptyInstance("h2", "", "", 1)
	h3 := NewEmptyInstance("h3", "", "", 1)
	h4 := NewEmptyInstance("h4", "", "", 1)
	h5 := NewEmptyInstance("h5", "", "", 1)
	h6 := NewEmptyInstance("h6", "", "", 1)

	i := []services.PlacementInstance{h1, h6, h4, h2, h3, h5}
	sort.Sort(ByIDAscending(i))

	assert.Equal(t, []services.PlacementInstance{h1, h2, h3, h4, h5, h6}, i)
}

func TestOptions(t *testing.T) {
	o := NewOptions()
	assert.False(t, o.LooseRackCheck())
	assert.False(t, o.AllowPartialReplace())
	o = o.SetLooseRackCheck(true)
	assert.True(t, o.LooseRackCheck())
	o = o.SetAllowPartialReplace(true)
	assert.True(t, o.AllowPartialReplace())

	dopts := NewDeploymentOptions()
	assert.Equal(t, defaultMaxStepSize, dopts.MaxStepSize())
	dopts = dopts.SetMaxStepSize(5)
	assert.Equal(t, 5, dopts.MaxStepSize())
}
