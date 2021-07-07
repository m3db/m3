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

package algo

import (
	"testing"

	"github.com/m3db/m3/src/cluster/placement"

	"github.com/stretchr/testify/assert"
)

func TestNonShardedAlgo(t *testing.T) {
	a := newNonShardedAlgorithm()

	i1 := placement.NewInstance().SetID("i1").SetEndpoint("e1")
	i2 := placement.NewInstance().SetID("i2").SetEndpoint("e2")
	i3 := placement.NewInstance().SetID("i3").SetEndpoint("e3")
	i4 := placement.NewInstance().SetID("i4").SetEndpoint("e4")
	_, err := a.InitialPlacement([]placement.Instance{i1, i2}, []uint32{1, 2}, 1)
	assert.Error(t, err)

	p, err := a.InitialPlacement([]placement.Instance{i1, i2}, []uint32{}, 1)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, 2, p.NumInstances())
	for _, instance := range p.Instances() {
		assert.Equal(t, 0, instance.Shards().NumShards())
	}
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 1, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, 2, p.NumInstances())
	for _, instance := range p.Instances() {
		assert.Equal(t, 0, instance.Shards().NumShards())
	}
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	p, err = a.AddInstances(p, []placement.Instance{i3})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, 3, p.NumInstances())
	for _, instance := range p.Instances() {
		assert.Equal(t, 0, instance.Shards().NumShards())
	}
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	_, err = a.AddInstances(p, []placement.Instance{i3})
	assert.Error(t, err)

	p, err = a.RemoveInstances(p, []string{"i1"})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, 2, p.NumInstances())
	for _, instance := range p.Instances() {
		assert.Equal(t, 0, instance.Shards().NumShards())
	}
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	_, err = a.RemoveInstances(p, []string{"i1"})
	assert.Error(t, err)

	p, err = a.ReplaceInstances(p, []string{"i2"}, []placement.Instance{i1, i4})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, 3, p.NumInstances())
	for _, instance := range p.Instances() {
		assert.Equal(t, 0, instance.Shards().NumShards())
	}
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	balancedPlacement, err := a.BalanceShards(p)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(balancedPlacement))
}

func TestIncompatibleWithNonShardedAlgo(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetEndpoint("e1").SetWeight(1)
	i2 := placement.NewInstance().SetID("i2").SetEndpoint("e2").SetWeight(1)
	i3 := placement.NewInstance().SetID("i3").SetEndpoint("e3").SetWeight(1)
	i4 := placement.NewInstance().SetID("i4").SetEndpoint("e4").SetWeight(1)
	p, err := newShardedAlgorithm(placement.NewOptions()).InitialPlacement([]placement.Instance{i1, i2}, []uint32{1, 2}, 1)
	assert.NoError(t, err)

	a := newNonShardedAlgorithm()

	_, err = a.AddReplica(p)
	assert.Error(t, err)
	assert.Equal(t, errInCompatibleWithNonShardedAlgo, err)

	_, err = a.AddInstances(p, []placement.Instance{i3})
	assert.Error(t, err)
	assert.Equal(t, errInCompatibleWithNonShardedAlgo, err)

	_, err = a.RemoveInstances(p, []string{"i1"})
	assert.Error(t, err)
	assert.Equal(t, errInCompatibleWithNonShardedAlgo, err)

	_, err = a.ReplaceInstances(p, []string{"i1"}, []placement.Instance{i3, i4})
	assert.Error(t, err)
	assert.Equal(t, errInCompatibleWithNonShardedAlgo, err)

	_, err = a.BalanceShards(p)
	assert.Error(t, err)
	assert.Equal(t, errInCompatibleWithNonShardedAlgo, err)
}
