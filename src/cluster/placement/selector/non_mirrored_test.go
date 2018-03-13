// Copyright (c) 2017 Uber Technologies, Inc.
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

package selector

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/m3db/m3cluster/placement"

	"github.com/stretchr/testify/assert"
)

func TestGroupInstancesByConflict(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 1)
	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "", "", "endpoint", 1)
	i4 := placement.NewEmptyInstance("i4", "", "", "endpoint", 2)
	instanceConflicts := []sortableValue{
		sortableValue{value: i1, weight: 1},
		sortableValue{value: i2, weight: 0},
		sortableValue{value: i3, weight: 3},
		sortableValue{value: i4, weight: 2},
	}

	testCases := []struct {
		opts     placement.Options
		expected [][]placement.Instance
	}{
		{
			opts: placement.NewOptions().SetAllowPartialReplace(true),
			expected: [][]placement.Instance{
				[]placement.Instance{i2},
				[]placement.Instance{i1},
				[]placement.Instance{i4},
				[]placement.Instance{i3},
			},
		},
		{
			opts: placement.NewOptions().SetAllowPartialReplace(false),
			expected: [][]placement.Instance{
				[]placement.Instance{i2},
			},
		},
	}
	for _, test := range testCases {
		assert.Equal(t, test.expected, groupInstancesByConflict(instanceConflicts, test.opts))
	}
}

func TestKnapSack(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 40000)
	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 20000)
	i3 := placement.NewEmptyInstance("i3", "", "", "endpoint", 80000)
	i4 := placement.NewEmptyInstance("i4", "", "", "endpoint", 50000)
	i5 := placement.NewEmptyInstance("i5", "", "", "endpoint", 190000)
	instances := []placement.Instance{i1, i2, i3, i4, i5}

	res, leftWeight := knapsack(instances, 10000)
	assert.Equal(t, -10000, leftWeight)
	assert.Equal(t, []placement.Instance{i2}, res)

	res, leftWeight = knapsack(instances, 20000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []placement.Instance{i2}, res)

	res, leftWeight = knapsack(instances, 30000)
	assert.Equal(t, -10000, leftWeight)
	assert.Equal(t, []placement.Instance{i1}, res)

	res, leftWeight = knapsack(instances, 60000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []placement.Instance{i1, i2}, res)

	res, leftWeight = knapsack(instances, 120000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []placement.Instance{i1, i3}, res)

	res, leftWeight = knapsack(instances, 170000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []placement.Instance{i1, i3, i4}, res)

	res, leftWeight = knapsack(instances, 190000)
	assert.Equal(t, 0, leftWeight)
	// will prefer i5 than i1+i2+i3+i4
	assert.Equal(t, []placement.Instance{i5}, res)

	res, leftWeight = knapsack(instances, 200000)
	assert.Equal(t, -10000, leftWeight)
	assert.Equal(t, []placement.Instance{i2, i5}, res)

	res, leftWeight = knapsack(instances, 210000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []placement.Instance{i2, i5}, res)

	res, leftWeight = knapsack(instances, 400000)
	assert.Equal(t, 20000, leftWeight)
	assert.Equal(t, []placement.Instance{i1, i2, i3, i4, i5}, res)
}

func TestFillWeight(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 4)
	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 2)
	i3 := placement.NewEmptyInstance("i3", "", "", "endpoint", 8)
	i4 := placement.NewEmptyInstance("i4", "", "", "endpoint", 5)
	i5 := placement.NewEmptyInstance("i5", "", "", "endpoint", 19)

	i6 := placement.NewEmptyInstance("i6", "", "", "endpoint", 3)
	i7 := placement.NewEmptyInstance("i7", "", "", "endpoint", 7)
	groups := [][]placement.Instance{
		[]placement.Instance{i1, i2, i3, i4, i5},
		[]placement.Instance{i6, i7},
	}

	// When targetWeight is smaller than 38, the first group will satisfy
	res, leftWeight := fillWeight(groups, 1)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []placement.Instance{i2}, res)

	res, leftWeight = fillWeight(groups, 2)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []placement.Instance{i2}, res)

	res, leftWeight = fillWeight(groups, 17)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []placement.Instance{i1, i3, i4}, res)

	res, leftWeight = fillWeight(groups, 20)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []placement.Instance{i2, i5}, res)

	// When targetWeight is bigger than 38, need to get instance from group 2
	res, leftWeight = fillWeight(groups, 40)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []placement.Instance{i1, i2, i3, i4, i5, i6}, res)

	res, leftWeight = fillWeight(groups, 41)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []placement.Instance{i1, i2, i3, i4, i5, i6}, res)

	res, leftWeight = fillWeight(groups, 47)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []placement.Instance{i1, i2, i3, i4, i5, i6, i7}, res)

	res, leftWeight = fillWeight(groups, 48)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []placement.Instance{i1, i2, i3, i4, i5, i6, i7}, res)

	res, leftWeight = fillWeight(groups, 50)
	assert.Equal(t, 2, leftWeight)
	assert.Equal(t, []placement.Instance{i1, i2, i3, i4, i5, i6, i7}, res)
}

func TestFillWeightDeterministic(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 1)
	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "", "", "endpoint", 1)
	i4 := placement.NewEmptyInstance("i4", "", "", "endpoint", 3)
	i5 := placement.NewEmptyInstance("i5", "", "", "endpoint", 4)

	i6 := placement.NewEmptyInstance("i6", "", "", "endpoint", 1)
	i7 := placement.NewEmptyInstance("i7", "", "", "endpoint", 1)
	i8 := placement.NewEmptyInstance("i8", "", "", "endpoint", 1)
	i9 := placement.NewEmptyInstance("i9", "", "", "endpoint", 2)
	groups := [][]placement.Instance{
		[]placement.Instance{i1, i2, i3, i4, i5},
		[]placement.Instance{i6, i7, i8, i9},
	}

	for i := 1; i < 17; i++ {
		testResultDeterministic(t, groups, i)
	}
}

func testResultDeterministic(t *testing.T, groups [][]placement.Instance, targetWeight int) {
	res, _ := fillWeight(groups, targetWeight)

	// shuffle the order of of each group of instances
	for _, group := range groups {
		for i := range group {
			j := rand.Intn(i + 1)
			group[i], group[j] = group[j], group[i]
		}
	}
	res1, _ := fillWeight(groups, targetWeight)
	assert.Equal(t, res, res1)
}

func TestIsolationGroupLenSort(t *testing.T) {
	r1 := sortableValue{value: "r1", weight: 1}
	r2 := sortableValue{value: "r2", weight: 2}
	r3 := sortableValue{value: "r3", weight: 3}
	r4 := sortableValue{value: "r4", weight: 2}
	r5 := sortableValue{value: "r5", weight: 1}
	r6 := sortableValue{value: "r6", weight: 2}
	r7 := sortableValue{value: "r7", weight: 3}
	rs := sortableValues{r1, r2, r3, r4, r5, r6, r7}
	sort.Sort(rs)

	seen := 0
	for _, rl := range rs {
		assert.True(t, seen <= rl.weight)
		seen = rl.weight
	}
}

func TestFilterZones(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetZone("z1")
	i2 := placement.NewInstance().SetID("i2").SetZone("z1")
	i3 := placement.NewInstance().SetID("i2").SetZone("z1")
	i4 := placement.NewInstance().SetID("i3").SetZone("z2")

	_, _ = i2, i3

	tests := map[*struct {
		p          placement.Placement
		candidates []placement.Instance
		opts       placement.Options
	}][]placement.Instance{
		{
			p:          placement.NewPlacement().SetInstances([]placement.Instance{i1}),
			candidates: []placement.Instance{},
			opts:       nil,
		}: []placement.Instance{},
		{
			p:          placement.NewPlacement().SetInstances([]placement.Instance{i1}),
			candidates: []placement.Instance{i2},
			opts:       nil,
		}: []placement.Instance{i2},
		{
			p:          placement.NewPlacement().SetInstances([]placement.Instance{i1}),
			candidates: []placement.Instance{i2, i4},
			opts:       nil,
		}: []placement.Instance{i2},
		{
			p:          placement.NewPlacement().SetInstances([]placement.Instance{i1}),
			candidates: []placement.Instance{i2, i3},
			opts:       nil,
		}: []placement.Instance{i2, i3},
		{
			p:          placement.NewPlacement(),
			candidates: []placement.Instance{i2},
			opts:       nil,
		}: []placement.Instance{},
		{
			p:          placement.NewPlacement(),
			candidates: []placement.Instance{i2},
			opts:       placement.NewOptions().SetValidZone("z1"),
		}: []placement.Instance{i2},
	}

	for args, exp := range tests {
		res := filterZones(args.p, args.candidates, args.opts)
		assert.Equal(t, exp, res)
	}
}
