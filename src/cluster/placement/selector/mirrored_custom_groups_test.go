// Copyright (c) 2019 Uber Technologies, Inc.
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
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	// format: <groupID>_<instanceID>
	instG1I1 = "g1_i1"
	instG1I2 = "g1_i2"
	instG1I3 = "g1_i3"

	instG2I1 = "g2_i1"
	instG2I2 = "g2_i2"

	instG3I1 = "g3_i1"
	instG3I2 = "g3_i2"
)

var (
	testGroups = map[string]string{
		instG1I1: "group1",
		instG1I2: "group1",

		// for replacement
		instG1I3: "group1",

		instG2I1: "group2",
		instG2I2: "group2",

		// additional instances
		instG3I1: "group3",
		instG3I2: "group3",
	}
)

var (
	logger = zap.NewNop()
	// uncomment for logging
	// logger, _ = zap.NewDevelopment()
)

// testAddingNodesBehavior contains assertions common to both GroupInitialInstances and
// GroupAddingInstances.
func testAddingNodesBehavior(
	t *testing.T,
	doAdd func(
		tctx *mirroredCustomGroupSelectorTestContext,
		candidates []placement.Instance) ([]placement.Instance, error),
) {
	t.Run("RF hosts in group", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)

		groups, err := doAdd(tctx, tctx.Instances)
		require.NoError(t, err)

		assertGroupsCorrect(
			t,
			[][]string{{instG1I1, instG1I2}, {instG2I1, instG2I2}},
			groups,
		)
	})

	t.Run("too many hosts in group shortens the group to RF", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)
		tctx.Selector = NewMirroredCustomGroupSelector(
			NewMapInstanceGroupIDFunc(map[string]string{
				instG1I1: "group1",
				instG2I1: "group1",
				instG1I2: "group1",
			}),
			newTestMirroredCustomGroupOptions(),
		)
		tctx.Placement = tctx.Placement.SetReplicaFactor(2)

		groups, err := doAdd(tctx, []placement.Instance{
			newInstanceWithID(instG1I1),
			newInstanceWithID(instG2I1),
			newInstanceWithID(instG1I2),
		})
		require.NoError(t, err)
		assert.Len(t, groups, 2)
	})

	t.Run("no group configured errors", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)
		_, err := doAdd(tctx, []placement.Instance{
			newInstanceWithID(instG1I1),
			newInstanceWithID("nogroup")})
		assert.EqualError(t,
			err,
			"finding group for nogroup: instance nogroup "+
				"doesn't have a corresponding group in ID to group map")
	})

	t.Run("insufficient hosts in group is ok", func(t *testing.T) {
		// case should be handled at a higher level.
		tctx := mirroredCustomGroupSelectorSetup(t)
		_, err := doAdd(tctx, []placement.Instance{
			newInstanceWithID(instG1I1),
		})
		assert.NoError(t, err)
	})

	t.Run("hosts in other zones are filtered", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)

		tctx.Selector = NewMirroredCustomGroupSelector(
			tctx.GroupFn,
			newTestMirroredCustomGroupOptions().
				SetValidZone("zone").
				SetAllowAllZones(false),
		)
		
		instances, err := doAdd(tctx, []placement.Instance{
			newInstanceWithID(instG1I1).SetZone("zone"),
			newInstanceWithID(instG1I2).SetZone("otherZone"),
		})

		require.NoError(t, err)
		// We didn't achieve RF here, but selector isn't responsible for that validation.
		assertGroupsCorrect(t, [][]string{{instG1I1}}, instances)
	})
}

func TestExplicitMirroredCustomGroupSelector_SelectAddingInstances(t *testing.T) {
	testAddingNodesBehavior(
		t,
		func(tctx *mirroredCustomGroupSelectorTestContext, candidates []placement.Instance) ([]placement.Instance, error) {
			return tctx.Selector.SelectAddingInstances(candidates, tctx.Placement)
		},
	)

	t.Run("adds only RF instances without AddAllInstances", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)

		tctx.Selector = NewMirroredCustomGroupSelector(
			tctx.GroupFn,
			newTestMirroredCustomGroupOptions().SetAddAllCandidates(false),
		)
		groups, err := tctx.Selector.SelectAddingInstances(tctx.Instances, tctx.Placement)
		require.NoError(t, err)

		assert.Len(t, groups, 2)
	})
}

func TestExplicitMirroredCustomGroupSelector_SelectInitialInstances(t *testing.T) {
	testAddingNodesBehavior(
		t,
		func(tctx *mirroredCustomGroupSelectorTestContext, candidates []placement.Instance) ([]placement.Instance, error) {
			return tctx.Selector.SelectInitialInstances(candidates, tctx.Placement.ReplicaFactor())
		},
	)
}

func TestExplicitMirroredCustomGroupSelector_SelectReplaceInstances(t *testing.T) {
	type testContext struct {
		*mirroredCustomGroupSelectorTestContext
		ToReplace placement.Instance
	}
	setup := func(t *testing.T) *testContext {
		tctx := mirroredCustomGroupSelectorSetup(t)

		instances, err := tctx.Selector.SelectAddingInstances(tctx.Instances, tctx.Placement)
		require.NoError(t, err)

		tctx.Placement = tctx.Placement.SetInstances(instances)

		toReplace, ok := tctx.Placement.Instance(instG1I1)
		require.True(t, ok)

		return &testContext{
			mirroredCustomGroupSelectorTestContext: tctx,
			ToReplace:                  toReplace,
		}
	}

	t.Run("correct replacement", func(t *testing.T) {
		tctx := setup(t)

		instG := newInstanceWithID(instG1I3)
		replaceInstances, err := tctx.Selector.SelectReplaceInstances(
			[]placement.Instance{instG, newInstanceWithID(instG3I1)},
			[]string{tctx.ToReplace.ID()},
			tctx.Placement,
		)
		require.NoError(t, err)

		assert.Equal(t, []placement.Instance{instG}, replaceInstances)

		toReplace, ok := tctx.Placement.Instance(tctx.ToReplace.ID())
		require.True(t, ok)

		require.NotZero(t, toReplace.ShardSetID())
		assert.Equal(t, tctx.ToReplace.ShardSetID(), replaceInstances[0].ShardSetID())
	})

	t.Run("no valid replacements", func(t *testing.T) {
		tctx := setup(t)

		_, err := tctx.Selector.SelectReplaceInstances(
			[]placement.Instance{newInstanceWithID(instG3I1), newInstanceWithID(instG3I2)},
			[]string{tctx.ToReplace.ID()},
			tctx.Placement,
		)
		require.EqualError(t, err, newErrNoValidReplacement(instG1I1, "group1").Error())
	})

	t.Run("filters out invalid zone", func(t *testing.T) {
		tctx := setup(t)

		// sanity check that this is otherwise valid.
		instG := newInstanceWithID(instG1I3)
		_, err := tctx.Selector.SelectReplaceInstances(
			[]placement.Instance{instG, newInstanceWithID(instG3I1)},
			[]string{tctx.ToReplace.ID()},
			tctx.Placement,
		)
		require.NoError(t, err)

		tctx.Selector = NewMirroredCustomGroupSelector(
			tctx.GroupFn,
			newTestMirroredCustomGroupOptions().
				SetValidZone("zone").
				SetAllowAllZones(false),
		)

		_, err = tctx.Selector.SelectReplaceInstances(
			[]placement.Instance{newInstanceWithID(instG1I3).SetZone("someOtherZone")},
			[]string{tctx.ToReplace.ID()},
			tctx.Placement,
		)
		require.EqualError(t, err, errNoValidCandidateInstance.Error())
	})
}

type mirroredCustomGroupSelectorTestContext struct {
	Selector  placement.InstanceSelector
	Instances []placement.Instance
	Placement placement.Placement
	Groups    map[string]string
	GroupFn   InstanceGroupIDFunc
}

func mirroredCustomGroupSelectorSetup(_ *testing.T) *mirroredCustomGroupSelectorTestContext {
	tctx := &mirroredCustomGroupSelectorTestContext{}

	tctx.Instances = []placement.Instance{
		newInstanceWithID(instG1I1),
		newInstanceWithID(instG2I1),
		newInstanceWithID(instG1I2),
		newInstanceWithID(instG2I2),
	}

	tctx.Groups = testGroups
	
	tctx.GroupFn = NewMapInstanceGroupIDFunc(tctx.Groups)

	tctx.Selector = NewMirroredCustomGroupSelector(
		tctx.GroupFn,
		newTestMirroredCustomGroupOptions(),
	)

	tctx.Placement = placement.NewPlacement().SetReplicaFactor(2)
	return tctx
}

func newInstanceWithID(id string) placement.Instance {
	return placement.NewInstance().SetID(id)
}

func newTestMirroredCustomGroupOptions() placement.Options {
	return placement.NewOptions().
		SetAllowAllZones(true).
		SetAddAllCandidates(true).
		SetInstrumentOptions(instrument.NewOptions().SetLogger(logger))
}

func assertGroupsCorrect(t *testing.T, expectedGroupIds [][]string, instances []placement.Instance) {
	instancesByID := make(map[string]placement.Instance, len(instances))
	for _, inst := range instances {
		instancesByID[inst.ID()] = inst
	}

	groups := make([][]placement.Instance, 0, len(expectedGroupIds))
	// convert
	for _, group := range expectedGroupIds {
		groupInstances := make([]placement.Instance, 0, len(group))
		for _, id := range group {
			inst, ok := instancesByID[id]
			if !ok {
				require.True(t, ok, "instance %s not found", id)
			}
			groupInstances = append(groupInstances, inst)
		}
	}

	assertShardsetIDsEqual(t, groups)
	assertShardsetsUnique(t, groups)
}

func assertShardsetIDsEqual(
	t *testing.T,
	groups [][]placement.Instance,
) {
	// check that shardset IDs for each group are the same.
	for _, instances := range groups {
		require.True(t, len(instances) >= 1, "group must have at least one instance")
		groupShardsetID := instances[0].ShardSetID()
		for _, inst := range instances[1:] {
			require.Equal(t, groupShardsetID, inst.ShardSetID(),
				"instance %s has a different shardset (%d) "+
					"than other instances in the group (%d)",
				inst.ID(), inst.ShardSetID(), groupShardsetID)
		}
	}
}

func assertShardsetsUnique(t *testing.T, groups [][]placement.Instance) {
	allShardsets := map[uint32]struct{}{}
	for _, group := range groups {
		require.NotEmpty(t, group)

		groupShardsetID := group[0].ShardSetID()
		_, exists := allShardsets[groupShardsetID]
		require.False(t, exists,
			"multiple groups have the same shardset ID %d", groupShardsetID)
		allShardsets[groupShardsetID] = struct{}{}
	}
}
