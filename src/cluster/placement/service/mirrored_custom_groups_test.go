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

package service

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placement/selector"
	placementstorage "github.com/m3db/m3/src/cluster/placement/storage"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	zone          = "zone1"
	defaultWeight = 1
	numShards     = 12
	rf            = 2
)

// fixtures
const (
	// format: <groupID>_<instanceID>
	instG1I1 = "g1_i1"
	instG1I2 = "g1_i2"
	instG1I3 = "g1_i3"
	instG1I4 = "g1_i4"

	instG2I1 = "g2_i1"
	instG2I2 = "g2_i2"

	instG3I1 = "g3_i1"
	instG3I2 = "g3_i2"
	instG3I3 = "g3_i3"
)

var (
	testGroups = map[string]string{
		instG1I2: "group1",
		instG1I1: "group1",

		// for replacement
		instG1I3: "group1",
		instG1I4: "group1",

		instG2I1: "group2",
		instG2I2: "group2",

		// additional instances
		instG3I1: "group3",
		instG3I2: "group3",
		instG3I3: "group3",
	}
)

var (
	logger, _ = zap.NewDevelopment()
)

// TestExplicitMirroredCustomGroupSelector contains functional tests starting at the placement.Service level.
func TestExplicitMirroredCustomGroupSelector(t *testing.T) {
	mustBuildInitialPlacement := func(
		t *testing.T, tctx *mirroredCustomGroupSelectorTestContext) placement.Placement {
		p, err := tctx.Service.BuildInitialPlacement(tctx.Instances, numShards, rf)
		require.NoError(t, err)
		assertPlacementRespectsGroups(t, p, tctx.Groups)
		return p
	}

	t.Run("BuildInitialPlacement", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)

		mustBuildInitialPlacement(t, tctx)
	})

	t.Run("BuildInitialPlacement insufficient nodes", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)
		_, err := tctx.Service.BuildInitialPlacement([]placement.Instance{
			newInstanceWithID(instG1I1),
		}, numShards, rf)
		assert.EqualError(t, err, "found 1 count of shard set id 1, expecting 2")
		// assertPlacementRespectsGroups(t, p, tctx.Groups)
	})

	t.Run("BuildInitialPlacement given too many nodes should use only RF", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)
		p, err := tctx.Service.BuildInitialPlacement([]placement.Instance{
			newInstanceWithID(instG1I1),
			newInstanceWithID(instG1I2),
			newInstanceWithID(instG1I3),
			newInstanceWithID(instG1I4),
		}, numShards, rf)
		require.NoError(t, err)
		assertPlacementRespectsGroups(t, p, tctx.Groups)
	})

	t.Run("AddInstances", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)
		mustBuildInitialPlacement(t, tctx)

		toAdd := []placement.Instance{
			newInstanceWithID(instG3I1),
			newInstanceWithID(instG3I2),
		}

		p, addedInstances, err := tctx.Service.AddInstances(toAdd)
		require.NoError(t, err)
		assert.Equal(t, toAdd, addedInstances)
		assertPlacementRespectsGroups(t, p, tctx.Groups)
	})

	t.Run("AddInstances given too many nodes should use only RF", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)
		mustBuildInitialPlacement(t, tctx)

		toAdd := []placement.Instance{
			newInstanceWithID(instG3I1),
			newInstanceWithID(instG3I2),
			newInstanceWithID(instG3I3),
		}

		p, addedInstances, err := tctx.Service.AddInstances(toAdd)
		require.NoError(t, err)
		assert.Len(t, addedInstances, p.ReplicaFactor())
		assertPlacementRespectsGroups(t, p, tctx.Groups)
	})

	t.Run("ReplaceInstances", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)
		mustBuildInitialPlacement(t, tctx)

		group1Instance := newInstanceWithID(instG1I3)
		candidates := []placement.Instance{
			group1Instance,
			newInstanceWithID(instG3I1),
		}
		p, usedInstances, err := tctx.Service.ReplaceInstances([]string{instG1I1}, candidates)
		require.NoError(t, err)
		require.Len(t, usedInstances, 1)
		assert.Equal(t, group1Instance.ID(), usedInstances[0].ID())
		assertPlacementRespectsGroups(t, p, tctx.Groups)
	})

	// N.B.: at time of this writing this technically doesn't need grouping, but check that
	// the grouping is respected anyhow.
	t.Run("RemoveInstances", func(t *testing.T) {
		tctx := mirroredCustomGroupSelectorSetup(t)
		mustBuildInitialPlacement(t, tctx)

		p, err := tctx.Service.RemoveInstances([]string{instG1I1, instG1I2})
		require.NoError(t, err)

		assertPlacementRespectsGroups(t, p, tctx.Groups)
	})
}

type mirroredCustomGroupSelectorTestContext struct {
	KVStore   kv.Store
	Opts      placement.Options
	Storage   placement.Storage
	Service   placement.Service
	Selector  placement.InstanceSelector
	Instances []placement.Instance
	Groups    map[string]string
}

func mirroredCustomGroupSelectorSetup(t *testing.T) *mirroredCustomGroupSelectorTestContext {
	tctx := &mirroredCustomGroupSelectorTestContext{}

	tctx.Instances = []placement.Instance{
		newInstanceWithID(instG1I1),
		newInstanceWithID(instG2I1),
		newInstanceWithID(instG1I2),
		newInstanceWithID(instG2I2),
	}

	tctx.Groups = testGroups

	opts := placement.NewOptions().
		SetValidZone(zone).
		SetIsMirrored(true)

	tctx.Selector = selector.NewMirroredCustomGroupSelector(
		selector.NewMapInstanceGroupIDFunc(tctx.Groups),
		opts,
	)

	tctx.Opts = opts.SetInstanceSelector(tctx.Selector)

	tctx.KVStore = mem.NewStore()
	tctx.Storage = placementstorage.NewPlacementStorage(tctx.KVStore, "placement", tctx.Opts)
	tctx.Service = NewPlacementService(tctx.Storage, WithPlacementOptions(tctx.Opts))
	return tctx
}

// assertGroupsAreRespected checks that each group in the given group map has:
//   - the same shards assigned
//   - the same shardset ID
//
// Note: shard comparison ignores SourceID, as this is expected to differ between instances in
// a group (new instances take shards from different replicas in an existing group)
func assertPlacementRespectsGroups(t *testing.T, p placement.Placement, groups map[string]string) bool {
	// check that the groups are respected.
	instancesByGroupID := make(map[string][]placement.Instance, len(groups))

	for _, inst := range p.Instances() {
		groupID, ok := groups[inst.ID()]
		if !assert.True(t, ok, "instance %s has no group", inst.ID()) {
			return false
		}
		instancesByGroupID[groupID] = append(instancesByGroupID[groupID], inst)
	}

	rtn := true
	for groupID, groupInsts := range instancesByGroupID {
		// make sure all shards are the same.
		if !assert.True(t, len(groupInsts) >= 1, "groupID %s", groupID) {
			continue
		}
		compareInst := groupInsts[0]
		for _, inst := range groupInsts[1:] {
			instIDs := []string{inst.ID(), compareInst.ID()}

			rtn = rtn && assert.Equal(t,
				shardIDs(compareInst.Shards()), shardIDs(inst.Shards()),
				"%s (actual) != %s (expected) for instances %v",
				inst.Shards(), compareInst.Shards(),
				instIDs)
			rtn = rtn && assert.Equal(t,
				compareInst.ShardSetID(), inst.ShardSetID(),
				"shard not equal for instances %v", instIDs)
		}
	}
	return rtn
}

func shardIDs(ss shard.Shards) []uint32 {
	ids := make([]uint32, 0, len(ss.All()))
	for _, s := range ss.All() {
		ids = append(ids, s.ID())
	}
	return ids
}

func newTestInstance() placement.Instance {
	return newInstanceWithID(nextTestInstanceID())
}

func newInstanceWithID(id string) placement.Instance {
	return placement.NewEmptyInstance(
		id,
		nextTestIsolationGroup(),
		zone,
		fmt.Sprintf("localhost:%d", randPort()),
		defaultWeight,
	)
}

var curInstanceNo int64

// Uses global state; factor into a factory object if you need guarantees about the specific results.
func nextTestInstanceID() string {
	myNumber := atomic.AddInt64(&curInstanceNo, 1)
	return fmt.Sprintf("instance-%d", myNumber)
}

// completely random valid port, not necessarily open.
func randPort() int {
	return rand.Intn(1 << 16)
}

var curISOGroup int64

// Uses global state; factor into a factory object if you need guarantees about the specific results.
func nextTestIsolationGroup() string {
	myGroup := atomic.AddInt64(&curISOGroup, 1)
	return fmt.Sprintf("iso-group-%d", myGroup)
}
