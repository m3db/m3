// Copyright (c) 2018 Uber Technologies, Inc.
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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

// getRandomSubclusterIDs randomly selects the specified number of subcluster IDs from the placement
func getRandomSubclusterIDs(p placement.Placement, numToRemove int) []uint32 {
	subclusterMap := make(map[uint32]struct{})
	for _, instance := range p.Instances() {
		subclusterMap[instance.SubClusterID()] = struct{}{}
	}

	subclusterIDs := make([]uint32, 0, len(subclusterMap))
	for subclusterID := range subclusterMap {
		subclusterIDs = append(subclusterIDs, subclusterID)
	}

	// Randomly shuffle and select the first numToRemove
	rand.Shuffle(len(subclusterIDs), func(i, j int) {
		subclusterIDs[i], subclusterIDs[j] = subclusterIDs[j], subclusterIDs[i]
	})

	if numToRemove > len(subclusterIDs) {
		numToRemove = len(subclusterIDs)
	}

	return subclusterIDs[:numToRemove]
}

func TestSubclusteredAlgorithm_IsCompatibleWith(t *testing.T) {
	algo := newSubclusteredAlgorithm(placement.NewOptions())

	tests := []struct {
		name        string
		placement   placement.Placement
		expectError bool
	}{
		{
			name:        "nil placement",
			placement:   nil,
			expectError: true,
		},
		{
			name: "not sharded placement",
			placement: placement.NewPlacement().
				SetIsSharded(false).
				SetHasSubClusters(true),
			expectError: true,
		},
		{
			name: "no subclusters placement",
			placement: placement.NewPlacement().
				SetIsSharded(true).
				SetHasSubClusters(false),
			expectError: true,
		},
		{
			name: "compatible placement",
			placement: placement.NewPlacement().
				SetIsSharded(true).
				SetHasSubClusters(true),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := algo.IsCompatibleWith(tt.placement)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSubclusteredAlgorithm_InitialPlacement(t *testing.T) {
	tests := []struct {
		name                   string
		instancesPerSubcluster int
		replicaFactor          int
		totalInstances         int
		totalShards            int
		expectError            bool
		errorMessage           string
		expectedSubclusters    int
	}{
		{
			name:                   "valid configuration - rf=2, instancesPerSubcluster=6",
			instancesPerSubcluster: 6,
			replicaFactor:          2,
			totalInstances:         12,
			totalShards:            64,
			expectError:            false,
			expectedSubclusters:    2,
		},
		{
			name:                   "valid configuration - rf=3, instancesPerSubcluster=9",
			instancesPerSubcluster: 9,
			replicaFactor:          3,
			totalInstances:         27,
			totalShards:            1024,
			expectError:            false,
			expectedSubclusters:    3,
		},
		{
			name:                   "valid configuration - rf=1, instancesPerSubcluster=4",
			instancesPerSubcluster: 4,
			replicaFactor:          1,
			totalInstances:         4,
			totalShards:            16,
			expectError:            false,
			expectedSubclusters:    1,
		},
		{
			name:                   "valid configuration - rf=4, instancesPerSubcluster=8",
			instancesPerSubcluster: 8,
			replicaFactor:          4,
			totalInstances:         16,
			totalShards:            512,
			expectError:            false,
			expectedSubclusters:    2,
		},
		{
			name:                   "valid configuration - multiple subclusters",
			instancesPerSubcluster: 3,
			replicaFactor:          1,
			totalInstances:         6,
			totalShards:            6,
			expectError:            false,
			expectedSubclusters:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := placement.NewOptions().SetInstancesPerSubCluster(tt.instancesPerSubcluster)
			algo := newSubclusteredAlgorithm(opts)

			// Generate instances dynamically
			instances := make([]placement.Instance, tt.totalInstances)
			for i := 0; i < tt.totalInstances; i++ {
				instances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.replicaFactor)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", i)).
					SetShards(shard.NewShards(nil))
			}

			// Generate shards dynamically
			shards := make([]uint32, tt.totalShards)
			for i := 0; i < tt.totalShards; i++ {
				shards[i] = uint32(i)
			}

			result, err := algo.InitialPlacement(instances, shards, tt.replicaFactor)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.errorMessage, err.Error())
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				subclusterMap := make(map[uint32]struct{})

				instances := result.Instances()
				for _, instance := range instances {
					subclusterMap[instance.SubClusterID()] = struct{}{}
				}

				// Verify subcluster assignments
				assert.Equal(t, tt.expectedSubclusters, len(subclusterMap))
				// Verify placement properties
				assert.Equal(t, tt.replicaFactor, result.ReplicaFactor())
				assert.True(t, result.IsSharded())
				assert.True(t, result.HasSubClusters())
				assert.Equal(t, tt.instancesPerSubcluster, result.InstancesPerSubCluster())
			}
		})
	}
}

func TestSubclusteredAlgorithm_InitialPlacement_ErrorCases(t *testing.T) {
	tests := []struct {
		name                   string
		instancesPerSubcluster int
		replicaFactor          int
		totalInstances         int
		totalShards            int
		expectError            bool
		errorMessage           string
	}{
		{
			name:                   "instances per subcluster not multiple of replica factor - rf=2, instancesPerSubcluster=5",
			instancesPerSubcluster: 5,
			replicaFactor:          2,
			totalInstances:         5,
			totalShards:            5,
			expectError:            true,
			errorMessage:           "instances per subcluster is not a multiple of replica factor",
		},
		{
			name:                   "instances per subcluster not multiple of replica factor - rf=3, instancesPerSubcluster=8",
			instancesPerSubcluster: 8,
			replicaFactor:          3,
			totalInstances:         8,
			totalShards:            8,
			expectError:            true,
			errorMessage:           "instances per subcluster is not a multiple of replica factor",
		},
		{
			name:                   "replica factor greater than instances per subcluster",
			instancesPerSubcluster: 2,
			replicaFactor:          3,
			totalInstances:         2,
			totalShards:            2,
			expectError:            true,
			errorMessage:           "instances per subcluster is not a multiple of replica factor",
		},
		{
			name:                   "instances per subcluster not set",
			instancesPerSubcluster: 0,
			replicaFactor:          1,
			totalInstances:         1,
			totalShards:            1,
			expectError:            true,
			errorMessage:           "instances per subcluster is not set",
		},
		{
			name:                   "negative instances per subcluster",
			instancesPerSubcluster: -1,
			replicaFactor:          1,
			totalInstances:         1,
			totalShards:            1,
			expectError:            true,
			errorMessage:           "instances per subcluster is not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := placement.NewOptions().SetInstancesPerSubCluster(tt.instancesPerSubcluster)
			algo := subclusteredPlacementAlgorithm{opts: opts}

			// Generate instances dynamically
			instances := make([]placement.Instance, tt.totalInstances)
			for i := 0; i < tt.totalInstances; i++ {
				instances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.replicaFactor)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", i)).
					SetShards(shard.NewShards(nil))
			}

			// Generate shards dynamically
			shards := make([]uint32, tt.totalShards)
			for i := 0; i < tt.totalShards; i++ {
				shards[i] = uint32(i)
			}

			result, err := algo.InitialPlacement(instances, shards, tt.replicaFactor)

			assert.Error(t, err)
			assert.Equal(t, tt.errorMessage, err.Error())
			assert.Nil(t, result)
		})
	}
}

func TestAddInstancesValidCases(t *testing.T) {
	tests := []struct {
		name                   string
		instancesPerSubcluster int
		replicaFactor          int
		instancesToAdd         int
		totalShards            int
	}{
		{
			name:                   "valid configuration - rf=3, instancesPerSubcluster=6",
			instancesPerSubcluster: 6,
			replicaFactor:          3,
			instancesToAdd:         12,
			totalShards:            128,
		},
		{
			name:                   "valid configuration - rf=3, instancesPerSubcluster=9",
			instancesPerSubcluster: 9,
			replicaFactor:          3,
			instancesToAdd:         27,
			totalShards:            128,
		},
		{
			name:                   "valid configuration - rf=4, instancesPerSubcluster=8",
			instancesPerSubcluster: 8,
			replicaFactor:          4,
			instancesToAdd:         16,
			totalShards:            128,
		},
		{
			name:                   "valid configuration - rf=2, instancesPerSubcluster=6`",
			instancesPerSubcluster: 8,
			replicaFactor:          2,
			instancesToAdd:         16,
			totalShards:            128,
		},
		{
			name:                   "partial subcluster configuration - rf=3, instancesPerSubcluster=6",
			instancesPerSubcluster: 6,
			replicaFactor:          3,
			instancesToAdd:         10,
			totalShards:            128,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := placement.NewOptions().SetInstancesPerSubCluster(tt.instancesPerSubcluster)
			algo := subclusteredPlacementAlgorithm{opts: opts}

			initialInstances := make([]placement.Instance, tt.instancesPerSubcluster)
			for i := 0; i < tt.instancesPerSubcluster; i++ {
				initialInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.replicaFactor)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", i)).
					SetShards(shard.NewShards(nil))
			}

			initialShards := make([]uint32, tt.totalShards)
			for i := 0; i < tt.totalShards; i++ {
				initialShards[i] = uint32(i)
			}

			result, err := algo.InitialPlacement(initialInstances, initialShards, tt.replicaFactor)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.NoError(t, placement.Validate(result))

			instancesToAdd := make([]placement.Instance, tt.instancesToAdd)
			for i := 0; i < tt.instancesToAdd; i++ {
				instancesToAdd[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", tt.instancesPerSubcluster+i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.replicaFactor)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", tt.instancesPerSubcluster+i)).
					SetShards(shard.NewShards(nil))
			}
			currentPlacement := result.Clone()
			for i := 0; i < tt.instancesToAdd; i++ {
				instance := instancesToAdd[i]
				newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{instance})
				assert.NoError(t, err)
				assert.NotNil(t, newPlacement)
				assert.Equal(t, tt.instancesPerSubcluster+i+1, len(newPlacement.Instances()))
				assert.NoError(t, placement.Validate(newPlacement))

				newPlacement, marked2, err := algo.MarkAllShardsAvailable(newPlacement)
				assert.NoError(t, err)
				assert.True(t, marked2)
				assert.NoError(t, placement.Validate(newPlacement))

				currentPlacement = newPlacement
			}
			assert.NoError(t, placement.Validate(currentPlacement))
		})
	}
}

func TestAddInstancesErrorCases(t *testing.T) {
	tests := []struct {
		name                   string
		replicaFactor          int
		instancesPerSubcluster int
		instancesToAdd         []placement.Instance
		expectError            bool
	}{
		{
			name:                   "number of isolation groups is not equal to replica factor",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			instancesToAdd: []placement.Instance{
				placement.NewEmptyInstance("I7", "R8", "R8", "E0", 1),
			},
			expectError: true,
		},
		{
			name:                   "instances per isolation group != instancesPerSubcluster/replicaFactor",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			instancesToAdd: []placement.Instance{
				placement.NewEmptyInstance("I9", "R0", "R0", "E0", 1),
				placement.NewEmptyInstance("I10", "R0", "R0", "E1", 1),
				placement.NewEmptyInstance("I11", "R0", "R0", "E2", 1),
			},
			expectError: true,
		},
		{
			name:                   "instances per isolation group != instancesPerSubcluster/replicaFacto (full subcluster)",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			instancesToAdd: []placement.Instance{
				placement.NewEmptyInstance("I9", "R0", "R0", "E0", 1),
				placement.NewEmptyInstance("I10", "R1", "R1", "E1", 1),
				placement.NewEmptyInstance("I11", "R2", "R2", "E2", 1),
				placement.NewEmptyInstance("I12", "R0", "R0", "E0", 1),
				placement.NewEmptyInstance("I13", "R1", "R1", "E1", 1),
				placement.NewEmptyInstance("I14", "R0", "R0", "E2", 1),
			},
			expectError: true,
		},
		{
			name:                   "instances do not have same weight",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			instancesToAdd: []placement.Instance{
				placement.NewEmptyInstance("I15", "R0", "R0", "E0", 10),
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := placement.NewOptions().SetInstancesPerSubCluster(tt.instancesPerSubcluster)
			algo := subclusteredPlacementAlgorithm{opts: opts}

			initialInstances := make([]placement.Instance, tt.instancesPerSubcluster)
			for i := 0; i < tt.instancesPerSubcluster; i++ {
				initialInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.replicaFactor)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", i)).
					SetShards(shard.NewShards(nil))
			}

			totalShards := 128
			shards := make([]uint32, totalShards)
			for i := 0; i < totalShards; i++ {
				shards[i] = uint32(i)
			}

			result, err := algo.InitialPlacement(initialInstances, shards, tt.replicaFactor)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.NoError(t, placement.Validate(result))

			newPlacement, err := algo.AddInstances(result, tt.instancesToAdd)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, newPlacement)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, newPlacement)
				assert.NoError(t, placement.Validate(newPlacement))
			}
		})
	}
}

func TestRemoveInstancesValidCases(t *testing.T) {
	tests := []struct {
		name                   string
		replicaFactor          int
		initialSubClusters     int
		instancesPerSubcluster int
		subClustersToRemove    int
		totalShards            int
	}{
		{
			name:                   "valid configuration - rf=3, instancesPerSubcluster=6",
			replicaFactor:          3,
			initialSubClusters:     4,
			instancesPerSubcluster: 6,
			subClustersToRemove:    1,
			totalShards:            128,
		},
		{
			name:                   "valid configuration - rf=3, instancesPerSubcluster=9",
			replicaFactor:          3,
			initialSubClusters:     24,
			instancesPerSubcluster: 9,
			subClustersToRemove:    5,
			totalShards:            1024,
		},
		{
			name:                   "valid configuration - rf=4, instancesPerSubcluster=8",
			replicaFactor:          4,
			initialSubClusters:     10,
			instancesPerSubcluster: 8,
			subClustersToRemove:    4,
			totalShards:            1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := placement.NewOptions().SetInstancesPerSubCluster(tt.instancesPerSubcluster).
				SetHasSubClusters(true)
			algo := subclusteredPlacementAlgorithm{opts: opts}

			initialInstances := make([]placement.Instance, tt.instancesPerSubcluster*tt.initialSubClusters)
			for i := 0; i < tt.instancesPerSubcluster*tt.initialSubClusters; i++ {
				subclusterID := uint32(i/tt.instancesPerSubcluster + 1)
				initialInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.replicaFactor)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", i)).
					SetSubClusterID(subclusterID).
					SetShards(shard.NewShards(nil))
			}

			initialShards := make([]uint32, tt.totalShards)
			for i := 0; i < tt.totalShards; i++ {
				initialShards[i] = uint32(i)
			}

			result, err := algo.InitialPlacement(initialInstances, initialShards, tt.replicaFactor)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.NoError(t, placement.Validate(result))

			// Randomly select subclusters to remove
			subclustersToRemove := getRandomSubclusterIDs(result, tt.subClustersToRemove)

			// Get all instances from the selected subclusters
			var instancesToRemove []string
			for _, subclusterID := range subclustersToRemove {
				for _, instance := range result.Instances() {
					if instance.SubClusterID() == subclusterID {
						instancesToRemove = append(instancesToRemove, instance.ID())
					}
				}
			}

			// Remove the instances
			newPlacement, err := algo.RemoveInstances(result, instancesToRemove)
			assert.NoError(t, err)
			assert.NotNil(t, newPlacement)
			assert.NoError(t, placement.Validate(newPlacement))

			// Verify that the expected number of instances were removed
			expectedRemainingInstances := tt.instancesPerSubcluster * (tt.initialSubClusters - tt.subClustersToRemove)
			assert.Equal(t, expectedRemainingInstances, len(newPlacement.Instances()))
		})
	}
}

func TestPartialSubclustersRemoveOperation(t *testing.T) {
	tests := []struct {
		name                   string
		replicaFactor          int
		instancesPerSubcluster int
		instancesToAdd         int
		totalShards            int
		subClustersToRemove    int
	}{
		{
			name:                   "remove subcluster while removal of subcluster is going on",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			instancesToAdd:         18,
			totalShards:            128,
			subClustersToRemove:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := placement.NewOptions().SetInstancesPerSubCluster(tt.instancesPerSubcluster)
			algo := subclusteredPlacementAlgorithm{opts: opts}

			initialInstances := make([]placement.Instance, tt.instancesPerSubcluster)
			for i := 0; i < tt.instancesPerSubcluster; i++ {
				initialInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.replicaFactor)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", i)).
					SetShards(shard.NewShards(nil))
			}

			initialShards := make([]uint32, tt.totalShards)
			for i := 0; i < tt.totalShards; i++ {
				initialShards[i] = uint32(i)
			}

			result, err := algo.InitialPlacement(initialInstances, initialShards, tt.replicaFactor)
			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.NoError(t, placement.Validate(result))

			currentPlacement, marked, err := algo.MarkAllShardsAvailable(result)
			assert.NoError(t, err)
			assert.True(t, marked)
			assert.NoError(t, placement.Validate(currentPlacement))

			instancesToAdd := make([]placement.Instance, tt.instancesToAdd)
			for i := 0; i < tt.instancesToAdd; i++ {
				instancesToAdd[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", tt.instancesPerSubcluster+i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.replicaFactor)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", tt.instancesPerSubcluster+i)).
					SetShards(shard.NewShards(nil))
			}

			for i := 0; i < tt.instancesToAdd; i++ {
				instance := instancesToAdd[i]
				newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{instance})
				assert.NoError(t, err)
				assert.NotNil(t, newPlacement)
				assert.NoError(t, placement.Validate(newPlacement))

				newPlacement, marked2, err := algo.MarkAllShardsAvailable(newPlacement)
				assert.NoError(t, err)
				assert.True(t, marked2)
				assert.NoError(t, placement.Validate(newPlacement))

				currentPlacement = newPlacement
			}
			// Randomly select subclusters to remove
			subclustersToRemove := getRandomSubclusterIDs(currentPlacement, tt.subClustersToRemove)

			// Get all instances from the selected subclusters
			var instancesToRemove []string
			for _, subclusterID := range subclustersToRemove {
				for _, instance := range currentPlacement.Instances() {
					if instance.SubClusterID() == subclusterID {
						instancesToRemove = append(instancesToRemove, instance.ID())
					}
				}
			}

			if tt.subClustersToRemove > 1 {
				rand.Shuffle(len(instancesToRemove), func(i, j int) {
					instancesToRemove[i], instancesToRemove[j] = instancesToRemove[j], instancesToRemove[i]
				})
			}

			// Remove the instances
			newPlacement, err := algo.RemoveInstances(currentPlacement, instancesToRemove)
			assert.Error(t, err)
			assert.Nil(t, newPlacement)
		})
	}
}

func TestPartialSubclustersAddOperation(t *testing.T) {
	shards := make([]uint32, 1024)
	for i := 0; i < 1024; i++ {
		shards[i] = uint32(i)
	}

	instancesPerSubCluster := 9
	replicaFactor := 3
	subclustersToAdd := 5
	subclusterIDToRemove := uint32(3)

	initialInstances := make([]placement.Instance, instancesPerSubCluster*subclustersToAdd)
	for i := 0; i < len(initialInstances); i++ {
		subclusterID := uint32(i/instancesPerSubCluster + 1)
		initialInstances[i] = placement.NewInstance().
			SetID(fmt.Sprintf("I%d", i)).
			SetIsolationGroup(fmt.Sprintf("R%d", i%replicaFactor)).
			SetWeight(1).
			SetEndpoint(fmt.Sprintf("E%d", i)).
			SetSubClusterID(subclusterID).
			SetShards(shard.NewShards(nil))
	}
	opts := placement.NewOptions().SetInstancesPerSubCluster(instancesPerSubCluster).
		SetHasSubClusters(true)
	algo := subclusteredPlacementAlgorithm{opts: opts}

	result, err := algo.InitialPlacement(initialInstances, shards, replicaFactor)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NoError(t, placement.Validate(result))

	currentPlacement, marked, err := algo.MarkAllShardsAvailable(result)
	assert.NoError(t, err)
	assert.True(t, marked)
	assert.NoError(t, placement.Validate(currentPlacement))

	instanceToRemove := ""
	for _, instance := range currentPlacement.Instances() {
		if instance.SubClusterID() == subclusterIDToRemove {
			instanceToRemove = instance.ID()
			break
		}
	}

	newPlacement, err := algo.RemoveInstances(currentPlacement, []string{instanceToRemove})
	assert.NoError(t, err)
	assert.NotNil(t, newPlacement)
	assert.NoError(t, placement.Validate(newPlacement))

	instanceToAdd := placement.NewInstance().
		SetID("RI0").
		SetIsolationGroup("R0").
		SetWeight(1).
		SetEndpoint("E0").
		SetShards(shard.NewShards(nil))

	newPlacement, err = algo.AddInstances(newPlacement, []placement.Instance{instanceToAdd})
	assert.Error(t, err)
	assert.Nil(t, newPlacement)
}

func TestReplaceInstancesValidCases(t *testing.T) {
	tests := []struct {
		name                        string
		rf                          int
		instancesPerSub             int
		totalInstances              int
		shards                      int
		instancesToAddBeforeReplace int
	}{
		{
			name:                        "RF=3, 6 instance/subcluster, add 5 instances (not multiple of instancesPerSubcluster)",
			rf:                          3,
			instancesPerSub:             6,
			totalInstances:              12,
			shards:                      256,
			instancesToAddBeforeReplace: 5,
		},
		{
			name:                        "RF=3, 6 instance/subcluster, add 12 instances (multiple of instancesPerSubcluster)",
			rf:                          3,
			instancesPerSub:             6,
			totalInstances:              6,
			shards:                      256,
			instancesToAddBeforeReplace: 12,
		},
		{
			name:                        "RF=3, 9 instance/subcluster, add 7 instances (not multiple of instancesPerSubcluster)",
			rf:                          3,
			instancesPerSub:             9,
			totalInstances:              9,
			shards:                      256,
			instancesToAddBeforeReplace: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create initial test instances
			instances := make([]placement.Instance, tt.totalInstances)
			for i := 0; i < tt.totalInstances; i++ {
				instances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.rf)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", i)).
					SetShards(shard.NewShards(nil))
			}

			// Generate shard IDs from 0 to shards-1
			shardIDs := make([]uint32, tt.shards)
			for i := 0; i < tt.shards; i++ {
				shardIDs[i] = uint32(i)
			}

			// Create algorithm
			opts := placement.NewOptions().
				SetValidZone("zone1").
				SetIsSharded(true).
				SetInstancesPerSubCluster(tt.instancesPerSub).
				SetHasSubClusters(true)
			algo := newSubclusteredAlgorithm(opts)

			// Perform initial placement
			p, err := algo.InitialPlacement(instances, shardIDs, tt.rf)
			assert.NoError(t, err)
			assert.NotNil(t, p)
			assert.NoError(t, placement.Validate(p))

			// Verify initial placement
			currentPlacement, marked, err := algo.MarkAllShardsAvailable(p)
			assert.NoError(t, err)
			assert.True(t, marked)
			assert.NoError(t, placement.Validate(currentPlacement))

			// Create new instances to add
			totalInstances := tt.instancesToAddBeforeReplace + tt.instancesPerSub -
				(tt.instancesToAddBeforeReplace % tt.instancesPerSub)
			newInstances := make([]placement.Instance, totalInstances)
			for i := range totalInstances {
				newInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", tt.totalInstances+i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.rf)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", tt.totalInstances+i)).
					SetShards(shard.NewShards(nil))
			}

			// Add new instances
			j := 0
			for _, instance := range newInstances {
				if j == tt.instancesToAddBeforeReplace {
					break
				}
				newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{instance})
				assert.NoError(t, err)
				assert.NotNil(t, newPlacement)
				newPlacement, marked2, err := algo.MarkAllShardsAvailable(newPlacement)
				assert.NoError(t, err)
				assert.True(t, marked2)
				assert.NoError(t, placement.Validate(newPlacement))
				currentPlacement = newPlacement
				j++
			}

			// Get instances to replace (one from each subcluster)
			instancesToReplace := make([]string, 0)
			subClusterMap := make(map[uint32][]string)
			for _, instance := range currentPlacement.Instances() {
				subClusterMap[instance.SubClusterID()] = append(subClusterMap[instance.SubClusterID()], instance.ID())
			}

			// Select one instance from each subcluster for replacement
			for _, instances := range subClusterMap {
				if len(instances) > 0 {
					instancesToReplace = append(instancesToReplace, instances[0])
				}
			}

			// Create replacement instances with same isolation groups
			replacementInstances := make([]placement.Instance, len(instancesToReplace))
			for i, instanceID := range instancesToReplace {
				instance, _ := currentPlacement.Instance(instanceID)
				replacementInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("R%d", i)).
					SetIsolationGroup(instance.IsolationGroup()).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("RE%d", i)).
					SetShards(shard.NewShards(nil))
			}

			// Perform replacement
			newPlacement, err := algo.ReplaceInstances(currentPlacement, instancesToReplace, replacementInstances)
			assert.NoError(t, err)
			assert.NotNil(t, newPlacement)
			newPlacement, marked3, err := algo.MarkAllShardsAvailable(newPlacement)
			assert.NoError(t, err)
			assert.True(t, marked3)

			// Verify final placement
			assert.NoError(t, placement.Validate(newPlacement))
			if j < totalInstances {
				// Add remaining instances
				finalPlacement, err := algo.AddInstances(newPlacement, newInstances[j:])
				assert.NoError(t, err)
				assert.NotNil(t, finalPlacement)
				assert.NoError(t, placement.Validate(finalPlacement))
				finalPlacement, marked4, err := algo.MarkAllShardsAvailable(finalPlacement)
				assert.NoError(t, err)
				assert.True(t, marked4)
				assert.NoError(t, placement.Validate(finalPlacement))
				newPlacement = finalPlacement
			}

			// Verify that replaced instances are gone and new ones are present
			for _, instanceID := range instancesToReplace {
				_, exists := newPlacement.Instance(instanceID)
				assert.False(t, exists, "Replaced instance should not exist in final placement")
			}

			for _, instance := range replacementInstances {
				_, exists := newPlacement.Instance(instance.ID())
				assert.True(t, exists, "Replacement instance should exist in final placement")
			}
		})
	}
}

func TestRemoveInstancesErrorCases(t *testing.T) {
	tests := []struct {
		name                   string
		replicaFactor          int
		instancesPerSubcluster int
		initialSubClusters     int
		instanceIDsToRemove    []string
		expectError            bool
		errorContains          string
		setupPlacement         func() placement.Placement
	}{
		{
			name:                   "nil placement",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			initialSubClusters:     2,
			instanceIDsToRemove:    []string{"I0"},
			expectError:            true,
			errorContains:          "placement is nil",
			setupPlacement:         func() placement.Placement { return nil },
		},
		{
			name:                   "non-sharded placement",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			initialSubClusters:     2,
			instanceIDsToRemove:    []string{"I0"},
			expectError:            true,
			errorContains:          "could not apply subclustered algo on the placement",
			setupPlacement: func() placement.Placement {
				opts := placement.NewOptions().SetInstancesPerSubCluster(6).SetHasSubClusters(true)
				algo := subclusteredPlacementAlgorithm{opts: opts}

				instances := make([]placement.Instance, 6)
				for i := 0; i < 6; i++ {
					instances[i] = placement.NewInstance().
						SetID(fmt.Sprintf("I%d", i)).
						SetIsolationGroup(fmt.Sprintf("R%d", i%3)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d", i)).
						SetSubClusterID(1).
						SetShards(shard.NewShards(nil))
				}

				shards := make([]uint32, 128)
				for i := 0; i < 128; i++ {
					shards[i] = uint32(i)
				}

				p, err := algo.InitialPlacement(instances, shards, 3)
				if err != nil {
					t.Fatalf("Failed to create placement: %v", err)
				}

				// Create a non-sharded placement by cloning and modifying
				nonShardedInstances := make([]placement.Instance, len(p.Instances()))
				for i, instance := range p.Instances() {
					nonShardedInstances[i] = placement.NewInstance().
						SetID(instance.ID()).
						SetIsolationGroup(instance.IsolationGroup()).
						SetWeight(instance.Weight()).
						SetEndpoint(instance.Endpoint()).
						SetShards(instance.Shards())
				}

				return placement.NewPlacement().
					SetInstances(nonShardedInstances).
					SetShards(p.Shards()).
					SetReplicaFactor(p.ReplicaFactor()).
					SetIsSharded(false).
					SetHasSubClusters(true).
					SetInstancesPerSubCluster(p.InstancesPerSubCluster())
			},
		},
		{
			name:                   "placement without subclusters",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			initialSubClusters:     2,
			instanceIDsToRemove:    []string{"I0"},
			expectError:            true,
			errorContains:          "could not apply subclustered algo on the placement",
			setupPlacement: func() placement.Placement {
				opts := placement.NewOptions().SetInstancesPerSubCluster(6).SetHasSubClusters(true)
				algo := newSubclusteredAlgorithm(opts)

				instances := make([]placement.Instance, 6)
				for i := 0; i < 6; i++ {
					instances[i] = placement.NewInstance().
						SetID(fmt.Sprintf("I%d", i)).
						SetIsolationGroup(fmt.Sprintf("R%d", i%3)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d", i)).
						SetShards(shard.NewShards(nil))
				}

				shards := make([]uint32, 128)
				for i := 0; i < 128; i++ {
					shards[i] = uint32(i)
				}

				p, err := algo.InitialPlacement(instances, shards, 3)
				if err != nil {
					t.Fatalf("Failed to create placement: %v", err)
				}

				// Create a placement without subclusters
				return placement.NewPlacement().
					SetInstances(p.Instances()).
					SetShards(p.Shards()).
					SetReplicaFactor(p.ReplicaFactor()).
					SetIsSharded(true).
					SetHasSubClusters(false).
					SetInstancesPerSubCluster(p.InstancesPerSubCluster())
			},
		},
		{
			name:                   "instance does not exist",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			initialSubClusters:     2,
			instanceIDsToRemove:    []string{"non-existent-instance"},
			expectError:            true,
			errorContains:          "instance non-existent-instance does not exist in placement",
			// nolint: dupl
			setupPlacement: func() placement.Placement {
				opts := placement.NewOptions().SetInstancesPerSubCluster(6).SetHasSubClusters(true)
				algo := newSubclusteredAlgorithm(opts)

				instances := make([]placement.Instance, 12)
				for i := 0; i < 12; i++ {
					subclusterID := uint32(i/6 + 1)
					instances[i] = placement.NewInstance().
						SetID(fmt.Sprintf("I%d", i)).
						SetIsolationGroup(fmt.Sprintf("R%d", i%3)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d", i)).
						SetSubClusterID(subclusterID).
						SetShards(shard.NewShards(nil))
				}

				shards := make([]uint32, 128)
				for i := 0; i < 128; i++ {
					shards[i] = uint32(i)
				}

				p, err := algo.InitialPlacement(instances, shards, 3)
				if err != nil {
					t.Fatalf("Failed to create placement: %v", err)
				}
				return p
			},
		},
		{
			name:                   "removing instance from partial subcluster",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			initialSubClusters:     2,
			instanceIDsToRemove:    []string{"I0"},
			expectError:            true,
			errorContains:          "partial subcluster",
			setupPlacement: func() placement.Placement {
				opts := placement.NewOptions().SetInstancesPerSubCluster(6).SetHasSubClusters(true)
				algo := subclusteredPlacementAlgorithm{opts: opts}

				// Create instances with one subcluster having fewer instances than instancesPerSubcluster
				instances := make([]placement.Instance, 9) // 6 + 3 instead of 6 + 6
				for i := 0; i < 9; i++ {
					subclusterID := uint32(1)
					if i >= 6 {
						subclusterID = 2
					}
					instances[i] = placement.NewInstance().
						SetID(fmt.Sprintf("I%d", i)).
						SetIsolationGroup(fmt.Sprintf("R%d", i%3)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d", i)).
						SetSubClusterID(subclusterID).
						SetShards(shard.NewShards(nil))
				}

				shards := make([]uint32, 128)
				for i := 0; i < 128; i++ {
					shards[i] = uint32(i)
				}

				p, err := algo.InitialPlacement(instances, shards, 3)
				if err != nil {
					t.Fatalf("Failed to create placement: %v", err)
				}
				return p
			},
		},
		{
			name:                   "inconsistent instance weights",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			initialSubClusters:     2,
			instanceIDsToRemove:    []string{"I0"},
			expectError:            true,
			errorContains:          "inconsistent instance weights",
			setupPlacement: func() placement.Placement {
				opts := placement.NewOptions().SetInstancesPerSubCluster(6).SetHasSubClusters(true)
				algo := subclusteredPlacementAlgorithm{opts: opts}

				// Create instances with consistent weights first
				instances := make([]placement.Instance, 12)
				for i := 0; i < 12; i++ {
					subclusterID := uint32(i/6 + 1)
					instances[i] = placement.NewInstance().
						SetID(fmt.Sprintf("I%d", i)).
						SetIsolationGroup(fmt.Sprintf("R%d", i%3)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d", i)).
						SetSubClusterID(subclusterID).
						SetShards(shard.NewShards(nil))
				}

				shards := make([]uint32, 128)
				for i := 0; i < 128; i++ {
					shards[i] = uint32(i)
				}

				p, err := algo.InitialPlacement(instances, shards, 3)
				if err != nil {
					t.Fatalf("Failed to create placement: %v", err)
				}

				// Now modify one instance to have a different weight
				modifiedInstances := make([]placement.Instance, len(p.Instances()))
				for i, instance := range p.Instances() {
					if instance.ID() == "I1" {
						modifiedInstances[i] = placement.NewInstance().
							SetID(instance.ID()).
							SetIsolationGroup(instance.IsolationGroup()).
							SetWeight(2). // Different weight
							SetEndpoint(instance.Endpoint()).
							SetSubClusterID(instance.SubClusterID()).
							SetShards(instance.Shards())
					} else {
						modifiedInstances[i] = instance
					}
				}

				return placement.NewPlacement().
					SetInstances(modifiedInstances).
					SetShards(p.Shards()).
					SetReplicaFactor(p.ReplicaFactor()).
					SetIsSharded(true).
					SetHasSubClusters(true).
					SetInstancesPerSubCluster(p.InstancesPerSubCluster()).
					SetIsMirrored(p.IsMirrored())
			},
		},
		{
			name:                   "valid removal - should not error",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			initialSubClusters:     2,
			instanceIDsToRemove:    []string{"I0"},
			expectError:            false,
			// nolint: dupl
			setupPlacement: func() placement.Placement {
				opts := placement.NewOptions().SetInstancesPerSubCluster(6).SetHasSubClusters(true)
				algo := subclusteredPlacementAlgorithm{opts: opts}

				instances := make([]placement.Instance, 12)
				for i := 0; i < 12; i++ {
					subclusterID := uint32(i/6 + 1)
					instances[i] = placement.NewInstance().
						SetID(fmt.Sprintf("I%d", i)).
						SetIsolationGroup(fmt.Sprintf("R%d", i%3)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d", i)).
						SetSubClusterID(subclusterID).
						SetShards(shard.NewShards(nil))
				}

				shards := make([]uint32, 128)
				for i := 0; i < 128; i++ {
					shards[i] = uint32(i)
				}

				p, err := algo.InitialPlacement(instances, shards, 3)
				if err != nil {
					t.Fatalf("Failed to create placement: %v", err)
				}
				return p
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := placement.NewOptions().SetInstancesPerSubCluster(tt.instancesPerSubcluster).SetHasSubClusters(true)
			algo := subclusteredPlacementAlgorithm{opts: opts}

			p := tt.setupPlacement()
			if p == nil && !tt.expectError {
				t.Fatal("Setup placement returned nil but test doesn't expect error")
			}

			// Skip the test if placement is nil and we expect an error
			if p == nil && tt.expectError {
				return
			}

			newPlacement, err := algo.RemoveInstances(p, tt.instanceIDsToRemove)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, newPlacement)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, newPlacement)
				assert.NoError(t, placement.Validate(newPlacement))

				// Verify that the expected number of instances were removed
				expectedRemainingInstances := len(p.Instances()) - len(tt.instanceIDsToRemove)
				assert.Equal(t, expectedRemainingInstances, len(newPlacement.Instances()))
			}
		})
	}
}

func TestReclaimLeavingInstance(t *testing.T) {
	opts := placement.NewOptions().SetInstancesPerSubCluster(6).SetHasSubClusters(true)
	algo := newSubclusteredAlgorithm(opts)

	instances := make([]placement.Instance, 12)
	for i := 0; i < 12; i++ {
		instances[i] = placement.NewInstance().
			SetID(fmt.Sprintf("I%d", i)).
			SetIsolationGroup(fmt.Sprintf("R%d", i%3)).
			SetWeight(1).
			SetEndpoint(fmt.Sprintf("E%d", i)).
			SetShards(shard.NewShards(nil))
	}

	shards := make([]uint32, 32)
	for i := 0; i < 32; i++ {
		shards[i] = uint32(i)
	}

	p, err := algo.InitialPlacement(instances, shards, 3)
	assert.NoError(t, err)

	currentPlacement, marked, err := algo.MarkAllShardsAvailable(p)
	assert.NoError(t, err)
	assert.True(t, marked)
	assert.NoError(t, placement.Validate(currentPlacement))

	instanceToRemove := "I0"
	newPlacement, err := algo.RemoveInstances(currentPlacement, []string{instanceToRemove})
	assert.NoError(t, err)
	assert.NotNil(t, newPlacement)
	assert.NoError(t, placement.Validate(newPlacement))

	// Add the removed instance again to the placement
	addingInstance, exists := newPlacement.Instance(instanceToRemove)
	assert.True(t, exists)

	finalPlacement, err := algo.AddInstances(newPlacement, []placement.Instance{addingInstance})
	assert.NoError(t, err)
	assert.NotNil(t, finalPlacement)
	assert.NoError(t, placement.Validate(finalPlacement))

	// check if the removed instance is still present and has all shards in AVAILABLE state in the same subcluster
	instance, exists := finalPlacement.Instance(instanceToRemove)
	assert.True(t, exists)
	assert.False(t, instance.IsLeaving())

}
