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
				SetIsSubclustered(true),
			expectError: true,
		},
		{
			name: "no subclusters placement",
			placement: placement.NewPlacement().
				SetIsSharded(true).
				SetIsSubclustered(false),
			expectError: true,
		},
		{
			name: "compatible placement",
			placement: placement.NewPlacement().
				SetIsSharded(true).
				SetIsSubclustered(true),
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

func TestInitialPlacement(t *testing.T) {
	tests := []struct {
		name                   string
		instancesPerSubcluster int
		instances              []placement.Instance
		shards                 int
		replicaFactor          int
		expectError            bool
		errorMessage           string
	}{
		{
			name:                   "instances per subcluster not multiple of replica factor",
			instancesPerSubcluster: 5,
			shards:                 5,
			replicaFactor:          3,
			expectError:            true,
			errorMessage:           "instances per subcluster is not a multiple of replica factor",
		},
		{
			name:                   "instances per subcluster is not set",
			instancesPerSubcluster: 0,
			shards:                 5,
			replicaFactor:          3,
			expectError:            true,
			errorMessage:           "instances per subcluster is not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := placement.NewOptions().SetInstancesPerSubCluster(tt.instancesPerSubcluster)
			algo := newSubclusteredAlgorithm(opts)

			// Clone instances to avoid modifying the original test data
			instances := make([]placement.Instance, len(tt.instances))
			for i, instance := range tt.instances {
				instances[i] = instance.Clone()
			}

			// Clone shards to avoid modifying the original test data
			shards := make([]uint32, tt.shards)
			for i := range shards {
				shards[i] = uint32(i)
			}

			result, err := algo.InitialPlacement(instances, shards, tt.replicaFactor)
			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.errorMessage, err.Error())
				assert.Nil(t, result)
			} else {
				// For now, the method returns nil, nil when successful
				// This will need to be updated when the implementation is complete
				assert.NoError(t, err)
				assert.Nil(t, result) // Current implementation returns nil
			}

			// Verify that instances were assigned subcluster IDs correctly
			if !tt.expectError && len(instances) > 0 {
				// Check that all instances have valid subcluster IDs
				for i, instance := range instances {
					assert.True(t, instance.SubClusterID() > 0,
						"Instance %d (ID: %s) should have a valid subcluster ID",
						i, instance.ID())
				}

				// Verify subcluster assignment logic
				expectedSubclusters := make(map[uint32]int)
				for _, instance := range instances {
					expectedSubclusters[instance.SubClusterID()]++
				}

				// Each subcluster should not exceed the configured limit
				for subclusterID, count := range expectedSubclusters {
					assert.True(t, count <= tt.instancesPerSubcluster,
						"Subcluster %d should not exceed %d instances, got %d",
						subclusterID, tt.instancesPerSubcluster, count)
				}
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
				assert.True(t, result.IsSubclustered())
				assert.NoError(t, placement.Validate(result))
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

				newPlacement, marked, err := algo.MarkAllShardsAvailable(newPlacement)
				assert.NoError(t, err)
				assert.True(t, marked)
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
				SetIsSubclustered(true)
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
			name:                   "remove subcluster while addition of subcluster is going on",
			replicaFactor:          3,
			instancesPerSubcluster: 6,
			instancesToAdd:         14,
			totalShards:            128,
			subClustersToRemove:    1,
		},
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

				newPlacement, marked, err = algo.MarkAllShardsAvailable(newPlacement)
				assert.NoError(t, err)
				assert.True(t, marked)
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
		SetIsSubclustered(true)
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
