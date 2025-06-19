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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

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
