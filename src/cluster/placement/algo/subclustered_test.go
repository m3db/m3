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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/cluster/placement"
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
			algo := subclusteredPlacementAlgorithm{opts: opts}

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

func TestSubclusteredAlgorithm_InitialPlacement_SubclusterAssignment(t *testing.T) {
	// Test specific subcluster assignment patterns
	tests := []struct {
		name                   string
		instancesPerSubcluster int
		instances              []placement.Instance
		expectedSubclusterIDs  []uint32
		replicaFactor          int
	}{
		{
			name:                   "exactly one subcluster",
			instancesPerSubcluster: 3,
			replicaFactor:          3,
			instances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1),
				placement.NewEmptyInstance("i2", "r2", "z1", "endpoint2", 1),
				placement.NewEmptyInstance("i3", "r3", "z1", "endpoint3", 1),
			},
			expectedSubclusterIDs: []uint32{1, 1, 1},
		},
		{
			name:                   "exactly two subclusters",
			instancesPerSubcluster: 2,
			replicaFactor:          2,
			instances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1),
				placement.NewEmptyInstance("i2", "r2", "z1", "endpoint2", 1),
				placement.NewEmptyInstance("i3", "r1", "z1", "endpoint3", 1),
				placement.NewEmptyInstance("i4", "r2", "z1", "endpoint4", 1),
			},
			expectedSubclusterIDs: []uint32{1, 1, 2, 2},
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

			shards := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
			_, err := algo.InitialPlacement(instances, shards, tt.replicaFactor)
			assert.NoError(t, err)

			// Verify subcluster assignments match expected pattern
			for i, expectedID := range tt.expectedSubclusterIDs {
				assert.Equal(t, expectedID, instances[i].SubClusterID(),
					"Instance %d (ID: %s) should be in subcluster %d",
					i, instances[i].ID(), expectedID)
			}
		})
	}
}

func TestSubclusteredAlgorithm_InitialPlacement_ErrorCases(t *testing.T) {
	tests := []struct {
		name                   string
		instancesPerSubcluster int
		instances              []placement.Instance
		shards                 []uint32
		replicaFactor          int
		expectError            bool
		errorMessage           string
	}{
		{
			name:                   "instances per subcluster not set",
			instancesPerSubcluster: 0,
			instances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1),
			},
			shards:        []uint32{0},
			replicaFactor: 1,
			expectError:   true,
			errorMessage:  "instances per subcluster is not set",
		},
		{
			name:                   "negative instances per subcluster",
			instancesPerSubcluster: -1,
			instances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1),
			},
			shards:        []uint32{0},
			replicaFactor: 1,
			expectError:   true,
			errorMessage:  "instances per subcluster is not set",
		},
		{
			name:                   "replica factor greater than instances per subcluster",
			instancesPerSubcluster: 2,
			instances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1),
				placement.NewEmptyInstance("i2", "r2", "z1", "endpoint2", 1),
			},
			shards:        []uint32{0, 1},
			replicaFactor: 3,
			expectError:   true,
			errorMessage:  "instances per subcluster is not a multiple of replica factor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := placement.NewOptions().SetInstancesPerSubCluster(tt.instancesPerSubcluster)
			algo := subclusteredPlacementAlgorithm{opts: opts}

			// Clone instances to avoid modifying the original test data
			instances := make([]placement.Instance, len(tt.instances))
			for i, instance := range tt.instances {
				instances[i] = instance.Clone()
			}

			// Clone shards to avoid modifying the original test data
			shards := make([]uint32, len(tt.shards))
			copy(shards, tt.shards)

			result, err := algo.InitialPlacement(instances, shards, tt.replicaFactor)

			assert.Error(t, err)
			assert.Equal(t, tt.errorMessage, err.Error())
			assert.Nil(t, result)
		})
	}
}
