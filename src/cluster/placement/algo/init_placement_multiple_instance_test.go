package algo

import (
	"fmt"
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/stretchr/testify/require"
)

func TestSubclusteredV2InitialPlacement(t *testing.T) {
	tests := []struct {
		name                string
		rf                  int
		instancesPerSub     int
		totalInstances      int
		shards              int
		expectedSubClusters int
	}{
		{
			name:                "RF=3, 6 instances per subcluster, 12 total instances",
			rf:                  3,
			instancesPerSub:     6,
			totalInstances:      12,
			shards:              64,
			expectedSubClusters: 2,
		},
		{
			name:                "RF=3, 6 instances per subcluster, 720 total instances",
			rf:                  3,
			instancesPerSub:     6,
			totalInstances:      720,
			shards:              4096,
			expectedSubClusters: 120,
		},
		{
			name:                "RF=3, 9 instances per subcluster, 18 total instances",
			rf:                  3,
			instancesPerSub:     9,
			totalInstances:      720,
			shards:              8192,
			expectedSubClusters: 80,
		},
		{
			name:                "RF=2, 4 instances per subcluster, 12 total instances",
			rf:                  2,
			instancesPerSub:     4,
			totalInstances:      12,
			shards:              1024,
			expectedSubClusters: 3,
		},
		{
			name:                "RF=4, 8 instances per subcluster, 16 total instances",
			rf:                  4,
			instancesPerSub:     8,
			totalInstances:      640,
			shards:              1024,
			expectedSubClusters: 80,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test instances
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
			algo := newSubclusteredv2(opts)

			// Perform initial placement
			p, err := algo.InitialPlacement(instances, shardIDs, tt.rf)
			require.NoError(t, err)
			require.NotNil(t, p)

			// Verify RF
			require.Equal(t, tt.rf, p.ReplicaFactor())

			// Verify shard distribution
			shardDistribution := make(map[uint32]int)
			for _, instance := range p.Instances() {
				for _, shard := range instance.Shards().All() {
					shardDistribution[shard.ID()]++
				}
			}
			for _, count := range shardDistribution {
				require.Equal(t, tt.rf, count, "Each shard should be replicated RF times")
			}

			// Verify isolation group distribution within subclusters
			subClusterIsolationGroups := make(map[uint32]map[string]int)
			for _, instance := range p.Instances() {
				subClusterID := instance.SubClusterID()
				if _, exists := subClusterIsolationGroups[subClusterID]; !exists {
					subClusterIsolationGroups[subClusterID] = make(map[string]int)
				}
				subClusterIsolationGroups[subClusterID][instance.IsolationGroup()]++
			}

			// Verify each subcluster has equal distribution of isolation groups
			for _, isolationGroupCounts := range subClusterIsolationGroups {
				expectedCount := tt.instancesPerSub / tt.rf
				for _, count := range isolationGroupCounts {
					require.Equal(t, expectedCount, count, "Each isolation group should have equal instances in a subcluster")
				}
			}

			// Verify total number of subclusters
			require.Equal(t, tt.expectedSubClusters, len(subClusterIsolationGroups))

			require.NoError(t, placement.Validate(p))
			printPlacementAndValidate(t, p)
		})
	}
}

func TestSubclusteredV2InitialPlacementEdgeCases(t *testing.T) {
	tests := []struct {
		name            string
		rf              int
		instancesPerSub int
		totalInstances  int
		shards          int
		expectError     bool
	}{
		{
			name:            "Invalid: RF > instances per subcluster",
			rf:              4,
			instancesPerSub: 3,
			totalInstances:  12,
			shards:          3,
			expectError:     true,
		},
		{
			name:            "Invalid: Total instances not multiple of RF",
			rf:              3,
			instancesPerSub: 6,
			totalInstances:  10,
			shards:          3,
			expectError:     true,
		},
		{
			name:            "Invalid: Instances per subcluster not multiple of RF",
			rf:              3,
			instancesPerSub: 5,
			totalInstances:  15,
			shards:          3,
			expectError:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test instances
			instances := make([]placement.Instance, tt.totalInstances)
			for i := 0; i < tt.totalInstances; i++ {
				instances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("instance%d", i)).
					SetIsolationGroup(fmt.Sprintf("rack%d", i%tt.rf)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("endpoint%d", i)).
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
				SetIsSharded(true)
			algo := newSubclusteredv2(opts)

			// Perform initial placement
			p, err := algo.InitialPlacement(instances, shardIDs, tt.rf)
			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, p)
			} else {
				require.NoError(t, err)
				require.NotNil(t, p)
			}
		})
	}
}
