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
		subclusterToAdd     int
	}{
		// {
		// 	name:                "RF=3, 6 instances per subcluster, 12 total instances",
		// 	rf:                  3,
		// 	instancesPerSub:     6,
		// 	totalInstances:      12,
		// 	shards:              64,
		// 	expectedSubClusters: 2,
		// },
		{
			name:                "RF=3, 6 instances per subcluster, 168 total instances, 3 new subclusters`",
			rf:                  3,
			instancesPerSub:     6,
			totalInstances:      168,
			shards:              4096,
			expectedSubClusters: 28,
			subclusterToAdd:     56,
		},
		{
			name:                "RF=3, 6 instances per subcluster, 168 total instances, 2 new subclusters",
			rf:                  3,
			instancesPerSub:     6,
			totalInstances:      168,
			shards:              4096,
			expectedSubClusters: 28,
			subclusterToAdd:     2,
		},
		{
			name:                "RF=3, 6 instances per subcluster, 168 total instances, 1 new subclusters",
			rf:                  3,
			instancesPerSub:     6,
			totalInstances:      168,
			shards:              4096,
			expectedSubClusters: 28,
			subclusterToAdd:     1,
		},
		// {
		// 	name:                "RF=3, 9 instances per subcluster, 18 total instances",
		// 	rf:                  3,
		// 	instancesPerSub:     9,
		// 	totalInstances:      720,
		// 	shards:              8192,
		// 	expectedSubClusters: 80,
		// },
		// {
		// 	name:                "RF=2, 4 instances per subcluster, 12 total instances",
		// 	rf:                  2,
		// 	instancesPerSub:     4,
		// 	totalInstances:      12,
		// 	shards:              1024,
		// 	expectedSubClusters: 3,
		// },
		// {
		// 	name:                "RF=4, 8 instances per subcluster, 16 total instances",
		// 	rf:                  4,
		// 	instancesPerSub:     8,
		// 	totalInstances:      640,
		// 	shards:              1024,
		// 	expectedSubClusters: 80,
		// },
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
			algo := newSubclusteredShardedAlgorithm(opts)

			// Perform initial placement
			p, err := algo.InitialPlacement(instances, shardIDs, tt.rf)
			require.NoError(t, err)
			require.NotNil(t, p)

			p, marked, err := algo.MarkAllShardsAvailable(p)
			require.NoError(t, err)
			require.True(t, marked)

			require.NoError(t, placement.Validate(p))
			require.NoError(t, validateSubClusteredPlacement(p))
			printPlacement(p)

			if tt.subclusterToAdd > 0 {
				totalInstances := tt.instancesPerSub * (tt.subclusterToAdd)
				instancesToAdd := make([]placement.Instance, totalInstances)

				for i := 0; i < totalInstances; i++ {
					instancesToAdd[i] = placement.NewInstance().
						SetID(fmt.Sprintf("RI%d", tt.instancesPerSub+i)).
						SetIsolationGroup(fmt.Sprintf("R%d", (tt.instancesPerSub+i)%tt.rf)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d", tt.instancesPerSub+i))
				}

				for i := 0; i < len(instancesToAdd); i++ {
					newPlacement, err := algo.AddInstances(p, []placement.Instance{instancesToAdd[i]})
					require.NoError(t, err)
					require.NotNil(t, newPlacement)
					p = newPlacement
				}

				newPlacement, marked, err := algo.MarkAllShardsAvailable(p)
				require.NoError(t, err)
				require.True(t, marked)

				require.NoError(t, validateSubClusteredPlacement(newPlacement))

				subclusterSkews := getMaxShardDiffInSubclusters(newPlacement)
				// Find the maximum skew and its subcluster ID
				var maxDiffSubclusterID uint32
				var maxBeforeDiff int
				for subclusterID, skew := range subclusterSkews {
					if skew > maxBeforeDiff {
						maxBeforeDiff = skew
						maxDiffSubclusterID = subclusterID
					}
				}
				t.Logf("Maximum shard difference before rebalancing: %d (subcluster %d)", maxBeforeDiff, maxDiffSubclusterID)

				balancedPlacement, err := algo.BalanceShards(newPlacement)
				require.NoError(t, err)
				require.NotNil(t, balancedPlacement)

				balancedPlacement, marked, err = algo.MarkAllShardsAvailable(balancedPlacement)
				require.NoError(t, err)

				require.NoError(t, validateSubClusteredPlacement(balancedPlacement))

				subclusterSkewsAfter := getMaxShardDiffInSubclusters(balancedPlacement)
				// Find the maximum skew and its subcluster ID after rebalancing
				var maxDiffSubclusterIDAfter uint32
				var maxAfterDiff int
				for subclusterID, skew := range subclusterSkewsAfter {
					if skew > maxAfterDiff {
						maxAfterDiff = skew
						maxDiffSubclusterIDAfter = subclusterID
					}
				}
				t.Logf("Maximum shard difference after rebalancing: %d (subcluster %d)", maxAfterDiff, maxDiffSubclusterIDAfter)
			}
		})
	}
}

// func TestSubclusteredV2InitialPlacementEdgeCases(t *testing.T) {
// 	tests := []struct {
// 		name            string
// 		rf              int
// 		instancesPerSub int
// 		totalInstances  int
// 		shards          int
// 		expectError     bool
// 	}{
// 		{
// 			name:            "Invalid: RF > instances per subcluster",
// 			rf:              4,
// 			instancesPerSub: 3,
// 			totalInstances:  12,
// 			shards:          3,
// 			expectError:     true,
// 		},
// 		{
// 			name:            "Invalid: Total instances not multiple of RF",
// 			rf:              3,
// 			instancesPerSub: 6,
// 			totalInstances:  10,
// 			shards:          3,
// 			expectError:     true,
// 		},
// 		{
// 			name:            "Invalid: Instances per subcluster not multiple of RF",
// 			rf:              3,
// 			instancesPerSub: 5,
// 			totalInstances:  15,
// 			shards:          3,
// 			expectError:     true,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			// Create test instances
// 			instances := make([]placement.Instance, tt.totalInstances)
// 			for i := 0; i < tt.totalInstances; i++ {
// 				instances[i] = placement.NewInstance().
// 					SetID(fmt.Sprintf("instance%d", i)).
// 					SetIsolationGroup(fmt.Sprintf("rack%d", i%tt.rf)).
// 					SetWeight(1).
// 					SetEndpoint(fmt.Sprintf("endpoint%d", i)).
// 					SetShards(shard.NewShards(nil))
// 			}

// 			// Generate shard IDs from 0 to shards-1
// 			shardIDs := make([]uint32, tt.shards)
// 			for i := 0; i < tt.shards; i++ {
// 				shardIDs[i] = uint32(i)
// 			}

// 			// Create algorithm
// 			opts := placement.NewOptions().
// 				SetValidZone("zone1").
// 				SetIsSharded(true)
// 			algo := newSubclusteredv2(opts)

// 			// Perform initial placement
// 			p, err := algo.InitialPlacement(instances, shardIDs, tt.rf)
// 			if tt.expectError {
// 				require.Error(t, err)
// 				require.Nil(t, p)
// 			} else {
// 				require.NoError(t, err)
// 				require.NotNil(t, p)
// 			}
// 		})
// 	}
// }
