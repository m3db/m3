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
			instancesPerSub:     9,
			totalInstances:      162,
			shards:              16384,
			expectedSubClusters: 18,
			subclusterToAdd:     82,
		},
		// {
		// 	name:                "RF=3, 6 instances per subcluster, 168 total instances, 2 new subclusters",
		// 	rf:                  3,
		// 	instancesPerSub:     6,
		// 	totalInstances:      168,
		// 	shards:              4096,
		// 	expectedSubClusters: 28,
		// 	subclusterToAdd:     2,
		// },
		// {
		// 	name:                "RF=3, 6 instances per subcluster, 168 total instances, 1 new subclusters",
		// 	rf:                  3,
		// 	instancesPerSub:     6,
		// 	totalInstances:      168,
		// 	shards:              4096,
		// 	expectedSubClusters: 28,
		// 	subclusterToAdd:     1,
		// },
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
			//printPlacement(p)

			// Calculate node triplet shard analysis
			tripletAnalysis := calculateNodeTripletShardAnalysis(p)
			var maxPercentage float64
			for _, analyses := range tripletAnalysis {
				for _, analysis := range analyses {
					if analysis.TotalUniqueShards > 0 {
						percentage := float64(analysis.SharedShards) / float64(analysis.TotalUniqueShards) * 100.0
						if percentage > maxPercentage {
							maxPercentage = percentage
						}
					}
				}
			}
			if maxPercentage > 0 {
				t.Logf("Maximum shared shard percentage among all triplets: %.2f%%", maxPercentage)
			}

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
				instanceCount := 0

				for i := 0; i < len(instancesToAdd); i++ {
					newPlacement, err := algo.AddInstances(p, []placement.Instance{instancesToAdd[i]})
					require.NoError(t, err)
					require.NotNil(t, newPlacement)
					newPlacement, marked, err = algo.MarkAllShardsAvailable(newPlacement)
					require.NoError(t, err)
					require.True(t, marked)
					p = newPlacement
					instanceCount++
					if instanceCount%tt.instancesPerSub == 0 {
						t.Logf("Added %d subclusters", instanceCount/tt.instancesPerSub)
						require.NoError(t, placement.Validate(p))
						require.NoError(t, validateSubClusteredPlacement(p))
						// Get max shard differences before rebalancing
						_, globalMaxSkew, subclustersWithMaxSkewGTTwo := getMaxShardDiffInSubclusters(p)
						// Find the maximum skew and its subcluster ID
						t.Logf("Maximum shard difference before rebalancing: %d with %d subclusters with > 2", globalMaxSkew, subclustersWithMaxSkewGTTwo)

						// Calculate node triplet shard analysis
						tripletAnalysis := calculateNodeTripletShardAnalysis(p)
						var maxPercentage float64
						for _, analyses := range tripletAnalysis {
							for _, analysis := range analyses {
								if analysis.TotalUniqueShards > 0 {
									percentage := float64(analysis.SharedShards) / float64(analysis.TotalUniqueShards) * 100.0
									if percentage > maxPercentage {
										maxPercentage = percentage
									}
								}
							}
						}
						if maxPercentage > 0 {
							t.Logf("Maximum shared shard percentage among all triplets: %.2f%%", maxPercentage)
						}
					}
				}

				require.NoError(t, placement.Validate(p))

				require.NoError(t, validateSubClusteredPlacement(p))
				//printPlacement(currentPlacement)

				// Get max shard differences before rebalancing
				_, globalMaxSkew, subclustersWithMaxSkewGTTwo := getMaxShardDiffInSubclusters(p)
				// Find the maximum skew and its subcluster ID
				t.Logf("Maximum shard difference before rebalancing: %d (subcluster %d)", globalMaxSkew, subclustersWithMaxSkewGTTwo)

				// Calculate node triplet shard analysis
				tripletAnalysis = calculateNodeTripletShardAnalysis(p)

				// Analyze percentage distribution with bucketing
				buckets, maxPercentage, minPercentage, avgPercentage := analyzeTripletPercentageDistribution(tripletAnalysis)

				// Count total triplets
				totalTriplets := 0
				for _, count := range buckets {
					totalTriplets += count
				}

				t.Logf("=== FINAL TRIPLET ANALYSIS RESULTS ===")
				t.Logf("Total triplets analyzed: %d", totalTriplets)
				t.Logf("Maximum shared shard percentage: %.2f%%", maxPercentage)
				t.Logf("Minimum shared shard percentage: %.2f%%", minPercentage)
				t.Logf("Average shared shard percentage: %.2f%%", avgPercentage)

				t.Logf("=== TRIPLET SHARING PERCENTAGE DISTRIBUTION ===")
				bucketOrder := []string{"0-10%", "10-20%", "20-30%", "30-40%", "40-50%", "50-60%", "60-70%", "70-80%", "80-90%", "90-100%"}
				for _, bucket := range bucketOrder {
					count := buckets[bucket]
					if count > 0 {
						percentage := float64(count) / float64(totalTriplets) * 100.0
						t.Logf("%s: %d triplets (%.1f%%)", bucket, count, percentage)
					}
				}
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
