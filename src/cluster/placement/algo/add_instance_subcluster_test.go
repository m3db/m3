package algo

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/stretchr/testify/require"
)

// NodeTripletAnalysis represents shard analysis for a triplet of nodes from different isolation groups
type NodeTripletAnalysis struct {
	Node1ID           string
	Node2ID           string
	Node3ID           string
	Node1IG           string
	Node2IG           string
	Node3IG           string
	SharedShards      int // Shards present on all 3 nodes
	TotalUniqueShards int // Total unique shards across all 3 nodes
}

// calculateNodeTripletShardAnalysis calculates shard distribution for each combination of 3 nodes from different isolation groups
func calculateNodeTripletShardAnalysis(p placement.Placement) map[uint32][]NodeTripletAnalysis {
	subclusterAnalysis := make(map[uint32][]NodeTripletAnalysis)

	// Group instances by subcluster
	subclusterInstances := make(map[uint32][]placement.Instance)
	for _, instance := range p.Instances() {
		subclusterID := instance.SubClusterID()
		subclusterInstances[subclusterID] = append(subclusterInstances[subclusterID], instance)
	}

	// Calculate for each subcluster
	for subclusterID, instances := range subclusterInstances {
		if len(instances) < 3 {
			continue
		}

		// Group instances by isolation group
		isolationGroups := make(map[string][]placement.Instance)
		for _, instance := range instances {
			ig := instance.IsolationGroup()
			isolationGroups[ig] = append(isolationGroups[ig], instance)
		}

		// Need at least 3 different isolation groups
		if len(isolationGroups) < 3 {
			continue
		}

		var tripletAnalyses []NodeTripletAnalysis

		// Get isolation group names as slice for easier iteration
		igNames := make([]string, 0, len(isolationGroups))
		for igName := range isolationGroups {
			igNames = append(igNames, igName)
		}

		// Try all combinations of 3 isolation groups
		for i := 0; i < len(igNames); i++ {
			for j := i + 1; j < len(igNames); j++ {
				for k := j + 1; k < len(igNames); k++ {
					ig1, ig2, ig3 := igNames[i], igNames[j], igNames[k]

					// For each combination of 3 isolation groups,
					// analyze all possible triplets of nodes (one from each group)
					for _, node1 := range isolationGroups[ig1] {
						for _, node2 := range isolationGroups[ig2] {
							for _, node3 := range isolationGroups[ig3] {
								// Get shards for each node
								node1Shards := make(map[uint32]struct{})
								node2Shards := make(map[uint32]struct{})
								node3Shards := make(map[uint32]struct{})

								for _, shardID := range node1.Shards().All() {
									node1Shards[shardID.ID()] = struct{}{}
								}
								for _, shardID := range node2.Shards().All() {
									node2Shards[shardID.ID()] = struct{}{}
								}
								for _, shardID := range node3.Shards().All() {
									node3Shards[shardID.ID()] = struct{}{}
								}

								// Calculate intersection (shards present on all 3 nodes)
								sharedShards := make(map[uint32]struct{})
								for shardID := range node1Shards {
									if _, exists2 := node2Shards[shardID]; exists2 {
										if _, exists3 := node3Shards[shardID]; exists3 {
											sharedShards[shardID] = struct{}{}
										}
									}
								}

								// Calculate union (total unique shards across all 3 nodes)
								uniqueShards := make(map[uint32]struct{})
								for shardID := range node1Shards {
									uniqueShards[shardID] = struct{}{}
								}
								for shardID := range node2Shards {
									uniqueShards[shardID] = struct{}{}
								}
								for shardID := range node3Shards {
									uniqueShards[shardID] = struct{}{}
								}

								// Create analysis for this triplet
								analysis := NodeTripletAnalysis{
									Node1ID:           node1.ID(),
									Node2ID:           node2.ID(),
									Node3ID:           node3.ID(),
									Node1IG:           node1.IsolationGroup(),
									Node2IG:           node2.IsolationGroup(),
									Node3IG:           node3.IsolationGroup(),
									SharedShards:      len(sharedShards),
									TotalUniqueShards: len(uniqueShards),
								}

								tripletAnalyses = append(tripletAnalyses, analysis)
							}
						}
					}
				}
			}
		}

		subclusterAnalysis[subclusterID] = tripletAnalyses
	}

	return subclusterAnalysis
}

// getMaxShardDiffInSubclusters returns skew information for all subclusters and prints summary statistics
func getMaxShardDiffInSubclusters(p placement.Placement) (map[uint32]int, int, int) {
	// Map to store shard counts per instance in each subcluster
	subclusterShardCounts := make(map[uint32]map[string]int)

	// Count shards for each instance in each subcluster
	for _, instance := range p.Instances() {
		subclusterID := instance.SubClusterID()
		if _, exists := subclusterShardCounts[subclusterID]; !exists {
			subclusterShardCounts[subclusterID] = make(map[string]int)
		}
		subclusterShardCounts[subclusterID][instance.ID()] = instance.Shards().NumShards()
	}

	// Calculate skew for each subcluster
	subclusterSkews := make(map[uint32]int)
	var globalMaxSkew int

	for subclusterID, instanceCounts := range subclusterShardCounts {
		var minCount, maxCount int
		first := true
		for _, count := range instanceCounts {
			if first {
				minCount = count
				maxCount = count
				first = false
				continue
			}
			if count < minCount {
				minCount = count
			}
			if count > maxCount {
				maxCount = count
			}
		}

		skew := maxCount - minCount
		subclusterSkews[subclusterID] = skew
		if skew > globalMaxSkew {
			globalMaxSkew = skew
		}
	}

	// Count how many subclusters have the maximum skew
	subclustersWithMaxSkewGTTwo := 0
	for _, skew := range subclusterSkews {
		if skew > 3 {
			subclustersWithMaxSkewGTTwo++
		}
	}

	return subclusterSkews, globalMaxSkew, subclustersWithMaxSkewGTTwo
}

// analyzeTripletPercentageDistribution analyzes triplet sharing percentages and creates buckets
func analyzeTripletPercentageDistribution(tripletAnalysis map[uint32][]NodeTripletAnalysis) (map[string]int, float64, float64, float64) {
	buckets := map[string]int{
		"0-10%":   0,
		"10-20%":  0,
		"20-30%":  0,
		"30-40%":  0,
		"40-50%":  0,
		"50-60%":  0,
		"60-70%":  0,
		"70-80%":  0,
		"80-90%":  0,
		"90-100%": 0,
	}

	var totalPercentage float64
	var maxPercentage float64
	var minPercentage float64 = 100.0
	validTriplets := 0

	// Process all triplets across all subclusters
	for _, analyses := range tripletAnalysis {
		for _, analysis := range analyses {
			if analysis.TotalUniqueShards > 0 {
				percentage := float64(analysis.SharedShards) / float64(analysis.TotalUniqueShards) * 100.0

				totalPercentage += percentage
				validTriplets++

				if percentage > maxPercentage {
					maxPercentage = percentage
				}
				if percentage < minPercentage {
					minPercentage = percentage
				}

				// Add to appropriate bucket
				switch {
				case percentage < 10:
					buckets["0-10%"]++
				case percentage < 20:
					buckets["10-20%"]++
				case percentage < 30:
					buckets["20-30%"]++
				case percentage < 40:
					buckets["30-40%"]++
				case percentage < 50:
					buckets["40-50%"]++
				case percentage < 60:
					buckets["50-60%"]++
				case percentage < 70:
					buckets["60-70%"]++
				case percentage < 80:
					buckets["70-80%"]++
				case percentage < 90:
					buckets["80-90%"]++
				default:
					buckets["90-100%"]++
				}
			}
		}
	}

	var avgPercentage float64
	if validTriplets > 0 {
		avgPercentage = totalPercentage / float64(validTriplets)
	}

	return buckets, maxPercentage, minPercentage, avgPercentage
}

func TestSubclusteredV2AddInstances(t *testing.T) {
	tests := []struct {
		name                string
		rf                  int
		instancesPerSub     int
		subclustersToAdd    int
		shards              int
		subclustersToRemove int
	}{
		{
			name:                "RF=3, 6 instances per subcluster, start with 12 add 6",
			rf:                  3,
			instancesPerSub:     9,
			subclustersToAdd:    50,
			shards:              16384,
			subclustersToRemove: 0,
		},
		// {
		// 	name:             "RF=3, 9 instances per subcluster, start with 18 add 9",
		// 	rf:               3,
		// 	instancesPerSub:  9,
		// 	subclustersToAdd: 100,
		// 	shards:           8192,
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create initial test instances
			instances := make([]placement.Instance, tt.instancesPerSub)
			for i := 0; i < tt.instancesPerSub; i++ {
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

			// Verify initial placement
			require.NoError(t, placement.Validate(p))
			require.NoError(t, validateSubClusteredPlacement(p))

			// Create new instances to add
			newInstances := make([]placement.Instance, tt.instancesPerSub*tt.subclustersToAdd)
			for i := 0; i < len(newInstances); i++ {
				newInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", tt.instancesPerSub+i)).
					SetIsolationGroup(fmt.Sprintf("R%d", (tt.instancesPerSub+i)%tt.rf)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", tt.instancesPerSub+i)).
					SetShards(shard.NewShards(nil))
			}

			// Add instances one by one
			currentPlacement := p
			instanceCount := 0

			for i := 0; i < len(newInstances); i++ {
				newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{newInstances[i]})
				require.NoError(t, err)
				require.NotNil(t, newPlacement)
				newPlacement, marked, err = algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)
				currentPlacement = newPlacement
				instanceCount++
				if instanceCount%tt.instancesPerSub == 0 {
					t.Logf("Added %d subclusters", instanceCount/tt.instancesPerSub)
					require.NoError(t, placement.Validate(currentPlacement))
					require.NoError(t, validateSubClusteredPlacement(currentPlacement))
					// Get max shard differences before rebalancing
					_, globalMaxSkew, subclustersWithMaxSkewGTTwo := getMaxShardDiffInSubclusters(currentPlacement)
					// Find the maximum skew and its subcluster ID
					t.Logf("Maximum shard difference before rebalancing: %d with %d subclusters with > 2", globalMaxSkew, subclustersWithMaxSkewGTTwo)

					// Calculate node triplet shard analysis
					tripletAnalysis := calculateNodeTripletShardAnalysis(currentPlacement)
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

			// Verify the placement after addition

			// Verify the placement after addition
			require.NoError(t, placement.Validate(currentPlacement))

			require.NoError(t, validateSubClusteredPlacement(currentPlacement))
			//printPlacement(currentPlacement)

			// Get max shard differences before rebalancing
			_, globalMaxSkew, subclustersWithMaxSkewGTTwo := getMaxShardDiffInSubclusters(currentPlacement)
			// Find the maximum skew and its subcluster ID
			t.Logf("Maximum shard difference before rebalancing: %d (subcluster %d)", globalMaxSkew, subclustersWithMaxSkewGTTwo)

			// Calculate node triplet shard analysis
			tripletAnalysis := calculateNodeTripletShardAnalysis(currentPlacement)

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

			if tt.subclustersToRemove > 0 {

				// Get instances from random N subclusters
				instancesToRemove := make([]placement.Instance, 0, tt.instancesPerSub*tt.subclustersToRemove)

				// Create a list of all available subcluster IDs
				availableSubclusters := make(map[uint32]bool)
				for _, instance := range currentPlacement.Instances() {
					availableSubclusters[instance.SubClusterID()] = true
				}

				subclusterIDs := make([]uint32, 0, len(availableSubclusters))
				for id := range availableSubclusters {
					subclusterIDs = append(subclusterIDs, id)
				}

				// Randomly select subclusters to remove
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				rng.Shuffle(len(subclusterIDs), func(i, j int) {
					subclusterIDs[i], subclusterIDs[j] = subclusterIDs[j], subclusterIDs[i]
				})

				selectedSubclusters := subclusterIDs[:tt.subclustersToRemove]

				for _, subClusterID := range selectedSubclusters {
					for _, instance := range currentPlacement.Instances() {
						if instance.SubClusterID() == subClusterID {
							instancesToRemove = append(instancesToRemove, instance)
						}
					}
				}
				require.Equal(t, tt.instancesPerSub*tt.subclustersToRemove, len(instancesToRemove),
					"Should have correct number of instances to remove")

				// Remove instances one by one
				for i, instance := range instancesToRemove {
					t.Logf("Removing instance %s (%d/%d)", instance.ID(), i+1, len(instancesToRemove))

					// Remove the instance
					newPlacement, err := algo.RemoveInstances(currentPlacement, []string{instance.ID()})
					require.NoError(t, err)
					require.NotNil(t, newPlacement)
					// printPlacement(newPlacement)

					newPlacement, marked, err := algo.MarkAllShardsAvailable(newPlacement)
					require.NoError(t, err)
					require.True(t, marked)

					// Verify the placement after removal
					require.NoError(t, placement.Validate(newPlacement))

					currentPlacement = newPlacement
				}

				// Final validation after all removals
				require.NoError(t, placement.Validate(currentPlacement))
				require.NoError(t, validateSubClusteredPlacement(currentPlacement))

				_, globalMaxSkew, subclustersWithMaxSkewGTTwo = getMaxShardDiffInSubclusters(currentPlacement)
				t.Logf("Maximum shard difference after removals: %d (subcluster %d)", globalMaxSkew, subclustersWithMaxSkewGTTwo)

				// Calculate node triplet shard analysis after removals
				tripletAnalysis = calculateNodeTripletShardAnalysis(currentPlacement)
				maxPercentage = 0
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
					t.Logf("Maximum shared shard percentage after removals: %.2f%%", maxPercentage)
				}

				// Create new instances to add
				newInstances = make([]placement.Instance, tt.instancesPerSub*tt.subclustersToRemove)
				for i := 0; i < len(newInstances); i++ {
					newInstances[i] = placement.NewInstance().
						SetID(generateRandomInstanceName()).
						SetIsolationGroup(fmt.Sprintf("R%d", (i)%tt.rf)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d", i)).
						SetShards(shard.NewShards(nil))
				}

				for i := 0; i < len(newInstances); i++ {
					newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{newInstances[i]})
					require.NoError(t, err)
					require.NotNil(t, newPlacement)
					newPlacement, marked, err = algo.MarkAllShardsAvailable(newPlacement)
					require.NoError(t, err)
					require.True(t, marked)
					currentPlacement = newPlacement
					instanceCount++
					if instanceCount%tt.instancesPerSub == 0 {
						t.Logf("Added %d instances", instanceCount)
						require.NoError(t, placement.Validate(currentPlacement))
						require.NoError(t, validateSubClusteredPlacement(currentPlacement))
						// Get max shard differences before rebalancing
						_, globalMaxSkew, subclustersWithMaxSkewGTTwo := getMaxShardDiffInSubclusters(currentPlacement)
						// Find the maximum skew and its subcluster ID
						t.Logf("Maximum shard difference before rebalancing: %d (subcluster %d)", globalMaxSkew, subclustersWithMaxSkewGTTwo)

						// Calculate node triplet shard analysis after re-adding instances
						tripletAnalysis := calculateNodeTripletShardAnalysis(currentPlacement)
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
							t.Logf("Maximum shared shard percentage after re-adding: %.2f%%", maxPercentage)
						}
					}
				}
				tripletAnalysis = calculateNodeTripletShardAnalysis(currentPlacement)

				// Analyze percentage distribution with bucketing
				buckets, maxPercentage, minPercentage, avgPercentage = analyzeTripletPercentageDistribution(tripletAnalysis)

				// Count total triplets
				totalTriplets = 0
				for _, count := range buckets {
					totalTriplets += count
				}

				t.Logf("=== FINAL TRIPLET ANALYSIS RESULTS ===")
				t.Logf("Total triplets analyzed: %d", totalTriplets)
				t.Logf("Maximum shared shard percentage: %.2f%%", maxPercentage)
				t.Logf("Minimum shared shard percentage: %.2f%%", minPercentage)
				t.Logf("Average shared shard percentage: %.2f%%", avgPercentage)

				t.Logf("=== TRIPLET SHARING PERCENTAGE DISTRIBUTION ===")
				bucketOrder = []string{"0-10%", "10-20%", "20-30%", "30-40%", "40-50%", "50-60%", "60-70%", "70-80%", "80-90%", "90-100%"}
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
