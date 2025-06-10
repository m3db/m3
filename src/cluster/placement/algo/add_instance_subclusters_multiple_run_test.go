package algo

import (
	"fmt"
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/stretchr/testify/require"
)

func TestSubclusteredV2AddInstancesMultipleRuns(t *testing.T) {
	tests := []struct {
		name                          string
		rf                            int
		instancesPerSub               int
		subclustersToAdd              int
		shards                        int
		subclusterToAddAfterRebalance int
		timesToRun                    int
	}{
		{
			name:                          "RF=3, 6 instances per subcluster, start with 12 add 6",
			rf:                            3,
			instancesPerSub:               6,
			subclustersToAdd:              28,
			shards:                        8192,
			subclusterToAddAfterRebalance: 2,
			timesToRun:                    1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Statistics tracking across multiple runs
			maxSkewsAfterRebalance := make([]int, 0, tt.timesToRun)
			maxSkewsAfterFinalAddition := make([]int, 0, tt.timesToRun)

			t.Logf("Running test %d times to collect skew statistics", tt.timesToRun)

			for run := 0; run < tt.timesToRun; run++ {
				// Create initial test instances
				instances := make([]placement.Instance, tt.instancesPerSub)
				for i := 0; i < tt.instancesPerSub; i++ {
					instances[i] = placement.NewInstance().
						SetID(fmt.Sprintf("I%d_%d", i, run)). // Add run ID to ensure uniqueness
						SetIsolationGroup(fmt.Sprintf("R%d", i%tt.rf)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d_%d", i, run)).
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
						SetID(fmt.Sprintf("I%d_%d", tt.instancesPerSub+i, run)).
						SetIsolationGroup(fmt.Sprintf("R%d", (tt.instancesPerSub+i)%tt.rf)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d_%d", tt.instancesPerSub+i, run)).
						SetShards(shard.NewShards(nil))
				}

				// Add instances one by one
				currentPlacement := p

				for i := 0; i < len(newInstances); i++ {
					newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{newInstances[i]})
					require.NoError(t, err)
					require.NotNil(t, newPlacement)
					currentPlacement = newPlacement
				}

				// Verify the placement after addition
				require.NoError(t, placement.Validate(currentPlacement))

				currentPlacement, marked, err = algo.MarkAllShardsAvailable(currentPlacement)
				require.NoError(t, err)
				require.True(t, marked)

				require.NoError(t, validateSubClusteredPlacement(currentPlacement))

				// Rebalance
				balancedplacement := currentPlacement.Clone()

				balancedplacement, err = algo.BalanceShards(balancedplacement)
				require.NoError(t, err)
				require.NotNil(t, balancedplacement)

				balancedplacement, _, err = algo.MarkAllShardsAvailable(balancedplacement)
				require.NoError(t, err)

				// Get max shard differences after rebalancing (suppress detailed output for multiple runs)
				subclusterSkewsAfter := make(map[uint32]int)
				if run == 0 {
					// Show detailed output only for first run
					subclusterSkewsAfter = getMaxShardDiffInSubclusters(balancedplacement)
				} else {
					// Calculate skews without printing for other runs
					subclusterShardCounts := make(map[uint32]map[string]int)
					for _, instance := range balancedplacement.Instances() {
						subclusterID := instance.SubClusterID()
						if _, exists := subclusterShardCounts[subclusterID]; !exists {
							subclusterShardCounts[subclusterID] = make(map[string]int)
						}
						subclusterShardCounts[subclusterID][instance.ID()] = instance.Shards().NumShards()
					}

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
						subclusterSkewsAfter[subclusterID] = maxCount - minCount
					}
				}

				// Find the maximum skew after rebalancing
				var maxAfterDiff int
				for _, skew := range subclusterSkewsAfter {
					if skew > maxAfterDiff {
						maxAfterDiff = skew
					}
				}
				maxSkewsAfterRebalance = append(maxSkewsAfterRebalance, maxAfterDiff)
				if maxAfterDiff > 2 {
					t.Logf("Run %d: Maximum skew after rebalancing: %d", run+1, maxAfterDiff)
				}

				// Final validation after rebalancing
				require.NoError(t, placement.Validate(balancedplacement))
				require.NoError(t, validateSubClusteredPlacement(balancedplacement))

				// Add additional subclusters if specified
				if tt.subclusterToAddAfterRebalance > 0 {
					instancesToAdd := make([]placement.Instance, tt.instancesPerSub*tt.subclusterToAddAfterRebalance)
					for i := 0; i < (tt.instancesPerSub * tt.subclusterToAddAfterRebalance); i++ {
						instancesToAdd[i] = placement.NewInstance().
							SetID(fmt.Sprintf("RI%d_%d", tt.instancesPerSub+i, run)).
							SetIsolationGroup(fmt.Sprintf("R%d", (tt.instancesPerSub+i)%tt.rf)).
							SetWeight(1).
							SetEndpoint(fmt.Sprintf("E%d_%d", tt.instancesPerSub+i, run)).
							SetShards(shard.NewShards(nil))
					}
					newPlacement, err := algo.AddInstances(balancedplacement, instancesToAdd)
					require.NoError(t, err)
					require.NotNil(t, newPlacement)
					balancedplacement = newPlacement

					balancedplacement, marked, err = algo.MarkAllShardsAvailable(balancedplacement)
					require.NoError(t, err)
					require.True(t, marked)

					require.NoError(t, validateSubClusteredPlacement(balancedplacement))

					// Calculate final skews (suppress detailed output for multiple runs)
					subclusterSkewsFinal := make(map[uint32]int)
					if run == 0 {
						// Show detailed output only for first run
						subclusterSkewsFinal = getMaxShardDiffInSubclusters(balancedplacement)
					} else {
						// Calculate skews without printing for other runs
						subclusterShardCounts := make(map[uint32]map[string]int)
						for _, instance := range balancedplacement.Instances() {
							subclusterID := instance.SubClusterID()
							if _, exists := subclusterShardCounts[subclusterID]; !exists {
								subclusterShardCounts[subclusterID] = make(map[string]int)
							}
							subclusterShardCounts[subclusterID][instance.ID()] = instance.Shards().NumShards()
						}

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
							subclusterSkewsFinal[subclusterID] = maxCount - minCount
						}
					}

					// Find the maximum skew after final addition
					var maxFinalDiff int
					for _, skew := range subclusterSkewsFinal {
						if skew > maxFinalDiff {
							maxFinalDiff = skew
						}
					}
					maxSkewsAfterFinalAddition = append(maxSkewsAfterFinalAddition, maxFinalDiff)
					if maxFinalDiff > 2 {
						t.Logf("Run %d: Maximum skew after final addition: %d", run+1, maxFinalDiff)
					}
				}
			}

			// Calculate and report statistics
			t.Logf("\n=== STATISTICS ACROSS %d RUNS ===", tt.timesToRun)

			// Statistics for skew after rebalancing
			if len(maxSkewsAfterRebalance) > 0 {
				sumAfterRebalance := 0
				maxAfterRebalance := 0
				for _, skew := range maxSkewsAfterRebalance {
					sumAfterRebalance += skew
					if skew > maxAfterRebalance {
						maxAfterRebalance = skew
					}
				}
				avgAfterRebalance := float64(sumAfterRebalance) / float64(len(maxSkewsAfterRebalance))

				t.Logf("After Rebalancing:")
				t.Logf("  Average maximum skew: %.2f", avgAfterRebalance)
				t.Logf("  Maximum value of maximum skew: %d", maxAfterRebalance)
			}

			// Statistics for skew after final addition
			if len(maxSkewsAfterFinalAddition) > 0 {
				sumAfterFinal := 0
				maxAfterFinal := 0
				for _, skew := range maxSkewsAfterFinalAddition {
					sumAfterFinal += skew
					if skew > maxAfterFinal {
						maxAfterFinal = skew
					}
				}
				avgAfterFinal := float64(sumAfterFinal) / float64(len(maxSkewsAfterFinalAddition))

				t.Logf("After Final Addition:")
				t.Logf("  Average maximum skew: %.2f", avgAfterFinal)
				t.Logf("  Maximum value of maximum skew: %d", maxAfterFinal)
			}

			t.Logf("===============================")
		})
	}
}
