package algo

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"golang.org/x/sync/errgroup"
)

func TestSubclusteredV2AddInstancesMultipleRuns(t *testing.T) {
	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	tests := []struct {
		name                                  string
		rf                                    int
		instancesPerSub                       int
		subclustersToAdd                      int
		subclustersToAddAfterInitialPlacement int
		shards                                int
		timesToRun                            int
	}{
		{
			name:                                  "RF=3, 9 instances per subcluster, start with 9 add 8192 shards",
			rf:                                    3,
			instancesPerSub:                       9,
			subclustersToAdd:                      18,
			subclustersToAddAfterInitialPlacement: 4,
			shards:                                16384,
			timesToRun:                            10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Statistics tracking across multiple runs
			maxSkewsAfterAddition := make([]int, tt.timesToRun)
			maxClusterWithSkewGTTwo := make([]int, tt.timesToRun)
			maxPercentageAfterAddition := make([]float64, tt.timesToRun)

			// Create errgroup with context
			g, ctx := errgroup.WithContext(context.Background())

			// Semaphore to limit concurrent goroutines to 100
			semaphore := make(chan struct{}, 2)

			t.Logf("Running test %d times in parallel (max 2 concurrent) to collect skew statistics", tt.timesToRun)

			for run := 0; run < tt.timesToRun; run++ {
				run := run // Capture loop variable
				g.Go(func() error {
					// Acquire semaphore
					select {
					case semaphore <- struct{}{}:
						defer func() { <-semaphore }()
					case <-ctx.Done():
						return ctx.Err()
					}

					// Create initial test instances
					instances := make([]placement.Instance, tt.instancesPerSub*tt.subclustersToAdd)
					for i := 0; i < len(instances); i++ {
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
					if err != nil {
						return fmt.Errorf("run %d: InitialPlacement failed: %v", run+1, err)
					}
					if p == nil {
						return fmt.Errorf("run %d: InitialPlacement returned nil", run+1)
					}

					p, marked, err := algo.MarkAllShardsAvailable(p)
					if err != nil {
						return fmt.Errorf("run %d: MarkAllShardsAvailable failed: %v", run+1, err)
					}
					if !marked {
						return fmt.Errorf("run %d: MarkAllShardsAvailable returned false", run+1)
					}

					// Verify initial placement
					if err := placement.Validate(p); err != nil {
						return fmt.Errorf("run %d: placement validation failed: %v", run+1, err)
					}
					if err := validateSubClusteredPlacement(p); err != nil {
						return fmt.Errorf("run %d: subcluster placement validation failed: %v", run+1, err)
					}

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

					// Create new instances to add
					newInstances := make([]placement.Instance, tt.instancesPerSub*tt.subclustersToAddAfterInitialPlacement)
					initialInstanceCount := tt.instancesPerSub * tt.subclustersToAdd
					for i := 0; i < len(newInstances); i++ {
						newInstances[i] = placement.NewInstance().
							SetID(fmt.Sprintf("I%d", initialInstanceCount+i)).
							SetIsolationGroup(fmt.Sprintf("R%d", (initialInstanceCount+i)%tt.rf)).
							SetWeight(1).
							SetEndpoint(fmt.Sprintf("E%d", initialInstanceCount+i)).
							SetShards(shard.NewShards(nil))
					}

					// Add instances one by one
					currentPlacement := p
					for i := 0; i < len(newInstances); i++ {
						newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{newInstances[i]})
						if err != nil {
							return fmt.Errorf("run %d: AddInstances failed: %v", run+1, err)
						}
						if newPlacement == nil {
							return fmt.Errorf("run %d: AddInstances returned nil", run+1)
						}
						newPlacement, marked, err = algo.MarkAllShardsAvailable(newPlacement)
						if err != nil {
							return fmt.Errorf("run %d: MarkAllShardsAvailable failed: %v", run+1, err)
						}
						if !marked {
							return fmt.Errorf("run %d: MarkAllShardsAvailable returned false", run+1)
						}
						currentPlacement = newPlacement
					}

					// Verify the placement after addition
					if err := placement.Validate(currentPlacement); err != nil {
						return fmt.Errorf("run %d: placement validation after addition failed: %v", run+1, err)
					}

					if err := validateSubClusteredPlacement(currentPlacement); err != nil {
						return fmt.Errorf("run %d: subcluster placement validation after addition failed: %v", run+1, err)
					}

					_, globalMaxSkew, subclustersWithMaxSkewGTTwo := getMaxShardDiffInSubclusters(currentPlacement)
					// Find the maximum skew and its subcluster ID
					maxSkewsAfterAddition[run] = globalMaxSkew
					maxClusterWithSkewGTTwo[run] = subclustersWithMaxSkewGTTwo

					tripletAnalysis = calculateNodeTripletShardAnalysis(currentPlacement)

					// Analyze percentage distribution with bucketing
					buckets, maxPercentage, minPercentage, avgPercentage := analyzeTripletPercentageDistribution(tripletAnalysis)
					maxPercentageAfterAddition[run] = maxPercentage

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

					return nil
				})
			}

			// Wait for all goroutines to complete and check for errors
			if err := g.Wait(); err != nil {
				t.Fatalf("Test failed: %v", err)
			}

			// Calculate and report statistics
			t.Logf("\n=== STATISTICS ACROSS %d RUNS ===", tt.timesToRun)

			// Statistics for skew after final addition
			if len(maxSkewsAfterAddition) > 0 {
				sumAfterFinal := 0
				maxAfterFinal := 0
				clusterWithSkewLTTwo := 0
				for _, skew := range maxSkewsAfterAddition {
					sumAfterFinal += skew
					if skew > maxAfterFinal {
						maxAfterFinal = skew
					}
					if skew <= 2 {
						clusterWithSkewLTTwo++
					}
				}
				avgAfterFinal := float64(sumAfterFinal) / float64(len(maxSkewsAfterAddition))

				t.Logf("After Final Addition:")
				t.Logf("  Average maximum skew: %.2f", avgAfterFinal)
				t.Logf("  Maximum value of maximum skew: %d", maxAfterFinal)
			}

			if len(maxClusterWithSkewGTTwo) > 0 {
				sumAfterFinal := 0
				maxAfterFinal := 0
				for _, skew := range maxClusterWithSkewGTTwo {
					sumAfterFinal += skew
					if skew > maxAfterFinal {
						maxAfterFinal = skew
					}
				}
				avgAfterFinal := float64(sumAfterFinal) / float64(len(maxClusterWithSkewGTTwo))

				t.Logf("After Final Addition:")
				t.Logf("  Average maximum cluster with skew > 3: %.2f", avgAfterFinal)
				t.Logf("  Maximum value of maximum cluster with skew > 3: %d", maxAfterFinal)
			}

			if len(maxPercentageAfterAddition) > 0 {
				sumAfterFinal := 0.0
				maxAfterFinal := 0.0
				for _, percentage := range maxPercentageAfterAddition {
					sumAfterFinal += percentage
					if percentage > maxAfterFinal {
						maxAfterFinal = percentage
					}
				}
				avgAfterFinal := sumAfterFinal / float64(len(maxPercentageAfterAddition))

				t.Logf("After Final Addition:")
				t.Logf("  Average maximum shared shard percentage: %.2f%%", avgAfterFinal)
				t.Logf("  Maximum value of maximum shared shard percentage: %.2f%%", maxAfterFinal)
			}

			t.Logf("===============================")
		})
	}
}
