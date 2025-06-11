package algo

import (
	"context"
	"fmt"
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"golang.org/x/sync/errgroup"
)

func TestSubclusteredV2AddInstancesMultipleRuns(t *testing.T) {
	tests := []struct {
		name             string
		rf               int
		instancesPerSub  int
		subclustersToAdd int
		shards           int
		timesToRun       int
	}{
		{
			name:             "RF=3, 6 instances per subcluster, start with 12 add 6",
			rf:               3,
			instancesPerSub:  6,
			subclustersToAdd: 28,
			shards:           16384,
			timesToRun:       2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Statistics tracking across multiple runs
			maxSkewsAfterAddition := make([]int, tt.timesToRun)
			maxClusterWithSkewGTTwo := make([]int, tt.timesToRun)

			// Create errgroup with context
			g, ctx := errgroup.WithContext(context.Background())

			// Semaphore to limit concurrent goroutines to 100
			semaphore := make(chan struct{}, 100)

			t.Logf("Running test %d times in parallel (max 100 concurrent) to collect skew statistics", tt.timesToRun)

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

					// Create new instances to add
					newInstances := make([]placement.Instance, tt.instancesPerSub)
					for i := 0; i < len(newInstances); i++ {
						newInstances[i] = placement.NewInstance().
							SetID(fmt.Sprintf("R%d_%d", tt.instancesPerSub+i, run)).
							SetIsolationGroup(fmt.Sprintf("R%d", (tt.instancesPerSub+i)%tt.rf)).
							SetWeight(1).
							SetEndpoint(fmt.Sprintf("E%d_%d", tt.instancesPerSub+i, run)).
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
				t.Logf("  Average maximum cluster with skew > 2: %.2f", avgAfterFinal)
				t.Logf("  Maximum value of maximum cluster with skew > 2: %d", maxAfterFinal)
			}
			t.Logf("===============================")
		})
	}
}
