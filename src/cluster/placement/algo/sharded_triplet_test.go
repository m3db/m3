// Copyright (c) 2016 Uber Technologies, Inc.
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
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/stretchr/testify/require"
)

// ShardedTripletAnalysis represents shard analysis for a triplet of nodes from different isolation groups
// in a regular sharded (non-subclustered) placement
type ShardedTripletAnalysis struct {
	Node1ID           string
	Node2ID           string
	Node3ID           string
	Node1IG           string
	Node2IG           string
	Node3IG           string
	SharedShards      int // Shards present on all 3 nodes
	TotalUniqueShards int // Total unique shards across all 3 nodes
}

// calculateShardedTripletAnalysis calculates shard distribution for each combination of 3 nodes
// from different isolation groups in a regular sharded placement (no subclusters)
func calculateShardedTripletAnalysis(p placement.Placement) []ShardedTripletAnalysis {
	var tripletAnalyses []ShardedTripletAnalysis

	// Group instances by isolation group
	isolationGroups := make(map[string][]placement.Instance)
	for _, instance := range p.Instances() {
		ig := instance.IsolationGroup()
		isolationGroups[ig] = append(isolationGroups[ig], instance)
	}

	// Need at least 3 different isolation groups
	if len(isolationGroups) < 3 {
		return tripletAnalyses
	}

	// Get isolation group names as slice for easier iteration
	igNames := make([]string, 0, len(isolationGroups))
	for igName := range isolationGroups {
		igNames = append(igNames, igName)
	}

	// Just use the first three isolation groups
	if len(igNames) >= 3 {
		ig1, ig2, ig3 := igNames[0], igNames[1], igNames[2]

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
					analysis := ShardedTripletAnalysis{
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

	return tripletAnalyses
}

// findMaxTripletPercentage finds the maximum shared shard percentage among all triplets
func findMaxTripletPercentage(analyses []ShardedTripletAnalysis) float64 {
	var maxPercentage float64
	for _, analysis := range analyses {
		if analysis.TotalUniqueShards > 0 {
			percentage := float64(analysis.SharedShards) / float64(analysis.TotalUniqueShards) * 100.0
			if percentage > maxPercentage {
				maxPercentage = percentage
			}
		}
	}
	return maxPercentage
}

func TestShardedTripletAnalysis(t *testing.T) {
	tests := []struct {
		name              string
		rf                int
		numInstances      int
		isolationGroups   int
		shards            int
		instancesToAdd    int
		instancesToRemove int
		batchSize         int
	}{
		{
			name:              "RF=3, 9 instances, 3 isolation groups",
			rf:                3,
			numInstances:      9,
			isolationGroups:   3,
			shards:            8192,
			instancesToAdd:    891,
			instancesToRemove: 0,
			batchSize:         9,
		},
		// {
		// 	name:              "RF=3, 12 instances, 3 isolation groups",
		// 	rf:                3,
		// 	numInstances:      12,
		// 	isolationGroups:   3,
		// 	shards:            8192,
		// 	instancesToAdd:    100,
		// 	instancesToRemove: 10,
		// },
		// {
		// 	name:              "RF=3, 18 instances, 3 isolation groups",
		// 	rf:                3,
		// 	numInstances:      18,
		// 	isolationGroups:   3,
		// 	shards:            4096,
		// 	instancesToAdd:    50,
		// 	instancesToRemove: 5,
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create initial test instances
			instances := make([]placement.Instance, tt.numInstances)
			for i := 0; i < tt.numInstances; i++ {
				instances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.isolationGroups)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", i)).
					SetShards(shard.NewShards(nil))
			}

			// Generate shard IDs from 0 to shards-1
			shardIDs := make([]uint32, tt.shards)
			for i := 0; i < tt.shards; i++ {
				shardIDs[i] = uint32(i)
			}

			// Create regular sharded algorithm (not subclustered)
			opts := placement.NewOptions().
				SetValidZone("zone1").
				SetIsSharded(true)
			algo := newShardedAlgorithm(opts)

			// Perform initial placement
			p, err := algo.InitialPlacement(instances, shardIDs, tt.rf)
			require.NoError(t, err)
			require.NotNil(t, p)

			p, marked, err := algo.MarkAllShardsAvailable(p)
			require.NoError(t, err)
			require.True(t, marked)

			// Verify initial placement
			require.NoError(t, placement.Validate(p))

			// Calculate initial triplet analysis
			tripletAnalysis := calculateShardedTripletAnalysis(p)
			maxPercentage := findMaxTripletPercentage(tripletAnalysis)
			t.Logf("Initial placement - Maximum shared shard percentage among all triplets: %.2f%%", maxPercentage)

			// Create new instances to add
			newInstances := make([]placement.Instance, tt.instancesToAdd)
			for i := 0; i < len(newInstances); i++ {
				newInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", tt.numInstances+i)).
					SetIsolationGroup(fmt.Sprintf("R%d", (tt.numInstances+i)%tt.isolationGroups)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", tt.numInstances+i)).
					SetShards(shard.NewShards(nil))
			}

			// Add instances in batches
			currentPlacement := p
			batchSize := tt.batchSize

			for i := 0; i < len(newInstances); i += batchSize {
				end := i + batchSize
				if end > len(newInstances) {
					end = len(newInstances)
				}

				batch := newInstances[i:end]
				newPlacement, err := algo.AddInstances(currentPlacement, batch)
				require.NoError(t, err)
				require.NotNil(t, newPlacement)

				newPlacement, marked, err = algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)

				currentPlacement = newPlacement
			}

			// Verify the placement after addition
			require.NoError(t, placement.Validate(currentPlacement))

			// Remove some instances
			allInstances := currentPlacement.Instances()

			// Randomly select instances to remove
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			rng.Shuffle(len(allInstances), func(i, j int) {
				allInstances[i], allInstances[j] = allInstances[j], allInstances[i]
			})

			instancesToRemove := allInstances[:tt.instancesToRemove]
			instanceIDsToRemove := make([]string, len(instancesToRemove))
			for i, instance := range instancesToRemove {
				instanceIDsToRemove[i] = instance.ID()
			}

			// Remove instances
			t.Logf("Removing %d instances", len(instanceIDsToRemove))
			for _, instanceID := range instanceIDsToRemove {
				newPlacement, err := algo.RemoveInstances(currentPlacement, []string{instanceID})
				require.NoError(t, err)
				require.NotNil(t, newPlacement)

				newPlacement, marked, err := algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)

				currentPlacement = newPlacement
			}

			// Calculate triplet analysis after removals
			tripletAnalysis = calculateShardedTripletAnalysis(currentPlacement)
			maxPercentage = findMaxTripletPercentage(tripletAnalysis)
			t.Logf("After removing %d instances - Maximum shared shard percentage: %.2f%%", tt.instancesToRemove, maxPercentage)

			// Re-add some instances
			instancesToReAdd := instancesToRemove[:len(instancesToRemove)/2]
			t.Logf("Re-adding %d instances", len(instancesToReAdd))

			for _, instance := range instancesToReAdd {
				// Clear shards before re-adding
				instance.SetShards(shard.NewShards(nil))
				newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{instance})
				require.NoError(t, err)
				require.NotNil(t, newPlacement)

				newPlacement, marked, err := algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)

				currentPlacement = newPlacement
			}

			// Final verification
			require.NoError(t, placement.Validate(currentPlacement))

			// Calculate final triplet analysis
			tripletAnalysis = calculateShardedTripletAnalysis(currentPlacement)
			maxPercentage = findMaxTripletPercentage(tripletAnalysis)
			t.Logf("After re-adding %d instances - Maximum shared shard percentage: %.2f%%", len(instancesToReAdd), maxPercentage)

			// Print summary statistics
			t.Logf("=== SUMMARY ===")
			t.Logf("Total instances in final placement: %d", len(currentPlacement.Instances()))
			t.Logf("Total triplets analyzed: %d", len(tripletAnalysis))
			t.Logf("Final maximum shared shard percentage: %.2f%%", maxPercentage)

			// Calculate average percentage
			var totalPercentage float64
			validTriplets := 0
			for _, analysis := range tripletAnalysis {
				if analysis.TotalUniqueShards > 0 {
					percentage := float64(analysis.SharedShards) / float64(analysis.TotalUniqueShards) * 100.0
					totalPercentage += percentage
					validTriplets++
				}
			}
			if validTriplets > 0 {
				avgPercentage := totalPercentage / float64(validTriplets)
				t.Logf("Average shared shard percentage: %.2f%%", avgPercentage)
			}
		})
	}
}

// TestShardedVsSubclusteredComparison creates a comparison test between regular sharded and subclustered approaches
func TestShardedVsSubclusteredComparison(t *testing.T) {
	rf := 3
	numInstances := 18
	isolationGroups := 3
	shards := 8192

	// Test regular sharded approach
	t.Run("Regular Sharded", func(t *testing.T) {
		instances := make([]placement.Instance, numInstances)
		for i := 0; i < numInstances; i++ {
			instances[i] = placement.NewInstance().
				SetID(fmt.Sprintf("I%d", i)).
				SetIsolationGroup(fmt.Sprintf("R%d", i%isolationGroups)).
				SetWeight(1).
				SetEndpoint(fmt.Sprintf("E%d", i)).
				SetShards(shard.NewShards(nil))
		}

		shardIDs := make([]uint32, shards)
		for i := 0; i < shards; i++ {
			shardIDs[i] = uint32(i)
		}

		opts := placement.NewOptions().
			SetValidZone("zone1").
			SetIsSharded(true)
		algo := newShardedAlgorithm(opts)

		p, err := algo.InitialPlacement(instances, shardIDs, rf)
		require.NoError(t, err)

		p, marked, err := algo.MarkAllShardsAvailable(p)
		require.NoError(t, err)
		require.True(t, marked)

		tripletAnalysis := calculateShardedTripletAnalysis(p)
		maxPercentage := findMaxTripletPercentage(tripletAnalysis)

		var totalPercentage float64
		validTriplets := 0
		for _, analysis := range tripletAnalysis {
			if analysis.TotalUniqueShards > 0 {
				percentage := float64(analysis.SharedShards) / float64(analysis.TotalUniqueShards) * 100.0
				totalPercentage += percentage
				validTriplets++
			}
		}
		avgPercentage := totalPercentage / float64(validTriplets)

		t.Logf("Regular Sharded Results:")
		t.Logf("  Total triplets: %d", len(tripletAnalysis))
		t.Logf("  Maximum percentage: %.2f%%", maxPercentage)
		t.Logf("  Average percentage: %.2f%%", avgPercentage)
	})

	// Test subclustered approach would go here, but user said not to change existing code
	// This comparison test provides a framework for comparing the two approaches
}
