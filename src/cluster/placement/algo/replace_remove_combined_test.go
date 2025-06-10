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

func TestSubclusteredV2RemovePartialSubclusterThenReplaceOthers(t *testing.T) {
	tests := []struct {
		name              string
		rf                int
		instancesPerSub   int
		totalInstances    int
		shards            int
		instancesToRemove int // Instances to remove from one subcluster (not all)
	}{
		{
			name:              "RF=3, 6 instances per subcluster, 36 total instances, remove 3 from one subcluster then replace in others",
			rf:                3,
			instancesPerSub:   6,
			totalInstances:    36,
			shards:            256,
			instancesToRemove: 4, // Remove half the instances from one subcluster
		},
		{
			name:              "RF=3, 9 instances per subcluster, 36 total instances, remove 4 from one subcluster then replace in others",
			rf:                3,
			instancesPerSub:   9,
			totalInstances:    36,
			shards:            256,
			instancesToRemove: 4, // Remove less than half from one subcluster
		},
		{
			name:              "RF=3, 12 instances per subcluster, 48 total instances, remove 6 from one subcluster then replace in others",
			rf:                3,
			instancesPerSub:   12,
			totalInstances:    48,
			shards:            512,
			instancesToRemove: 6, // Remove half the instances from one subcluster
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Seed random number generator for consistent test behavior
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))

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
				SetHasSubClusters(true).
				SetAllowPartialReplace(false)
			algo := newSubclusteredShardedAlgorithm(opts)

			// Perform initial placement
			p, err := algo.InitialPlacement(instances, shardIDs, tt.rf)
			require.NoError(t, err)
			require.NotNil(t, p)

			// Verify initial placement
			currentPlacement, marked, err := algo.MarkAllShardsAvailable(p)
			require.NoError(t, err)
			require.True(t, marked)
			require.NoError(t, placement.Validate(currentPlacement))

			t.Logf("Initial placement created with %d instances across %d subclusters",
				len(currentPlacement.Instances()), len(getSubclusterMap(currentPlacement)))

			// PHASE 1: Remove some instances from one subcluster (not all)
			// Get all subcluster IDs and their instances
			subClusterMap := getSubclusterMap(currentPlacement)
			require.True(t, len(subClusterMap) >= 2, "Need at least 2 subclusters for test")

			// Select a random subcluster to partially remove instances from
			subClusterIDs := make([]uint32, 0, len(subClusterMap))
			for id := range subClusterMap {
				subClusterIDs = append(subClusterIDs, id)
			}
			targetSubClusterID := subClusterIDs[rng.Intn(len(subClusterIDs))]
			targetSubClusterInstances := subClusterMap[targetSubClusterID]

			t.Logf("Selected subcluster %d with %d instances for partial removal",
				targetSubClusterID, len(targetSubClusterInstances))

			// Ensure we don't remove all instances from the subcluster
			require.True(t, len(targetSubClusterInstances) > tt.instancesToRemove,
				"Cannot remove more instances than available in subcluster")
			require.True(t, len(targetSubClusterInstances)-tt.instancesToRemove > 0,
				"Must leave at least one instance in the subcluster")

			// Randomly select instances to remove from the target subcluster
			instancesToRemove := make([]string, tt.instancesToRemove)
			rng.Shuffle(len(targetSubClusterInstances), func(i, j int) {
				targetSubClusterInstances[i], targetSubClusterInstances[j] =
					targetSubClusterInstances[j], targetSubClusterInstances[i]
			})
			for i := 0; i < tt.instancesToRemove; i++ {
				instancesToRemove[i] = targetSubClusterInstances[i]
			}

			t.Logf("Removing %d instances from subcluster %d: %v",
				len(instancesToRemove), targetSubClusterID, instancesToRemove)

			// Remove instances one by one
			for i, instanceID := range instancesToRemove {
				t.Logf("Removing instance %s (%d/%d)", instanceID, i+1, len(instancesToRemove))

				newPlacement, err := algo.RemoveInstances(currentPlacement, []string{instanceID})
				require.NoError(t, err)
				require.NotNil(t, newPlacement)

				newPlacement, marked, err := algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)
				require.NoError(t, placement.Validate(newPlacement))

				currentPlacement = newPlacement
			}

			t.Logf("Successfully removed %d instances, remaining instances: %d",
				len(instancesToRemove), len(currentPlacement.Instances()))

			// PHASE 2: Replace instances in all OTHER subclusters
			// Get updated subcluster map after removals
			updatedSubClusterMap := getSubclusterMap(currentPlacement)

			// Collect instances to replace from all subclusters except the one we partially removed from
			var allInstancesToReplace []string
			var allReplacementInstances []placement.Instance
			replacementCounter := 0

			for subClusterID, instanceIDs := range updatedSubClusterMap {
				if subClusterID == targetSubClusterID {
					// Skip the subcluster we partially removed instances from
					continue
				}

				// Select one instance from each of the other subclusters for replacement
				instanceToReplace := instanceIDs[0] // Take first instance from each subcluster
				allInstancesToReplace = append(allInstancesToReplace, instanceToReplace)

				// Create replacement instance with same isolation group
				originalInstance, exists := currentPlacement.Instance(instanceToReplace)
				require.True(t, exists, "Original instance should exist")

				replacementInstance := placement.NewInstance().
					SetID(fmt.Sprintf("R%d", replacementCounter)).
					SetIsolationGroup(originalInstance.IsolationGroup()).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("RE%d", replacementCounter)).
					SetShards(shard.NewShards(nil))
				allReplacementInstances = append(allReplacementInstances, replacementInstance)
				replacementCounter++
			}

			t.Logf("Replacing %d instances across %d subclusters (excluding subcluster %d)",
				len(allInstancesToReplace), len(updatedSubClusterMap)-1, targetSubClusterID)

			// Perform replacements all at once
			if len(allInstancesToReplace) > 0 {
				finalPlacement, err := algo.ReplaceInstances(currentPlacement, allInstancesToReplace, allReplacementInstances)
				require.NoError(t, err)
				require.NotNil(t, finalPlacement)

				finalPlacement, marked, err = algo.MarkAllShardsAvailable(finalPlacement)
				require.NoError(t, err)
				require.True(t, marked)
				require.NoError(t, placement.Validate(finalPlacement))

				t.Logf("Successfully replaced %d instances", len(allInstancesToReplace))

				// Verify replacements were successful
				for i, originalID := range allInstancesToReplace {
					// Original instance should be gone
					_, exists := finalPlacement.Instance(originalID)
					require.False(t, exists, "Original instance %s should not exist after replacement", originalID)

					// Replacement instance should exist
					replacementID := allReplacementInstances[i].ID()
					_, exists = finalPlacement.Instance(replacementID)
					require.True(t, exists, "Replacement instance %s should exist", replacementID)
				}

				currentPlacement = finalPlacement
			}

			// PHASE 3: Remove the remaining instances from the target subcluster
			// Get updated subcluster map after replacements
			finalSubClusterMap := getSubclusterMap(currentPlacement)

			// Find remaining instances in the target subcluster
			remainingInstancesInTarget, exists := finalSubClusterMap[targetSubClusterID]
			require.True(t, exists, "Target subcluster should still exist after replacements")

			if len(remainingInstancesInTarget) > 0 {
				t.Logf("Removing remaining %d instances from target subcluster %d: %v",
					len(remainingInstancesInTarget), targetSubClusterID, remainingInstancesInTarget)

				// Remove remaining instances one by one
				for i, instanceID := range remainingInstancesInTarget {
					t.Logf("Removing remaining instance %s (%d/%d)", instanceID, i+1, len(remainingInstancesInTarget))

					newPlacement, err := algo.RemoveInstances(currentPlacement, []string{instanceID})
					require.NoError(t, err)
					require.NotNil(t, newPlacement)

					newPlacement, marked, err := algo.MarkAllShardsAvailable(newPlacement)
					require.NoError(t, err)
					require.True(t, marked)
					require.NoError(t, placement.Validate(newPlacement))

					currentPlacement = newPlacement
				}

				t.Logf("Successfully removed all remaining instances from target subcluster %d", targetSubClusterID)
			}

			// Final validation after all operations
			require.NoError(t, placement.Validate(currentPlacement))
			printPlacementAndValidate(t, currentPlacement)
		})
	}
}

func TestSubclusteredV2AddReplicaWithDifferentIsolationGroups(t *testing.T) {
	tests := []struct {
		name            string
		initialRF       int
		targetRF        int
		instancesPerSub int
		totalInstances  int
		shards          int
		isolationGroups []string
	}{
		{
			name:            "RF=2 to RF=3, 4 instances per subcluster with different isolation groups",
			initialRF:       2,
			targetRF:        3,
			instancesPerSub: 4,
			totalInstances:  12,
			shards:          128,
			isolationGroups: []string{"zone1", "zone2", "zone3", "zone4"},
		},
		{
			name:            "RF=1 to RF=2, 3 instances per subcluster with different isolation groups",
			initialRF:       1,
			targetRF:        2,
			instancesPerSub: 3,
			totalInstances:  9,
			shards:          64,
			isolationGroups: []string{"rack1", "rack2", "rack3"},
		},
		{
			name:            "RF=3 to RF=5, 6 instances per subcluster with different isolation groups",
			initialRF:       3,
			targetRF:        5,
			instancesPerSub: 6,
			totalInstances:  18,
			shards:          256,
			isolationGroups: []string{"az1", "az2", "az3", "az4", "az5", "az6"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Ensure we have enough isolation groups for the instances per subcluster
			require.True(t, len(tt.isolationGroups) >= tt.instancesPerSub,
				"Need at least %d isolation groups for %d instances per subcluster",
				tt.instancesPerSub, tt.instancesPerSub)

			// Create test instances with different isolation groups
			instances := make([]placement.Instance, tt.totalInstances)
			for i := 0; i < tt.totalInstances; i++ {
				// Cycle through isolation groups to ensure different isolation groups within each subcluster
				isolationGroup := tt.isolationGroups[i%len(tt.isolationGroups)]
				instances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", i)).
					SetIsolationGroup(isolationGroup).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", i)).
					SetShards(shard.NewShards(nil))
			}

			// Generate shard IDs
			shardIDs := make([]uint32, tt.shards)
			for i := 0; i < tt.shards; i++ {
				shardIDs[i] = uint32(i)
			}

			// Create algorithm with initial RF
			opts := placement.NewOptions().
				SetValidZone("zone1").
				SetIsSharded(true).
				SetInstancesPerSubCluster(tt.instancesPerSub).
				SetHasSubClusters(true).
				SetAllowPartialReplace(false)
			algo := newSubclusteredShardedAlgorithm(opts)

			// Perform initial placement with initial RF
			initialPlacement, err := algo.InitialPlacement(instances, shardIDs, tt.initialRF)
			require.NoError(t, err)
			require.NotNil(t, initialPlacement)

			// Verify initial placement
			currentPlacement, marked, err := algo.MarkAllShardsAvailable(initialPlacement)
			require.NoError(t, err)
			require.True(t, marked)
			require.NoError(t, placement.Validate(currentPlacement))
			printPlacementAndValidate(t, currentPlacement)

			t.Logf("Initial placement created with RF=%d:", tt.initialRF)
			t.Logf("- Total instances: %d", len(currentPlacement.Instances()))
			t.Logf("- Instances per subcluster: %d", tt.instancesPerSub)
			t.Logf("- Total subclusters: %d", len(getSubclusterMap(currentPlacement)))

			// Verify isolation groups are different within each subcluster
			subClusterMap := getSubclusterMap(currentPlacement)
			for subClusterID, instanceIDs := range subClusterMap {
				isolationGroupsInSubcluster := make(map[string]bool)
				for _, instanceID := range instanceIDs {
					instance, exists := currentPlacement.Instance(instanceID)
					require.True(t, exists, "Instance should exist")
					isolationGroupsInSubcluster[instance.IsolationGroup()] = true
				}
				isolationGroupList := make([]string, 0, len(isolationGroupsInSubcluster))
				for ig := range isolationGroupsInSubcluster {
					isolationGroupList = append(isolationGroupList, ig)
				}
				t.Logf("Subcluster %d has %d different isolation groups: %v",
					subClusterID, len(isolationGroupsInSubcluster), isolationGroupList)

				// Each subcluster should have instances with different isolation groups
				require.True(t, len(isolationGroupsInSubcluster) >= tt.initialRF,
					"Subcluster %d should have at least %d different isolation groups for RF=%d",
					subClusterID, tt.initialRF, tt.initialRF)
			}

			// Test adding replicas from initial RF to target RF
			replicasToAdd := tt.targetRF - tt.initialRF
			t.Logf("Adding %d replicas to increase RF from %d to %d", replicasToAdd, tt.initialRF, tt.targetRF)

			for i := 0; i < replicasToAdd; i++ {
				currentRF := tt.initialRF + i
				newRF := currentRF + 1
				t.Logf("Adding replica %d: RF %d -> %d", i+1, currentRF, newRF)

				// Add replica using the algorithm
				newPlacement, err := algo.AddReplica(currentPlacement)
				require.NoError(t, err)
				require.NotNil(t, newPlacement)
				require.Equal(t, newRF, newPlacement.ReplicaFactor(),
					"New placement should have RF=%d", newRF)

				// Mark all shards as available
				newPlacement, marked, err = algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)
				require.NoError(t, placement.Validate(newPlacement))
				printPlacementAndValidate(t, newPlacement)

				currentPlacement = newPlacement

				// Verify that each shard now has the correct number of replicas
				shardToInstanceCount := make(map[uint32]int)
				shardToIsolationGroups := make(map[uint32]map[string]bool)

				for _, instance := range currentPlacement.Instances() {
					for _, s := range instance.Shards().All() {
						if s.State() != shard.Leaving {
							shardToInstanceCount[s.ID()]++

							if shardToIsolationGroups[s.ID()] == nil {
								shardToIsolationGroups[s.ID()] = make(map[string]bool)
							}
							shardToIsolationGroups[s.ID()][instance.IsolationGroup()] = true
						}
					}
				}

				// Verify each shard has the correct number of replicas and different isolation groups
				for shardID, count := range shardToInstanceCount {
					require.Equal(t, newRF, count,
						"Shard %d should have %d replicas after adding replica", shardID, newRF)

					isolationGroups := shardToIsolationGroups[shardID]
					require.Equal(t, newRF, len(isolationGroups),
						"Shard %d should be placed across %d different isolation groups", shardID, newRF)
				}

				t.Logf("Successfully added replica %d, current RF: %d", i+1, newRF)
			}

			// Final validation
			require.NoError(t, placement.Validate(currentPlacement))
			require.Equal(t, tt.targetRF, currentPlacement.ReplicaFactor(),
				"Final placement should have target RF=%d", tt.targetRF)

			t.Logf("Final placement validation successful:")
			t.Logf("- Final RF: %d", currentPlacement.ReplicaFactor())
			t.Logf("- Total instances: %d", len(currentPlacement.Instances()))
			t.Logf("- Total subclusters: %d", len(getSubclusterMap(currentPlacement)))

			// Verify final placement has correct isolation group distribution
			finalSubClusterMap := getSubclusterMap(currentPlacement)
			for subClusterID, instanceIDs := range finalSubClusterMap {
				isolationGroupsInSubcluster := make(map[string]bool)
				for _, instanceID := range instanceIDs {
					instance, exists := currentPlacement.Instance(instanceID)
					require.True(t, exists, "Instance should exist")
					isolationGroupsInSubcluster[instance.IsolationGroup()] = true
				}

				t.Logf("Final subcluster %d has %d different isolation groups",
					subClusterID, len(isolationGroupsInSubcluster))

				// Each subcluster should have enough isolation groups for the target RF
				require.True(t, len(isolationGroupsInSubcluster) >= tt.targetRF,
					"Subcluster %d should have at least %d different isolation groups for target RF=%d",
					subClusterID, tt.targetRF, tt.targetRF)
			}

			t.Logf("Test completed successfully: RF increased from %d to %d with proper isolation group distribution",
				tt.initialRF, tt.targetRF)
		})
	}
}

// Helper function to get a map of subcluster ID to instance IDs
func getSubclusterMap(p placement.Placement) map[uint32][]string {
	subClusterMap := make(map[uint32][]string)
	for _, instance := range p.Instances() {
		subClusterMap[instance.SubClusterID()] = append(subClusterMap[instance.SubClusterID()], instance.ID())
	}
	return subClusterMap
}
