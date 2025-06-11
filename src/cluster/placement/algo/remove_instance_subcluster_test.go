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

func TestSubclusteredV2RemoveInstances(t *testing.T) {
	tests := []struct {
		name                string
		rf                  int
		instancesPerSub     int
		totalInstances      int
		shards              int
		subclustersToRemove int
	}{
		{
			name:                "RF=3, 6 instances per subcluster, 24 total instances, remove 1 subcluster",
			rf:                  3,
			instancesPerSub:     6,
			totalInstances:      180,
			shards:              4096,
			subclustersToRemove: 1,
		},
		// {
		// 	name:                "RF=3, 6 instances per subcluster, 36 total instances, remove 2 subclusters",
		// 	rf:                  3,
		// 	instancesPerSub:     6,
		// 	totalInstances:      36,
		// 	shards:              128,
		// 	subclustersToRemove: 2,
		// },
		// {
		// 	name:                "RF=3, 9 instances per subcluster, 27 total instances, remove 1 subcluster",
		// 	rf:                  3,
		// 	instancesPerSub:     9,
		// 	totalInstances:      27,
		// 	shards:              64,
		// 	subclustersToRemove: 1,
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

			// Verify initial placement
			currentPlacement, marked, err := algo.MarkAllShardsAvailable(p)
			require.NoError(t, err)
			require.True(t, marked)
			require.NoError(t, placement.Validate(currentPlacement))
			printPlacementAndValidate(t, currentPlacement)

			// Get instances from random N subclusters
			instancesToRemove := make([]string, 0, tt.instancesPerSub*tt.subclustersToRemove)

			// Create a list of all available subcluster IDs
			availableSubclusters := make(map[uint32]bool)
			for _, instance := range p.Instances() {
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
				for _, instance := range p.Instances() {
					if instance.SubClusterID() == subClusterID {
						instancesToRemove = append(instancesToRemove, instance.ID())
					}
				}
			}
			require.Equal(t, tt.instancesPerSub*tt.subclustersToRemove, len(instancesToRemove),
				"Should have correct number of instances to remove")

			// Remove instances one by one
			for i, instanceID := range instancesToRemove {
				t.Logf("Removing instance %s (%d/%d)", instanceID, i+1, len(instancesToRemove))

				// Remove the instance
				newPlacement, err := algo.RemoveInstances(currentPlacement, []string{instanceID})
				require.NoError(t, err)
				require.NotNil(t, newPlacement)
				printPlacement(newPlacement)

				if i == 0 {
					t.Logf("Testing reclaiming instance %s", instanceID)
					addingInstance, _ := newPlacement.Instance(instanceID)
					newPlacement, err = algo.AddInstances(newPlacement, []placement.Instance{addingInstance})
					require.NoError(t, err)
					require.NotNil(t, newPlacement)
					printPlacementAndValidate(t, newPlacement)
					newPlacement, err = algo.RemoveInstances(newPlacement, []string{instanceID})
					require.NoError(t, err)
					require.NotNil(t, newPlacement)
				}

				newPlacement, marked, err := algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)

				// Verify the placement after removal
				require.NoError(t, placement.Validate(newPlacement))

				currentPlacement = newPlacement
			}

			// Final validation after all removals
			require.NoError(t, placement.Validate(currentPlacement))
			printPlacementAndValidate(t, currentPlacement)

			_, globalMaxSkew, subclustersWithMaxSkewGTTwo := getMaxShardDiffInSubclusters(currentPlacement)
			t.Logf("Maximum shard difference after removals: %d (subcluster %d)", globalMaxSkew, subclustersWithMaxSkewGTTwo)

			// Verify final state
			finalInstances := currentPlacement.Instances()
			expectedInstances := tt.totalInstances - (tt.instancesPerSub * tt.subclustersToRemove)
			require.Equal(t, expectedInstances, len(finalInstances),
				"Final placement should have correct number of instances")
		})
	}
}

func TestSubclusteredV2ReplaceInstanceAndRemove(t *testing.T) {
	tests := []struct {
		name            string
		rf              int
		instancesPerSub int
		totalInstances  int
		shards          int
	}{
		{
			name:            "RF=3, 6 instances per subcluster, 24 total instances",
			rf:              3,
			instancesPerSub: 6,
			totalInstances:  24,
			shards:          512,
		},
		{
			name:            "RF=3, 9 instances per subcluster, 27 total instances",
			rf:              3,
			instancesPerSub: 9,
			totalInstances:  27,
			shards:          128,
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
			printPlacementAndValidate(t, currentPlacement)

			// Select an instance to replace (first instance)
			instanceToReplace := currentPlacement.Instances()[0]
			t.Logf("Replacing instance %s", instanceToReplace.ID())

			// Create a new replacing instance
			replacingInstance := placement.NewInstance().
				SetID(fmt.Sprintf("REPLACING_%s", instanceToReplace.ID())).
				SetIsolationGroup(instanceToReplace.IsolationGroup()).
				SetWeight(instanceToReplace.Weight()).
				SetEndpoint(fmt.Sprintf("REPLACING_%s", instanceToReplace.Endpoint())).
				SetShards(shard.NewShards(nil))

			// Replace the instance
			replacePlacement, err := algo.ReplaceInstances(currentPlacement, []string{instanceToReplace.ID()}, []placement.Instance{replacingInstance})
			require.NoError(t, err)
			require.NotNil(t, replacePlacement)

			// Verify the replacement placement
			require.NoError(t, placement.Validate(replacePlacement))
			printPlacement(replacePlacement)

			// Find the replacing instance in the new placement
			var replacingInstanceInPlacement placement.Instance
			for _, inst := range replacePlacement.Instances() {
				if inst.ID() == replacingInstance.ID() {
					replacingInstanceInPlacement = inst
					break
				}
			}
			require.NotNil(t, replacingInstanceInPlacement, "Replacing instance should be in placement")

			// // Get half of the shards from the replacing instance and mark them as available
			// replacingShards := replacingInstanceInPlacement.Shards().All()
			// halfCount := len(replacingShards) / 2
			// shardsToMarkAvailable := replacingShards[:halfCount]

			// t.Logf("Marking %d out of %d shards as available on replacing instance %s",
			// 	len(shardsToMarkAvailable), len(replacingShards), replacingInstance.ID())

			// // Create slice of shard IDs to mark available
			// shardIDsToMarkAvailable := make([]uint32, len(shardsToMarkAvailable))
			// for i, s := range shardsToMarkAvailable {
			// 	shardIDsToMarkAvailable[i] = s.ID()
			// }

			// // Mark half the shards as available using the algorithm's method
			// halfAvailablePlacement, err := algo.MarkShardsAvailable(replacePlacement, replacingInstance.ID(), shardIDsToMarkAvailable...)
			// require.NoError(t, err)
			// require.NoError(t, placement.Validate(halfAvailablePlacement))
			// printPlacement(halfAvailablePlacement)

			// cleanReplacingInstance, found := halfAvailablePlacement.Instance(replacingInstance.ID())
			// require.True(t, found, "Replacing instance should be in clean placement")

			// initializingShards := cleanReplacingInstance.Shards().ShardsForState(shard.Initializing)
			// for _, s := range initializingShards {
			// 	cleanReplacingInstance.Shards().Remove(s.ID())
			// }

			// instanceReplaced, found := halfAvailablePlacement.Instance(instanceToReplace.ID())
			// require.True(t, found, "Instance replaced should be in clean placement")

			// for _, s := range instanceReplaced.Shards().All() {
			// 	s.SetState(shard.Available)
			// }

			// partialPlacement := halfAvailablePlacement.Clone()
			// printPlacement(partialPlacement)

			// Now remove the replacing instance
			t.Logf("Removing replacing instance %s", replacingInstance.ID())
			finalPlacement, err := algo.RemoveInstances(replacePlacement, []string{replacingInstance.ID()})
			require.NoError(t, err)
			require.NotNil(t, finalPlacement)

			// Mark all shards as available in the final placement
			finalPlacement, marked, err = algo.MarkAllShardsAvailable(finalPlacement)
			require.NoError(t, err)
			require.True(t, marked)

			// Final validation
			require.NoError(t, placement.Validate(finalPlacement))
			printPlacementAndValidate(t, finalPlacement)
		})
	}
}
