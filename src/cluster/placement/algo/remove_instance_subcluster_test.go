package algo

import (
	"fmt"
	"testing"

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
			name:                "RF=3, 6 instances per subcluster, 12 total instances, remove 1 subcluster",
			rf:                  3,
			instancesPerSub:     6,
			totalInstances:      1200,
			shards:              8192,
			subclustersToRemove: 20,
		},
		{
			name:                "RF=3, 6 instances per subcluster, 12 total instances, remove 1 subcluster",
			rf:                  3,
			instancesPerSub:     6,
			totalInstances:      24,
			shards:              64,
			subclustersToRemove: 2,
		},
		{
			name:                "RF=3, 9 instances per subcluster, 18 total instances, remove 2 subclusters",
			rf:                  3,
			instancesPerSub:     9,
			totalInstances:      720,
			shards:              4196,
			subclustersToRemove: 10,
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

			// Verify initial placement
			currentPlacement, marked, err := algo.MarkAllShardsAvailable(p)
			require.NoError(t, err)
			require.True(t, marked)
			require.NoError(t, placement.Validate(currentPlacement))
			printPlacementAndValidate(t, currentPlacement)

			// Get instances from the last N subclusters
			totalSubclusters := tt.totalInstances / tt.instancesPerSub
			instancesToRemove := make([]string, 0, tt.instancesPerSub*tt.subclustersToRemove)

			for subClusterOffset := 0; subClusterOffset < tt.subclustersToRemove; subClusterOffset++ {
				subClusterID := uint32(totalSubclusters - 1 - subClusterOffset)
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

			// Verify final state
			finalInstances := currentPlacement.Instances()
			expectedInstances := tt.totalInstances - (tt.instancesPerSub * tt.subclustersToRemove)
			require.Equal(t, expectedInstances, len(finalInstances),
				"Final placement should have correct number of instances")
		})
	}
}
