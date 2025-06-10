package algo

import (
	"fmt"
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/stretchr/testify/require"
)

func TestSubclusteredV2ReplaceInstances(t *testing.T) {
	tests := []struct {
		name            string
		rf              int
		instancesPerSub int
		totalInstances  int
		shards          int
		instancesToAdd  int
	}{
		{
			name:            "RF=3, 6 instances per subcluster, add 4 instances (not multiple of instances per subcluster)",
			rf:              3,
			instancesPerSub: 6,
			totalInstances:  1200,
			shards:          8192,
			instancesToAdd:  5,
		},
		{
			name:            "RF=3, 6 instances per subcluster, add 12 instances (multiple of instances per subcluster)",
			rf:              3,
			instancesPerSub: 6,
			totalInstances:  6,
			shards:          128,
			instancesToAdd:  12,
		},
		{
			name:            "RF=3, 9 instances per subcluster, add 7 instances (not multiple of instances per subcluster)",
			rf:              3,
			instancesPerSub: 9,
			totalInstances:  9,
			shards:          256,
			instancesToAdd:  7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create initial test instances
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
			printPlacementAndValidate(t, currentPlacement)

			// Create new instances to add
			totalInstances := tt.instancesToAdd + tt.instancesPerSub - (tt.instancesToAdd % tt.instancesPerSub)
			fmt.Println(fmt.Sprintf("totalInstances: %d", totalInstances))
			newInstances := make([]placement.Instance, totalInstances)
			for i := range totalInstances {
				newInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", tt.totalInstances+i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.rf)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", tt.totalInstances+i)).
					SetShards(shard.NewShards(nil))
			}

			// Add new instances
			j := 0
			for _, instance := range newInstances {
				if j == tt.instancesToAdd {
					break
				}
				newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{instance})
				require.NoError(t, err)
				require.NotNil(t, newPlacement)
				newPlacement, marked, err := algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)
				require.NoError(t, placement.Validate(newPlacement))
				currentPlacement = newPlacement
				j++
			}
			printPlacement(currentPlacement)

			// Get instances to replace (one from each subcluster)
			instancesToReplace := make([]string, 0)
			subClusterMap := make(map[uint32][]string)
			for _, instance := range currentPlacement.Instances() {
				subClusterMap[instance.SubClusterID()] = append(subClusterMap[instance.SubClusterID()], instance.ID())
			}

			// Select one instance from each subcluster for replacement
			for _, instances := range subClusterMap {
				if len(instances) > 0 {
					instancesToReplace = append(instancesToReplace, instances[0])
				}
			}

			// Create replacement instances with same isolation groups
			replacementInstances := make([]placement.Instance, len(instancesToReplace))
			for i, instanceID := range instancesToReplace {
				instance, _ := currentPlacement.Instance(instanceID)
				replacementInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("R%d", i)).
					SetIsolationGroup(instance.IsolationGroup()).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("RE%d", i)).
					SetShards(shard.NewShards(nil))
			}

			// Perform replacement
			newPlacement, err := algo.ReplaceInstances(currentPlacement, instancesToReplace, replacementInstances)
			require.NoError(t, err)
			require.NotNil(t, newPlacement)
			newPlacement, marked, err = algo.MarkAllShardsAvailable(newPlacement)
			require.NoError(t, err)
			require.True(t, marked)

			// Verify final placement
			require.NoError(t, placement.Validate(newPlacement))
			if tt.instancesToAdd%tt.instancesPerSub == 0 {
				printPlacementAndValidate(t, newPlacement)
			} else {
				printPlacement(newPlacement)

				// Add remaining instances
				finalPlacement, err := algo.AddInstances(newPlacement, newInstances[j:])
				require.NoError(t, err)
				require.NotNil(t, finalPlacement)
				finalPlacement, marked, err = algo.MarkAllShardsAvailable(finalPlacement)
				require.NoError(t, err)
				require.True(t, marked)
				require.NoError(t, placement.Validate(finalPlacement))
				printPlacementAndValidate(t, finalPlacement)
			}

			// Verify that replaced instances are gone and new ones are present
			for _, instanceID := range instancesToReplace {
				_, exists := newPlacement.Instance(instanceID)
				require.False(t, exists, "Replaced instance should not exist in final placement")
			}

			for _, instance := range replacementInstances {
				_, exists := newPlacement.Instance(instance.ID())
				require.True(t, exists, "Replacement instance should exist in final placement")
			}
		})
	}
}
