package algo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/x/instrument"
)

func TestValidateInstanceWeight(t *testing.T) {
	tests := []struct {
		name        string
		instances   []placement.Instance
		expectError bool
		errorMsg    string
	}{
		{
			name:        "empty instances",
			instances:   []placement.Instance{},
			expectError: false,
		},
		{
			name: "single instance",
			instances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
			},
			expectError: false,
		},
		{
			name: "multiple instances with same weight",
			instances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
				placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1),
				placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1),
			},
			expectError: false,
		},
		{
			name: "multiple instances with different weights",
			instances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
				placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 2),
			},
			expectError: true,
			errorMsg:    "inconsistent instance weights:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ph := &subclusteredHelper{
				instances: make(map[string]placement.Instance),
			}

			for _, instance := range tt.instances {
				ph.instances[instance.ID()] = instance
			}

			err := ph.validateInstanceWeight()
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewSubclusteredHelper(t *testing.T) {
	tests := []struct {
		name                string
		placement           placement.Placement
		targetRF            int
		opts                placement.Options
		subClusterToExclude uint32
		expectError         bool
		errorMsg            string
	}{
		{
			name: "valid placement with consistent weights",
			placement: placement.NewPlacement().
				SetInstances([]placement.Instance{
					placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1).SetSubClusterID(1),
				}).
				SetShards([]uint32{1, 2, 3}).
				SetReplicaFactor(3).
				SetInstancesPerSubCluster(3).
				SetIsSubclustered(true),
			targetRF:            3,
			opts:                placement.NewOptions().SetInstancesPerSubCluster(3).SetIsSubclustered(true),
			subClusterToExclude: 0,
			expectError:         false,
		},
		{
			name: "placement with inconsistent weights",
			placement: placement.NewPlacement().
				SetInstances([]placement.Instance{
					placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 2).SetSubClusterID(1),
				}).
				SetShards([]uint32{1, 2}).
				SetReplicaFactor(2).
				SetInstancesPerSubCluster(2).
				SetIsSubclustered(true),
			targetRF:            2,
			opts:                placement.NewOptions().SetInstancesPerSubCluster(2).SetIsSubclustered(true),
			subClusterToExclude: 0,
			expectError:         true,
			errorMsg:            "inconsistent instance weights",
		},
		{
			name: "valid placement with multiple subclusters",
			placement: placement.NewPlacement().
				SetInstances([]placement.Instance{
					// Subcluster 1
					placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1).SetSubClusterID(1),
					// Subcluster 2
					placement.NewEmptyInstance("i4", "r1", "z1", "endpoint", 1).SetSubClusterID(2),
					placement.NewEmptyInstance("i5", "r2", "z1", "endpoint", 1).SetSubClusterID(2),
					placement.NewEmptyInstance("i6", "r3", "z1", "endpoint", 1).SetSubClusterID(2),
				}).
				SetShards([]uint32{1, 2, 3, 4, 5, 6}).
				SetReplicaFactor(3).
				SetInstancesPerSubCluster(3).
				SetIsSubclustered(true),
			targetRF:            3,
			opts:                placement.NewOptions().SetInstancesPerSubCluster(3).SetIsSubclustered(true),
			subClusterToExclude: 0,
			expectError:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			helper, err := newSubclusteredHelper(tt.placement, tt.opts, tt.subClusterToExclude)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, helper)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, helper)

				// Verify helper properties
				sh := helper.(*subclusteredHelper)
				assert.Equal(t, tt.targetRF, sh.rf)
				assert.Equal(t, tt.placement.InstancesPerSubCluster(), sh.instancesPerSubcluster)
				assert.Equal(t, len(tt.placement.Instances()), len(sh.instances))
				assert.Equal(t, len(tt.placement.Shards()), len(sh.uniqueShards))
			}
		})
	}
}

func TestValidateSubclusterDistribution(t *testing.T) {
	tests := []struct {
		name        string
		ph          *subclusteredHelper
		expectError bool
		errorMsg    string
	}{
		{
			name: "empty instances",
			ph: &subclusteredHelper{
				instances: make(map[string]placement.Instance),
				opts:      placement.NewOptions().SetInstancesPerSubCluster(3),
			},
			expectError: false,
		},
		{
			name: "replica factor not set",
			ph: &subclusteredHelper{
				instances: map[string]placement.Instance{
					"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
				},
				opts: placement.NewOptions().SetInstancesPerSubCluster(3),
				rf:   0,
			},
			expectError: true,
			errorMsg:    "replica factor should be greater than 0",
		},
		{
			name: "instances per subcluster not set",
			ph: &subclusteredHelper{
				instances: map[string]placement.Instance{
					"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
				},
				opts: placement.NewOptions().SetInstancesPerSubCluster(0),
				rf:   3,
			},
			expectError: true,
			errorMsg:    "instances per subcluster is not set",
		},
		{
			name: "number of isolation groups matches replica factor",
			ph: &subclusteredHelper{
				instances: map[string]placement.Instance{
					"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
					"i2": placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1).SetSubClusterID(1),
					"i3": placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1).SetSubClusterID(1),
				},
				rf:                     3,
				instancesPerSubcluster: 3,
				opts:                   placement.NewOptions().SetInstancesPerSubCluster(3),
				groupToInstancesMap: map[string]map[placement.Instance]struct{}{
					"r1": {placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1): {}},
					"r2": {placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1): {}},
					"r3": {placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1): {}},
				},
				subClusters: map[uint32]*subcluster{
					1: {
						id: 1,
						instances: map[string]placement.Instance{
							"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
							"i2": placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1),
							"i3": placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1),
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "number of isolation groups does not match replica factor",
			ph: &subclusteredHelper{
				instances: map[string]placement.Instance{
					"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
					"i2": placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1).SetSubClusterID(1),
				},
				rf:                     3,
				instancesPerSubcluster: 2,
				opts:                   placement.NewOptions().SetInstancesPerSubCluster(2),
				groupToInstancesMap: map[string]map[placement.Instance]struct{}{
					"r1": {placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1): {}},
					"r2": {placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1): {}},
				},
				subClusters: map[uint32]*subcluster{
					1: {
						id: 1,
						instances: map[string]placement.Instance{
							"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
							"i2": placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1),
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "number of isolation groups (2) does not match replica factor (3)",
		},
		{
			name: "complete subcluster with incorrect distribution",
			ph: &subclusteredHelper{
				instances: map[string]placement.Instance{
					"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
					"i2": placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
					"i3": placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1).SetSubClusterID(1),
				},
				rf:                     2,
				instancesPerSubcluster: 3,
				opts:                   placement.NewOptions().SetInstancesPerSubCluster(3),
				groupToInstancesMap: map[string]map[placement.Instance]struct{}{
					"r1": {
						placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1): {},
						placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1): {},
					},
					"r2": {placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1): {}},
				},
				subClusters: map[uint32]*subcluster{
					1: {
						id: 1,
						instances: map[string]placement.Instance{
							"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
							"i2": placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
							"i3": placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1),
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "subcluster 1 isolation group r1 has 2 instances, expected 1",
		},
		{
			name: "incomplete subcluster with too many instances in group",
			ph: &subclusteredHelper{
				instances: map[string]placement.Instance{
					"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
					"i2": placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
				},
				rf:                     2,
				instancesPerSubcluster: 3,
				opts:                   placement.NewOptions().SetInstancesPerSubCluster(3),
				groupToInstancesMap: map[string]map[placement.Instance]struct{}{
					"r1": {
						placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1): {},
						placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1): {},
					},
				},
				subClusters: map[uint32]*subcluster{
					1: {
						id: 1,
						instances: map[string]placement.Instance{
							"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
							"i2": placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "number of isolation groups (1) does not match replica factor (2)",
		},
		{
			name: "incomplete subcluster with correct distribution",
			ph: &subclusteredHelper{
				instances: map[string]placement.Instance{
					"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
					"i2": placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1).SetSubClusterID(1),
				},
				rf:                     2,
				instancesPerSubcluster: 3,
				opts:                   placement.NewOptions().SetInstancesPerSubCluster(3),
				groupToInstancesMap: map[string]map[placement.Instance]struct{}{
					"r1": {placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1): {}},
					"r2": {placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1): {}},
				},
				subClusters: map[uint32]*subcluster{
					1: {
						id: 1,
						instances: map[string]placement.Instance{
							"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
							"i2": placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1),
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "multiple subclusters with correct distribution",
			ph: &subclusteredHelper{
				instances: map[string]placement.Instance{
					// Subcluster 1
					"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1).SetSubClusterID(1),
					"i2": placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1).SetSubClusterID(1),
					"i3": placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1).SetSubClusterID(1),
					// Subcluster 2
					"i4": placement.NewEmptyInstance("i4", "r1", "z1", "endpoint", 1).SetSubClusterID(2),
					"i5": placement.NewEmptyInstance("i5", "r2", "z1", "endpoint", 1).SetSubClusterID(2),
					"i6": placement.NewEmptyInstance("i6", "r3", "z1", "endpoint", 1).SetSubClusterID(2),
				},
				rf:                     3,
				instancesPerSubcluster: 3,
				opts:                   placement.NewOptions().SetInstancesPerSubCluster(3),
				groupToInstancesMap: map[string]map[placement.Instance]struct{}{
					"r1": {
						placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1): {},
						placement.NewEmptyInstance("i4", "r1", "z1", "endpoint", 1): {},
					},
					"r2": {
						placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1): {},
						placement.NewEmptyInstance("i5", "r2", "z1", "endpoint", 1): {},
					},
					"r3": {
						placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1): {},
						placement.NewEmptyInstance("i6", "r3", "z1", "endpoint", 1): {},
					},
				},
				subClusters: map[uint32]*subcluster{
					1: {
						id: 1,
						instances: map[string]placement.Instance{
							"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
							"i2": placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1),
							"i3": placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1),
						},
					},
					2: {
						id: 2,
						instances: map[string]placement.Instance{
							"i4": placement.NewEmptyInstance("i4", "r1", "z1", "endpoint", 1),
							"i5": placement.NewEmptyInstance("i5", "r2", "z1", "endpoint", 1),
							"i6": placement.NewEmptyInstance("i6", "r3", "z1", "endpoint", 1),
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ph.validateSubclusterDistribution()
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Helper function to create a test placement with instances
func createTestPlacement(
	instances []placement.Instance,
	shards []uint32,
	rf int,
	instancesPerSubcluster int,
) placement.Placement {
	return placement.NewPlacement().
		SetInstances(instances).
		SetShards(shards).
		SetReplicaFactor(rf).
		SetInstancesPerSubCluster(instancesPerSubcluster).
		SetIsSubclustered(true)
}

// Helper function to create test options
func createTestOptions(instancesPerSubcluster int) placement.Options {
	return placement.NewOptions().
		SetInstancesPerSubCluster(instancesPerSubcluster).
		SetIsSubclustered(true).
		SetInstrumentOptions(instrument.NewOptions().SetLogger(zap.NewNop()))
}

func TestNewSubclusteredHelperIntegration(t *testing.T) {
	instances := []placement.Instance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1).SetSubClusterID(1),
		placement.NewEmptyInstance("i2", "r2", "z1", "endpoint2", 1).SetSubClusterID(1),
		placement.NewEmptyInstance("i3", "r3", "z1", "endpoint3", 1).SetSubClusterID(1),
	}

	placement := createTestPlacement(instances, []uint32{1, 2, 3}, 3, 3)
	opts := createTestOptions(3)

	helper, err := newSubclusteredHelper(placement, opts, 0)
	require.NoError(t, err)
	require.NotNil(t, helper)

	sh := helper.(*subclusteredHelper)
	assert.Equal(t, 3, sh.rf)
	assert.Equal(t, 3, sh.instancesPerSubcluster)
	assert.Equal(t, 3, len(sh.instances))
	assert.Equal(t, 3, len(sh.uniqueShards))
	assert.Equal(t, 3, len(sh.groupToInstancesMap))
	assert.Equal(t, 1, len(sh.subClusters))
}

func TestValidateInstanceWeightIntegration(t *testing.T) {
	instance1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1)
	instance1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	instance1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	instance2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint2", 1)
	instance2.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	ph := &subclusteredHelper{
		instances: map[string]placement.Instance{
			"i1": instance1,
			"i2": instance2,
		},
	}

	err := ph.validateInstanceWeight()
	assert.NoError(t, err)
}

func TestAssignSubClusterIDs(t *testing.T) {
	tests := []struct {
		name                   string
		instancesPerSubcluster int
		currentPlacement       placement.Placement
		newInstances           []placement.Instance
		expectedSubclusterIDs  []uint32
		expectError            bool
		errorMessage           string
	}{
		{
			name:                   "no current placement, 3 instances per subcluster",
			instancesPerSubcluster: 3,
			currentPlacement:       nil,
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1),
				placement.NewEmptyInstance("i2", "r2", "z1", "endpoint2", 1),
				placement.NewEmptyInstance("i3", "r3", "z1", "endpoint3", 1),
				placement.NewEmptyInstance("i4", "r4", "z1", "endpoint4", 1),
				placement.NewEmptyInstance("i5", "r5", "z1", "endpoint5", 1),
			},
			expectedSubclusterIDs: []uint32{1, 1, 1, 2, 2},
			expectError:           false,
		},
		{
			name:                   "empty new instances",
			instancesPerSubcluster: 3,
			currentPlacement:       nil,
			newInstances:           []placement.Instance{},
			expectedSubclusterIDs:  []uint32{},
			expectError:            false,
		},
		{
			name:                   "exactly fill one subcluster",
			instancesPerSubcluster: 3,
			currentPlacement:       nil,
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1),
				placement.NewEmptyInstance("i2", "r2", "z1", "endpoint2", 1),
				placement.NewEmptyInstance("i3", "r3", "z1", "endpoint3", 1),
			},
			expectedSubclusterIDs: []uint32{1, 1, 1},
			expectError:           false,
		},
		{
			name:                   "fill multiple subclusters exactly",
			instancesPerSubcluster: 2,
			currentPlacement:       nil,
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1),
				placement.NewEmptyInstance("i2", "r2", "z1", "endpoint2", 1),
				placement.NewEmptyInstance("i3", "r3", "z1", "endpoint3", 1),
				placement.NewEmptyInstance("i4", "r4", "z1", "endpoint4", 1),
				placement.NewEmptyInstance("i5", "r5", "z1", "endpoint5", 1),
				placement.NewEmptyInstance("i6", "r6", "z1", "endpoint6", 1),
			},
			expectedSubclusterIDs: []uint32{1, 1, 2, 2, 3, 3},
			expectError:           false,
		},
		{
			name:                   "invalid instances per subcluster - zero",
			instancesPerSubcluster: 0,
			currentPlacement:       nil,
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1),
			},
			expectedSubclusterIDs: []uint32{},
			expectError:           true,
			errorMessage:          "instances per subcluster is not set",
		},
		{
			name:                   "invalid instances per subcluster - negative",
			instancesPerSubcluster: -1,
			currentPlacement:       nil,
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1),
			},
			expectedSubclusterIDs: []uint32{},
			expectError:           true,
			errorMessage:          "instances per subcluster is not set",
		},
		{
			name:                   "fill incomplete subclusters in order of increasing ID",
			instancesPerSubcluster: 3,
			currentPlacement: placement.NewPlacement().
				SetInstances([]placement.Instance{
					// Subcluster 1: 2 instances (incomplete)
					placement.NewEmptyInstance("existing1", "r1", "z1", "endpoint1", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("existing2", "r2", "z1", "endpoint2", 1).SetSubClusterID(1),
					// Subcluster 3: 1 instance (incomplete)
					placement.NewEmptyInstance("existing3", "r3", "z1", "endpoint3", 1).SetSubClusterID(3),
					// Subcluster 5: 2 instances (incomplete)
					placement.NewEmptyInstance("existing4", "r4", "z1", "endpoint4", 1).SetSubClusterID(5),
					placement.NewEmptyInstance("existing5", "r5", "z1", "endpoint5", 1).SetSubClusterID(5),
				}).
				SetIsSharded(true).
				SetIsSubclustered(true),
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("new1", "r6", "z1", "endpoint6", 1),
				placement.NewEmptyInstance("new2", "r7", "z1", "endpoint7", 1),
				placement.NewEmptyInstance("new3", "r8", "z1", "endpoint8", 1),
				placement.NewEmptyInstance("new4", "r9", "z1", "endpoint9", 1),
				placement.NewEmptyInstance("new5", "r10", "z1", "endpoint10", 1),
			},
			expectedSubclusterIDs: []uint32{1, 3, 3, 5, 6}, // Fill 1, then 3, then 5, then create 6
			expectError:           false,
		},
		{
			name:                   "current placement with leaving instances",
			instancesPerSubcluster: 3,
			currentPlacement: placement.NewPlacement().
				SetInstances([]placement.Instance{
					placement.NewEmptyInstance("existing1", "r1", "z1", "endpoint1", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("existing2", "r2", "z1", "endpoint2", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("leaving1", "r3", "z1", "endpoint3", 1).
						SetSubClusterID(1).
						SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Leaving)})),
					placement.NewEmptyInstance("existing3", "r4", "z1", "endpoint4", 1).SetSubClusterID(2),
				}).
				SetIsSharded(true).
				SetIsSubclustered(true),
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("new1", "r5", "z1", "endpoint5", 1),
				placement.NewEmptyInstance("new2", "r6", "z1", "endpoint6", 1),
				placement.NewEmptyInstance("new3", "r7", "z1", "endpoint7", 1),
			},
			expectedSubclusterIDs: []uint32{1, 2, 2}, // Fill subcluster 1 (2 non-leaving), then 2
			expectError:           false,
		},
		{
			name:                   "single instance fills incomplete subcluster",
			instancesPerSubcluster: 3,
			currentPlacement: placement.NewPlacement().
				SetInstances([]placement.Instance{
					placement.NewEmptyInstance("existing1", "r1", "z1", "endpoint1", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("existing2", "r2", "z1", "endpoint2", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("existing3", "r3", "z1", "endpoint3", 1).SetSubClusterID(2),
				}).
				SetIsSharded(true).
				SetIsSubclustered(true),
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("new1", "r4", "z1", "endpoint4", 1),
			},
			expectedSubclusterIDs: []uint32{1}, // Fill subcluster 1 (smaller ID)
			expectError:           false,
		},
		{
			name:                   "multiple incomplete subclusters with same count",
			instancesPerSubcluster: 4,
			currentPlacement: placement.NewPlacement().
				SetInstances([]placement.Instance{
					// Subcluster 5: 2 instances
					placement.NewEmptyInstance("existing1", "r1", "z1", "endpoint1", 1).SetSubClusterID(5),
					placement.NewEmptyInstance("existing2", "r2", "z1", "endpoint2", 1).SetSubClusterID(5),
					// Subcluster 2: 2 instances
					placement.NewEmptyInstance("existing3", "r3", "z1", "endpoint3", 1).SetSubClusterID(2),
					placement.NewEmptyInstance("existing4", "r4", "z1", "endpoint4", 1).SetSubClusterID(2),
					// Subcluster 7: 2 instances
					placement.NewEmptyInstance("existing5", "r5", "z1", "endpoint5", 1).SetSubClusterID(7),
					placement.NewEmptyInstance("existing6", "r6", "z1", "endpoint6", 1).SetSubClusterID(7),
				}).
				SetIsSharded(true).
				SetIsSubclustered(true),
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("new1", "r7", "z1", "endpoint7", 1),
				placement.NewEmptyInstance("new2", "r8", "z1", "endpoint8", 1),
				placement.NewEmptyInstance("new3", "r9", "z1", "endpoint9", 1),
				placement.NewEmptyInstance("new4", "r10", "z1", "endpoint10", 1),
			},
			expectedSubclusterIDs: []uint32{2, 2, 5, 5}, // Fill in order: 2, 5, 7 (by ID)
			expectError:           false,
		},
		{
			name:                   "all subclusters full, create new ones",
			instancesPerSubcluster: 2,
			currentPlacement: placement.NewPlacement().
				SetInstances([]placement.Instance{
					placement.NewEmptyInstance("existing1", "r1", "z1", "endpoint1", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("existing2", "r2", "z1", "endpoint2", 1).SetSubClusterID(1), // subcluster 1 full
					placement.NewEmptyInstance("existing3", "r3", "z1", "endpoint3", 1).SetSubClusterID(3),
					placement.NewEmptyInstance("existing4", "r4", "z1", "endpoint4", 1).SetSubClusterID(3), // subcluster 3 full
				}).
				SetIsSharded(true).
				SetIsSubclustered(true),
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("new1", "r5", "z1", "endpoint5", 1),
				placement.NewEmptyInstance("new2", "r6", "z1", "endpoint6", 1),
				placement.NewEmptyInstance("new3", "r7", "z1", "endpoint7", 1),
			},
			expectedSubclusterIDs: []uint32{4, 4, 5}, // Create new subclusters 4 and 5
			expectError:           false,
		},
		{
			name:                   "edge case: exactly one instance needed to complete subcluster",
			instancesPerSubcluster: 3,
			currentPlacement: placement.NewPlacement().
				SetInstances([]placement.Instance{
					placement.NewEmptyInstance("existing1", "r1", "z1", "endpoint1", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("existing2", "r2", "z1", "endpoint2", 1).SetSubClusterID(1),
					placement.NewEmptyInstance("existing3", "r3", "z1", "endpoint3", 1).SetSubClusterID(2),
					placement.NewEmptyInstance("existing4", "r4", "z1", "endpoint4", 1).SetSubClusterID(2),
				}).
				SetIsSharded(true).
				SetIsSubclustered(true),
			newInstances: []placement.Instance{
				placement.NewEmptyInstance("new1", "r5", "z1", "endpoint5", 1),
				placement.NewEmptyInstance("new2", "r6", "z1", "endpoint6", 1),
			},
			expectedSubclusterIDs: []uint32{1, 2}, // Complete subcluster 1, then 2
			expectError:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := assignSubClusterIDs(tt.newInstances, tt.currentPlacement, tt.instancesPerSubcluster)
			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.errorMessage, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(tt.expectedSubclusterIDs), len(tt.newInstances))

				for i, expectedID := range tt.expectedSubclusterIDs {
					assert.Equal(t, expectedID, tt.newInstances[i].SubClusterID(),
						"Instance %d (ID: %s) should be in subcluster %d",
						i, tt.newInstances[i].ID(), expectedID)
				}
			}
		})
	}
}
