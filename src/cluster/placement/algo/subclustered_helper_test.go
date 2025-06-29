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
			errorMsg:    "inconsistent instance weights: instance i2 has weight 2, expected 1",
		},
		{
			name: "multiple instances with different weights - first instance has higher weight",
			instances: []placement.Instance{
				placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 3),
				placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1),
			},
			expectError: true,
			errorMsg:    "inconsistent instance weights: instance i2 has weight 1, expected 3",
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
				SetHasSubClusters(true),
			targetRF:            3,
			opts:                placement.NewOptions().SetInstancesPerSubCluster(3).SetHasSubClusters(true),
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
				SetHasSubClusters(true),
			targetRF:            2,
			opts:                placement.NewOptions().SetInstancesPerSubCluster(2).SetHasSubClusters(true),
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
				SetHasSubClusters(true),
			targetRF:            3,
			opts:                placement.NewOptions().SetInstancesPerSubCluster(3).SetHasSubClusters(true),
			subClusterToExclude: 0,
			expectError:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			helper, err := newSubclusteredHelper(tt.placement, tt.targetRF, tt.opts, tt.subClusterToExclude)
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
			name: "instances per subcluster not set",
			ph: &subclusteredHelper{
				instances: map[string]placement.Instance{
					"i1": placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
				},
				opts: placement.NewOptions().SetInstancesPerSubCluster(0),
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
func createTestPlacement(instances []placement.Instance, shards []uint32, rf int, instancesPerSubcluster int) placement.Placement {
	return placement.NewPlacement().
		SetInstances(instances).
		SetShards(shards).
		SetReplicaFactor(rf).
		SetInstancesPerSubCluster(instancesPerSubcluster).
		SetHasSubClusters(true)
}

// Helper function to create test options
func createTestOptions(instancesPerSubcluster int) placement.Options {
	return placement.NewOptions().
		SetInstancesPerSubCluster(instancesPerSubcluster).
		SetHasSubClusters(true).
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

	helper, err := newSubclusteredHelper(placement, 3, opts, 0)
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
