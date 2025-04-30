package topology

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/sharding"
)

func TestStaticInitializer_InitSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	opts := NewMockStaticOptions(ctrl)
	opts.EXPECT().Validate().Return(nil).AnyTimes()
	opts.EXPECT().ShardSet().Return(sharding.NewEmptyShardSet(sharding.DefaultHashFn(1))).AnyTimes()
	mockHostShardSet := NewMockHostShardSet(ctrl)
	mockHost := NewMockHost(ctrl)
	mockHostShardSet.EXPECT().Host().Return(mockHost).AnyTimes()
	mockHost.EXPECT().ID().Return("testHost").AnyTimes()
	mockHostShardSet.EXPECT().ShardSet().Return(sharding.NewEmptyShardSet(sharding.DefaultHashFn(1))).AnyTimes()
	opts.EXPECT().HostShardSets().Return([]HostShardSet{mockHostShardSet}).AnyTimes()
	opts.EXPECT().Replicas().Return(3).AnyTimes()
	initializer := NewStaticInitializer(opts)

	topo, err := initializer.Init()
	require.NoError(t, err)
	require.NotNil(t, topo)

	// Validate returned topology is staticTopology
	staticTopo, ok := topo.(*staticTopology)
	require.True(t, ok)

	// Confirm Get() returns expected type
	mapVal := staticTopo.Get()
	require.NotNil(t, mapVal)

	watch, err := topo.Watch()
	require.NoError(t, err)
	require.NotNil(t, watch)

	require.NotPanics(t, func() {
		topo.Close() // Should be a no-op
	})
}

func TestStaticInitializer_InitFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	opts := NewMockStaticOptions(ctrl)
	opts.EXPECT().Validate().Return(fmt.Errorf("error"))
	initializer := NewStaticInitializer(opts)

	topo, err := initializer.Init()
	require.Error(t, err)
	require.Nil(t, topo)
}

func TestStaticInitializer_TopologyIsSet(t *testing.T) {
	opts := NewStaticOptions()
	initializer := NewStaticInitializer(opts)

	ok, err := initializer.TopologyIsSet()
	require.True(t, ok)
	require.NoError(t, err)
}
