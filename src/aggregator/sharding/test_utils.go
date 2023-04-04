package sharding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func ValidateShardSet(t *testing.T, expectedShards []uint32, actual ShardSet) {
	expectedSet := make(ShardSet)
	for _, s := range expectedShards {
		expectedSet.Add(s)
	}
	require.Equal(t, expectedSet, actual)
	for _, shard := range expectedShards {
		require.True(t, actual.Contains(shard))
	}
}
