package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestBootstrapperGauge(t *testing.T) {
	bs0 := []string{
		"filesystem",
		"peers",
		"commitlog",
		"uninitialized_topology",
	}

	bs1 := []string{
		"commitlog",
		"peers",
		"uninitialized_topology",
		"filesystem",
	}

	s := tally.NewTestScope("testScope", nil)

	bsg0 := emitBootstrappersGauge(s, bs0)
	bsg0.Update(1)
	gauges := s.Snapshot().Gauges()
	require.Equal(t, 1, len(gauges))
	g, ok := gauges[fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs0[0], bs0[1], bs0[2], bs0[3])]
	require.True(t, ok)
	require.Equal(t, float64(1), g.Value())

	bsg1 := emitBootstrappersGauge(s, bs1)
	bsg1.Update(1)
	gauges = s.Snapshot().Gauges()
	require.Equal(t, 2, len(gauges))
	for _, id := range []string{
		fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs0[0], bs0[1], bs0[2], bs0[3]),
		fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs1[0], bs1[1], bs1[2], bs1[3]),
	} {
		g, ok = gauges[id]
		require.True(t, ok)
		require.Equal(t, float64(1), g.Value())
	}

	bsg2 := emitBootstrappersGauge(s, bs0)
	bsg2.Update(1)
	gauges = s.Snapshot().Gauges()
	require.Equal(t, 2, len(gauges))
	for _, id := range []string{
		fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs0[0], bs0[1], bs0[2], bs0[3]),
		fmt.Sprintf("testScope.bootstrapper_bootstrappers+bootstrapper0=%s,bootstrapper1=%s,bootstrapper2=%s,bootstrapper3=%s", bs1[0], bs1[1], bs1[2], bs1[3]),
	} {
		g, ok = gauges[id]
		require.True(t, ok)
		require.Equal(t, float64(1), g.Value())
	}
}
