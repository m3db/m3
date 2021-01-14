package instrument

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestDetailedExtendedMetrics(t *testing.T) {
	scope := tally.NewTestScope("test", nil)
	metrics := newRuntimeMetrics(DetailedExtendedMetrics, scope)
	metrics.report(DetailedExtendedMetrics)

	snapshot := scope.Snapshot()
	totalAlloc := snapshot.Counters()["test.runtime.memory.total-allocated+"]
	require.NotNil(t, totalAlloc)
	require.True(t, totalAlloc.Value() > 0)

	metrics.report(DetailedExtendedMetrics)
	snapshot = scope.Snapshot()
	totalAlloc2 := snapshot.Counters()["test.runtime.memory.total-allocated+"]
	require.NotNil(t, totalAlloc2)
	require.True(t, totalAlloc2.Value() > totalAlloc.Value())
}

func TestCumulativeCounter(t *testing.T) {
	scope := tally.NewTestScope("test", nil)
	counter := newCumulativeCounter(scope, "foo")

	last := counter.update(5)
	require.Equal(t, uint64(0), last)
	snapshot := scope.Snapshot()
	foo := snapshot.Counters()["test.foo+"]
	require.NotNil(t, foo)
	require.Equal(t, int64(5), foo.Value())

	last = counter.update(10)
	require.Equal(t, uint64(5), last)
	snapshot = scope.Snapshot()
	foo = snapshot.Counters()["test.foo+"]
	require.NotNil(t, foo)
	require.Equal(t, int64(10), foo.Value())
}