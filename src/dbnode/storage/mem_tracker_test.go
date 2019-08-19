package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryTrackerLoadLimitEnforcedIfSet(t *testing.T) {
	limit := 100
	memTracker := NewMemoryTracker(NewMemoryTrackerOptions(limit))
	require.False(t, memTracker.IncNumLoadedBytes(limit+1))
	require.True(t, memTracker.IncNumLoadedBytes(limit))
}

func TestMemoryTrackerLoadLimitNotEnforcedIfNotSet(t *testing.T) {
	memTracker := NewMemoryTracker(NewMemoryTrackerOptions(0))
	require.True(t, memTracker.IncNumLoadedBytes(100))
}

func TestMemoryTrackerIncMarkAndDec(t *testing.T) {
	var (
		limit         = 100
		oneTenthLimit = limit / 10
		memTracker    = NewMemoryTracker(NewMemoryTrackerOptions(limit))
	)
	require.True(t, oneTenthLimit > 1)

	// Set the maximum.
	require.True(t, memTracker.IncNumLoadedBytes(limit))
	require.Equal(t, limit, memTracker.NumLoadedBytes())
	// Ensure no more can be loaded.
	require.False(t, memTracker.IncNumLoadedBytes(1))
	memTracker.MarkLoadedAsPending()
	// Ensure num loaded wasn't changed by mark.
	require.Equal(t, limit, memTracker.NumLoadedBytes())
	// Ensure no more still can't be loaded even after marking.
	require.False(t, memTracker.IncNumLoadedBytes(1))
	memTracker.DecPendingLoadedBytes()
	// Ensure num loaded is affected by combination of mark + dec.
	require.Equal(t, 0, memTracker.NumLoadedBytes())
	// Ensure limit is reset after a combination mark + dec.
	require.True(t, memTracker.IncNumLoadedBytes(limit))

	// Clear.
	memTracker.MarkLoadedAsPending()
	memTracker.DecPendingLoadedBytes()
	require.Equal(t, 0, memTracker.NumLoadedBytes())

	// Ensure interactions between concurrent loads and marks/decs behave as expected.
	require.True(t, memTracker.IncNumLoadedBytes(oneTenthLimit))
	require.Equal(t, oneTenthLimit, memTracker.NumLoadedBytes())
	memTracker.MarkLoadedAsPending()
	require.True(t, memTracker.IncNumLoadedBytes(oneTenthLimit))
	require.Equal(t, 2*oneTenthLimit, memTracker.NumLoadedBytes())
	memTracker.DecPendingLoadedBytes()
	// There should still be 1/10th pending since the second load was called after the
	// last call to mark before the call to dec.
	require.Equal(t, oneTenthLimit, memTracker.NumLoadedBytes())

	// Clear.
	memTracker.MarkLoadedAsPending()
	memTracker.DecPendingLoadedBytes()
	require.Equal(t, 0, memTracker.NumLoadedBytes())

	// Ensure calling mark multiple times before a single dec behaves
	// as expected.
	require.True(t, memTracker.IncNumLoadedBytes(oneTenthLimit))
	require.Equal(t, oneTenthLimit, memTracker.NumLoadedBytes())
	memTracker.MarkLoadedAsPending()
	// Imagine an error happened here outside the context of the memtracker
	// so instead of calling dec the process tries again by calling mark once
	// more and then dec'ing after that. Also, in the mean time some more data
	// has been loaded.
	require.True(t, memTracker.IncNumLoadedBytes(oneTenthLimit))
	require.Equal(t, 2*oneTenthLimit, memTracker.NumLoadedBytes())
	memTracker.MarkLoadedAsPending()
	memTracker.DecPendingLoadedBytes()
	require.Equal(t, 0, memTracker.NumLoadedBytes())
}
