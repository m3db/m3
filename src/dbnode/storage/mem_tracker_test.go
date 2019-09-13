// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryTrackerLoadLimitEnforcedIfSet(t *testing.T) {
	limit := int64(100)
	memTracker := NewMemoryTracker(NewMemoryTrackerOptions(limit))
	// First one will always be allowed, see comment in IncNumLoadedBytes()
	// for explanation.
	require.True(t, memTracker.IncNumLoadedBytes(limit))
	require.False(t, memTracker.IncNumLoadedBytes(1))
}

func TestMemoryTrackerLoadLimitNotEnforcedIfNotSet(t *testing.T) {
	memTracker := NewMemoryTracker(NewMemoryTrackerOptions(0))
	require.True(t, memTracker.IncNumLoadedBytes(100))
}

// TestMemoryTrackerDoubleDec ensures that calling Dec twice in a row (which
// should not happen anyways with a well behaved caller) does not cause the
// number of loaded bytes to decrement twice.
func TestMemoryTrackerDoubleDec(t *testing.T) {
	limit := int64(100)
	memTracker := NewMemoryTracker(NewMemoryTrackerOptions(limit))
	require.True(t, memTracker.IncNumLoadedBytes(50))
	memTracker.MarkLoadedAsPending()
	memTracker.DecPendingLoadedBytes()
	memTracker.DecPendingLoadedBytes()
	require.Equal(t, int64(0), memTracker.NumLoadedBytes())
}

func TestMemoryTrackerIncMarkAndDec(t *testing.T) {
	var (
		limit         = int64(100)
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
	require.Equal(t, int64(0), memTracker.NumLoadedBytes())
	// Ensure limit is reset after a combination mark + dec.
	require.True(t, memTracker.IncNumLoadedBytes(limit))

	// Clear.
	memTracker.MarkLoadedAsPending()
	memTracker.DecPendingLoadedBytes()
	require.Equal(t, int64(0), memTracker.NumLoadedBytes())

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
	require.Equal(t, int64(0), memTracker.NumLoadedBytes())

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
	require.Equal(t, int64(0), memTracker.NumLoadedBytes())
}

// TestMemTrackerWaitForDec spins up several goroutines that call MarkLoadedAsPending,
// DecPendingLoadedBytes, and WaitForDec in a loop to ensure that there are no deadlocks
// or race conditions.
func TestMemTrackerWaitForDec(t *testing.T) {
	var (
		numIterations = 1000
		memTracker    = NewMemoryTracker(NewMemoryTrackerOptions(100))
		doneCh        = make(chan struct{})
		wg            sync.WaitGroup
	)

	// Start a goroutine to call MarkLoadedAsPending() in a loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numIterations; i++ {
			memTracker.MarkLoadedAsPending()
		}
	}()

	// Start a goroutine to call WaitForDec() in a loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numIterations; i++ {
			memTracker.WaitForDec()
		}
	}()

	// Start a goroutine to call DecPendingLoadedBytes() in a loop. Note that
	// unlike the other two goroutines this one loops infinitely until the test
	// is over. This is to prevent calls to WaitForDec() from getting stuck forever
	// because a call to WaitForDec() was made after the goroutine that calls
	// DecPendingLoadedBytes() had already shut down.
	go func() {
		for {
			select {
			case <-doneCh:
				return
			default:
				memTracker.DecPendingLoadedBytes()
			}
		}
	}()

	// Ensure all the goroutines exit cleanly (ensuring no deadlocks.)
	wg.Wait()

	// Stop the background goroutine calling DecPendingLoadedBytes().
	close(doneCh)
}
