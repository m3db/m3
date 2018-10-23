package cost

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTracker(t *testing.T) {
	tests := []struct {
		input    Cost
		expected Cost
	}{
		{
			input:    10,
			expected: 10,
		},
		{
			input:    5,
			expected: 15,
		},
		{
			input:    0,
			expected: 15,
		},
		{
			input:    20,
			expected: 35,
		},
	}

	tracker := NewTracker()
	for _, test := range tests {
		t.Run(fmt.Sprintf("testing input: %v", test.input), func(t *testing.T) {
			require.Equal(t, test.expected, tracker.Add(test.input))
			require.Equal(t, test.expected, tracker.Current())
		})
	}
}

func TestTrackerConcurrentUpdates(t *testing.T) {
	var (
		wg            sync.WaitGroup
		tracker       = NewTracker()
		numGoroutines = 10
		numIterations = 1000
	)
	wg.Add(10)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < numIterations; j++ {
				tracker.Add(1)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	actual := tracker.Current()
	require.Equal(t, Cost(numGoroutines*numIterations), actual)
}

func TestNoopTracker(t *testing.T) {
	tests := []struct {
		input Cost
	}{
		{
			input: 0,
		},
		{
			input: 1,
		},
		{
			input: 5,
		},
		{
			input: 20,
		},
	}

	tracker := NewNoopTracker()
	for _, test := range tests {
		t.Run(fmt.Sprintf("input %v", test.input), func(t *testing.T) {
			require.Equal(t, Cost(0), tracker.Add(test.input))
			require.Equal(t, Cost(0), tracker.Current())
		})
	}
}
