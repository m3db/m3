// Copyright (c) 2018 Uber Technologies, Inc.
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
	wg := sync.WaitGroup{}
	tracker := NewTracker()
	numGoroutines := 10
	numIterations := 1000

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
