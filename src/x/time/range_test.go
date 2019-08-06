// Copyright (c) 2016 Uber Technologies, Inc.
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

package time

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	testStart = time.Now()
)

type testRanges struct {
	r1 Range
	r2 Range
}

func testInput() []testRanges {
	return []testRanges{
		{
			// r1 before r2, r1.end == r2.start
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart.Add(10 * time.Second), testStart.Add(20 * time.Second)},
		},
		{
			// r1 before r2, r1.end < r2.start
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart.Add(20 * time.Second), testStart.Add(30 * time.Second)},
		},
		{
			// r1 contains r2, r1.end == r2.end
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart.Add(5 * time.Second), testStart.Add(10 * time.Second)},
		},
		{
			// r1 contains r2, r1.end > r2.end
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart.Add(5 * time.Second), testStart.Add(8 * time.Second)},
		},
		{
			// r1 overlaps r2, r1.end < r2.end
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart.Add(5 * time.Second), testStart.Add(15 * time.Second)},
		},
		{
			// r2 before r1, r1.start == r2.end
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart.Add(-5 * time.Second), testStart},
		},
		{
			// r2 before r1, r1.start > r2.end
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart.Add(-10 * time.Second), testStart.Add(-5 * time.Second)},
		},
		{
			// r2 contains r1, r1.end == r2.end
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart.Add(-5 * time.Second), testStart.Add(10 * time.Second)},
		},
		{
			// r2 contains r1, r1.end < r2.end
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart.Add(-5 * time.Second), testStart.Add(20 * time.Second)},
		},
		{
			// r1 overlaps r2, r1.end > r2.end
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart.Add(-5 * time.Second), testStart.Add(5 * time.Second)},
		},
		{
			// r1 == r2
			Range{testStart, testStart.Add(10 * time.Second)},
			Range{testStart, testStart.Add(10 * time.Second)},
		},
	}
}

func TestRangeIsEmpty(t *testing.T) {
	r := Range{testStart, testStart}
	require.True(t, r.IsEmpty())

	r.End = testStart.Add(time.Second)
	require.False(t, r.IsEmpty())
}

func TestEqual(t *testing.T) {
	inputs := []struct {
		a, b     Range
		expected bool
	}{
		{
			a:        Range{Start: time.Unix(12, 0), End: time.Unix(34, 0)},
			b:        Range{Start: time.Unix(12, 0), End: time.Unix(34, 0)},
			expected: true,
		},
		{
			a:        Range{Start: time.Unix(12, 0), End: time.Unix(34, 0)},
			b:        Range{Start: time.Unix(12, 0).UTC(), End: time.Unix(34, 0).UTC()},
			expected: true,
		},
		{
			a:        Range{Start: time.Unix(12, 0), End: time.Unix(34, 0)},
			b:        Range{Start: time.Unix(13, 0), End: time.Unix(34, 0)},
			expected: false,
		},
		{
			a:        Range{Start: time.Unix(12, 0), End: time.Unix(34, 0)},
			b:        Range{Start: time.Unix(12, 0), End: time.Unix(36, 0)},
			expected: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.a.Equal(input.b))
		require.Equal(t, input.expected, input.b.Equal(input.a))
	}
}

func TestRangeBefore(t *testing.T) {
	input := testInput()
	expected := []bool{
		true, true, false, false, false, false, false, false, false, false, false,
	}
	for i := 0; i < len(input); i++ {
		require.Equal(t, expected[i], input[i].r1.Before(input[i].r2))
	}
}

func TestRangeAfter(t *testing.T) {
	input := testInput()
	expected := []bool{
		false, false, false, false, false, true, true, false, false, false, false,
	}
	for i := 0; i < len(input); i++ {
		require.Equal(t, expected[i], input[i].r1.After(input[i].r2))
	}
}

func TestRangeContains(t *testing.T) {
	input := testInput()
	expected := []bool{
		false, false, true, true, false, false, false, false, false, false, true,
	}
	for i := 0; i < len(input); i++ {
		require.Equal(t, expected[i], input[i].r1.Contains(input[i].r2))
	}

	expected = []bool{
		false, false, false, false, false, false, false, true, true, false, true,
	}
	for i := 0; i < len(input); i++ {
		require.Equal(t, input[i].r2.Contains(input[i].r1), expected[i])
	}
}

func TestRangeOverlaps(t *testing.T) {
	input := testInput()
	expected := []bool{
		false, false, true, true, true, false, false, true, true, true, true,
	}
	for i := 0; i < len(input); i++ {
		require.Equal(t, expected[i], input[i].r1.Overlaps(input[i].r2))
	}
}

func TestRangeDuration(t *testing.T) {
	inputs := []struct {
		r        Range
		expected time.Duration
	}{
		{
			r:        Range{Start: time.Unix(12, 0), End: time.Unix(34, 0)},
			expected: 22 * time.Second,
		},
		{
			r:        Range{Start: time.Unix(8, 0), End: time.Unix(8, 0)},
			expected: 0,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, input.r.Duration())
	}
}

func TestRangeIntersect(t *testing.T) {
	input := testInput()
	for i := 0; i < len(input); i++ {
		var (
			r1                 = input[i].r1
			r2                 = input[i].r2
			r1Intersected, ok1 = r1.Intersect(r2)
			r2Intersected, ok2 = r2.Intersect(r1)
		)
		switch i {
		case 0: // r1 before r2, r1.end == r2.start
			require.False(t, ok1)
			require.False(t, ok2)
		case 1: // r1 before r2, r1.end < r2.start
			require.False(t, ok1)
			require.False(t, ok2)
		case 2: // r1 contains r2, r1.end == r2.end
			require.True(t, ok1)
			require.True(t, ok2)
			require.Equal(t, r1Intersected.Start, r2.Start)
			require.Equal(t, r2Intersected.Start, r2.Start)
		case 3: // r1 contains r2, r1.end > r2.end
			require.True(t, ok1)
			require.True(t, ok2)
		case 4: // r1 overlaps r2, r1.end < r2.end
			require.True(t, ok1)
			require.True(t, ok2)
			require.Equal(t, r1Intersected.Start, r2.Start)
			require.Equal(t, r1Intersected.End, r1.End)
			require.Equal(t, r2Intersected.Start, r2.Start)
			require.Equal(t, r2Intersected.End, r1.End)
		case 5: // r2 before r1, r1.start == r2.end
			require.False(t, ok1)
			require.False(t, ok2)
		case 6: // r2 before r1, r1.start > r2.end
			require.False(t, ok1)
			require.False(t, ok2)
		case 7: // r2 contains r1, r1.end == r2.end
			require.True(t, ok1)
			require.True(t, ok2)
			require.Equal(t, r1Intersected.Start, r1.Start)
			require.Equal(t, r1Intersected.End, r1.End)
			require.Equal(t, r2Intersected.Start, r1.Start)
			require.Equal(t, r2Intersected.End, r2.End)
		case 8: // r2 contains r1, r1.end < r2.end
			require.True(t, ok1)
			require.True(t, ok2)
			require.Equal(t, r1Intersected.Start, r1.Start)
			require.Equal(t, r1Intersected.End, r1.End)
			require.Equal(t, r2Intersected.Start, r1.Start)
			require.Equal(t, r2Intersected.End, r1.End)
		case 9: // r1 overlaps r2, r1.end > r2.end
			require.True(t, ok1)
			require.True(t, ok2)
			require.Equal(t, r1Intersected.Start, r1.Start)
			require.Equal(t, r1Intersected.End, r2.End)
			require.Equal(t, r2Intersected.Start, r1.Start)
			require.Equal(t, r2Intersected.End, r2.End)
		case 10: // r1 == r2
			require.True(t, ok1)
			require.True(t, ok2)
			require.Equal(t, r1Intersected.Start, r1.Start)
			require.Equal(t, r1Intersected.End, r1.End)
			require.Equal(t, r2Intersected.Start, r2.Start)
			require.Equal(t, r2Intersected.End, r2.End)
		}
	}

}

func TestRangeSince(t *testing.T) {
	r := Range{testStart, testStart.Add(10 * time.Second)}
	require.Equal(t, r, r.Since(testStart.Add(-time.Second)))
	require.Equal(t, r, r.Since(testStart))
	require.Equal(t, Range{Start: testStart.Add(5 * time.Second), End: testStart.Add(10 * time.Second)}, r.Since(testStart.Add(5*time.Second)))
	require.Equal(t, Range{Start: testStart.Add(10 * time.Second), End: testStart.Add(10 * time.Second)}, r.Since(testStart.Add(10*time.Second)))
	require.Equal(t, Range{}, r.Since(testStart.Add(20*time.Second)))
}

func TestRangeMerge(t *testing.T) {
	input := testInput()
	expected := []Range{
		{testStart, testStart.Add(20 * time.Second)},
		{testStart, testStart.Add(30 * time.Second)},
		{testStart, testStart.Add(10 * time.Second)},
		{testStart, testStart.Add(10 * time.Second)},
		{testStart, testStart.Add(15 * time.Second)},
		{testStart.Add(-5 * time.Second), testStart.Add(10 * time.Second)},
		{testStart.Add(-10 * time.Second), testStart.Add(10 * time.Second)},
		{testStart.Add(-5 * time.Second), testStart.Add(10 * time.Second)},
		{testStart.Add(-5 * time.Second), testStart.Add(20 * time.Second)},
		{testStart.Add(-5 * time.Second), testStart.Add(10 * time.Second)},
		{testStart, testStart.Add(10 * time.Second)},
	}
	for i := 0; i < len(input); i++ {
		require.Equal(t, expected[i], input[i].r1.Merge(input[i].r2))
	}
}

func TestRangeSubtract(t *testing.T) {
	input := testInput()
	expected := [][]Range{
		{
			{testStart, testStart.Add(10 * time.Second)},
		},
		{
			{testStart, testStart.Add(10 * time.Second)},
		},
		{
			{testStart, testStart.Add(5 * time.Second)},
		},
		{
			{testStart, testStart.Add(5 * time.Second)},
			{testStart.Add(8 * time.Second), testStart.Add(10 * time.Second)},
		},
		{
			{testStart, testStart.Add(5 * time.Second)},
		},
		{
			{testStart, testStart.Add(10 * time.Second)},
		},
		{
			{testStart, testStart.Add(10 * time.Second)},
		},
		nil,
		nil,
		{
			{testStart.Add(5 * time.Second), testStart.Add(10 * time.Second)},
		},
		nil,
	}
	for i := 0; i < len(input); i++ {
		require.Equal(t, expected[i], input[i].r1.Subtract(input[i].r2))
	}
}

func TestRangeIterateForward(t *testing.T) {
	testCases := []struct {
		r        Range
		stepSize time.Duration
		expected []time.Time
	}{
		{
			r:        Range{Start: time.Time{}, End: time.Time{}},
			stepSize: time.Second,
		},
		{
			r:        Range{Start: time.Time{}, End: time.Time{}.Add(time.Millisecond)},
			stepSize: time.Second,
			expected: []time.Time{time.Time{}},
		},
		{
			r:        Range{Start: time.Time{}, End: time.Time{}.Add(time.Second)},
			stepSize: time.Second,
			expected: []time.Time{time.Time{}},
		},
		{
			r:        Range{Start: time.Time{}, End: time.Time{}.Add(3 * time.Second)},
			stepSize: time.Second,
			expected: []time.Time{time.Time{}, time.Time{}.Add(time.Second), time.Time{}.Add(2 * time.Second)},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s", tc.r.String()), func(t *testing.T) {
			var actual []time.Time
			tc.r.IterateForward(tc.stepSize, func(currStep time.Time) bool {
				actual = append(actual, currStep)
				return true
			})
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestRangeIterateBackward(t *testing.T) {
	testCases := []struct {
		r        Range
		stepSize time.Duration
		expected []time.Time
	}{
		{
			r:        Range{Start: time.Time{}, End: time.Time{}},
			stepSize: time.Second,
		},
		{
			r:        Range{Start: time.Time{}, End: time.Time{}.Add(time.Millisecond)},
			stepSize: time.Second,
			expected: []time.Time{time.Time{}.Add(time.Millisecond)},
		},
		{
			r:        Range{Start: time.Time{}, End: time.Time{}.Add(time.Second)},
			stepSize: time.Second,
			expected: []time.Time{time.Time{}.Add(time.Second)},
		},
		{
			r:        Range{Start: time.Time{}, End: time.Time{}.Add(3 * time.Second)},
			stepSize: time.Second,
			expected: []time.Time{time.Time{}.Add(3 * time.Second), time.Time{}.Add(2 * time.Second), time.Time{}.Add(time.Second)},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s", tc.r.String()), func(t *testing.T) {
			var actual []time.Time
			tc.r.IterateBackward(tc.stepSize, func(currStep time.Time) bool {
				actual = append(actual, currStep)
				return true
			})
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestRangeString(t *testing.T) {
	start := time.Unix(1465430400, 0).UTC()
	r := Range{Start: start, End: start.Add(2 * time.Hour)}
	require.Equal(t, "(2016-06-09 00:00:00 +0000 UTC,2016-06-09 02:00:00 +0000 UTC)", r.String())
}
