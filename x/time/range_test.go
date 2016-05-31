package time

import (
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
