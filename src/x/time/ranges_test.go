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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func validateIter(t *testing.T, it *RangeIter, expected []Range) {
	idx := 0
	for it.Next() {
		r := it.Value()
		require.Equal(t, expected[idx], r)
		idx++
	}
}

func getRangesToAdd() []Range {
	return []Range{
		{Start: testStart, End: testStart.Add(time.Second)},
		{Start: testStart.Add(10 * time.Second), End: testStart.Add(15 * time.Second)},
		{Start: testStart.Add(-3 * time.Second), End: testStart.Add(-1 * time.Second)},
		{Start: testStart.Add(-8 * time.Second), End: testStart.Add(-5 * time.Second)},
		{Start: testStart.Add(-1 * time.Second), End: testStart},
		{Start: testStart.Add(time.Second), End: testStart.Add(8 * time.Second)},
		{Start: testStart.Add(-10 * time.Second), End: testStart.Add(12 * time.Second)},
	}
}

func getRangesToRemove() []Range {
	return []Range{
		{Start: testStart.Add(-5 * time.Second), End: testStart.Add(-3 * time.Second)},
		{Start: testStart.Add(-6 * time.Second), End: testStart.Add(8 * time.Second)},
		{Start: testStart.Add(12 * time.Second), End: testStart.Add(13 * time.Second)},
		{Start: testStart.Add(10 * time.Second), End: testStart.Add(12 * time.Second)},
	}
}

func getPopulatedRanges(ranges []Range, start, end int) Ranges {
	tr := NewRanges()
	for _, r := range ranges[start:end] {
		tr.AddRange(r)
	}
	return tr
}

func TestIsEmpty(t *testing.T) {
	tr := NewRanges()
	require.True(t, tr.IsEmpty())

	tr.AddRange(getRangesToAdd()[0])
	require.False(t, tr.IsEmpty())
}

func TestNewRanges(t *testing.T) {
	rangesToAdd := getRangesToAdd()
	exp := getPopulatedRanges(rangesToAdd, 0, len(rangesToAdd))
	clone := exp.Clone()
	obs := NewRanges(rangesToAdd...)
	exp.RemoveRanges(clone)
	require.True(t, exp.IsEmpty())
	obs.RemoveRanges(clone)
	require.True(t, obs.IsEmpty())
}

func TestClone(t *testing.T) {
	rangesToAdd := getRangesToAdd()
	tr := getPopulatedRanges(rangesToAdd, 0, 4)

	expectedResults := []Range{rangesToAdd[3], rangesToAdd[2], rangesToAdd[0], rangesToAdd[1]}
	validateIter(t, tr.Iter(), expectedResults)

	cloned := tr.Clone()
	tr.RemoveRange(rangesToAdd[0])
	validateIter(t, cloned.Iter(), expectedResults)
	validateIter(t, tr.Iter(), []Range{rangesToAdd[3], rangesToAdd[2], rangesToAdd[1]})
}

func TestAddRange(t *testing.T) {
	tr := NewRanges()
	tr.AddRange(Range{})
	validateIter(t, tr.Iter(), []Range{})

	rangestoAdd := getRangesToAdd()
	expectedResults := [][]Range{
		{rangestoAdd[0]},
		{rangestoAdd[0], rangestoAdd[1]},
		{rangestoAdd[2], rangestoAdd[0], rangestoAdd[1]},
		{rangestoAdd[3], rangestoAdd[2], rangestoAdd[0], rangestoAdd[1]},
		{rangestoAdd[3], rangestoAdd[2], rangestoAdd[4], rangestoAdd[0], rangestoAdd[1]},
		{rangestoAdd[3], rangestoAdd[2], rangestoAdd[4], rangestoAdd[0], rangestoAdd[5], rangestoAdd[1]},
		{{Start: testStart.Add(-10 * time.Second), End: testStart.Add(15 * time.Second)}},
	}

	saved := tr.Clone()
	for i, r := range rangestoAdd {
		tr.AddRange(r)
		validateIter(t, tr.Iter(), expectedResults[i])
	}
	validateIter(t, saved.Iter(), []Range{})
}

func TestAddRanges(t *testing.T) {
	rangesToAdd := getRangesToAdd()

	tr := getPopulatedRanges(rangesToAdd, 0, 4)
	tr.AddRanges(NewRanges())

	expectedResults := []Range{rangesToAdd[3], rangesToAdd[2], rangesToAdd[0], rangesToAdd[1]}
	validateIter(t, tr.Iter(), expectedResults)

	tr2 := getPopulatedRanges(rangesToAdd, 4, 7)
	saved := tr.Clone()
	tr.AddRanges(tr2)

	expectedResults2 := []Range{{Start: testStart.Add(-10 * time.Second), End: testStart.Add(15 * time.Second)}}
	validateIter(t, tr.Iter(), expectedResults2)
	validateIter(t, saved.Iter(), expectedResults)
}

func TestRemoveRange(t *testing.T) {
	tr := getPopulatedRanges(getRangesToAdd(), 0, 4)

	rangesToRemove := getRangesToRemove()
	expectedResults := [][]Range{
		{
			{Start: testStart.Add(-8 * time.Second), End: testStart.Add(-5 * time.Second)},
			{Start: testStart.Add(-3 * time.Second), End: testStart.Add(-1 * time.Second)},
			{Start: testStart, End: testStart.Add(time.Second)},
			{Start: testStart.Add(10 * time.Second), End: testStart.Add(15 * time.Second)},
		},
		{
			{Start: testStart.Add(-8 * time.Second), End: testStart.Add(-6 * time.Second)},
			{Start: testStart.Add(10 * time.Second), End: testStart.Add(15 * time.Second)},
		},
		{
			{Start: testStart.Add(-8 * time.Second), End: testStart.Add(-6 * time.Second)},
			{Start: testStart.Add(10 * time.Second), End: testStart.Add(12 * time.Second)},
			{Start: testStart.Add(13 * time.Second), End: testStart.Add(15 * time.Second)},
		},
		{
			{Start: testStart.Add(-8 * time.Second), End: testStart.Add(-6 * time.Second)},
			{Start: testStart.Add(13 * time.Second), End: testStart.Add(15 * time.Second)},
		},
	}

	saved := tr.Clone()
	for i, r := range rangesToRemove {
		tr.RemoveRange(r)
		validateIter(t, tr.Iter(), expectedResults[i])
	}

	tr.RemoveRange(Range{})
	validateIter(t, tr.Iter(), expectedResults[3])

	tr.RemoveRange(Range{
		Start: testStart.Add(-10 * time.Second),
		End:   testStart.Add(15 * time.Second),
	})
	require.True(t, tr.IsEmpty())
	validateIter(t, saved.Iter(), expectedResults[0])
}

func TestRemoveRanges(t *testing.T) {
	tr := getPopulatedRanges(getRangesToAdd(), 0, 4)
	tr.RemoveRanges(NewRanges())

	expectedResults := []Range{
		{Start: testStart.Add(-8 * time.Second), End: testStart.Add(-5 * time.Second)},
		{Start: testStart.Add(-3 * time.Second), End: testStart.Add(-1 * time.Second)},
		{Start: testStart, End: testStart.Add(time.Second)},
		{Start: testStart.Add(10 * time.Second), End: testStart.Add(15 * time.Second)},
	}
	validateIter(t, tr.Iter(), expectedResults)

	saved := tr.Clone()
	tr2 := getPopulatedRanges(getRangesToRemove(), 0, 4)
	tr.RemoveRanges(tr2)

	expectedResults2 := []Range{
		{Start: testStart.Add(-8 * time.Second), End: testStart.Add(-6 * time.Second)},
		{Start: testStart.Add(13 * time.Second), End: testStart.Add(15 * time.Second)},
	}
	validateIter(t, tr.Iter(), expectedResults2)
	validateIter(t, saved.Iter(), expectedResults)
}

func TestOverlaps(t *testing.T) {
	tr := getPopulatedRanges(getRangesToAdd(), 0, 4)
	require.True(t, tr.Overlaps(Range{Start: testStart, End: testStart.Add(time.Second)}))
	require.True(t, tr.Overlaps(Range{Start: testStart.Add(-7 * time.Second), End: testStart.Add(-5 * time.Second)}))
	require.True(t, tr.Overlaps(Range{Start: testStart.Add(-7 * time.Second), End: testStart.Add(-4 * time.Second)}))
	require.True(t, tr.Overlaps(Range{Start: testStart.Add(-3 * time.Second), End: testStart.Add(1 * time.Second)}))
	require.True(t, tr.Overlaps(Range{Start: testStart.Add(9 * time.Second), End: testStart.Add(15 * time.Second)}))
	require.True(t, tr.Overlaps(Range{Start: testStart.Add(12 * time.Second), End: testStart.Add(13 * time.Second)}))
	require.False(t, tr.Overlaps(Range{Start: testStart, End: testStart}))
	require.False(t, tr.Overlaps(Range{Start: testStart.Add(time.Second), End: testStart.Add(2 * time.Second)}))
}

func TestRangesIter(t *testing.T) {
	rangesToAdd := getRangesToAdd()
	tr := getPopulatedRanges(rangesToAdd, 0, 4)
	expectedResults := []Range{
		{Start: testStart.Add(-8 * time.Second), End: testStart.Add(-5 * time.Second)},
		{Start: testStart.Add(-3 * time.Second), End: testStart.Add(-1 * time.Second)},
		{Start: testStart, End: testStart.Add(time.Second)},
		{Start: testStart.Add(10 * time.Second), End: testStart.Add(15 * time.Second)},
	}
	validateIter(t, tr.Iter(), expectedResults)
	tr.RemoveRange(rangesToAdd[2])
	validateIter(t, tr.Iter(), append(expectedResults[:1], expectedResults[2:]...))
}

// func TestRangesString(t *testing.T) {
// 	tr := NewRanges()
// 	require.Equal(t, "[]", tr.String())
// 	start := time.Unix(1465430400, 0).UTC()
// 	tr.AddRanges(NewRanges(
// 		Range{Start: start, End: start.Add(2 * time.Hour)},
// 		Range{Start: start.Add(4 * time.Hour), End: start.Add(5 * time.Hour)}))
// 	require.Equal(t, "[(2016-06-09 00:00:00 +0000 UTC,2016-06-09 02:00:00 +0000 UTC),(2016-06-09 04:00:00 +0000 UTC,2016-06-09 05:00:00 +0000 UTC)]", tr.String())
// }
