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

func validateResult(t *testing.T, tr Ranges, expected []Range) {
	l := tr.(*ranges).sortedRanges
	require.Equal(t, len(expected), l.Len())
	idx := 0
	for e := l.Front(); e != nil; e = e.Next() {
		require.Equal(t, e.Value.(Range), expected[idx])
		idx++
	}
}

func validateIter(t *testing.T, it RangeIter, expected []Range) {
	idx := 0
	for it.Next() {
		r := it.Value()
		require.Equal(t, expected[idx], r)
		idx++
	}
}

func getTypedTimeRanges() *ranges {
	return NewRanges().(*ranges)
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
		tr = tr.AddRange(r)
	}
	return tr
}

func TestIsEmpty(t *testing.T) {
	var tr *ranges
	require.True(t, tr.IsEmpty())

	tr = getTypedTimeRanges()
	require.True(t, tr.IsEmpty())

	tr.sortedRanges.PushBack(Range{})
	require.False(t, tr.IsEmpty())

}

func TestClone(t *testing.T) {
	rangesToAdd := getRangesToAdd()
	tr := getPopulatedRanges(rangesToAdd, 0, 4)

	expectedResults := []Range{rangesToAdd[3], rangesToAdd[2], rangesToAdd[0], rangesToAdd[1]}
	validateResult(t, tr, expectedResults)

	cloned := tr.(*ranges).clone()
	tr = tr.RemoveRange(rangesToAdd[0])
	validateResult(t, cloned, expectedResults)
	validateResult(t, tr, []Range{rangesToAdd[3], rangesToAdd[2], rangesToAdd[1]})
}

func TestAddRange(t *testing.T) {
	tr := NewRanges()
	tr = tr.AddRange(Range{})
	validateResult(t, tr, []Range{})

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

	saved := tr
	for i, r := range rangestoAdd {
		tr = tr.AddRange(r)
		validateResult(t, tr, expectedResults[i])
	}
	validateResult(t, saved, []Range{})
}

func TestAddRanges(t *testing.T) {
	rangesToAdd := getRangesToAdd()

	tr := getPopulatedRanges(rangesToAdd, 0, 4)
	tr = tr.AddRanges(nil)

	expectedResults := []Range{rangesToAdd[3], rangesToAdd[2], rangesToAdd[0], rangesToAdd[1]}
	validateResult(t, tr, expectedResults)

	tr2 := getPopulatedRanges(rangesToAdd, 4, 7)
	saved := tr
	tr = tr.AddRanges(tr2)

	expectedResults2 := []Range{{Start: testStart.Add(-10 * time.Second), End: testStart.Add(15 * time.Second)}}
	validateResult(t, tr, expectedResults2)
	validateResult(t, saved, expectedResults)
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

	saved := tr
	for i, r := range rangesToRemove {
		tr = tr.RemoveRange(r)
		validateResult(t, tr, expectedResults[i])
	}

	tr = tr.RemoveRange(Range{})
	validateResult(t, tr, expectedResults[3])

	tr = tr.RemoveRange(Range{
		Start: testStart.Add(-10 * time.Second),
		End:   testStart.Add(15 * time.Second),
	})
	require.True(t, tr.IsEmpty())
	validateResult(t, saved, expectedResults[0])
}

func TestRemoveRanges(t *testing.T) {
	tr := getPopulatedRanges(getRangesToAdd(), 0, 4)
	tr = tr.RemoveRanges(nil)

	expectedResults := []Range{
		{Start: testStart.Add(-8 * time.Second), End: testStart.Add(-5 * time.Second)},
		{Start: testStart.Add(-3 * time.Second), End: testStart.Add(-1 * time.Second)},
		{Start: testStart, End: testStart.Add(time.Second)},
		{Start: testStart.Add(10 * time.Second), End: testStart.Add(15 * time.Second)},
	}
	validateResult(t, tr, expectedResults)

	saved := tr
	tr2 := getPopulatedRanges(getRangesToRemove(), 0, 4)
	tr = tr.RemoveRanges(tr2)

	expectedResults2 := []Range{
		{Start: testStart.Add(-8 * time.Second), End: testStart.Add(-6 * time.Second)},
		{Start: testStart.Add(13 * time.Second), End: testStart.Add(15 * time.Second)},
	}
	validateResult(t, tr, expectedResults2)
	validateResult(t, saved, expectedResults)
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
	tr = tr.RemoveRange(rangesToAdd[2])
	validateIter(t, tr.Iter(), append(expectedResults[:1], expectedResults[2:]...))
}

func TestRangesString(t *testing.T) {
	tr := NewRanges()
	require.Equal(t, "[]", tr.String())
	start := time.Unix(1465430400, 0).UTC()
	tr = tr.AddRange(Range{Start: start, End: start.Add(2 * time.Hour)}).
		AddRange(Range{Start: start.Add(4 * time.Hour), End: start.Add(5 * time.Hour)})
	require.Equal(t, "[(2016-06-09 00:00:00 +0000 UTC,2016-06-09 02:00:00 +0000 UTC),(2016-06-09 04:00:00 +0000 UTC,2016-06-09 05:00:00 +0000 UTC)]", tr.String())
}
