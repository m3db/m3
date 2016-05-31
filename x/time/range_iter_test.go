package time

import (
	"container/list"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	testTimeRanges = []Range{
		{Start: testStart, End: testStart.Add(time.Second)},
		{Start: testStart.Add(2 * time.Second), End: testStart.Add(10 * time.Second)},
		{Start: testStart.Add(20 * time.Second), End: testStart.Add(25 * time.Second)},
	}
)

func getTestList() *list.List {
	l := list.New()
	for _, r := range testTimeRanges {
		l.PushBack(r)
	}
	return l
}

func TestRangeIter(t *testing.T) {
	it := newRangeIter(nil)
	require.False(t, it.Next())

	it = newRangeIter(getTestList())
	for i := 0; i < len(testTimeRanges); i++ {
		require.True(t, it.Next())
		require.Equal(t, testTimeRanges[i], it.Value())
	}
	require.False(t, it.Next())

}
