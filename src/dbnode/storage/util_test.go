// Copyright (c) 2017 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/storage/series"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestNumIntervals(t *testing.T) {
	var (
		bs      = 10
		timeFor = func(i int) xtime.UnixNano {
			return xtime.FromSeconds(int64(bs * i))
		}
	)

	w := time.Second * time.Duration(bs)
	t0 := timeFor(0)
	t1 := timeFor(1)

	require.Equal(t, 0, numIntervals(t1, t0, w))
	require.Equal(t, 1, numIntervals(t0, t0, w))
	require.Equal(t, 2, numIntervals(t0, t1, w))
}

func TestNumIntervalsStartEqualsEnd(t *testing.T) {
	s := xtime.Now()
	w := time.Minute
	require.Equal(t, 1, numIntervals(s, s, w))
}

func TestFilterTimes(t *testing.T) {
	// Test with empty slice
	times := []xtime.UnixNano{}
	predicate := func(t xtime.UnixNano) bool { return true }
	require.Empty(t, filterTimes(times, predicate))

	// Test with nil slice
	require.Empty(t, filterTimes(nil, predicate))

	// Test with all times matching predicate
	times = []xtime.UnixNano{
		xtime.FromSeconds(1),
		xtime.FromSeconds(2),
		xtime.FromSeconds(3),
	}
	filtered := filterTimes(times, func(t xtime.UnixNano) bool { return true })
	require.Equal(t, times, filtered)

	// Test with no times matching predicate
	filtered = filterTimes(times, func(t xtime.UnixNano) bool { return false })
	require.Empty(t, filtered)

	// Test with some times matching predicate
	filtered = filterTimes(times, func(t xtime.UnixNano) bool {
		return t.Seconds()%2 == 0
	})
	require.Equal(t, []xtime.UnixNano{xtime.FromSeconds(2)}, filtered)

	// Test with complex predicate
	times = []xtime.UnixNano{
		xtime.FromSeconds(1),
		xtime.FromSeconds(2),
		xtime.FromSeconds(3),
		xtime.FromSeconds(4),
		xtime.FromSeconds(5),
	}
	filtered = filterTimes(times, func(t xtime.UnixNano) bool {
		return t.Seconds() > 2 && t.Seconds() < 5
	})
	require.Equal(t, []xtime.UnixNano{
		xtime.FromSeconds(3),
		xtime.FromSeconds(4),
	}, filtered)
}

func TestTimesInRangeEdgeCases(t *testing.T) {
	// Test with zero window size
	start := xtime.Now()
	end := start.Add(time.Hour)
	require.Empty(t, timesInRange(start, end, 0))

	// Test with negative window size
	require.Empty(t, timesInRange(start, end, -time.Hour))

	// Test with end before start
	require.Empty(t, timesInRange(end, start, time.Hour))

	// Test with very small window size
	window := time.Second
	times := timesInRange(start, end, window)
	require.NotEmpty(t, times)
	require.True(t, times[0].Equal(end))
	require.True(t, times[len(times)-1].Equal(start))

	// Test with window size larger than range
	window = 2 * time.Hour
	times = timesInRange(start, end, window)
	require.Equal(t, []xtime.UnixNano{end}, times)
}

func TestNumIntervalsEdgeCases(t *testing.T) {
	// Test with very large numbers
	start := xtime.FromSeconds(0)
	end := xtime.FromSeconds(1000000)
	window := time.Second
	require.Equal(t, 1000001, numIntervals(start, end, window))

	// Test with very small window
	window = time.Nanosecond
	require.Equal(t, 1, numIntervals(start, start, window))

	// Test with window equal to range
	window = time.Hour
	start = xtime.Now()
	end = start.Add(window)
	require.Equal(t, 2, numIntervals(start, end, window))

	// Test with window larger than range
	window = 2 * time.Hour
	require.Equal(t, 1, numIntervals(start, end, window))
}

func TestDefaultTestOptions(t *testing.T) {
	// Test that options are properly initialized
	opts := DefaultTestOptions()
	require.NotNil(t, opts)

	// Test that options have expected values
	require.False(t, opts.RepairEnabled())
	require.Equal(t, series.CacheAll, opts.SeriesCachePolicy())
	require.NotNil(t, opts.PersistManager())
	require.NotNil(t, opts.BlockLeaseManager())
	require.NotNil(t, opts.IndexOptions().PostingsListCache())
}

func TestNumIntervalsStartAfterEnd(t *testing.T) {
	w := time.Minute
	end := xtime.Now()
	start := end.Add(w)
	require.Equal(t, 0, numIntervals(start, end, w))
}

func TestNumIntervalsZeroWindow(t *testing.T) {
	w := time.Duration(0)
	s := xtime.Now()
	require.Equal(t, 0, numIntervals(s, s, w))
}

func TestNumIntervalsNegativeWindow(t *testing.T) {
	w := -time.Minute
	s := xtime.Now()
	require.Equal(t, 0, numIntervals(s, s, w))
}

func TestTimesInRange(t *testing.T) {
	var (
		w       = 10 * time.Second
		timeFor = func(i int64) xtime.UnixNano {
			return xtime.UnixNano(int64(w) * i)
		}
		timesForN = func(i, j int64) []xtime.UnixNano {
			times := make([]xtime.UnixNano, 0, j-i+1)
			for k := j; k >= i; k-- {
				times = append(times, timeFor(k))
			}
			return times
		}
	)
	// [0, 2] with a gap of 1 ==> [0, 1, 2]
	require.Equal(t, []xtime.UnixNano{timeFor(2), timeFor(1), timeFor(0)},
		timesInRange(timeFor(0), timeFor(2), w))

	// [2, 1] with a gap of 1 ==> empty result
	require.Empty(t, timesInRange(timeFor(2), timeFor(1), w))

	// [2, 2] with a gap of 1 ==> [2]
	require.Equal(t, []xtime.UnixNano{timeFor(2)},
		timesInRange(timeFor(2), timeFor(2), w))

	// [0, 9] with a gap of 3 ==> [9, 6, 3, 0]
	require.Equal(t, []xtime.UnixNano{timeFor(9), timeFor(6), timeFor(3), timeFor(0)},
		timesInRange(timeFor(0), timeFor(9), 3*w))

	// [1, 9] with a gap of 3 ==> [9, 6, 3]
	require.Equal(t, []xtime.UnixNano{timeFor(9), timeFor(6), timeFor(3)},
		timesInRange(timeFor(1), timeFor(9), 3*w))

	// [1, 100] with a gap of 1 ==> [100, 99, ..., 1]
	require.Equal(t, timesForN(1, 100), timesInRange(timeFor(1), timeFor(100), w))
}
