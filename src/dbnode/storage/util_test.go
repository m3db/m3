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

	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestNumIntervals(t *testing.T) {
	var (
		bs      = 10
		timeFor = func(i int) xtime.UnixNano {
			return xtime.ToUnixNano(time.Unix(int64(bs*i), 0))
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
