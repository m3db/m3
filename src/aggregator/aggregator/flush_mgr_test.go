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

package aggregator

import (
	"testing"
	"time"

	"github.com/m3db/m3x/clock"

	"github.com/stretchr/testify/require"
)

func TestFlushManagerRegisterClosed(t *testing.T) {
	mgr, _ := testFlushManager()
	mgr.closed = true
	require.Equal(t, errFlushManagerClosed, mgr.Register(nil))
}

func TestFlushManagerRegisterSuccess(t *testing.T) {
	mgr, now := testFlushManager()
	*now = time.Unix(1234, 0)

	flushers := []PeriodicFlusher{
		&mockFlusher{flushInterval: time.Second},
		&mockFlusher{flushInterval: time.Minute},
		&mockFlusher{flushInterval: time.Second},
		&mockFlusher{flushInterval: time.Hour},
	}
	for _, flusher := range flushers {
		require.NoError(t, mgr.Register(flusher))
	}
	expectedBuckets := []*flushBucket{
		&flushBucket{
			interval: time.Second,
			flushers: []PeriodicFlusher{flushers[0], flushers[2]},
		},
		&flushBucket{
			interval: time.Minute,
			flushers: []PeriodicFlusher{flushers[1]},
		},
		&flushBucket{
			interval: time.Hour,
			flushers: []PeriodicFlusher{flushers[3]},
		},
	}
	require.Equal(t, len(expectedBuckets), len(mgr.buckets))
	for i := 0; i < len(expectedBuckets); i++ {
		require.Equal(t, expectedBuckets[i].interval, mgr.buckets[i].interval)
		require.Equal(t, expectedBuckets[i].flushers, mgr.buckets[i].flushers)
	}
	expectedFlushTimes := []flushMetadata{
		{timeNanos: 1235000000000, bucketIdx: 0},
		{timeNanos: 1294000000000, bucketIdx: 1},
		{timeNanos: 4834000000000, bucketIdx: 2},
	}
	require.Equal(t, flushMetadataHeap(expectedFlushTimes), mgr.flushTimes)
}

func TestFlushManagerCloseSuccess(t *testing.T) {
	opts, _ := testFlushManagerOptions()
	opts = opts.SetCheckEvery(time.Second)
	mgr := NewFlushManager(opts).(*flushManager)

	// Wait a little for the flush goroutine to start.
	time.Sleep(100 * time.Millisecond)

	mgr.Close()
	require.True(t, mgr.closed)
	require.Panics(t, func() { mgr.wgFlush.Done() })
}

func TestFlushManagerFlush(t *testing.T) {
	opts := NewFlushManagerOptions().
		SetCheckEvery(100 * time.Millisecond).
		SetJitterEnabled(false)
	mgr := NewFlushManager(opts).(*flushManager)

	var flushCounts [4]int
	flushers := []PeriodicFlusher{
		&mockFlusher{
			flushInterval: 100 * time.Millisecond,
			flushFn:       func() { flushCounts[0]++ },
		},
		&mockFlusher{
			flushInterval: 200 * time.Millisecond,
			flushFn:       func() { flushCounts[1]++ },
		},
		&mockFlusher{
			flushInterval: 100 * time.Millisecond,
			flushFn:       func() { flushCounts[2]++ },
		},
		&mockFlusher{
			flushInterval: 500 * time.Millisecond,
			flushFn:       func() { flushCounts[3]++ },
		},
	}

	for _, flusher := range flushers {
		require.NoError(t, mgr.Register(flusher))
	}
	time.Sleep(1200 * time.Millisecond)
	mgr.Close()

	require.True(t, flushCounts[0] >= 12)
	require.True(t, flushCounts[1] >= 6)
	require.True(t, flushCounts[2] >= 12)
	require.True(t, flushCounts[3] == 3)

}

func testFlushManager() (*flushManager, *time.Time) {
	opts, now := testFlushManagerOptions()
	return NewFlushManager(opts).(*flushManager), now
}

func testFlushManagerOptions() (FlushManagerOptions, *time.Time) {
	var now time.Time
	nowFn := func() time.Time { return now }
	clockOpts := clock.NewOptions().SetNowFn(nowFn)
	return NewFlushManagerOptions().
		SetClockOptions(clockOpts).
		SetCheckEvery(0).
		SetJitterEnabled(false), &now
}

type flushFn func()

type mockFlusher struct {
	flushInterval time.Duration
	flushFn       flushFn
}

func (f *mockFlusher) FlushInterval() time.Duration { return f.flushInterval }

func (f *mockFlusher) Flush() { f.flushFn() }
