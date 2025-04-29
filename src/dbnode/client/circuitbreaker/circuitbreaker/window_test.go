package circuitbreaker

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type mockClock struct {
	now time.Time
}

func (m *mockClock) Now() time.Time {
	return m.now
}

type atomicMockClock struct {
	nowInNano atomic.Int64
}

func (a *atomicMockClock) Now() time.Time {
	nowInNano := a.nowInNano.Load()
	return time.Unix(0, nowInNano)
}

func (a *atomicMockClock) updateTime(now time.Time) {
	a.nowInNano.Store(now.UnixNano())
}

func TestBucketReset(t *testing.T) {
	t.Run("nil_bucket", func(t *testing.T) {
		assert.NotPanics(t, func() {
			var bucket *bucket
			bucket.reset()
		})
	})

	t.Run("non_nil_bucket", func(t *testing.T) {
		bucket := bucket{
			startTime: time.Now(),
			endTime:   time.Now(),
			counters: counters{
				successfulRequests: *atomic.NewInt64(1),
			},
		}
		bucket.reset()
		assert.Zero(t, bucket.startTime, "expected zero value start time")
		assert.Zero(t, bucket.endTime, "expected zero value end time")
		assert.Zero(t, bucket.counters, "expected zero value counters")
	})
}

func TestBucketIsExpired(t *testing.T) {
	t.Run("nil_bucket", func(t *testing.T) {
		var bucket *bucket
		require.True(t, bucket.isExpired(time.Unix(10, 0)), "expected bucket expired for nil bucket")
	})

	t.Run("non_nil_bucket", func(t *testing.T) {
		bucket := bucket{endTime: time.Unix(10, 0)}
		require.True(t, bucket.isExpired(time.Unix(10, 1)), "expected bucket expired")
		require.False(t, bucket.isExpired(time.Unix(10, 0)), "expected bucket expired")
	})
}

func TestBucketShouldDrop(t *testing.T) {
	t.Run("nil_bucket", func(t *testing.T) {
		var bucket *bucket
		require.False(t, bucket.shouldDrop(time.Unix(10, 0)), "expected bucket should not drop")
	})

	t.Run("non_nil_bucket", func(t *testing.T) {
		bucket := bucket{startTime: time.Unix(10, 0)}
		require.False(t, bucket.shouldDrop(time.Unix(9, 1)), "expected bucket should not drop")
		require.True(t, bucket.shouldDrop(time.Unix(11, 0)), "expected bucket should drop")
	})
}

func TestWindowReset(t *testing.T) {
	sw := &slidingWindow{
		bucketDuration: time.Second,
		buckets: []bucket{
			{startTime: time.Unix(10, 0), endTime: time.Unix(21, 0), counters: counters{totalRequests: *atomic.NewInt64(100)}},
			{startTime: time.Unix(11, 0), endTime: time.Unix(22, 0), counters: counters{totalRequests: *atomic.NewInt64(100)}},
			{startTime: time.Unix(12, 0), endTime: time.Unix(23, 0), counters: counters{totalRequests: *atomic.NewInt64(100)}},
			{startTime: time.Unix(13, 0), endTime: time.Unix(24, 0), counters: counters{totalRequests: *atomic.NewInt64(100)}},
		},
		oldestBucketIndex: 2,
		aggregatedCounters: counters{
			totalRequests: *atomic.NewInt64(400),
		},
		windowStartTimeDelta: -3 * time.Second,
	}
	sw.reset()

	assert.Equal(t, counters{}, sw.aggregatedCounters, "expected empty counters")
	assert.Equal(t, time.Second, sw.bucketDuration, "expected bucket size to remain unchanged")
	assert.Equal(t, -1, sw.oldestBucketIndex, "expected oldest bucket index to be -1")
	assert.Equal(t, -1, sw.currentBucketIndex, "expected current bucket index to be -1")
	assert.Equal(t, -3*time.Second, sw.windowStartTimeDelta, "expected unchanged window start delta")
	require.Len(t, sw.buckets, 4, "expected length of buckets to be unchanged")
	for i := 0; i < 4; i++ {
		assert.Equal(t, bucket{}, sw.buckets[i], "expected empty bucket")
	}
}

func TestNewSlidingWindow(t *testing.T) {
	sw := newSlidingWindow(10, time.Second)
	assert.Equal(t, counters{}, sw.aggregatedCounters, "expected empty counters")
	assert.Equal(t, time.Second, sw.bucketDuration, "expected bucket size to remain unchanged")
	assert.Equal(t, -1, sw.oldestBucketIndex, "expected oldest bucket index to be -1")
	assert.Equal(t, -1, sw.currentBucketIndex, "expected current bucket index to be -1")
	assert.Equal(t, -9*time.Second, sw.windowStartTimeDelta, "expected window start delta")
	require.Len(t, sw.buckets, 10, "expected length of buckets to be unchanged")
}

func TestNextBucketIndex(t *testing.T) {
	sw := newSlidingWindow(10, time.Second)
	assert.Equal(t, 9, sw.nextBucketIndex(8), "expected last bucket index")
	assert.Equal(t, 0, sw.nextBucketIndex(9), "expected zero bucket index")
}

func TestIsWindowEmpty(t *testing.T) {
	sw := newSlidingWindow(10, time.Second)
	require.True(t, sw.isWindowEmpty(), "expected sliding window to be empty when newly created")

	sw.currentBucketAndSlideIfNeeded()
	require.False(t, sw.isWindowEmpty(), "expected sliding window to have a single bucket")
}

func TestGetBucketEndTime(t *testing.T) {
	sw := slidingWindow{bucketDuration: time.Millisecond}
	gotEndTime := sw.getBucketEndTime(time.Unix(0, 0))
	expectedEndTime := time.Unix(0, int64(time.Millisecond)-1)
	assert.Equal(t, expectedEndTime, gotEndTime, "unexpected end time")
}

func TestDropExpiredBuckets(t *testing.T) {
	tests := []struct {
		description string
		mockTime    time.Time
		sw          *slidingWindow
		expectedSw  *slidingWindow
	}{
		{
			description: "empty_sliding_window",
			mockTime:    time.Unix(10, 0),
			sw: &slidingWindow{
				oldestBucketIndex:  -1,
				currentBucketIndex: -1,
				bucketDuration:     time.Second,
				buckets:            []bucket{{}, {}, {}, {}},
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  -1,
				currentBucketIndex: -1,
				bucketDuration:     time.Second,
				buckets:            []bucket{{}, {}, {}, {}},
			},
		},
		{
			description: "must_drop_expired_initial_bucket",
			mockTime:    time.Unix(30, 0),
			sw: &slidingWindow{
				oldestBucketIndex:  0,
				currentBucketIndex: 0,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{startTime: time.Unix(20, 1), endTime: time.Unix(21, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
					{},
					{},
					{},
				},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  -1,
				currentBucketIndex: -1,
				bucketDuration:     time.Second,
				buckets:            []bucket{{}, {}, {}, {}},
			},
		},
		{
			description: "must_drop_expired_bucket",
			mockTime:    time.Unix(24, 1),
			sw: &slidingWindow{
				oldestBucketIndex:  1,
				currentBucketIndex: 0,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{startTime: time.Unix(23, 1), endTime: time.Unix(24, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
					{startTime: time.Unix(20, 1), endTime: time.Unix(21, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
					{startTime: time.Unix(21, 1), endTime: time.Unix(22, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
					{startTime: time.Unix(22, 1), endTime: time.Unix(23, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
				},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  2,
				currentBucketIndex: 0,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{startTime: time.Unix(23, 1), endTime: time.Unix(24, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
					{},
					{startTime: time.Unix(21, 1), endTime: time.Unix(22, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
					{startTime: time.Unix(22, 1), endTime: time.Unix(23, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
				},
				windowStartTimeDelta: -3 * time.Second,
			},
		},
		{
			description: "must_drop_all_expired_buckets_and_start_from_zero_bucket",
			mockTime:    time.Unix(26, 1),
			sw: &slidingWindow{
				oldestBucketIndex:  1,
				currentBucketIndex: 0,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{startTime: time.Unix(23, 1), endTime: time.Unix(24, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
					{startTime: time.Unix(20, 1), endTime: time.Unix(21, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
					{startTime: time.Unix(21, 1), endTime: time.Unix(22, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
					{startTime: time.Unix(22, 1), endTime: time.Unix(23, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
				},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  0,
				currentBucketIndex: 0,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{startTime: time.Unix(23, 1), endTime: time.Unix(24, 0), counters: counters{
						totalRequests: *atomic.NewInt64(10),
					}},
					{}, {}, {},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			tt.sw.dropExpiredBuckets(tt.mockTime)
			assert.Equal(t, tt.expectedSw.oldestBucketIndex, tt.sw.oldestBucketIndex, "unexpected oldest bucket index")
			assert.Equal(t, tt.expectedSw.currentBucketIndex, tt.sw.currentBucketIndex, "unexpected current bucket index")
			assert.Equal(t, tt.expectedSw.activeBucketsCount(), tt.sw.activeBucketsCount(), "unexpected active buckets counts")
			require.Len(t, tt.sw.buckets, len(tt.expectedSw.buckets), "unexpected bucket length")
			for i := range tt.sw.buckets {
				assert.Equal(t, tt.expectedSw.buckets[i], tt.sw.buckets[i])
			}
		})
	}
}

func TestCreateInitialBucket(t *testing.T) {
	sw := newSlidingWindow(10, time.Second)
	mockTime := time.Unix(10, 1)
	sw.createInitialBucket(mockTime)
	assert.Equal(t, 0, sw.oldestBucketIndex, "expected zero oldest bucket index")
	assert.Equal(t, 0, sw.currentBucketIndex, "expected zero current bucket index")
	assert.Equal(t, mockTime, sw.buckets[0].startTime, "unexpected start time")
	assert.Equal(t, mockTime.Add(time.Second-1), sw.buckets[0].endTime, "unexpected end time")
}

func TestSlide(t *testing.T) {
	tests := []struct {
		description string
		mockTime    time.Time
		sw          *slidingWindow
		expectedSw  *slidingWindow
	}{
		{
			description: "empty_sliding_window",
			mockTime:    time.Unix(10, 1),
			sw: &slidingWindow{
				oldestBucketIndex:  -1,
				currentBucketIndex: -1,
				bucketDuration:     time.Second,
				buckets:            []bucket{{}, {}, {}, {}},
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  0,
				currentBucketIndex: 0,
				bucketDuration:     time.Second,
				buckets:            []bucket{{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)}, {}, {}, {}},
			},
		},
		{
			description: "must_slide_next_window",
			mockTime:    time.Unix(11, 1),
			sw: &slidingWindow{
				oldestBucketIndex:    0,
				currentBucketIndex:   0,
				bucketDuration:       time.Second,
				buckets:              []bucket{{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)}, {}, {}, {}},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  0,
				currentBucketIndex: 1,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)},
					{startTime: time.Unix(11, 1), endTime: time.Unix(12, 0)},
					{}, {}},
			},
		},
		{
			description: "must_slide_over",
			mockTime:    time.Unix(11, 1),
			sw: &slidingWindow{
				oldestBucketIndex:  3,
				currentBucketIndex: 3,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{}, {}, {}, {startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)},
				},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  3,
				currentBucketIndex: 0,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{startTime: time.Unix(11, 1), endTime: time.Unix(12, 0)},
					{},
					{},
					{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)},
				},
				windowStartTimeDelta: -3 * time.Second,
			},
		},
		{
			description: "must_drop_and_slide_window",
			mockTime:    time.Unix(15, 1),
			sw: &slidingWindow{
				oldestBucketIndex:  1,
				currentBucketIndex: 3,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{},
					{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)},
					{startTime: time.Unix(11, 1), endTime: time.Unix(12, 0)},
					{startTime: time.Unix(12, 1), endTime: time.Unix(13, 0)},
				},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  3,
				currentBucketIndex: 0,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{startTime: time.Unix(15, 1), endTime: time.Unix(16, 0)},
					{},
					{},
					{startTime: time.Unix(12, 1), endTime: time.Unix(13, 0)},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			tt.sw.slide(tt.mockTime)
			assert.Equal(t, tt.expectedSw.oldestBucketIndex, tt.sw.oldestBucketIndex, "unexpected oldest bucket index")
			assert.Equal(t, tt.expectedSw.currentBucketIndex, tt.sw.currentBucketIndex, "unexpected current bucket index")
			assert.Equal(t, tt.expectedSw.activeBucketsCount(), tt.sw.activeBucketsCount(), "unexpected active buckets counts")
			require.Len(t, tt.sw.buckets, len(tt.expectedSw.buckets), "unexpected bucket length")
			for i := range tt.sw.buckets {
				assert.Equal(t, tt.expectedSw.buckets[i], tt.sw.buckets[i])
			}
		})
	}
}

func TestCurrentBucket(t *testing.T) {
	t.Run("must_be_nil_when_queue_is_empty", func(t *testing.T) {
		sw := newSlidingWindow(5, time.Second)
		assert.Nil(t, sw.currentBucket(), "expected nil current bucket")
	})

	t.Run("must_get_current_bucket", func(t *testing.T) {
		sw := &slidingWindow{
			currentBucketIndex: 2,
			oldestBucketIndex:  2,
			buckets: []bucket{
				{}, {}, {startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)},
			},
		}

		expectedBucket := bucket{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)}
		assert.Equal(t, expectedBucket, *sw.currentBucket(), "unexpected bucket")
	})
}

func TestCurrentBucketAndSlideIfNeeded(t *testing.T) {
	tests := []struct {
		description    string
		mockTime       time.Time
		sw             *slidingWindow
		expectedSw     *slidingWindow
		expectedBucket bucket
	}{
		{
			description: "must_create_initial_bucket",
			mockTime:    time.Unix(10, 1),
			sw: &slidingWindow{
				oldestBucketIndex:    -1,
				currentBucketIndex:   -1,
				bucketDuration:       time.Second,
				buckets:              []bucket{{}, {}, {}, {}},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  0,
				currentBucketIndex: 0,
				bucketDuration:     time.Second,
				buckets:            []bucket{{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)}, {}, {}, {}},
			},
			expectedBucket: bucket{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)},
		},
		{
			description: "must_return_existing_bucket",
			mockTime:    time.Unix(10, 1),
			sw: &slidingWindow{
				oldestBucketIndex:  1,
				currentBucketIndex: 1,
				bucketDuration:     time.Second,
				buckets:            []bucket{{}, {startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)}, {}, {}},
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  1,
				currentBucketIndex: 1,
				bucketDuration:     time.Second,
				buckets:            []bucket{{}, {startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)}, {}, {}},
			},
			expectedBucket: bucket{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)},
		},
		{
			description: "must_return_next_bucket",
			mockTime:    time.Unix(11, 1),
			sw: &slidingWindow{
				oldestBucketIndex:    1,
				currentBucketIndex:   1,
				bucketDuration:       time.Second,
				buckets:              []bucket{{}, {startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)}, {}, {}},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  1,
				currentBucketIndex: 2,
				bucketDuration:     time.Second,
				buckets: []bucket{{},
					{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)},
					{startTime: time.Unix(11, 1), endTime: time.Unix(12, 0)},
					{},
				},
			},
			expectedBucket: bucket{startTime: time.Unix(11, 1), endTime: time.Unix(12, 0)},
		},
		{
			description: "must_slide_over_window",
			mockTime:    time.Unix(14, 1),
			sw: &slidingWindow{
				oldestBucketIndex:  1,
				currentBucketIndex: 2,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{},
					{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)},
					{startTime: time.Unix(11, 1), endTime: time.Unix(12, 0)},
					{},
				},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedSw: &slidingWindow{
				oldestBucketIndex:  2,
				currentBucketIndex: 3,
				bucketDuration:     time.Second,
				buckets: []bucket{{},
					{},
					{startTime: time.Unix(11, 1), endTime: time.Unix(12, 0)},
					{startTime: time.Unix(14, 1), endTime: time.Unix(15, 0)},
				},
			},
			expectedBucket: bucket{startTime: time.Unix(14, 1), endTime: time.Unix(15, 0)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			tt.sw.clock = &mockClock{now: tt.mockTime}
			bucket := tt.sw.currentBucketAndSlideIfNeeded()
			require.Equal(t, tt.expectedBucket, *bucket, "unexpected bucket")
			assert.Equal(t, tt.expectedSw.oldestBucketIndex, tt.sw.oldestBucketIndex, "unexpected oldest bucket index")
			assert.Equal(t, tt.expectedSw.currentBucketIndex, tt.sw.currentBucketIndex, "unexpected current bucket index")
			require.Len(t, tt.sw.buckets, 4, "expected length of buckets to be unchanged")
			for i := range tt.sw.buckets {
				assert.Equal(t, tt.expectedSw.buckets[i], tt.sw.buckets[i])
			}
		})
	}

	t.Run("must_create_bucket_for_now_only_once", func(t *testing.T) {
		sw := newSlidingWindow(4, time.Second)
		mockTime := &atomicMockClock{nowInNano: *atomic.NewInt64(time.Unix(10, 1).UnixNano())}
		sw.clock = mockTime
		// Simulate multiple calls with multiple goroutine to reproduce pattern
		// from production traffic.
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				for j := 0; j < 1000; j++ {
					if j == 400 {
						mockTime.updateTime(time.Unix(11, 1))
					}
					_ = sw.currentBucketAndSlideIfNeeded()
				}
				wg.Done()
			}()
		}

		wg.Wait()
		assert.Equal(t, 0, sw.oldestBucketIndex, "unexpected oldest bucket index")
		assert.Equal(t, 1, sw.currentBucketIndex, "unexpected current bucket index")

		expectedBuckets := []bucket{
			{startTime: time.Unix(10, 1), endTime: time.Unix(11, 0)},
			{startTime: time.Unix(11, 1), endTime: time.Unix(12, 0)},
			{}, {},
		}
		require.Len(t, sw.buckets, 4, "expected length of buckets to be unchanged")
		for i := range expectedBuckets {
			assert.Equal(t, expectedBuckets[i], sw.buckets[i], "unexpected bucket")
		}
	})
}

func TestCounters(t *testing.T) {
	tests := []struct {
		description  string
		mockTime     time.Time
		sw           *slidingWindow
		expectedStat counters
	}{
		{
			description: "must_return_current_aggregate",
			mockTime:    time.Unix(10, 1),
			sw: &slidingWindow{
				oldestBucketIndex:  0,
				currentBucketIndex: 0,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{
						startTime: time.Unix(10, 1),
						endTime:   time.Unix(11, 0),
						counters: counters{
							totalRequests:           *atomic.NewInt64(10),
							totalProbeRequests:      *atomic.NewInt64(11),
							successfulRequests:      *atomic.NewInt64(12),
							successfulProbeRequests: *atomic.NewInt64(13),
							failedRequests:          *atomic.NewInt64(14),
							failedProbeRequests:     *atomic.NewInt64(15),
						},
					},
					{}, {},
				},
				aggregatedCounters: counters{
					totalRequests:           *atomic.NewInt64(10),
					totalProbeRequests:      *atomic.NewInt64(11),
					successfulRequests:      *atomic.NewInt64(12),
					successfulProbeRequests: *atomic.NewInt64(13),
					failedRequests:          *atomic.NewInt64(14),
					failedProbeRequests:     *atomic.NewInt64(15),
				},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedStat: counters{
				totalRequests:           *atomic.NewInt64(10),
				totalProbeRequests:      *atomic.NewInt64(11),
				successfulRequests:      *atomic.NewInt64(12),
				successfulProbeRequests: *atomic.NewInt64(13),
				failedRequests:          *atomic.NewInt64(14),
				failedProbeRequests:     *atomic.NewInt64(15),
			},
		},
		{
			description: "must_slide_and_reduce_aggregate",
			mockTime:    time.Unix(14, 1),
			sw: &slidingWindow{
				oldestBucketIndex:  0,
				currentBucketIndex: 2,
				bucketDuration:     time.Second,
				buckets: []bucket{
					{
						startTime: time.Unix(10, 1),
						endTime:   time.Unix(11, 0),
						counters: counters{
							totalRequests:           *atomic.NewInt64(10),
							totalProbeRequests:      *atomic.NewInt64(20),
							successfulRequests:      *atomic.NewInt64(30),
							successfulProbeRequests: *atomic.NewInt64(40),
							failedRequests:          *atomic.NewInt64(50),
							failedProbeRequests:     *atomic.NewInt64(60),
						},
					},
					{
						startTime: time.Unix(11, 1),
						endTime:   time.Unix(12, 0),
						counters: counters{
							totalRequests:           *atomic.NewInt64(10),
							totalProbeRequests:      *atomic.NewInt64(20),
							successfulRequests:      *atomic.NewInt64(30),
							successfulProbeRequests: *atomic.NewInt64(40),
							failedRequests:          *atomic.NewInt64(50),
							failedProbeRequests:     *atomic.NewInt64(60),
						},
					},
					{
						startTime: time.Unix(12, 1),
						endTime:   time.Unix(13, 0),
						counters: counters{
							totalRequests:           *atomic.NewInt64(10),
							totalProbeRequests:      *atomic.NewInt64(20),
							successfulRequests:      *atomic.NewInt64(30),
							successfulProbeRequests: *atomic.NewInt64(40),
							failedRequests:          *atomic.NewInt64(50),
							failedProbeRequests:     *atomic.NewInt64(60),
						},
					},
					{},
				},
				aggregatedCounters: counters{
					totalRequests:           *atomic.NewInt64(30),
					totalProbeRequests:      *atomic.NewInt64(60),
					successfulRequests:      *atomic.NewInt64(90),
					successfulProbeRequests: *atomic.NewInt64(120),
					failedRequests:          *atomic.NewInt64(150),
					failedProbeRequests:     *atomic.NewInt64(180),
				},
				windowStartTimeDelta: -3 * time.Second,
			},
			expectedStat: counters{
				totalRequests:           *atomic.NewInt64(20),
				totalProbeRequests:      *atomic.NewInt64(40),
				successfulRequests:      *atomic.NewInt64(60),
				successfulProbeRequests: *atomic.NewInt64(80),
				failedRequests:          *atomic.NewInt64(100),
				failedProbeRequests:     *atomic.NewInt64(120),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			sw := tt.sw
			sw.clock = &mockClock{now: tt.mockTime}

			aggregate := sw.counters()
			require.Equal(t, tt.expectedStat, *aggregate, "unexpected aggregate")
		})
	}
}

func TestAllIncMethods(t *testing.T) {
	steps := []struct {
		description   string
		mockTime      time.Time
		expectedCount int64
	}{
		{description: "initial_inc", mockTime: time.Unix(20, 0), expectedCount: 1},
		{description: "inc_same_bucket", mockTime: time.Unix(19, 200), expectedCount: 2},
		{description: "inc_next_bucket", mockTime: time.Unix(20, 1), expectedCount: 3},
		{description: "inc_next_bucket_1", mockTime: time.Unix(21, 1), expectedCount: 4},
		{description: "inc_skip_bucket_time", mockTime: time.Unix(23, 1), expectedCount: 5},
		{description: "inc_jump_bucket_time", mockTime: time.Unix(27, 1), expectedCount: 2},

		{description: "inc_new_bucket", mockTime: time.Unix(40, 1), expectedCount: 1},
		{description: "inc_same_bucket", mockTime: time.Unix(40, int64(time.Second)), expectedCount: 2},
		{description: "inc_time_plus_400ms", mockTime: time.Unix(40, int64(time.Millisecond)*400), expectedCount: 3},
		{description: "inc_time_plus_1s", mockTime: time.Unix(41, 1), expectedCount: 4},
		{description: "inc_time_plus_2s", mockTime: time.Unix(42, 1), expectedCount: 5},
		{description: "inc_time_plus_3s", mockTime: time.Unix(43, 1), expectedCount: 6},
		{description: "inc_time_plus_4s", mockTime: time.Unix(43, 1), expectedCount: 7},
		{description: "inc_time_plus_5s", mockTime: time.Unix(44, 1), expectedCount: 8},
		{description: "inc_time_plus_6s_slide_first_bucket", mockTime: time.Unix(45, 1), expectedCount: 6},
		{description: "inc_time_plus_9s_slide_multiple_bucket", mockTime: time.Unix(47, 1), expectedCount: 5},
		{description: "inc_same_window_when_time_moves_behind", mockTime: time.Unix(45, 1), expectedCount: 6},
	}

	methods := []struct {
		methodName string
		inc        func(*slidingWindow)
		getValue   func(*counters) int64
	}{
		{
			methodName: "FailedProbes",
			inc:        func(sw *slidingWindow) { sw.incFailedProbeRequests() },
			getValue:   func(c *counters) int64 { return c.failedProbeRequests.Load() },
		},
		{
			methodName: "FailedRequests",
			inc:        func(sw *slidingWindow) { sw.incFailedRequests() },
			getValue:   func(c *counters) int64 { return c.failedRequests.Load() },
		},
		{
			methodName: "SuccessfulProbes",
			inc:        func(sw *slidingWindow) { sw.incSuccessfulProbeRequests() },
			getValue:   func(c *counters) int64 { return c.successfulProbeRequests.Load() },
		},
		{
			methodName: "SuccessfulRequests",
			inc:        func(sw *slidingWindow) { sw.incSuccessfulRequests() },
			getValue:   func(c *counters) int64 { return c.successfulRequests.Load() },
		},
		{
			methodName: "TotalProbes",
			inc:        func(sw *slidingWindow) { sw.incTotalProbeRequests() },
			getValue:   func(c *counters) int64 { return c.totalProbeRequests.Load() },
		},
		{
			methodName: "TotalRequests",
			inc:        func(sw *slidingWindow) { sw.incTotalRequests() },
			getValue:   func(c *counters) int64 { return c.totalRequests.Load() },
		},
	}
	for _, method := range methods {
		sw := newSlidingWindow(5, time.Second)
		clock := atomicMockClock{}
		sw.clock = &clock
		for _, step := range steps {
			t.Run(method.methodName+"/"+step.description, func(t *testing.T) {
				clock.updateTime(step.mockTime)
				method.inc(sw)
			})
			assert.Equal(t, step.expectedCount, method.getValue(sw.counters()), "unexpected count")
		}
	}
}

func TestParallelIncrements(t *testing.T) {
	tests := []struct {
		description       string
		giveWindowSize    int
		relativeTime      []int
		increments        int
		expectedAggregate counters
	}{
		{
			description:    "complete_window_size",
			increments:     10,
			relativeTime:   []int{1, 2, 3, 4, 5},
			giveWindowSize: 5,
			expectedAggregate: counters{
				totalRequests:           *atomic.NewInt64(50),
				totalProbeRequests:      *atomic.NewInt64(50),
				successfulRequests:      *atomic.NewInt64(50),
				successfulProbeRequests: *atomic.NewInt64(50),
				failedRequests:          *atomic.NewInt64(50),
				failedProbeRequests:     *atomic.NewInt64(50),
			},
		},
		{
			description:    "step_with_hole_at_time_4",
			increments:     100,
			relativeTime:   []int{1, 2, 3, 5},
			giveWindowSize: 5,
			expectedAggregate: counters{
				totalRequests:           *atomic.NewInt64(400),
				totalProbeRequests:      *atomic.NewInt64(400),
				successfulRequests:      *atomic.NewInt64(400),
				successfulProbeRequests: *atomic.NewInt64(400),
				failedRequests:          *atomic.NewInt64(400),
				failedProbeRequests:     *atomic.NewInt64(400),
			},
		},
		{
			description:    "slide_window_over_two_buckets",
			increments:     100,
			relativeTime:   []int{1, 2, 3, 5, 7},
			giveWindowSize: 5,
			expectedAggregate: counters{
				totalRequests:           *atomic.NewInt64(300),
				totalProbeRequests:      *atomic.NewInt64(300),
				successfulRequests:      *atomic.NewInt64(300),
				successfulProbeRequests: *atomic.NewInt64(300),
				failedRequests:          *atomic.NewInt64(300),
				failedProbeRequests:     *atomic.NewInt64(300),
			},
		},
		{
			description:    "slide_window_over_all_the_buckets",
			increments:     1000,
			relativeTime:   []int{1, 2, 3, 5, 7, 15},
			giveWindowSize: 5,
			expectedAggregate: counters{
				totalRequests:           *atomic.NewInt64(1000),
				totalProbeRequests:      *atomic.NewInt64(1000),
				successfulRequests:      *atomic.NewInt64(1000),
				successfulProbeRequests: *atomic.NewInt64(1000),
				failedRequests:          *atomic.NewInt64(1000),
				failedProbeRequests:     *atomic.NewInt64(1000),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			startTime := time.Unix(10, 0)
			clock := &atomicMockClock{}

			sw := newSlidingWindow(tt.giveWindowSize, time.Second)
			sw.clock = clock

			var wg sync.WaitGroup
			requests := make(chan struct{}, tt.increments)
			responses := make(chan struct{}, tt.increments)
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					for range requests {
						sw.incFailedProbeRequests()
						sw.incFailedRequests()
						sw.incSuccessfulProbeRequests()
						sw.incSuccessfulRequests()
						sw.incTotalProbeRequests()
						sw.incTotalRequests()
						responses <- struct{}{}
					}
					wg.Done()
				}()
			}

			for _, step := range tt.relativeTime {
				clock.updateTime(startTime.Add(time.Duration(step) * time.Second))
				for j := 0; j < tt.increments; j++ {
					requests <- struct{}{}
				}
				for j := 0; j < tt.increments; j++ {
					<-responses
				}
			}

			close(requests)
			wg.Wait()
			close(responses)
			assert.Equal(t, tt.expectedAggregate, *sw.counters())
		})
	}
}

func TestActiveBucketsCount(t *testing.T) {
	tests := []struct {
		description   string
		givenWindow   *slidingWindow
		expectedCount int
	}{
		{
			description:   "must_return_zero_when_empty",
			givenWindow:   newSlidingWindow(10, 10),
			expectedCount: 0,
		},
		{
			description: "must_return_one_with_single_bucket",
			givenWindow: &slidingWindow{
				oldestBucketIndex:  0,
				currentBucketIndex: 0,
				buckets:            make([]bucket, 10),
			},
			expectedCount: 1,
		},
		{
			description: "must_return_10",
			givenWindow: &slidingWindow{
				oldestBucketIndex:  0,
				currentBucketIndex: 9,
				buckets:            make([]bucket, 10),
			},
			expectedCount: 10,
		},
		{
			description: "must_return_4_when_queue_is_rotated",
			givenWindow: &slidingWindow{
				oldestBucketIndex:  7,
				currentBucketIndex: 0,
				buckets:            make([]bucket, 10),
			},
			expectedCount: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			assert.Equal(t, tt.expectedCount, tt.givenWindow.activeBucketsCount(), "unexpected buckets count")
		})
	}
}

func BenchmarkSlidingWindowIncCalls(b *testing.B) {
	benchmarks := []struct {
		description    string
		bucketDuration time.Duration
		windowSize     int
	}{
		{
			description:    "500ns_20",
			bucketDuration: time.Nanosecond * 500,
			windowSize:     20,
		},
		{
			description:    "100μs_20",
			bucketDuration: time.Microsecond * 100,
			windowSize:     20,
		},
		{
			description:    "100ms_20",
			bucketDuration: 100 * time.Millisecond,
			windowSize:     20,
		},
		{
			description:    "500ms_20",
			bucketDuration: 500 * time.Millisecond,
			windowSize:     20,
		},
		{
			description:    "1s_20",
			bucketDuration: time.Second,
			windowSize:     20,
		},
	}
	for _, bench := range benchmarks {
		b.Run(bench.description, func(b *testing.B) {
			b.ReportAllocs()

			sw := newSlidingWindow(bench.windowSize, bench.bucketDuration)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					sw.incSuccessfulProbeRequests()
				}
			})
		})
	}
}
