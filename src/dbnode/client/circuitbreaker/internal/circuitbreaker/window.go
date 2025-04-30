package circuitbreaker

import (
	"sync"
	"time"
)

// bucket holds a part of circuit breaker counters from the start time to the end
// time of bucket.
type bucket struct {
	// startTime is the start time of the bucket (inclusive).
	startTime time.Time
	// endTime is the end time of the bucket (inclusive).
	endTime  time.Time
	counters counters
}

// reset sets start and end time as empty, and resets its counters.
func (b *bucket) reset() {
	if b == nil {
		return
	}
	b.startTime = time.Time{}
	b.endTime = time.Time{}
	b.counters.reset()
}

// shouldDrop returns true when the bucket start time is before the given
// window start time.
func (b *bucket) shouldDrop(windowStart time.Time) bool {
	if b == nil {
		return false
	}
	return b.startTime.Before(windowStart)
}

// isExpired return true when the given time is after the bucket end time
// or when the bucket is nil.
func (b *bucket) isExpired(now time.Time) bool {
	if b == nil {
		return true
	}
	return b.endTime.Before(now)
}

// slidingWindow provides fast and zero allocation sliding window implementation
// specifically for the circuit breaker counters. It uses circular queue of
// buckets, where each bucket can fit counters of a particular defined time range
// (for instance [0ms-500ms]).
// Number of buckets and time duration of each bucket can be set defined by the
// clients. It maintains an aggregated circuit breaker counters belonging to the
// window.
type slidingWindow struct {
	mu    sync.RWMutex
	clock clock

	// bucketDuration is the time slice of each bucket.
	// Bucket duration of 1s means a bucket has a time range of [0s, 1s).
	bucketDuration time.Duration

	// buckets is a slice of circuit breaker bucket, where each bucket maintains
	// the circuit breaker counters falling under its start and end time.
	// It is used as a circular queue, where start of the queue is pointed by
	// oldestBucketIndex and the current element of the queue is pointed by
	// currentBucketIndex.
	// Time range of buckets are in an increasing order when traversed from the
	// oldestBucketIndex to the currentBucketIndex.
	buckets []bucket

	// currentBucketIndex points to an index of buckets which holds the last updated
	// counters.
	// For example, when you have 4 buckets [[0-1s],[1-2s],[2-3s],[empty]]
	// this points to index 2 where the bucket is [2-3s].
	// This index is updated by the slide method, only when the existing buckets
	// cannot fit its time.
	// It is set to -1 when the queue is empty.
	currentBucketIndex int

	// oldestBucketIndex points to an index of buckets which holds the counters
	// from the earliest time of the sliding window.
	// For example, when you have 4 buckets [[0-1s],[1-2s],[2-3s],[empty]]
	// this points is index 0 which the bucket is [0-1s].
	// This index is updated by the slide method only when the sliding window
	// slides ahead, dropping older buckets which are before the window.
	// It is set to -1 when the queue is empty.
	oldestBucketIndex int

	// windowStartTimeDelta holds a time duration, when added to the current time
	// gives the start time of the window.
	// For instance, window size of 10 and bucket size of 1s. This delta will be
	// set to -9s. At a time 1m 30s, adding this delta gives the window start time
	// as 1m 21s.
	windowStartTimeDelta time.Duration

	// aggregatedCounters holds the aggregated counter of the circuit breaker
	// counters belonging to the latest window.
	aggregatedCounters counters
}

// newSlidingWindow returns a sliding window based on the provided window size
// and bucket duration.
func newSlidingWindow(windowSize int, bucketDuration time.Duration) *slidingWindow {
	return &slidingWindow{
		bucketDuration:       bucketDuration,
		buckets:              make([]bucket, windowSize),
		currentBucketIndex:   -1,
		oldestBucketIndex:    -1,
		clock:                _baseClock,
		windowStartTimeDelta: -1 * time.Duration(windowSize-1) * bucketDuration,
	}
}

// incFailedProbeRequests increments the failed probe requests counter of the
// current bucket and aggregated counter.
// This method is safe for concurrent use.
func (s *slidingWindow) incFailedProbeRequests() {
	s.currentBucketAndSlideIfNeeded().counters.failedProbeRequests.Inc()
	s.aggregatedCounters.failedProbeRequests.Inc()
}

// incFailedRequests increments the failed requests counter of the current
// bucket and aggregated counter.
// This method is safe for concurrent use.
func (s *slidingWindow) incFailedRequests() {
	s.currentBucketAndSlideIfNeeded().counters.failedRequests.Inc()
	s.aggregatedCounters.failedRequests.Inc()
}

// incSuccessfulProbeRequests increments successful probe requests counter of
// the current bucket and aggregated counter.
// This method is safe for concurrent use.
func (s *slidingWindow) incSuccessfulProbeRequests() {
	s.currentBucketAndSlideIfNeeded().counters.successfulProbeRequests.Inc()
	s.aggregatedCounters.successfulProbeRequests.Inc()
}

// incSuccessfulRequests increments the successful requests counter of the
// current bucket and aggregated counter.
// This method is safe for concurrent use.
func (s *slidingWindow) incSuccessfulRequests() {
	s.currentBucketAndSlideIfNeeded().counters.successfulRequests.Inc()
	s.aggregatedCounters.successfulRequests.Inc()
}

// incTotalProbeRequests increments the total probe requests counter of the
// current bucket and aggregated counter.
// This method is safe for concurrent use.
func (s *slidingWindow) incTotalProbeRequests() {
	s.currentBucketAndSlideIfNeeded().counters.totalProbeRequests.Inc()
	s.aggregatedCounters.totalProbeRequests.Inc()
}

// incTotalRequests increments the total requests counter of the current bucket
// and aggregated counter.
// This method is safe for concurrent use.
func (s *slidingWindow) incTotalRequests() {
	s.currentBucketAndSlideIfNeeded().counters.totalRequests.Inc()
	s.aggregatedCounters.totalRequests.Inc()
}

// counters returns the aggregated counters belonging to the window of current time.
// This method is safe for concurrent use.
func (s *slidingWindow) counters() *counters {
	// This adjusts the buckets and updates aggregate counters with values of the
	// current window time.
	s.currentBucketAndSlideIfNeeded()
	return &s.aggregatedCounters
}

// reset empties all the buckets, aggregated counters and sliding window.
// This method is safe for concurrent use.
func (s *slidingWindow) reset() {
	s.mu.Lock()
	s.oldestBucketIndex = -1
	s.currentBucketIndex = -1
	s.aggregatedCounters.reset()
	for i := range s.buckets {
		s.buckets[i].reset()
	}
	s.mu.Unlock()
}

// currentBucketAndSlideIfNeeded returns a bucket belonging to the current time.
// Slides the window if the existing current bucket cannot fit the time.
// This method is safe for concurrent use.
func (s *slidingWindow) currentBucketAndSlideIfNeeded() *bucket {
	now := s.clock.Now()

	s.mu.RLock()
	currentBucket := s.currentBucket()
	// This check must happen inside a lock to avoid r/w race while accessing
	// the end time of the bucket.
	isExpired := currentBucket.isExpired(now)
	s.mu.RUnlock()
	if !isExpired {
		return currentBucket
	}

	s.mu.Lock()
	// Double checked locking after acquiring Write lock.
	// Make sure the bucket is not created between read and write lock.
	if s.currentBucket().isExpired(now) {
		s.slide(now)
	}
	currentBucket = s.currentBucket()
	s.mu.Unlock()
	return currentBucket
}

// currentBucket returns the bucket pointed by the current index.
// Return nil if the sliding window is empty.
func (s *slidingWindow) currentBucket() *bucket {
	if s.isWindowEmpty() {
		return nil
	}

	return &s.buckets[s.currentBucketIndex]
}

// slide drops the expired buckets starting from the oldest buckets of the circular
// queue if required and assigns a fresh bucket for the current time in the queue.
// Note: this must be invoked under a write lock.
func (s *slidingWindow) slide(now time.Time) {
	s.dropExpiredBuckets(now)
	if s.isWindowEmpty() {
		s.createInitialBucket(now)
		return
	}

	s.currentBucketIndex = s.nextBucketIndex(s.currentBucketIndex)
	s.buckets[s.currentBucketIndex].startTime = now
	s.buckets[s.currentBucketIndex].endTime = s.getBucketEndTime(now)
}

// dropExpiredBuckets removes buckets that have expired, beginning with the oldest
// bucket of the circular queue pointed by oldestBucketIndex. An expired bucket has
// a start time that falls before the current window's start time. For every expired
// bucket, reduce the aggregated counters and reset the bucket.
func (s *slidingWindow) dropExpiredBuckets(now time.Time) {
	if s.isWindowEmpty() {
		return
	}

	currentWindowStartTime := now.Add(s.windowStartTimeDelta)
	for {
		bucket := &s.buckets[s.oldestBucketIndex]
		if !bucket.shouldDrop(currentWindowStartTime) {
			break
		}

		s.aggregatedCounters.sub(&bucket.counters)
		bucket.reset()
		// Oldest bucket has reached the current bucket of the circular queue which
		// means the sliding window is now empty.
		if s.oldestBucketIndex == s.currentBucketIndex {
			s.oldestBucketIndex = -1
			s.currentBucketIndex = -1
			break
		}

		s.oldestBucketIndex = s.nextBucketIndex(s.oldestBucketIndex)
	}
}

// createInitialBucket starts sliding window with the initial bucket.
func (s *slidingWindow) createInitialBucket(now time.Time) {
	s.oldestBucketIndex = 0
	s.currentBucketIndex = 0
	s.buckets[0].startTime = now
	s.buckets[0].endTime = s.getBucketEndTime(now)
}

// nextBucketIndex returns next index from the given index.
// Returns 0 when given index already points to last bucket.
func (s *slidingWindow) nextBucketIndex(idx int) int {
	next := idx + 1
	if next == len(s.buckets) {
		return 0
	}
	return next
}

// isWindowEmpty returns true if the circular queue is empty.
func (s *slidingWindow) isWindowEmpty() bool {
	return s.currentBucketIndex == -1
}

// getBucketEndTime returns an end time of the bucket from the given time.
// End time is inclusive, so reduce the end time by a nanosecond to avoid overlapping
// end time and start time of two adjacent buckets. Example: [[0-999ns],[1000ns-1999ns]].
func (s *slidingWindow) getBucketEndTime(now time.Time) time.Time {
	return now.Add(s.bucketDuration - 1)
}

// activeBucketsCount returns the total number buckets used by the current sliding
// window.
func (s *slidingWindow) activeBucketsCount() int {
	s.mu.RLock()
	currentBucketIndex := s.currentBucketIndex
	oldestBucketIndex := s.oldestBucketIndex
	s.mu.RUnlock()

	if currentBucketIndex == -1 {
		return 0
	}

	activeBuckets := currentBucketIndex - oldestBucketIndex + 1
	if currentBucketIndex < oldestBucketIndex {
		activeBuckets = (len(s.buckets) - oldestBucketIndex) + currentBucketIndex + 1
	}
	return activeBuckets
}
