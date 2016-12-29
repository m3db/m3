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

package cm

import "math"

const (
	minSamplesToCompress = 3
)

var (
	nan = math.NaN()
)

// stream represents a data stream
type stream struct {
	capacity   int        // stream capacity
	eps        float64    // desired epsilon for errors
	quantiles  []float64  // sorted target quantiles
	samplePool SamplePool // pool of samples
	floatsPool FloatsPool // pool of float64 slices

	closed          bool       // whether the stream is closed
	numValues       int64      // number of values
	bufLess         minHeap    // sample buffer whose value is less than that at the insertion cursor
	bufMore         minHeap    // sample buffer whose value is more than that at the insertion cursor
	samples         sampleList // sample list
	insertCursor    *Sample    // insertion cursor
	compressCursor  *Sample    // compression cursor
	compressMinRank int64      // compression min rank
}

// NewStream creates a new sample stream
// TODO(xichen): the stream object itself should be pooled too
// TODO(xichen): add metrics
func NewStream(capacity int, opts Options) Stream {
	samplePool := opts.SamplePool()
	floatsPool := opts.FloatsPool()

	s := &stream{
		capacity:   capacity,
		eps:        opts.Eps(),
		quantiles:  opts.Quantiles(),
		samplePool: samplePool,
		floatsPool: floatsPool,
	}

	s.Reset()
	return s
}

func (s *stream) Add(value float64) {
	s.addToBuffer(value)
	s.insert()
	s.compress()
}

func (s *stream) Flush() {
	for s.bufLess.Len() > 0 || s.bufMore.Len() > 0 {
		if s.bufMore.Len() == 0 {
			s.resetInsertCursor()
		}
		s.insert()
		s.compress()
	}
}

func (s *stream) Min() float64 {
	return s.Quantile(0.0)
}

func (s *stream) Max() float64 {
	return s.Quantile(1.0)
}

func (s *stream) Quantile(q float64) float64 {
	if q < 0.0 || q > 1.0 {
		return nan
	}
	if s.samples.Empty() {
		return 0.0
	}
	if q == 0.0 {
		return s.samples.Front().value
	}
	if q == 1.0 {
		return s.samples.Back().value
	}

	var (
		minRank   int64
		maxRank   int64
		prev      = s.samples.Front()
		curr      = s.samples.Front()
		rank      = int64(math.Ceil(q * float64(s.numValues)))
		threshold = int64(math.Ceil(float64(s.threshold(rank)) / 2.0))
	)

	for curr != nil {
		maxRank = minRank + curr.numRanks + curr.delta
		if maxRank > rank+threshold || minRank > rank {
			break
		}
		minRank += curr.numRanks
		prev = curr
		curr = curr.next
	}
	return prev.value
}

func (s *stream) Reset() {
	s.closed = false
	s.numValues = 0
	s.bufLess = minHeap(s.floatsPool.Get(s.capacity))
	s.bufMore = minHeap(s.floatsPool.Get(s.capacity))
	s.samples.Reset()
	s.insertCursor = nil
	s.compressCursor = nil
	s.compressMinRank = 0
}

func (s *stream) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.floatsPool.Put(s.bufLess)
	s.floatsPool.Put(s.bufMore)
	for sample := s.samples.Front(); sample != nil; sample = sample.next {
		s.samplePool.Put(sample)
	}
}

// addToBuffer adds a new sample to the buffer
func (s *stream) addToBuffer(value float64) {
	if s.numValues > 0 && value < s.insertPointValue() {
		s.addToMinHeap(&s.bufLess, value)
	} else {
		s.addToMinHeap(&s.bufMore, value)
	}
}

// insert inserts a sample into the stream
func (s *stream) insert() {
	if s.samples.Len() == 0 {
		if s.bufMore.Len() == 0 {
			return
		}
		sample := s.samplePool.Get()
		sample.reset()
		sample.setData(s.bufMore.Pop(), 1, 0)
		s.samples.Pushback(sample)
		s.numValues++
		s.insertCursor = s.samples.Front()
		return
	}

	if s.insertCursor == nil {
		s.insertCursor = s.samples.Front()
	}

	incrementSize := s.cursorIncrement()
	for i := 0; i < incrementSize && s.insertCursor != nil; i++ {
		for s.bufMore.Len() > 0 && s.bufMore.Min() <= s.insertPointValue() {
			sample := s.samplePool.Get()
			sample.reset()
			sample.setData(s.bufMore.Pop(), 1, s.insertCursor.numRanks+s.insertCursor.delta-1)
			s.samples.InsertBefore(sample, s.insertCursor)
			s.numValues++

			if s.compressCursor != nil && s.compressCursor.value >= sample.value {
				s.compressMinRank++
			}
		}
		s.insertCursor = s.insertCursor.next
	}

	if s.insertCursor != nil {
		return
	}

	for s.bufMore.Len() > 0 && s.bufMore.Min() > s.samples.Back().value {
		sample := s.samplePool.Get()
		sample.reset()
		sample.setData(s.bufMore.Pop(), 1, 0)
		s.samples.Pushback(sample)
		s.numValues++
	}

	s.resetInsertCursor()
}

// compress compresses the samples in the stream
func (s *stream) compress() {
	// Bail early if there is nothing to compress
	if s.samples.Len() < minSamplesToCompress {
		return
	}

	if s.compressCursor == nil {
		s.compressCursor = s.samples.Back().prev
		s.compressMinRank = s.numValues - 1 - s.compressCursor.numRanks
		s.compressCursor = s.compressCursor.prev
	}

	incrementSize := s.cursorIncrement()
	for i := 0; i < incrementSize && s.compressCursor != s.samples.Front(); i++ {
		next := s.compressCursor.next
		maxRank := s.compressMinRank + s.compressCursor.numRanks + s.compressCursor.delta
		s.compressMinRank -= s.compressCursor.numRanks

		threshold := s.threshold(maxRank)
		testVal := s.compressCursor.numRanks + next.numRanks + next.delta
		if testVal <= threshold {
			if s.insertCursor == s.compressCursor {
				s.insertCursor = next
			}

			next.numRanks += s.compressCursor.numRanks

			prev := s.compressCursor.prev
			s.samples.Remove(s.compressCursor)
			s.samplePool.Put(s.compressCursor)
			s.compressCursor = prev
		} else {
			s.compressCursor = s.compressCursor.prev
		}
	}

	if s.compressCursor == s.samples.Front() {
		s.compressCursor = nil
	}
}

// threshold computes the minimum threshold value
func (s *stream) threshold(rank int64) int64 {
	minVal := int64(math.MaxInt64)
	for _, quantile := range s.quantiles {
		var quantileMin int64
		if float64(rank) >= quantile*float64(s.numValues) {
			quantileMin = int64(2 * s.eps * float64(rank) / quantile)
		} else {
			quantileMin = int64(2 * s.eps * float64(s.numValues-rank) / (1 - quantile))
		}
		if quantileMin < minVal {
			minVal = quantileMin
		}
	}
	return minVal
}

// resetInsertCursor resets the insert cursor
func (s *stream) resetInsertCursor() {
	s.bufLess, s.bufMore = s.bufMore, s.bufLess
	s.insertCursor = nil
}

// cursorIncrement computes the number of items to process
func (s *stream) cursorIncrement() int {
	return int(math.Ceil(float64(s.samples.Len()) * s.eps))
}

// insertPointValue returns the value under the insertion cursor
func (s *stream) insertPointValue() float64 {
	if s.insertCursor == nil {
		return 0.0
	}
	return s.insertCursor.value
}

// addToMinHeap adds a value to a min heap
func (s *stream) addToMinHeap(heap *minHeap, value float64) {
	curr := *heap
	// If we are at capacity, get a bigger heap from the pool
	// and return the current heap to the pool
	if len(curr) == cap(curr) {
		newHeap := s.floatsPool.Get(2 * len(curr))
		newHeap = append(newHeap, curr...)
		s.floatsPool.Put(curr)
		*heap = newHeap
	}
	heap.Push(value)
}
