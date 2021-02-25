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

import (
	"math"
)

const (
	minSamplesToCompress = 3
)

var (
	nan = math.NaN()
)

type threshold struct {
	rank      int64
	threshold int64
}

// Stream represents a data stream.
type Stream struct {
	streamPool               StreamPool
	eps                      float64     // desired epsilon for errors
	quantiles                []float64   // sorted target quantiles
	computedQuantiles        []float64   // sorted computed target quantiles
	thresholdBuf             []threshold // temporary buffer for computed thresholds
	capacity                 int         // initial stream sample buffer capacity
	insertAndCompressEvery   int         // stream insertion and compression frequency
	insertAndCompressCounter int         // insertion and compression counter
	numValues                int64       // number of values inserted into the sorted stream
	bufLess                  minHeap     // sample buffer whose value is less than that at the insertion cursor
	bufMore                  minHeap     // sample buffer whose value is more than that at the insertion cursor
	samples                  sampleList  // sample list
	insertCursor             *Sample     // insertion cursor
	compressCursor           *Sample     // compression cursor
	compressMinRank          int64       // compression min rank
	sampleBuf                []*Sample   // sample buffer
	closed                   bool        // whether the stream is closed
	flushed                  bool        // whether the stream is flushed
}

// NewStream creates a new sample stream.
func NewStream(opts Options) *Stream {
	if opts == nil {
		opts = NewOptions()
	}

	s := &Stream{
		streamPool:             opts.StreamPool(),
		eps:                    opts.Eps(),
		capacity:               opts.Capacity(),
		insertAndCompressEvery: opts.InsertAndCompressEvery(),
		sampleBuf:              make([]*Sample, opts.Capacity()),
	}

	for i := 0; i < len(s.sampleBuf); i++ {
		sample, ok := samplePool.Get().(*Sample)
		if !ok {
			sample = &Sample{}
		}
		s.sampleBuf[i] = sample
	}

	return s
}

// AddBatch adds a batch of sample values.
func (s *Stream) AddBatch(values []float64) {
	s.flushed = false

	if len(values) == 0 {
		return
	}

	if s.samples.Len() == 0 {
		sample := s.tryAcquireSample()
		sample.value = values[0]
		sample.numRanks = 1
		sample.delta = 0
		s.samples.PushBack(sample)
		s.insertCursor = s.samples.Front()
		s.numValues++
		values = values[1:]
	}

	var (
		insertPointValue = s.insertCursor.value
		insertCounter    = s.insertAndCompressCounter
	)

	for _, value := range values {
		if value < insertPointValue {
			s.bufLess.Push(value)
		} else {
			s.bufMore.Push(value)
		}

		if insertCounter == s.insertAndCompressEvery {
			s.insert()
			s.compress()
			insertCounter = 0
		}
		insertCounter++
	}
	s.insertAndCompressCounter = insertCounter
}

// Add adds a sample value.
func (s *Stream) Add(value float64) {
	s.AddBatch([]float64{value})
}

// Flush flushes the internal buffer.
func (s *Stream) Flush() {
	if s.flushed {
		return
	}

	for s.bufLess.Len() > 0 || s.bufMore.Len() > 0 {
		if s.bufMore.Len() == 0 {
			s.resetInsertCursor()
		}
		s.insert()
		s.compress()
	}
	s.calcQuantiles()
	s.flushed = true
}

// Min returns the minimum value.
func (s *Stream) Min() float64 {
	return s.Quantile(0.0)
}

// Max returns the maximum value.
func (s *Stream) Max() float64 {
	return s.Quantile(1.0)
}

// Quantile returns the quantile value.
func (s *Stream) Quantile(q float64) float64 {
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

	for i, qt := range s.quantiles {
		if qt >= q {
			return s.computedQuantiles[i]
		}
	}
	return math.NaN()
}

// ResetSetData resets the stream and sets data.
func (s *Stream) ResetSetData(quantiles []float64) {
	s.quantiles = quantiles

	if len(quantiles) > cap(s.computedQuantiles) {
		s.computedQuantiles = make([]float64, len(quantiles))
		s.thresholdBuf = make([]threshold, len(quantiles))
	} else {
		s.computedQuantiles = s.computedQuantiles[:len(quantiles)]
		s.thresholdBuf = s.thresholdBuf[:len(quantiles)]
	}

	s.closed = false
}

// Close closes the stream.
func (s *Stream) Close() {
	if s.closed {
		return
	}
	s.closed = true

	s.bufMore.Reset()
	s.bufLess.Reset()

	for curr := s.samples.Front(); curr != nil; {
		next := curr.next
		s.releaseSample(curr)
		curr = next
	}

	for i := 0; i < len(s.sampleBuf); i++ {
		samplePool.Put(s.sampleBuf[i])
		s.sampleBuf[i] = nil
	}
	s.sampleBuf = s.sampleBuf[:0]
	s.samples.Reset()
	s.insertCursor = nil
	s.compressCursor = nil
	s.insertAndCompressCounter = 0
	s.numValues = 0
	s.compressMinRank = 0
	s.streamPool.Put(s)
}

// quantilesFromBuf calculates quantiles from buffer if there were too few samples to compress
func (s *Stream) quantilesFromBuf() {
	var (
		curr = s.samples.Front()
		buf  = make([]float64, 0, minSamplesToCompress)
	)

	for curr != nil {
		buf = append(buf, curr.value)
		curr = curr.next
	}

	n := len(buf)
	for i, q := range s.quantiles {
		idx := int(q * float64(n))
		if idx >= n {
			idx = n - 1
		}
		s.computedQuantiles[i] = buf[idx]
	}
}

func (s *Stream) calcQuantiles() {
	if len(s.quantiles) == 0 || s.numValues == 0 {
		return
	} else if s.numValues <= minSamplesToCompress {
		// too few values for compress(), need to compute quantiles directly
		s.quantilesFromBuf()
		return
	}

	var (
		minRank int64
		maxRank int64
		idx     int
		curr    = s.samples.Front()
		prev    = s.samples.Front()
	)

	for i, q := range s.quantiles {
		rank := int64(math.Ceil(q * float64(s.numValues)))
		s.thresholdBuf[i].rank = rank
		s.thresholdBuf[i].threshold = int64(
			math.Ceil(float64(s.threshold(rank)) / 2.0),
		)
	}

	for curr != nil {
		if idx == len(s.quantiles) {
			break
		}
		maxRank = minRank + curr.numRanks + curr.delta
		rank, threshold := s.thresholdBuf[idx].rank, s.thresholdBuf[idx].threshold

		if maxRank > rank+threshold || minRank > rank {
			s.computedQuantiles[idx] = prev.value
			idx++
		}

		minRank += curr.numRanks
		prev = curr
		curr = curr.next
	}

	// check if the last sample value should satisfy unprocessed quantiles
	for i := idx; i < len(s.quantiles); i++ {
		rank, threshold := s.thresholdBuf[i].rank, s.thresholdBuf[i].threshold
		if maxRank >= rank+threshold || minRank > rank {
			s.computedQuantiles[i] = prev.value
		}
	}
}

// insert inserts a sample into the stream.
func (s *Stream) insert() {
	var (
		compCur          = s.compressCursor
		compValue        = math.NaN()
		insertPointValue float64
		sample           *Sample
	)

	if compCur != nil {
		compValue = compCur.value
	}

	samples := &s.samples

	for s.insertCursor != nil {
		curr := s.insertCursor
		insertPointValue = curr.value

		for s.bufMore.Len() > 0 && s.bufMore.Min() <= insertPointValue {
			sample = s.tryAcquireSample()
			val := s.bufMore.Pop()
			sample.value = val
			sample.numRanks = 1
			sample.delta = curr.numRanks + curr.delta - 1

			samples.InsertBefore(sample, curr)

			if compValue >= val {
				s.compressMinRank++
			}
			s.numValues++
		}

		s.insertCursor = s.insertCursor.next
	}

	if s.insertCursor != nil {
		return
	}

	for s.bufMore.Len() > 0 && s.bufMore.Min() >= samples.Back().value {
		sample = s.tryAcquireSample()
		sample.value = s.bufMore.Pop()
		sample.numRanks = 1
		sample.delta = 0
		samples.PushBack(sample)
		s.numValues++
	}

	s.resetInsertCursor()
}

// compress compresses the samples in the stream.
func (s *Stream) compress() {
	// Bail early if there is nothing to compress.
	if s.samples.Len() < minSamplesToCompress {
		return
	}

	if s.compressCursor == nil {
		s.compressCursor = s.samples.Back().prev
		s.compressMinRank = s.numValues - 1 - s.compressCursor.numRanks
		s.compressCursor = s.compressCursor.prev
	}

	for s.compressCursor != s.samples.Front() {
		next := s.compressCursor.next
		maxRank := s.compressMinRank + s.compressCursor.numRanks + s.compressCursor.delta
		threshold := s.threshold(maxRank)
		s.compressMinRank -= s.compressCursor.numRanks
		testVal := s.compressCursor.numRanks + next.numRanks + next.delta

		if testVal <= threshold {
			if s.insertCursor == s.compressCursor {
				s.insertCursor = next
			}

			next.numRanks += s.compressCursor.numRanks

			prev := s.compressCursor.prev
			s.samples.Remove(s.compressCursor)
			s.releaseSample(s.compressCursor)
			s.compressCursor = prev
		} else {
			s.compressCursor = s.compressCursor.prev
		}
	}

	if s.compressCursor == s.samples.Front() {
		s.compressCursor = nil
	}
}

// threshold computes the minimum threshold value.
func (s *Stream) threshold(rank int64) int64 {
	var (
		minVal      = int64(math.MaxInt64)
		numVals     = s.numValues
		eps         = 2.0 * s.eps
		quantileMin int64
	)
	for _, quantile := range s.quantiles {
		if rank >= int64(quantile*float64(numVals)) {
			quantileMin = int64(eps * float64(rank) / quantile)
		} else {
			quantileMin = int64(eps * float64(numVals-rank) / (1.0 - quantile))
		}
		if quantileMin < minVal {
			minVal = quantileMin
		}
	}

	return minVal
}

// resetInsertCursor resets the insert cursor.
func (s *Stream) resetInsertCursor() {
	s.bufLess, s.bufMore = s.bufMore, s.bufLess
	s.insertCursor = s.samples.Front()
}

func (s *Stream) tryAcquireSample() *Sample {
	l := len(s.sampleBuf)

	if l == 0 {
		return s.acquireSample()
	}

	sample := s.sampleBuf[l-1]
	s.sampleBuf = s.sampleBuf[:l-1]
	return sample
}

func (s *Stream) acquireSample() *Sample {
	capacity := s.capacity
	s.sampleBuf = s.sampleBuf[:capacity]
	for i := 0; i < capacity; i++ {
		sample, ok := samplePool.Get().(*Sample)
		if !ok {
			return &Sample{}
		}
		s.sampleBuf[i] = sample
	}

	return samplePool.Get().(*Sample)
}

func (s *Stream) releaseSample(sample *Sample) {
	(*sample) = Sample{}
	s.sampleBuf = append(s.sampleBuf, sample)
}
