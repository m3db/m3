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
	nan         = math.NaN()
	defaultOpts = NewOptions()
)

// Stream represents a data stream.
type Stream struct {
	eps                    float64    // desired epsilon for errors
	quantiles              []float64  // sorted target quantiles
	computedQuantiles      []float64  // sorted computed target quantiles
	capacity               int        // initial stream sample buffer capacity
	insertAndCompressEvery int        // stream insertion and compression frequency
	streamPool             StreamPool // pool of streams

	insertAndCompressCounter int        // insertion and compression counter
	numValues                int64      // number of values inserted into the sorted stream
	bufLess                  minHeap    // sample buffer whose value is less than that at the insertion cursor
	bufMore                  minHeap    // sample buffer whose value is more than that at the insertion cursor
	samples                  sampleList // sample list
	insertCursor             *Sample    // insertion cursor
	compressCursor           *Sample    // compression cursor
	compressMinRank          int64      // compression min rank
	sampleBuf                []*Sample  // sample buffer
	closed                   bool       // whether the stream is closed
	flushed                  bool       // whether the stream is flushed
}

// NewStream creates a new sample stream.
func NewStream(quantiles []float64, opts Options) *Stream {
	if opts == nil {
		opts = defaultOpts
	}

	s := &Stream{
		eps:                    opts.Eps(),
		quantiles:              quantiles,
		computedQuantiles:      make([]float64, len(quantiles)),
		capacity:               opts.Capacity(),
		insertAndCompressEvery: opts.InsertAndCompressEvery(),
		sampleBuf:              make([]*Sample, 0, opts.Capacity()),
		streamPool:             opts.StreamPool(),
	}

	return s
}

func (s *Stream) AddBatch(values []float64) {
	s.flushed = false

	insertPointValue := s.insertPointValue()
	for _, value := range values {
		if s.numValues > 0 && value < insertPointValue {
			s.bufLess.Push(value)
		} else {
			s.bufMore.Push(value)
		}
		s.insertAndCompressCounter++
		if s.insertAndCompressCounter == s.insertAndCompressEvery {
			s.insert()
			s.compress()
			s.insertAndCompressCounter = 0
		}
	}
}

func (s *Stream) Add(value float64) {
	s.AddBatch([]float64{value})
}

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

func (s *Stream) Min() float64 {
	return s.Quantile(0.0)
}

func (s *Stream) Max() float64 {
	return s.Quantile(1.0)
}

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

func (s *Stream) calcQuantiles() {
	if len(s.quantiles) == 0 {
		return
	}

	var (
		minRank   int64
		maxRank   int64
		idx       int
		q         = s.quantiles[0]
		qMax      = len(s.quantiles) - 1
		prev      = s.samples.Front()
		curr      = s.samples.Front()
		rank      = int64(math.Ceil(q * float64(s.numValues)))
		threshold = int64(math.Ceil(float64(s.threshold(rank)) / 2.0))
	)

	for curr != nil {
		maxRank = minRank + curr.numRanks + curr.delta
		if maxRank > rank+threshold || minRank > rank {
			s.computedQuantiles[idx] = prev.value
			if idx == qMax {
				return
			}
			idx++
			q = s.quantiles[idx]
			rank = int64(math.Ceil(q * float64(s.numValues)))
			threshold = int64(math.Ceil(float64(s.threshold(rank)) / 2.0))
		}
		minRank += curr.numRanks
		prev = curr
		curr = curr.next
	}
}

func (s *Stream) ResetSetData(quantiles []float64) {
	s.quantiles = quantiles
	if len(quantiles) != len(s.computedQuantiles) {
		s.computedQuantiles = make([]float64, len(quantiles))
	}
	s.closed = false
}

func (s *Stream) Close() {
	if s.closed {
		return
	}
	s.closed = true

	s.bufMore.Reset()
	s.bufLess.Reset()

	curr := s.samples.Front()
	for curr != nil {
		next := curr.next
		curr.next, curr.prev = nil, nil
		samplePool.Put(curr)
		curr = next
	}

	for i := range s.sampleBuf {
		s.sampleBuf[i].next, s.sampleBuf[i].prev = nil, nil
		samplePool.Put(s.sampleBuf[i])
	}

	s.samples.Reset()
	s.sampleBuf = s.sampleBuf[:0]
	s.insertCursor = nil
	s.compressCursor = nil
	s.insertAndCompressCounter = 0
	s.numValues = 0
	s.compressMinRank = 0

	if s.streamPool != nil {
		s.streamPool.Put(s)
	}
}

// insert inserts a sample into the stream.
func (s *Stream) insert() {
	var sample *Sample

	if s.samples.Len() == 0 {
		if s.bufMore.Len() == 0 {
			return
		}

		if sample = s.tryAcquireSample(); sample == nil {
			sample = s.acquireSample()
		}

		sample.value = s.bufMore.Pop()
		sample.numRanks = 1
		sample.delta = 0
		s.samples.PushBack(sample)
		s.insertCursor = s.samples.Front()
		s.numValues++
	}

	var (
		insertPointValue = s.insertCursor.value
		minRank          = s.compressMinRank
		numValues        = s.numValues
		compCur          = s.compressCursor
		compValue        float64
	)

	if compCur != nil {
		compValue = compCur.value
	}

	for s.insertCursor != nil {
		curr := *s.insertCursor

		for s.bufMore.Len() > 0 && s.bufMore.Min() <= insertPointValue {
			if sample = s.tryAcquireSample(); sample == nil {
				sample = s.acquireSample()
			}
			val := s.bufMore.Pop()
			sample.value = val
			sample.numRanks = 1
			sample.delta = curr.numRanks + curr.delta - 1

			s.samples.InsertBefore(sample, s.insertCursor)
			numValues++

			if compCur != nil && compValue >= val {
				minRank++
			}
		}

		s.insertCursor = s.insertCursor.next
		insertPointValue = s.insertPointValue()
	}
	s.numValues = numValues
	s.compressMinRank = minRank
	if s.insertCursor != nil {
		return
	}

	for s.bufMore.Len() > 0 && s.bufMore.Min() >= s.samples.Back().value {
		if sample = s.tryAcquireSample(); sample == nil {
			sample = s.acquireSample()
		}
		sample.value = s.bufMore.Pop()
		sample.numRanks = 1
		sample.delta = 0
		s.samples.PushBack(sample)
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

// resetInsertCursor resets the insert cursor.
func (s *Stream) resetInsertCursor() {
	s.bufLess, s.bufMore = s.bufMore, s.bufLess
	s.insertCursor = s.samples.Front()
}

// insertPointValue returns the value under the insertion cursor.
func (s *Stream) insertPointValue() float64 {
	if s.insertCursor == nil {
		return 0.0
	}

	return s.insertCursor.value
}

// tryAcquireSample is inline-friendly fastpath to get a sample from preallocated buffer
func (s *Stream) tryAcquireSample() *Sample {
	l := len(s.sampleBuf)

	if l == 0 {
		return nil
	}

	sample := s.sampleBuf[l-1]
	s.sampleBuf = s.sampleBuf[:l-1]
	return sample
}

func (s *Stream) acquireSample() *Sample {
	for i := 0; i < s.capacity; i++ {
		sample, ok := samplePool.Get().(*Sample)
		if !ok {
			return &Sample{}
		}
		s.sampleBuf = append(s.sampleBuf, sample)
	}

	return s.tryAcquireSample()
}

func (s *Stream) releaseSample(sample *Sample) {
	sample.prev, sample.next = nil, nil
	s.sampleBuf = append(s.sampleBuf, sample)
}
