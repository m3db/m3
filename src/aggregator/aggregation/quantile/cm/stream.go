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

	"github.com/m3db/m3/src/x/pool"
)

const (
	minSamplesToCompress = 3
)

var (
	nan = math.NaN()
)

// acquireSampleFn acquires a new sample.
type acquireSampleFn func() *Sample

// releaseSampleFn releases a sample.
type releaseSampleFn func(*Sample)

// stream represents a data stream.
type stream struct {
	eps                    float64         // desired epsilon for errors
	quantiles              []float64       // sorted target quantiles
	capacity               int             // stream capacity
	insertAndCompressEvery int             // stream insertion and compression frequency
	flushEvery             int             // stream flushing frequency
	streamPool             StreamPool      // pool of streams
	floatsPool             pool.FloatsPool // pool of float64 slices
	acquireSampleFn        acquireSampleFn // function to acquire samples
	releaseSampleFn        releaseSampleFn // function to release samples

	closed                   bool       // whether the stream is closed
	insertAndCompressCounter int        // insertion and compression counter
	flushCounter             int        // flush frequency counter
	numValues                int64      // number of values inserted into the sorted stream
	bufLess                  minHeap    // sample buffer whose value is less than that at the insertion cursor
	bufMore                  minHeap    // sample buffer whose value is more than that at the insertion cursor
	samples                  sampleList // sample list
	insertCursor             *Sample    // insertion cursor
	compressCursor           *Sample    // compression cursor
	compressMinRank          int64      // compression min rank
	c                        int
	smpbuf                   []*Sample
}

// NewStream creates a new sample stream.
func NewStream(quantiles []float64, opts Options) Stream {
	if opts == nil {
		opts = NewOptions()
	}
	var (
		acquireSampleFn acquireSampleFn
		releaseSampleFn releaseSampleFn
	)
	if samplePool := opts.SamplePool(); samplePool != nil {
		acquireSampleFn = func() *Sample {
			sample := samplePool.Get()
			sample.reset()
			return sample
		}
		releaseSampleFn = func(sample *Sample) {
			samplePool.Put(sample)
		}
	} else {
		acquireSampleFn = newSample
		releaseSampleFn = func(*Sample) {}
	}

	s := &stream{
		eps:                    opts.Eps(),
		capacity:               opts.Capacity(),
		flushEvery:             opts.FlushEvery(),
		insertAndCompressEvery: opts.InsertAndCompressEvery(),
		streamPool:             opts.StreamPool(),
		floatsPool:             opts.FloatsPool(),
		acquireSampleFn:        acquireSampleFn,
		releaseSampleFn:        releaseSampleFn,
		smpbuf:                 make([]*Sample, opts.InsertAndCompressEvery()*2),
	}
	smp := make([]Sample, len(s.smpbuf))
	for i := 0; i < len(s.smpbuf); i++ {
		s.smpbuf[i] = &smp[i]
	}
	s.ResetSetData(quantiles)
	return s
}

func (s *stream) Add(value float64) {
	if s.numValues > 0 && value < s.insertPointValue() {
		s.bufLess = s.ensureHeapSize(s.bufLess)
		s.bufLess.Push(value)
	} else {
		s.bufMore = s.ensureHeapSize(s.bufMore)
		s.bufMore.Push(value)
	}
	s.insertAndCompressCounter++
	if s.insertAndCompressCounter == s.insertAndCompressEvery {
		//for i := 0; i < s.insertAndCompressEvery; i++ {
		s.insert()
		////}
		//if s.bufLess.Len() != 0 {
		//	s.resetInsertCursor()
		//	s.insert()
		//}
		s.compress()
		s.insertAndCompressCounter = 0
	}
}

func (s *stream) Flush() {
	//fmt.Println("samples ", s.samples.len, " num ", s.numValues, " insertevery ",
	//	s.insertAndCompressEvery, " flushevery ", s.flushEvery,
	//	" bfmore ", s.bufMore.Len(), "bfless", s.bufLess.Len())
	for s.bufLess.Len() > 0 || s.bufMore.Len() > 0 {
		if s.bufMore.Len() == 0 {
			s.resetInsertCursor()
		}
		s.insert()
		s.compress()
	}
	// fmt.Println("inserts ", s.c)
	// fmt.Println("samples ", s.samples.len)
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

func (s *stream) ResetSetData(quantiles []float64) {
	s.quantiles = quantiles
	s.closed = false
	s.insertAndCompressCounter = 0
	s.flushCounter = 0
	s.numValues = 0
	// Returning resources back to pools.
	//sharedHeapPool.Put(&s.bufLess)
	//sharedHeapPool.Put(&s.bufMore)
	s.bufLess = s.bufLess[:]
	s.bufMore = s.bufMore[:]
	sample := s.samples.Front()
	for sample != nil {
		next := sample.next
		s.releaseSample(sample)
		sample = next
	}
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

	// Returning resources back to pools.
	//sharedHeapPool.Put(&s.bufLess)
	//sharedHeapPool.Put(&s.bufMore)
	sample := s.samples.Front()
	for sample != nil {
		next := sample.next
		s.releaseSample(sample)
		sample = next
	}

	// Clear out slices/lists/pointer to reduce GC overhead.
	s.bufLess = s.bufLess[:]
	s.bufMore = s.bufMore[:]
	s.samples.Reset()
	s.insertCursor = nil
	s.compressCursor = nil
	s.streamPool.Put(s)
}

func (s *stream) ensureHeapSize(heap minHeap) minHeap {
	if cap(heap) == len(heap) {
		//newHeap := sharedHeapPool.Get(heap.Len() * 2)
		//*newHeap = append((*newHeap), heap...)
		//sharedHeapPool.Put(&heap)
		newHeap := s.floatsPool.Get(2 * len(heap))
		newHeap = append(newHeap, heap...)
		s.floatsPool.Put(heap)
		return newHeap
	}
	//if cap(heap) == len(heap) {
	//	newHeap := make(minHeap, 0, 2*cap(heap))
	//	newHeap = append(newHeap, heap...)
	//	return newHeap
	//}
	return heap
}

// insert inserts a sample into the stream.
func (s *stream) insert() {
	s.c++
	if s.samples.Len() == 0 {
		if s.bufMore.Len() == 0 {
			return
		}
		sample := s.acquireSample()
		sample.value = s.bufMore.Pop()
		sample.numRanks = 1
		sample.delta = 0
		sample.prev, sample.next = nil, nil
		s.samples.PushBack(sample)
		s.numValues++
	}

	if s.insertCursor == nil {
		s.insertCursor = s.samples.Front()
	}

	var insertPointValue float64
	if s.insertCursor != nil {
		insertPointValue = s.insertCursor.value
	}
	minRank := s.compressMinRank
	numValues := s.numValues
	compCur := s.compressCursor
	var compValue float64
	if compCur != nil {
		compValue = compCur.value
	}
	for s.insertCursor != nil {
		curr := *s.insertCursor
		for s.bufMore.Len() > 0 && s.bufMore.Min() <= insertPointValue {
			sample := s.acquireSample()
			val := s.bufMore.Pop()
			sample.value = val
			sample.numRanks = 1
			sample.delta = curr.numRanks + curr.delta - 1
			sample.prev, sample.next = nil, nil

			s.samples.InsertBefore(sample, s.insertCursor)
			numValues++

			if compCur != nil && compValue >= val {
				minRank++
			}
		}
		s.insertCursor = s.insertCursor.next
		if s.insertCursor != nil {
			insertPointValue = s.insertCursor.value
		} else {
			insertPointValue = 0
		}
	}
	s.numValues = numValues
	s.compressMinRank = minRank
	if s.insertCursor != nil {
		return
	}

	for s.bufMore.Len() > 0 && s.bufMore.Min() >= s.samples.Back().value {
		sample := s.acquireSample()
		sample.value = s.bufMore.Pop()
		sample.numRanks = 1
		sample.delta = 0
		sample.prev, sample.next = nil, nil
		s.samples.PushBack(sample)
		s.numValues++
	}

	s.resetInsertCursor()
}

// compress compresses the samples in the stream.
func (s *stream) compress() {
	// Bail early if there is nothing to compress.
	if s.samples.Len() < minSamplesToCompress {
		return
	}
	//fmt.Println(s.samples.len)
	if s.compressCursor == nil {
		s.compressCursor = s.samples.Back().prev
		s.compressMinRank = s.numValues - 1 - s.compressCursor.numRanks
		s.compressCursor = s.compressCursor.prev
	}

	for s.compressCursor != s.samples.Front() {
		next := s.compressCursor.next
		maxRank := s.compressMinRank + s.compressCursor.numRanks + s.compressCursor.delta
		s.compressMinRank -= s.compressCursor.numRanks
		threshold := int64(math.MaxInt64)
		for _, quantile := range s.quantiles {
			var quantileMin int64
			if float64(maxRank) >= quantile*float64(s.numValues) {
				quantileMin = int64(2 * s.eps * float64(maxRank) / quantile)
			} else {
				quantileMin = int64(2 * s.eps * float64(s.numValues-maxRank) / (1 - quantile))
			}
			if quantileMin < threshold {
				threshold = quantileMin
			}
		}
		testVal := s.compressCursor.numRanks + next.numRanks + next.delta
		if testVal <= threshold {
			if s.insertCursor == s.compressCursor {
				s.insertCursor = next
			}

			next.numRanks += s.compressCursor.numRanks

			prev := s.compressCursor.prev
			s.releaseSample(s.compressCursor)
			s.samples.Remove(s.compressCursor)
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

// resetInsertCursor resets the insert cursor.
func (s *stream) resetInsertCursor() {
	s.bufLess, s.bufMore = s.bufMore, s.bufLess
	s.insertCursor = nil
}

// insertPointValue returns the value under the insertion cursor.
func (s *stream) insertPointValue() float64 {
	if s.insertCursor == nil {
		return 0.0
	}
	return s.insertCursor.value
}

// addToMinHeap adds a value to a min heap.
func (s *stream) addToMinHeap(heap *minHeap, value float64) {
	curr := *heap
	// If we are at capacity, get a bigger heap from the pool
	// and return the current heap to the pool.
	if len(curr) == cap(curr) {
		newHeap := s.floatsPool.Get(2 * len(curr))
		newHeap = append(newHeap, curr...)
		s.floatsPool.Put(curr)
		*heap = newHeap
	}
	heap.Push(value)
}

func (s *stream) acquireSample() *Sample {
	l := len(s.smpbuf)
	// fmt.Println("acquire", "idx", l, "samples", s.samples.len)
	if l <= 0 {
		return &Sample{}
	}
	sample := s.smpbuf[l-1]
	s.smpbuf = s.smpbuf[:l-1]
	//fmt.Println(sample, s.smpbuf, idx)
	return sample
}

func (s *stream) releaseSample(sample *Sample) {
	// fmt.Println("release", "idx", len(s.smpbuf), "samples", s.samples.len)
	s.smpbuf = append(s.smpbuf, sample)
}
