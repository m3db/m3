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
	nan         = math.NaN()
	defaultOpts = NewOptions()
)

// Stream represents a data stream.
type Stream struct {
	eps                    float64    // desired epsilon for errors
	quantiles              []float64  // sorted target quantiles
	computedQuantiles      []float64  // sorted target quantiles
	capacity               int        // stream capacity
	insertAndCompressEvery int        // stream insertion and compression frequency
	streamPool             StreamPool // pool of streams
	floatsPool             pool.FloatsPool

	insertAndCompressCounter int     // insertion and compression counter
	numValues                int64   // number of values inserted into the sorted stream
	bufLess                  minHeap // sample buffer whose value is less than that at the insertion cursor
	bufMore                  minHeap // sample buffer whose value is more than that at the insertion cursor
	bufLessOld, buffMoreOld  minHeap
	samples                  sampleList // sample list
	insertCursor             *Sample    // insertion cursor
	compressCursor           *Sample    // compression cursor
	compressMinRank          int64      // compression min rank
	heapBufs                 [][]float64
	sampleBuf                []*Sample
	sampleBufs               []*[]Sample
	closed                   bool // whether the stream is closed
	flushed                  bool
}

// NewStream creates a new sample stream.
func NewStream(quantiles []float64, opts Options) *Stream {
	if opts == nil {
		opts = defaultOpts
	}

	s := &Stream{
		eps:                    opts.Eps(),
		computedQuantiles:      make([]float64, len(quantiles)),
		capacity:               64,
		insertAndCompressEvery: 1024, //opts.InsertAndCompressEvery(),
		sampleBuf:              []*Sample{},
		sampleBufs:             make([]*[]Sample, 0, 4),
		floatsPool:             opts.FloatsPool(),

		streamPool: opts.StreamPool(),
	}

	if len(s.quantiles) > 0 { // do not pre-allocate
		s.ResetSetData(quantiles)
	}
	return s
}

func (s *Stream) AddBatch(values []float64) {
	s.flushed = false

	s.ensureHeapSize(&s.bufLess, len(values))
	s.ensureHeapSize(&s.bufMore, len(values))
	//fmt.Println("az", s.bufMore)
	for _, value := range values {
		if s.numValues > 0 && value < s.insertPointValue() {
			s.bufLess.Push(value)
		} else {
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
}

func (s *Stream) Add(value float64) {
	s.AddBatch([]float64{value})
}

func (s *Stream) Flush() {
	if s.flushed {
		return
	}
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
	s.calcQuantiles()
	s.flushed = true
	// fmt.Println("inserts ", s.c)
	// fmt.Println("samples ", s.samples.len)
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
		numVals   = float64(s.numValues)
		rank      = int64(math.Ceil(q * numVals))
		threshold = int64(math.Ceil(s.threshold(float64(rank), numVals) / 2.0))
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
			rank = int64(math.Ceil(q * numVals))
			threshold = int64(math.Ceil(s.threshold(float64(rank), numVals) / 2.0))
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
	s.insertAndCompressCounter = 0
	s.numValues = 0
	s.samples.Reset()

	s.insertCursor = nil
	s.compressCursor = nil
	s.compressMinRank = 0

	if s.bufMore != nil {
		s.bufMore = s.bufMore[:0]
	}

	if s.bufLess != nil {
		s.bufLess = s.bufLess[:0]
	}

}

func (s *Stream) Close() {
	if s.closed {
		return
	}
	s.closed = true

	if s.bufMore != nil {
		s.floatsPool.Put(s.bufMore)
	}

	if s.bufLess != nil {
		s.floatsPool.Put(s.bufLess)
	}

	s.sampleBuf = s.sampleBuf[:0]
	for i := range s.sampleBufs {
		buf := *s.sampleBufs[i]
		for j := range buf {
			smp := &buf[j]
			smp.next, smp.prev = nil, nil
		}
		sharedSamplePool.Put(s.sampleBufs[i])
		s.sampleBufs[i] = nil
	}
	s.sampleBufs = s.sampleBufs[:0]

	// Clear out slices/lists/pointer to reduce GC overhead.
	s.samples.Reset()
	s.bufMore = nil
	s.bufLess = nil
	s.insertCursor = nil
	s.compressCursor = nil
	s.streamPool.Put(s)
}

func (s *Stream) ensureHeapSize(heap *minHeap, new int) {
	var (
		curr      = *heap
		targetCap = new + len(curr)
	)

	if targetCap >= cap(curr) {
		if targetCap < s.capacity {
			targetCap = s.capacity
		}
		newHeap := s.floatsPool.Get(targetCap)
		s.heapBufs = append(s.heapBufs, newHeap)
		newHeap = append(newHeap, curr...)
		s.floatsPool.Put(curr)
		*heap = newHeap
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

	s.bufLess.ShiftUp()
	s.bufMore.ShiftUp()

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
	//fmt.Println(s.samples.len)
	if s.compressCursor == nil {
		s.compressCursor = s.samples.Back().prev
		s.compressMinRank = s.numValues - 1 - s.compressCursor.numRanks
		s.compressCursor = s.compressCursor.prev
	}

	numVals := float64(s.numValues)
	for s.compressCursor != s.samples.Front() {
		next := s.compressCursor.next
		maxRank := float64(s.compressMinRank + s.compressCursor.numRanks + s.compressCursor.delta)
		s.compressMinRank -= s.compressCursor.numRanks
		threshold := s.threshold(maxRank, numVals)

		testVal := float64(s.compressCursor.numRanks + next.numRanks + next.delta)
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
func (s *Stream) threshold(rank float64, numValues float64) float64 {
	var (
		minVal      = math.MaxFloat64
		idx         int
		quantileMin float64
		quantile    float64
	)
For: // TODO: remove me once on go 1.16+
	if idx == len(s.quantiles) {
		return minVal
	}
	quantile = s.quantiles[idx]
	if rank >= quantile*numValues {
		quantileMin = 2 * s.eps * rank / quantile
	} else {
		quantileMin = 2 * s.eps * (numValues - rank) / (1 - quantile)
	}
	if quantileMin < minVal {
		minVal = quantileMin
	}
	idx++
	goto For
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

// addToMinHeap adds a value to a min heap.
func (s *Stream) addToMinHeap(heap *minHeap, value float64) {
	s.ensureHeapSize(heap, 1)
	heap.Push(value)
}

// tryAcquireSample is inline-friendly fastpath to get a sample from preallocated buffer
func (s *Stream) tryAcquireSample() *Sample {
	var (
		sbuf = s.sampleBuf
		l    = len(sbuf)
	)

	if l == 0 {
		return nil
	}

	sample := sbuf[l-1]
	s.sampleBuf = sbuf[:l-1]
	sample.next, sample.prev = nil, nil

	return sample
}

func (s *Stream) acquireSample() *Sample {
	ss, ok := sharedSamplePool.Get().(*[]Sample)
	if !ok {
		return &Sample{}
	}

	sbuf := *ss
	for i := range sbuf {
		s.sampleBuf = append(s.sampleBuf, &sbuf[i])
	}

	// keep track of all bulk-allocated samples
	// so we can release memory used by samples to the pool
	s.sampleBufs = append(s.sampleBufs, ss)
	l := len(s.sampleBuf)
	sample := s.sampleBuf[l-1]
	s.sampleBuf = s.sampleBuf[:l-1]
	sample.next, sample.prev = nil, nil

	return sample
}

func (s *Stream) releaseSample(sample *Sample) {
	if sample == nil {
		return
	}

	sample.next, sample.prev = nil, nil
	s.sampleBuf = append(s.sampleBuf, sample)
}
