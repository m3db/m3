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

var (
	emptySampleList sampleList
)

// sampleList is a list of samples.
type sampleList struct {
	head    *Sample
	tail    *Sample
	samples []Sample
	free    []int32
}

// Empty returns true if the list is empty.
func (l *sampleList) Empty() bool { return len(l.samples) == 0 }

// Len returns the number of samples in the list.
func (l *sampleList) Len() int { return len(l.samples) - len(l.free) }

// Front returns the first sample in the list.
func (l *sampleList) Front() *Sample { return l.head }

// Back returns the last sample in the list.
func (l *sampleList) Back() *Sample { return l.tail }

// Reset resets the list.
func (l *sampleList) Reset() {
	for i := range l.samples {
		l.samples[i].next, l.samples[i].prev = nil, nil
	}
	l.samples = l.samples[:0]
	l.free = l.free[:0]
	l.head, l.tail = nil, nil
}

// PushBack pushes a sample to the end of the list.
func (l *sampleList) PushBack(sample *Sample) {
	sample.prev = l.tail
	sample.next = nil
	if l.head == nil {
		l.head = sample
	} else {
		l.tail.next = sample
	}

	l.tail = sample
}

// InsertBefore inserts a sample before the mark.
func (l *sampleList) InsertBefore(sample *Sample, mark *Sample) {
	var prev *Sample

	if mark.prev == nil {
		l.head = sample
	} else {
		prev = mark.prev
		prev.next = sample
	}

	mark.prev = sample
	sample.next = mark
	sample.prev = prev
}

// Remove removes a sample from the list.
func (l *sampleList) Remove(sample *Sample) {
	prev := sample.prev
	next := sample.next
	l.release(sample)
	if prev == nil {
		l.head = next
	} else {
		prev.next = next
	}
	if next == nil {
		l.tail = prev
	} else {
		next.prev = prev
	}
}

func (l *sampleList) Acquire() *Sample {
	idx := 0

	if len(l.free) > 0 {
		idx = int(l.free[len(l.free)-1])
		l.free = l.free[:len(l.free)-1]
		return &l.samples[idx]
	}

	if len(l.samples) < cap(l.samples) {
		l.samples = l.samples[:len(l.samples)+1]
	} else {
		l.samples = append(l.samples, Sample{})
	}

	idx = len(l.samples) - 1
	s := &l.samples[idx]
	s.idx = int32(idx)
	return s
}

func (l *sampleList) release(sample *Sample) {
	idx := sample.idx
	sample.next, sample.prev = nil, nil
	l.free = append(l.free, idx)
}
