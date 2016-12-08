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

// sampleList is a list of samples
type sampleList struct {
	head *Sample
	tail *Sample
	len  int
}

// Empty returns true if the list is empty
func (l *sampleList) Empty() bool { return l.len == 0 }

// Len returns the number of samples in the list
func (l *sampleList) Len() int { return l.len }

// Front returns the first sample in the list
func (l *sampleList) Front() *Sample { return l.head }

// Back returns the last sample in the list
func (l *sampleList) Back() *Sample { return l.tail }

// Reset resets the list
func (l *sampleList) Reset() { *l = emptySampleList }

// Pushback pushes a sample to the end of the list
func (l *sampleList) Pushback(sample *Sample) {
	if l.head == nil {
		l.head = sample
		l.tail = sample
	} else {
		sample.prev = l.tail
		l.tail.next = sample
		l.tail = sample
	}
	l.len++
}

// InsertBefore inserts a sample before the mark
func (l *sampleList) InsertBefore(sample *Sample, mark *Sample) {
	if mark.prev == nil {
		mark.prev = sample
		l.head = sample
		sample.next = mark
	} else {
		prev := mark.prev
		prev.next = sample
		mark.prev = sample
		sample.prev = prev
		sample.next = mark
	}
	l.len++
}

// Remove removes a sample from the list
func (l *sampleList) Remove(sample *Sample) {
	prev := sample.prev
	next := sample.next
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
	l.len--
}
