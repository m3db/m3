// Copyright (c) 2019 Uber Technologies, Inc.
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

package builder

import (
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	xerrors "github.com/m3db/m3x/errors"
)

// Ensure for our use case that the terms iter from segments we return
// matches the signature for the terms iterator.
var _ segment.TermsIterator = &termsIterFromSegments{}

type termsIterFromSegments struct {
	keyIter           *multiKeyIterator
	multiPostingsList *multiPostingsList

	field      []byte
	segments   []segmentMetadata
	termsIters []*termsKeyIter
}

func newTermsIterFromSegments() *termsIterFromSegments {
	return &termsIterFromSegments{
		keyIter:           newMultiKeyIterator(),
		multiPostingsList: newMultiPostingsList(),
	}
}

func (i *termsIterFromSegments) clear() {
	i.keyIter.reset()
	i.multiPostingsList.reset()
	i.field = nil
	i.segments = nil
	for _, termIter := range i.termsIters {
		termIter.iter = nil
		termIter.segment = segmentMetadata{}
	}
}

func (i *termsIterFromSegments) reset(
	field []byte,
	segments []segmentMetadata,
) error {
	i.clear()

	i.field = field
	i.segments = segments

	// Alloc any required terms iter containers
	numTermsIterAlloc := len(segments) - len(i.termsIters)
	for j := 0; j < numTermsIterAlloc; j++ {
		i.termsIters = append(i.termsIters, &termsKeyIter{})
	}

	// Add our de-duping multi key value iterator
	i.keyIter.reset()
	for j, seg := range segments {
		iter, err := seg.segment.Terms(field)
		if err != nil {
			return err
		}
		if !iter.Next() {
			// Don't consume this iterator if no results
			if err := xerrors.FirstError(iter.Err(), iter.Close()); err != nil {
				return err
			}
			continue
		}

		tersmKeyIter := i.termsIters[j]
		tersmKeyIter.iter = iter
		tersmKeyIter.segment = seg
		i.keyIter.add(tersmKeyIter)
	}

	return nil
}

func (i *termsIterFromSegments) Next() bool {
	return i.keyIter.Next()
}

func (i *termsIterFromSegments) Current() ([]byte, postings.List) {
	term := i.keyIter.Current()

	currIters := i.keyIter.CurrentIters()

	i.multiPostingsList.reset()
	for _, iter := range currIters {
		termsKeyIter := iter.(*termsKeyIter)
		i.multiPostingsList.add(termsKeyIter)
	}

	return term, i.multiPostingsList
}

func (i *termsIterFromSegments) Err() error {
	return i.keyIter.Err()
}

func (i *termsIterFromSegments) Close() error {
	err := i.keyIter.Close()
	// Free resources
	i.clear()
	return err
}

// termsKeyIter needs to be a keyIterator and contains a terms iterator
var _ keyIterator = &termsKeyIter{}

type termsKeyIter struct {
	iter    segment.TermsIterator
	segment segmentMetadata
}

func (i *termsKeyIter) Next() bool {
	return i.iter.Next()
}

func (i *termsKeyIter) Current() []byte {
	t, _ := i.iter.Current()
	return t
}

func (i *termsKeyIter) Err() error {
	return i.iter.Err()
}

func (i *termsKeyIter) Close() error {
	return i.iter.Close()
}

// multiPostingsList needs to be a postings list and has many postings lists
var _ postings.List = &multiPostingsList{}

type multiPostingsList struct {
	iter *multiPostingsListIterator

	curr []*termsKeyIter
}

func newMultiPostingsList() *multiPostingsList {
	return &multiPostingsList{
		iter: &multiPostingsListIterator{},
	}
}

func (l *multiPostingsList) reset() {
	for i := range l.curr {
		l.curr[i] = nil
	}
	l.curr = l.curr[:0]
}

func (l *multiPostingsList) add(iter *termsKeyIter) {
	l.curr = append(l.curr, iter)
}

func (l *multiPostingsList) Contains(id postings.ID) bool {
	for _, iter := range l.curr {
		offset := iter.segment.offset
		// NB(r): The search ID is relevant by the offset to this list and
		// hence need to decrement by the offset of this segment to
		// the rest of the segments
		searchID := id - offset
		_, list := iter.iter.Current()
		if list.Contains(searchID) {
			return true
		}
	}
	return false
}

func (l *multiPostingsList) IsEmpty() bool {
	for _, iter := range l.curr {
		_, list := iter.iter.Current()
		if !list.IsEmpty() {
			return false
		}
	}
	return true
}

func (l *multiPostingsList) Max() (postings.ID, error) {
	var result postings.ID
	for _, iter := range l.curr {
		_, list := iter.iter.Current()

		maxWithoutOffset, err := list.Max()
		if err != nil {
			return 0, err
		}

		max := maxWithoutOffset + iter.segment.offset
		if max > result {
			result = max
		}
	}

	return result, nil
}

func (l *multiPostingsList) Iterator() postings.Iterator {
	l.addToMultiIterator(l.iter)
	return l.iter
}

func (l *multiPostingsList) addToMultiIterator(
	multiIter *multiPostingsListIterator,
) {
	multiIter.reset()
	for _, iter := range l.curr {
		_, list := iter.iter.Current()
		multiIter.tryAdd(list.Iterator(), iter.segment)
	}
}

func (l *multiPostingsList) Len() int {
	// Need to actually generate an iterator and proceed through
	// since its unclear if this set contains duplicates or not
	var (
		size int
		iter = &multiPostingsListIterator{}
	)
	l.addToMultiIterator(iter)
	for iter.Next() {
		size++
	}
	return size
}

func (l *multiPostingsList) Clone() postings.MutableList {
	result := roaring.NewPostingsList()
	for _, iter := range l.curr {
		_, list := iter.iter.Current()
		offset := iter.segment.offset
		listIter := list.Iterator()
		for listIter.Next() {
			id := offset + listIter.Current()
			_ = result.Insert(id)
		}
	}
	return result
}

func (l *multiPostingsList) Equal(other postings.List) bool {
	if l.Len() != other.Len() {
		return false
	}

	iter := l.Iterator()
	otherIter := other.Iterator()

	for iter.Next() {
		if !otherIter.Next() {
			return false
		}
		if iter.Current() != otherIter.Current() {
			return false
		}
	}

	return true
}

var _ postings.Iterator = &multiPostingsListIterator{}

type multiPostingsListIterator struct {
	firstNext  bool
	curr       []postingsIterator
	currMinIdx int
}

type postingsIterator struct {
	iter         postings.Iterator
	segment      segmentMetadata
	dupesNotSeen []postings.ID
	dupesMatched int
}

func (pi postingsIterator) Current() postings.ID {
	return pi.iter.Current() + pi.segment.offset - postings.ID(pi.dupesMatched)
}

func (pi postingsIterator) CurrentRaw() postings.ID {
	return pi.iter.Current()
}

func (i *multiPostingsListIterator) reset() {
	i.firstNext = true

	for j := range i.curr {
		i.curr[j] = postingsIterator{}
	}
	i.curr = i.curr[:0]

	i.currMinIdx = -1
}

func (i *multiPostingsListIterator) tryAdd(
	iter postings.Iterator,
	segment segmentMetadata,
) {
	idx := len(i.curr)
	i.curr = append(i.curr, postingsIterator{
		iter:         iter,
		segment:      segment,
		dupesNotSeen: segment.duplicatesAsc,
		dupesMatched: 0,
	})
	if !i.moveToValidNext(idx) {
		// Failed to add iter
		i.curr = i.curr[:idx]
		return
	}

	i.trySetCurr(idx)
}

func (i *multiPostingsListIterator) moveToValidNext(idx int) bool {
	for i.curr[idx].iter.Next() {
		raw := i.curr[idx].CurrentRaw()
		// First skip any duplicates we missed
		for len(i.curr[idx].dupesNotSeen) > 0 && raw > i.curr[idx].dupesNotSeen[0] {
			newPostingsIter := i.curr[idx]
			newPostingsIter.dupesNotSeen = newPostingsIter.dupesNotSeen[1:]
			newPostingsIter.dupesMatched++
			i.curr[idx] = newPostingsIter
		}
		if len(i.curr[idx].dupesNotSeen) > 0 && raw == i.curr[idx].dupesNotSeen[0] {
			// Record duplicate seen
			newPostingsIter := i.curr[idx]
			newPostingsIter.dupesNotSeen = newPostingsIter.dupesNotSeen[1:]
			newPostingsIter.dupesMatched++
			i.curr[idx] = newPostingsIter
			continue
		}

		// Valid and not duplicate
		return true
	}

	// Broke out false from iter next
	return false
}

func (i *multiPostingsListIterator) Next() bool {
	if len(i.curr) == 0 {
		return false
	}

	if i.firstNext {
		i.firstNext = false
		return true
	}

	idx := i.currMinIdx

	// Find next, being sure to skip original documents
	// that were duplicates in other segments
	if i.moveToValidNext(idx) {
		// Valid value, recalculate current min value
		i.currEvaluate()
		return true
	}

	// Need to remove
	n := len(i.curr)
	i.curr[idx] = i.curr[n-1]
	i.curr[n-1] = postingsIterator{}
	i.curr = i.curr[:n-1]
	if len(i.curr) == 0 {
		return false
	}

	// Need to re-evaluate from remaining before returning
	i.currEvaluate()
	return true
}

func (i *multiPostingsListIterator) currEvaluate() {
	i.currMinIdx = -1
	for idx := range i.curr {
		i.trySetCurr(idx)
	}
}

func (i *multiPostingsListIterator) trySetCurr(idx int) {
	if i.currMinIdx == -1 {
		i.currMinIdx = idx
		return
	}
	value := i.curr[idx].Current()
	currMinValue := i.curr[i.currMinIdx].Current()
	if value < currMinValue {
		i.currMinIdx = idx
	}
}

func (i *multiPostingsListIterator) Current() postings.ID {
	return i.curr[i.currMinIdx].Current()
}

func (i *multiPostingsListIterator) Err() error {
	return nil
}

func (i *multiPostingsListIterator) Close() error {
	i.reset()
	return nil
}
