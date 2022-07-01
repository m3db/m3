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
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	xerrors "github.com/m3db/m3/src/x/errors"
	bitmap "github.com/m3dbx/pilosa/roaring"
)

const (
	defaultBitmapContainerPooling = 128
)

// Ensure for our use case that the terms iter from segments we return
// matches the signature for the terms iterator.
var _ segment.TermsIterator = &termsIterFromSegments{}

type termsIterFromSegments struct {
	keyIter          *multiKeyIterator
	currPostingsList postings.MutableList
	bitmapIter       *bitmap.Iterator

	segments []segmentTermsMetadata

	err        error
	termsIters []*termsKeyIter

	readOnlyBitmapIter roaring.ReadOnlyBitmapIterator
}

type segmentTermsMetadata struct {
	segment       segmentMetadata
	termsIterable segment.TermsIterable
}

func newTermsIterFromSegments() *termsIterFromSegments {
	var (
		b                  = bitmap.NewBitmapWithDefaultPooling(defaultBitmapContainerPooling)
		readOnlyBitmapIter roaring.ReadOnlyBitmapIterator
	)
	if index.MigrationReadOnlyPostings() {
		readOnlyBitmapIter = roaring.NewReadOnlyBitmapIterator(nil)
	}
	return &termsIterFromSegments{
		keyIter: newMultiKeyIterator(),
		currPostingsList: &skipResetOnEmptyMutableList{
			list: roaring.NewPostingsListFromBitmap(b),
		},
		bitmapIter:         &bitmap.Iterator{},
		readOnlyBitmapIter: readOnlyBitmapIter,
	}
}

func (i *termsIterFromSegments) clear() {
	i.segments = nil
	i.clearTermIters()
}

func (i *termsIterFromSegments) clearTermIters() {
	i.keyIter.reset()
	i.currPostingsList.Reset()
	i.err = nil
	for _, termIter := range i.termsIters {
		termIter.iter = nil
		termIter.segment = segmentMetadata{}
	}
}

func (i *termsIterFromSegments) reset(segments []segmentMetadata) {
	i.clear()

	for _, seg := range segments {
		i.segments = append(i.segments, segmentTermsMetadata{
			segment:       seg,
			termsIterable: seg.segment.TermsIterable(),
		})
	}
}

func (i *termsIterFromSegments) setField(field []byte) error {
	i.clearTermIters()

	// Alloc any required terms iter containers.
	numTermsIterAlloc := len(i.segments) - len(i.termsIters)
	for j := 0; j < numTermsIterAlloc; j++ {
		i.termsIters = append(i.termsIters, &termsKeyIter{})
	}

	// Add our de-duping multi key value iterator
	i.keyIter.reset()
	for j, seg := range i.segments {
		iter, err := seg.termsIterable.Terms(field)
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

		termsKeyIter := i.termsIters[j]
		termsKeyIter.iter = iter
		termsKeyIter.segment = seg.segment
		i.keyIter.add(termsKeyIter)
	}

	return nil
}

func (i *termsIterFromSegments) Next() bool {
	if i.err != nil {
		return false
	}

	if !i.keyIter.Next() {
		return false
	}

	// Create the overlayed postings list for this term
	i.currPostingsList.Reset()
	for _, iter := range i.keyIter.CurrentIters() {
		termsKeyIter := iter.(*termsKeyIter)
		_, list := termsKeyIter.iter.Current()

		if termsKeyIter.segment.offset == 0 && termsKeyIter.segment.skips == 0 {
			// No offset, which means is first segment we are combining from
			// so can just direct union.
			if index.MigrationReadOnlyPostings() {
				readOnlyBitmap := list.(*roaring.ReadOnlyBitmap)
				i.readOnlyBitmapIter.Reset(readOnlyBitmap)
				if err := i.currPostingsList.AddIterator(i.readOnlyBitmapIter); err != nil {
					i.err = err
					return false
				}
			} else {
				if err := i.currPostingsList.Union(list); err != nil {
					i.err = err
					return false
				}
			}
			continue
		}

		// We have to take into account offset and duplicates/skips.
		var (
			negativeOffsets = termsKeyIter.segment.negativeOffsets
			iter            postings.Iterator
		)
		if index.MigrationReadOnlyPostings() {
			readOnlyBitmap := list.(*roaring.ReadOnlyBitmap)
			i.readOnlyBitmapIter.Reset(readOnlyBitmap)
			iter = i.readOnlyBitmapIter
		} else {
			iter = list.Iterator()
		}
		for iter.Next() {
			curr := iter.Current()
			negativeOffset := negativeOffsets[curr]
			// Then skip the individual if matches.
			if negativeOffset == -1 {
				// Skip this value, as itself is a duplicate.
				continue
			}
			value := curr + termsKeyIter.segment.offset - postings.ID(negativeOffset)
			if err := i.currPostingsList.Insert(value); err != nil {
				iter.Close()
				i.err = err
				return false
			}
		}

		err := iter.Err()
		iter.Close()
		if err != nil {
			i.err = err
			return false
		}
	}

	if i.currPostingsList.IsEmpty() {
		// Everything skipped or term is empty.
		// TODO: make this non-stack based (i.e. not recursive).
		return i.Next()
	}

	return true
}

func (i *termsIterFromSegments) Current() ([]byte, postings.List) {
	return i.keyIter.Current(), i.currPostingsList
}

func (i *termsIterFromSegments) Err() error {
	if err := i.keyIter.Err(); err != nil {
		return err
	}
	return i.err
}

func (i *termsIterFromSegments) Close() error {
	err := i.keyIter.Close()
	// Free resources
	i.clearTermIters()
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

var _ postings.MutableList = (*skipResetOnEmptyMutableList)(nil)

type skipResetOnEmptyMutableList struct {
	list  postings.MutableList
	dirty bool
}

// Contains returns whether an ID is contained or not.
func (l *skipResetOnEmptyMutableList) Contains(id postings.ID) bool {
	return l.list.Contains(id)
}

// IsEmpty returns whether the postings list is empty. Some posting lists have an
// optimized implementation to determine if they are empty which is faster than
// calculating the size of the postings list.
func (l *skipResetOnEmptyMutableList) IsEmpty() bool {
	return l.list.IsEmpty()
}

// CountFast returns a count of cardinality quickly if available, returns
// false otherwise.
func (l *skipResetOnEmptyMutableList) CountFast() (int, bool) {
	return l.list.CountFast()
}

// CountSlow should be called when CountFast returns false and a count
// is still required, it will fallback to iterating over the posting lists
// and counting how many entries there were during an iteration.
func (l *skipResetOnEmptyMutableList) CountSlow() int {
	return l.list.CountSlow()
}

// Iterator returns an iterator over the IDs in the postings list.
func (l *skipResetOnEmptyMutableList) Iterator() postings.Iterator {
	return l.list.Iterator()
}

// Equal returns whether this postings list contains the same posting IDs as other.
func (l *skipResetOnEmptyMutableList) Equal(other postings.List) bool {
	return l.list.Equal(other)
}

// Insert inserts the given ID into the postings list.
func (l *skipResetOnEmptyMutableList) Insert(i postings.ID) error {
	l.dirty = true
	return l.list.Insert(i)
}

// Intersect updates this postings list in place to contain only those DocIDs which are
// in both this postings list and other.
func (l *skipResetOnEmptyMutableList) Intersect(other postings.List) error {
	l.dirty = true
	return l.list.Intersect(other)
}

// Difference updates this postings list in place to contain only those DocIDs which are
// in this postings list but not other.
func (l *skipResetOnEmptyMutableList) Difference(other postings.List) error {
	l.dirty = true
	return l.list.Difference(other)
}

// Union updates this postings list in place to contain those DocIDs which are in either
// this postings list or other.
func (l *skipResetOnEmptyMutableList) Union(other postings.List) error {
	l.dirty = true
	return l.list.Union(other)
}

// UnionMany updates this postings list in place to contain those DocIDs which are in
// either this postings list or multiple others.
func (l *skipResetOnEmptyMutableList) UnionMany(others []postings.List) error {
	l.dirty = true
	return l.list.UnionMany(others)
}

// AddIterator adds all IDs contained in the iterator.
func (l *skipResetOnEmptyMutableList) AddIterator(iter postings.Iterator) error {
	l.dirty = true
	err := l.list.AddIterator(iter)
	if l.IsEmpty() {
		// No-op
		l.dirty = false
	}
	return err
}

// AddRange adds all IDs between [min, max) to this postings list.
func (l *skipResetOnEmptyMutableList) AddRange(min, max postings.ID) error {
	l.dirty = true
	return l.list.AddRange(min, max)
}

// RemoveRange removes all IDs between [min, max) from this postings list.
func (l *skipResetOnEmptyMutableList) RemoveRange(min, max postings.ID) error {
	l.dirty = true
	return l.list.RemoveRange(min, max)
}

// Clone returns a copy of the postings list.
func (l *skipResetOnEmptyMutableList) Clone() postings.MutableList {
	return l.list.Clone()
}

// Reset resets the internal state of the postings list.
func (l *skipResetOnEmptyMutableList) Reset() {
	if !l.dirty {
		return
	}
	l.dirty = false
	l.list.Reset()
}
