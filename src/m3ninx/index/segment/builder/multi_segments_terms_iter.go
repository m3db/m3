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
	currPostingsList *skipResetOnEmptyMutableList
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
	i.currPostingsList.reset()
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
	i.currPostingsList.reset()
	for _, iter := range i.keyIter.CurrentIters() {
		termsKeyIter := iter.(*termsKeyIter)
		_, list := termsKeyIter.iter.Current()

		if termsKeyIter.segment.offset == 0 && termsKeyIter.segment.skips == 0 {
			// No offset, which means is first segment we are combining from
			// so can just direct union.
			if index.MigrationReadOnlyPostings() {
				readOnlyBitmap := list.(*roaring.ReadOnlyBitmap)
				i.readOnlyBitmapIter.Reset(readOnlyBitmap)
				if err := i.currPostingsList.addIterator(i.readOnlyBitmapIter); err != nil {
					i.err = err
					return false
				}
			} else {
				if err := i.currPostingsList.union(list); err != nil {
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
			if err := i.currPostingsList.insert(value); err != nil {
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

	if i.currPostingsList.list.IsEmpty() {
		// Everything skipped or term is empty.
		// TODO: make this non-stack based (i.e. not recursive).
		return i.Next()
	}

	return true
}

func (i *termsIterFromSegments) Current() ([]byte, postings.List) {
	return i.keyIter.Current(), i.currPostingsList.list
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

type skipResetOnEmptyMutableList struct {
	list  postings.MutableList
	dirty bool
}

func (l *skipResetOnEmptyMutableList) insert(i postings.ID) error {
	l.dirty = true
	return l.list.Insert(i)
}

func (l *skipResetOnEmptyMutableList) union(other postings.List) error {
	l.dirty = true
	return l.list.Union(other)
}

func (l *skipResetOnEmptyMutableList) addIterator(iter postings.Iterator) error {
	l.dirty = true
	err := l.list.AddIterator(iter)
	if l.list.IsEmpty() {
		// No-op
		l.dirty = false
	}
	return err
}

func (l *skipResetOnEmptyMutableList) reset() {
	if !l.dirty {
		return
	}
	l.dirty = false
	l.list.Reset()
}
