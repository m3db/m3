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
	bitmap "github.com/pilosa/pilosa/roaring"
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

	segments   []segmentTermsMetadata
	termsIters []*termsKeyIter
}

type segmentTermsMetadata struct {
	segment       segmentMetadata
	termsIterable segment.TermsIterable
}

func newTermsIterFromSegments() *termsIterFromSegments {
	b := bitmap.NewBitmapWithDefaultPooling(defaultBitmapContainerPooling)
	return &termsIterFromSegments{
		keyIter:          newMultiKeyIterator(),
		currPostingsList: roaring.NewPostingsListFromBitmap(b),
	}
}

func (i *termsIterFromSegments) clear() {
	i.segments = nil
	i.clearTermIters()
}

func (i *termsIterFromSegments) clearTermIters() {
	i.keyIter.reset()
	i.currPostingsList.Reset()
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

	// Alloc any required terms iter containers
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

		tersmKeyIter := i.termsIters[j]
		tersmKeyIter.iter = iter
		tersmKeyIter.segment = seg.segment
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

	i.currPostingsList.Reset()
	for _, iter := range currIters {
		termsKeyIter := iter.(*termsKeyIter)
		_, list := termsKeyIter.iter.Current()
		if termsKeyIter.segment.offset == 0 {
			i.currPostingsList.Union(list)
		} else {
			// We have to taken into account the offset and duplicates
			var (
				listIter       = list.Iterator()
				duplicates     = termsKeyIter.segment.duplicatesAsc
				negativeOffset postings.ID
			)
			for listIter.Next() {
				curr := listIter.Current()
				for len(duplicates) > 0 && curr > duplicates[0] {
					duplicates = duplicates[1:]
					negativeOffset++
				}
				if len(duplicates) > 0 && curr == duplicates[0] {
					duplicates = duplicates[1:]
					negativeOffset++
					// Also skip this value, as itself is a duplicate
					continue
				}
				value := curr + termsKeyIter.segment.offset - negativeOffset
				_ = i.currPostingsList.Insert(value)
			}
		}
	}

	return term, i.currPostingsList
}

func (i *termsIterFromSegments) Err() error {
	return i.keyIter.Err()
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
