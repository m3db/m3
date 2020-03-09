// Copyright (c) 2020 Uber Technologies, Inc.
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
	"bytes"

	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	xerrors "github.com/m3db/m3/src/x/errors"
	bitmap "github.com/m3db/pilosa/roaring"
)

var _ segment.FieldsPostingsListIterator = &multiKeyPostingsListIterator{}

type multiKeyPostingsListIterator struct {
	err                    error
	firstNext              bool
	closeIters             []keyIterator
	iters                  []keyIterator
	currIters              []keyIterator
	currReaders            []index.Reader
	segments               []segment.Segment
	currFieldPostingsList  postings.MutableList
	currFieldPostingsLists []postings.List
}

func newMultiKeyPostingsListIterator() *multiKeyPostingsListIterator {
	b := bitmap.NewBitmapWithDefaultPooling(defaultBitmapContainerPooling)
	i := &multiKeyPostingsListIterator{
		currFieldPostingsList: roaring.NewPostingsListFromBitmap(b),
	}
	i.reset()
	return i
}

func (i *multiKeyPostingsListIterator) reset() {
	i.firstNext = true
	i.currFieldPostingsList.Reset()

	for j := range i.closeIters {
		i.closeIters[j] = nil
	}
	i.closeIters = i.closeIters[:0]

	for j := range i.iters {
		i.iters[j] = nil
	}
	i.iters = i.iters[:0]

	for j := range i.currIters {
		i.currIters[j] = nil
	}
	i.currIters = i.currIters[:0]

	for j := range i.segments {
		i.segments[j] = nil
	}
	i.segments = i.segments[:0]
}

func (i *multiKeyPostingsListIterator) add(iter keyIterator, segment segment.Segment) {
	i.closeIters = append(i.closeIters, iter)
	i.iters = append(i.iters, iter)
	i.segments = append(i.segments, segment)
	i.tryAddCurr(iter)
}

func (i *multiKeyPostingsListIterator) Next() bool {
	if i.err != nil {
		return false
	}
	if len(i.iters) == 0 {
		return false
	}

	if i.firstNext {
		i.firstNext = false
		return true
	}

	for _, currIter := range i.currIters {
		currNext := currIter.Next()
		if currNext {
			// Next has a value, forward other matching too
			continue
		}

		// Remove iter
		n := len(i.iters)
		idx := -1
		for j, iter := range i.iters {
			if iter == currIter {
				idx = j
				break
			}
		}
		i.iters[idx] = i.iters[n-1]
		i.iters[n-1] = nil
		i.iters = i.iters[:n-1]
	}
	if len(i.iters) == 0 {
		return false
	}

	prevField := i.currIters[0].Current()

	// Re-evaluate current value
	i.currEvaluate()

	// NB(bodu): Build the postings list for this field if the field has changed.
	i.currFieldPostingsLists = i.currFieldPostingsLists[:0]
	i.currReaders = i.currReaders[:0]
	currField := i.currIters[0].Current()

	if !bytes.Equal(prevField, currField) {
		i.currFieldPostingsList.Reset()
		for _, segment := range i.segments {
			reader, err := segment.Reader()
			if err != nil {
				i.err = err
				return false
			}
			pl, err := reader.MatchField(currField)
			if err != nil {
				i.err = err
				return false
			}

			i.currFieldPostingsLists = append(i.currFieldPostingsLists, pl)
			i.currReaders = append(i.currReaders, reader)
		}

		i.currFieldPostingsList.UnionMany(i.currFieldPostingsLists)

		for _, reader := range i.currReaders {
			if err := reader.Close(); err != nil {
				i.err = err
				return false
			}
		}
	}
	return true
}

func (i *multiKeyPostingsListIterator) currEvaluate() {
	i.currIters = i.currIters[:0]
	for _, iter := range i.iters {
		i.tryAddCurr(iter)
	}
}

func (i *multiKeyPostingsListIterator) tryAddCurr(iter keyIterator) {
	var (
		hasCurr = len(i.currIters) > 0
		cmp     int
	)
	if hasCurr {
		curr, _ := i.Current()
		cmp = bytes.Compare(iter.Current(), curr)
	}
	if !hasCurr || cmp < 0 {
		// Set the current lowest key value
		i.currIters = i.currIters[:0]
		i.currIters = append(i.currIters, iter)
	} else if hasCurr && cmp == 0 {
		// Set a matching duplicate curr iter
		i.currIters = append(i.currIters, iter)
	}
}

func (i *multiKeyPostingsListIterator) Current() ([]byte, postings.List) {
	return i.currIters[0].Current(), i.currFieldPostingsList
}

func (i *multiKeyPostingsListIterator) CurrentIters() []keyIterator {
	return i.currIters
}

func (i *multiKeyPostingsListIterator) Err() error {
	multiErr := xerrors.NewMultiError()
	for _, iter := range i.closeIters {
		multiErr = multiErr.Add(iter.Err())
	}
	if i.err != nil {
		multiErr = multiErr.Add(i.err)
	}
	return multiErr.FinalError()
}

func (i *multiKeyPostingsListIterator) Close() error {
	multiErr := xerrors.NewMultiError()
	for _, iter := range i.closeIters {
		multiErr = multiErr.Add(iter.Close())
	}
	// Free resources
	i.reset()
	return multiErr.FinalError()
}
