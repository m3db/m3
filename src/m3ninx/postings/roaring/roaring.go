// Copyright (c) 2017 Uber Technologies, Inc.
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

package roaring

import (
	"errors"
	"sync"

	"github.com/m3db/m3ninx/postings"

	"github.com/RoaringBitmap/roaring"
)

var (
	errIntersectRoaringOnly  = errors.New("Intersect only supported between roaringDocId sets")
	errUnionRoaringOnly      = errors.New("Union only supported between roaringDocId sets")
	errDifferenceRoaringOnly = errors.New("Difference only supported between roaringDocId sets")
	errIteratorClosed        = errors.New("iterator has been closed")
)

// postingsList wraps a Roaring Bitmap with a mutex for thread safety.
type postingsList struct {
	sync.RWMutex
	bitmap *roaring.Bitmap
}

// NewPostingsList returns a new mutable postings list backed by a Roaring Bitmap.
func NewPostingsList() postings.MutableList {
	return &postingsList{
		bitmap: roaring.NewBitmap(),
	}
}

func (d *postingsList) Insert(i postings.ID) {
	d.Lock()
	d.bitmap.Add(uint32(i))
	d.Unlock()
}

func (d *postingsList) Intersect(other postings.List) error {
	o, ok := other.(*postingsList)
	if !ok {
		return errIntersectRoaringOnly
	}

	o.RLock()
	d.Lock()
	d.bitmap.And(o.bitmap)
	d.Unlock()
	o.RUnlock()
	return nil
}

func (d *postingsList) Difference(other postings.List) error {
	o, ok := other.(*postingsList)
	if !ok {
		return errDifferenceRoaringOnly
	}

	o.RLock()
	d.Lock()
	d.bitmap.AndNot(o.bitmap)
	d.Unlock()
	o.RUnlock()
	return nil
}

func (d *postingsList) Union(other postings.List) error {
	o, ok := other.(*postingsList)
	if !ok {
		return errUnionRoaringOnly
	}

	o.RLock()
	d.Lock()
	d.bitmap.Or(o.bitmap)
	d.Unlock()
	o.RUnlock()
	return nil
}

func (d *postingsList) AddRange(min, max postings.ID) {
	d.Lock()
	d.bitmap.AddRange(uint64(min), uint64(max))
	d.Unlock()
}

func (d *postingsList) RemoveRange(min, max postings.ID) {
	d.Lock()
	d.bitmap.RemoveRange(uint64(min), uint64(max))
	d.Unlock()
}

func (d *postingsList) Reset() {
	d.Lock()
	d.bitmap.Clear()
	d.Unlock()
}

func (d *postingsList) Contains(i postings.ID) bool {
	d.RLock()
	contains := d.bitmap.Contains(uint32(i))
	d.RUnlock()
	return contains
}

func (d *postingsList) IsEmpty() bool {
	d.RLock()
	empty := d.bitmap.IsEmpty()
	d.RUnlock()
	return empty
}

func (d *postingsList) Max() (postings.ID, error) {
	d.RLock()
	if d.bitmap.IsEmpty() {
		d.RUnlock()
		return 0, postings.ErrEmptyList
	}
	max := d.bitmap.Maximum()
	d.RUnlock()
	return postings.ID(max), nil
}

func (d *postingsList) Min() (postings.ID, error) {
	d.RLock()
	if d.bitmap.IsEmpty() {
		d.RUnlock()
		return 0, postings.ErrEmptyList
	}
	min := d.bitmap.Minimum()
	d.RUnlock()
	return postings.ID(min), nil
}

func (d *postingsList) Len() int {
	d.RLock()
	l := d.bitmap.GetCardinality()
	d.RUnlock()
	return int(l)
}

func (d *postingsList) Iterator() postings.Iterator {
	return &roaringIterator{
		iter: d.bitmap.Iterator(),
	}
}

func (d *postingsList) Clone() postings.MutableList {
	d.RLock()
	// TODO: It's cheaper to Clone than to cache roaring bitmaps, see
	// `postings_list_bench_test.go`. Their internals don't allow for
	// pooling at the moment. We should address this when get a chance
	// (move to another implementation / address deficiencies).
	clone := d.bitmap.Clone()
	d.RUnlock()
	return &postingsList{
		bitmap: clone,
	}
}

func (d *postingsList) Equal(other postings.List) bool {
	if d.Len() != other.Len() {
		return false
	}

	o, ok := other.(*postingsList)
	if ok {
		return d.bitmap.Equals(o.bitmap)
	}

	iter := d.Iterator()
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

type roaringIterator struct {
	iter    roaring.IntIterable
	current postings.ID
	closed  bool
}

func (it *roaringIterator) Current() postings.ID {
	return it.current
}

func (it *roaringIterator) Next() bool {
	if it.closed || !it.iter.HasNext() {
		return false
	}
	it.current = postings.ID(it.iter.Next())
	return true
}

func (it *roaringIterator) Err() error {
	return nil
}

func (it *roaringIterator) Close() error {
	if it.closed {
		return errIteratorClosed
	}
	it.closed = true
	return nil
}
