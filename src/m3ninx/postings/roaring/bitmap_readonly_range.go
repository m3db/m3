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

package roaring

import (
	"fmt"

	"github.com/m3db/m3/src/m3ninx/postings"
)

var _ postings.List = (*ReadOnlyRangePostingsList)(nil)
var _ readOnlyIterable = (*ReadOnlyRangePostingsList)(nil)

// ReadOnlyRangePostingsList is a read only range based postings list,
// useful since it imlements the read only iterable interface and can
// therefore be used with UnionReadOnly and IntersectAndNegateReadOnly.
type ReadOnlyRangePostingsList struct {
	startInclusive uint64
	endExclusive   uint64
}

// NewReadOnlyRangePostingsList returns a new read only range postings list
// that can be used with UnionReadOnly and IntersectAndNegateReadOnly.
func NewReadOnlyRangePostingsList(
	startInclusive, endExclusive uint64,
) (*ReadOnlyRangePostingsList, error) {
	if endExclusive < startInclusive {
		return nil, fmt.Errorf("end cannot be before start: start=%d, end=%d",
			startInclusive, endExclusive)
	}
	return &ReadOnlyRangePostingsList{
		startInclusive: startInclusive,
		endExclusive:   endExclusive,
	}, nil
}

// Contains returns whether postings ID is contained or not.
func (b *ReadOnlyRangePostingsList) Contains(id postings.ID) bool {
	return uint64(id) >= b.startInclusive && uint64(id) < b.endExclusive
}

func (b *ReadOnlyRangePostingsList) count() int {
	return int(b.endExclusive - b.startInclusive)
}

// IsEmpty returns true if no results contained by postings.
func (b *ReadOnlyRangePostingsList) IsEmpty() bool {
	return b.count() == 0
}

// CountFast returns the count of entries in postings, if available, false
// if cannot calculate quickly.
func (b *ReadOnlyRangePostingsList) CountFast() (int, bool) {
	return b.count(), true
}

// CountSlow returns the count of entries in postings.
func (b *ReadOnlyRangePostingsList) CountSlow() int {
	return b.count()
}

// Iterator returns a postings iterator.
func (b *ReadOnlyRangePostingsList) Iterator() postings.Iterator {
	return postings.NewRangeIterator(postings.ID(b.startInclusive),
		postings.ID(b.endExclusive))
}

// ContainerIterator returns a container iterator of the postings.
func (b *ReadOnlyRangePostingsList) ContainerIterator() containerIterator {
	return newReadOnlyRangePostingsListContainerIterator(b.startInclusive,
		b.endExclusive)
}

// Equal returns whether this postings list matches another.
func (b *ReadOnlyRangePostingsList) Equal(other postings.List) bool {
	return postings.Equal(b, other)
}

var _ containerIterator = (*readOnlyRangePostingsListContainerIterator)(nil)

type readOnlyRangePostingsListContainerIterator struct {
	startInclusive int64 // use int64 so endInclusive can be -1 if need be
	endInclusive   int64 // use int64 so endInclusive can be -1 if need be
	key            int64
}

func newReadOnlyRangePostingsListContainerIterator(
	startInclusive, endExclusive uint64,
) *readOnlyRangePostingsListContainerIterator {
	return &readOnlyRangePostingsListContainerIterator{
		startInclusive: int64(startInclusive),
		endInclusive:   int64(endExclusive - 1),
		key:            (int64(startInclusive) / containerValues) - 1,
	}
}

func (i *readOnlyRangePostingsListContainerIterator) startInKey() bool {
	return i.key == i.startInclusive/containerValues
}

func (i *readOnlyRangePostingsListContainerIterator) endInKey() bool {
	return i.key == i.endInclusive/containerValues
}

func (i *readOnlyRangePostingsListContainerIterator) validKey() bool {
	return i.key <= i.endInclusive/containerValues
}

func (i *readOnlyRangePostingsListContainerIterator) NextContainer() bool {
	if !i.validKey() {
		return false
	}

	i.key++
	return i.validKey()
}

func (i *readOnlyRangePostingsListContainerIterator) ContainerKey() uint64 {
	return uint64(i.key)
}

func (i *readOnlyRangePostingsListContainerIterator) ContainerUnion(
	ctx containerOpContext,
	target *bitmapContainer,
) {
	start := uint64(0)
	if i.startInKey() {
		start = uint64(i.startInclusive) % containerValues
	}

	end := uint64(containerValues) - 1
	if i.endInKey() {
		end = uint64(i.endInclusive) % containerValues
	}

	// Set from [start, end+1) to union.
	bitmapSetRange(target.bitmap, start, end+1)
}

func (i *readOnlyRangePostingsListContainerIterator) ContainerIntersect(
	ctx containerOpContext,
	target *bitmapContainer,
) {
	start := uint64(0)
	if i.startInKey() {
		start = uint64(i.startInclusive) % containerValues
	}

	end := uint64(containerValues) - 1
	if i.endInKey() {
		end = uint64(i.endInclusive) % containerValues
	}

	// Create temp overlay and intersect with that.
	ctx.tempBitmap.Reset(false)
	bitmapSetRange(ctx.tempBitmap.bitmap, start, end+1)
	intersectBitmapInPlace(target.bitmap, ctx.tempBitmap.bitmap)
}

func (i *readOnlyRangePostingsListContainerIterator) ContainerNegate(
	ctx containerOpContext,
	target *bitmapContainer,
) {
	start := uint64(0)
	if i.startInKey() {
		start = uint64(i.startInclusive) % containerValues
	}

	end := uint64(containerValues) - 1
	if i.endInKey() {
		end = uint64(i.endInclusive) % containerValues
	}

	// Create temp overlay and intersect with that.
	ctx.tempBitmap.Reset(false)
	bitmapSetRange(ctx.tempBitmap.bitmap, start, end+1)
	differenceBitmapInPlace(target.bitmap, ctx.tempBitmap.bitmap)
}

func (i *readOnlyRangePostingsListContainerIterator) Err() error {
	return nil
}

func (i *readOnlyRangePostingsListContainerIterator) Close() {
}
