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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"unsafe"

	"github.com/m3db/m3/src/m3ninx/postings"
)

const (
	headerBaseSize     = uint64(8)
	magicNumber        = uint32(12348)
	storageVersion     = uint32(0)
	bitmapN            = 1024
	runCountHeaderSize = uint32(2)
	containerValues    = 2 << 15 // 2^16 or 65k
	maxBitmap          = 0xFFFFFFFFFFFFFFFF
)

var (
	// ErrNotReadOnlyBitmap returned from operations that expect read only bitmaps.
	ErrNotReadOnlyBitmap = errors.New("not read only bitmap")

	errNotPilosaRoaring = errors.New("not pilosa roaring format")
)

type containerType byte

const (
	containerUnknown containerType = iota
	containerArray
	containerBitmap
	containerRun
)

func (t containerType) String() string {
	switch t {
	case containerArray:
		return "array"
	case containerBitmap:
		return "bitmap"
	case containerRun:
		return "run"
	default:
		return "unknown"
	}
}

func highbits(v uint64) uint64 {
	return v >> 16
}

func lowbits(v uint64) uint16 {
	return uint16(v & 0xFFFF)
}

// ReadOnlyBitmapFromPostingsList returns a bitmap from a postings list if it
// is a read only roaring bitmap postings list.
func ReadOnlyBitmapFromPostingsList(pl postings.List) (*ReadOnlyBitmap, bool) {
	result, ok := pl.(*ReadOnlyBitmap)
	if !ok {
		return nil, false
	}
	return result, true
}

var _ postings.List = (*ReadOnlyBitmap)(nil)
var _ readOnlyIterable = (*ReadOnlyBitmap)(nil)

// ReadOnlyBitmap is a read only roaring Bitmap of
// pilosa encoded roaring bitmaps, allocates very little on unmarshal
// except the number of keys and no allocations occur per operation
// except when creating an iterator (which allocates just the iterator,
// there are no allocations after the creation of the iterator).
type ReadOnlyBitmap struct {
	data []byte
	keyN uint64
}

// NewReadOnlyBitmap returns a new read only bitmap.
func NewReadOnlyBitmap(data []byte) (*ReadOnlyBitmap, error) {
	b := &ReadOnlyBitmap{}
	if err := b.Reset(data); err != nil {
		return nil, err
	}
	return b, nil
}

// Reset resets the read only bitmap.
func (b *ReadOnlyBitmap) Reset(data []byte) error {
	// Reset to nil.
	if len(data) == 0 {
		b.data = nil
		b.keyN = 0
		return nil
	}

	if n := len(data); uint64(n) < headerBaseSize {
		return fmt.Errorf("must be at least %d bytes: actual=%d",
			headerBaseSize, n)
	}

	fileMagic := uint32(binary.LittleEndian.Uint16(data[0:2]))
	fileVersion := uint32(binary.LittleEndian.Uint16(data[2:4]))
	if fileMagic != magicNumber {
		return fmt.Errorf("invalid roaring file, magic number %v is incorrect",
			fileMagic)
	}

	if fileVersion != storageVersion {
		return fmt.Errorf("wrong roaring version, file is v%d, server requires v%d",
			fileVersion, storageVersion)
	}

	// Read key count in bytes sizeof(cookie):(sizeof(cookie)+sizeof(uint32)).
	b.keyN = uint64(binary.LittleEndian.Uint32(data[4:8]))
	b.data = data

	// Validate all the containers.
	for i := uint64(0); i < b.keyN; i++ {
		if _, err := b.containerAtIndex(i); err != nil {
			return err
		}
	}
	return nil
}

type readOnlyContainer struct {
	data          []byte
	key           uint64
	containerType containerType
	cardinality   uint16
	offset        uint32
}

type bitmapReadOnlyContainer struct {
	values []uint64
}

func (b bitmapReadOnlyContainer) contains(v uint16) bool {
	return (b.values[v/64] & (1 << uint64(v%64))) != 0
}

func (b bitmapReadOnlyContainer) containsAnyRange(start, end int32) bool {
	i, j := start/64, end/64

	// Same uint64.
	if i == j {
		offi, offj := uint(start%64), uint(64-end%64)
		return popcount((b.values[i]>>offi)<<(offj+offi)) > 0
	}

	// At start.
	if off := uint(start) % 64; off != 0 {
		if popcount(b.values[i]>>off) > 0 {
			return true
		}
		i++
	}

	// Count uint64 in between.
	for ; i < j; i++ {
		if popcount(b.values[i]) > 0 {
			return true
		}
	}

	// Count partial ending uint64.
	if j < int32(len(b.values)) {
		off := 64 - (uint(end) % 64)
		if popcount(b.values[j]<<off) > 0 {
			return true
		}
	}

	return false
}

func popcount(x uint64) uint64 {
	return uint64(bits.OnesCount64(x))
}

func (b bitmapReadOnlyContainer) intersectsAnyBitmap(other bitmapReadOnlyContainer) bool {
	var (
		ab = b.values[:bitmapN]
		bb = other.values[:bitmapN]
	)
	for i := 0; i < bitmapN; i += 4 {
		if ab[i]&bb[i] != 0 {
			return true
		}
		if ab[i+1]&bb[i+1] != 0 {
			return true
		}
		if ab[i+2]&bb[i+2] != 0 {
			return true
		}
		if ab[i+3]&bb[i+3] != 0 {
			return true
		}
	}
	return false
}

type arrayReadOnlyContainer struct {
	values []uint16
}

func (a arrayReadOnlyContainer) contains(v uint16) bool {
	n := len(a.values)
	idx := sort.Search(n, func(i int) bool {
		return a.values[i] >= v
	})
	return idx < n && a.values[idx] == v
}

func (a arrayReadOnlyContainer) intersectsAnyArray(other arrayReadOnlyContainer) bool {
	for i, j := 0, 0; i < len(a.values) && j < len(other.values); {
		if a.values[i] < other.values[j] {
			i++
			continue
		}
		if other.values[j] < a.values[i] {
			j++
			continue
		}
		return true
	}
	return false
}

func (a arrayReadOnlyContainer) intersectsAnyBitmap(other bitmapReadOnlyContainer) bool {
	for _, value := range a.values {
		if other.contains(value) {
			return true
		}
	}
	return false
}

func (a arrayReadOnlyContainer) intersectsAnyRuns(other runReadOnlyContainer) bool {
	for _, value := range a.values {
		if other.contains(value) {
			return true
		}
	}
	return false
}

type runReadOnlyContainer struct {
	values []interval16
}

func (r runReadOnlyContainer) contains(v uint16) bool {
	n := len(r.values)
	idx := sort.Search(n, func(i int) bool {
		return r.values[i].last >= v
	})
	return idx < n && v >= r.values[idx].start && v <= r.values[idx].last
}

func (r runReadOnlyContainer) intersectsAnyRuns(other runReadOnlyContainer) bool {
	for i, j := 0, 0; i < len(r.values) && j < len(other.values); {
		va, vb := r.values[i], other.values[j]
		if va.last < vb.start {
			i++
		} else if va.start > vb.last {
			j++
		} else if va.last > vb.last && va.start >= vb.start {
			return true
		} else if va.last > vb.last && va.start < vb.start {
			return true
		} else if va.last <= vb.last && va.start >= vb.start {
			return true
		} else if va.last <= vb.last && va.start < vb.start {
			return true
		}
	}
	return false
}

func (r runReadOnlyContainer) intersectsAnyBitmap(other bitmapReadOnlyContainer) bool {
	for _, value := range r.values {
		if other.containsAnyRange(int32(value.start), int32(value.last)+1) {
			return true
		}
	}
	return false
}

func (c readOnlyContainer) validate() error {
	switch c.containerType {
	case containerBitmap:
		need := int(c.offset) + 8*bitmapN // entry uint64 bitmap 8 bytes
		if len(c.data) < need {
			return fmt.Errorf("data too small for bitmap: needs=%d, actual=%d",
				need, len(c.data))
		}
		return nil
	case containerArray:
		need := int(c.offset) + 2*int(c.cardinality) // entry is uint16 2 bytes
		if len(c.data) < need {
			return fmt.Errorf("data too small for array: needs=%d, actual=%d",
				need, len(c.data))
		}
		return nil
	case containerRun:
		need := int(c.offset) + int(runCountHeaderSize)
		if len(c.data) < need {
			return fmt.Errorf("data too small for runs header: needs=%d, actual=%d",
				need, len(c.data))
		}
		runCount := binary.LittleEndian.Uint16(c.data[c.offset : c.offset+runCountHeaderSize])
		need = int(c.offset) + int(runCountHeaderSize) + 4*int(runCount) // entry is two uint16s 4 bytes
		if len(c.data) < need {
			return fmt.Errorf("data too small for runs values: needs=%d, actual=%d",
				need, len(c.data))
		}
		return nil
	}
	return fmt.Errorf("unknown container: %d", c.containerType)
}

func (c readOnlyContainer) bitmap() (bitmapReadOnlyContainer, bool) {
	if c.containerType != containerBitmap {
		return bitmapReadOnlyContainer{}, false
	}
	return bitmapReadOnlyContainer{
		values: (*[0xFFFFFFF]uint64)(unsafe.Pointer(&c.data[c.offset]))[:bitmapN:bitmapN],
	}, true
}

func (c readOnlyContainer) array() (arrayReadOnlyContainer, bool) {
	if c.containerType != containerArray {
		return arrayReadOnlyContainer{}, false
	}
	return arrayReadOnlyContainer{
		values: (*[0xFFFFFFF]uint16)(unsafe.Pointer(&c.data[c.offset]))[:c.cardinality:c.cardinality],
	}, true
}

func (c readOnlyContainer) runs() (runReadOnlyContainer, bool) {
	if c.containerType != containerRun {
		return runReadOnlyContainer{}, false
	}
	runCount := binary.LittleEndian.Uint16(c.data[c.offset : c.offset+runCountHeaderSize])
	return runReadOnlyContainer{
		values: (*[0xFFFFFFF]interval16)(unsafe.Pointer(&c.data[c.offset+runCountHeaderSize]))[:runCount:runCount],
	}, true
}

type interval16 struct {
	start uint16
	last  uint16
}

func (i interval16) n() uint16 {
	return i.last - i.start
}

func (b *ReadOnlyBitmap) container(key uint64) (readOnlyContainer, bool) {
	index, ok := b.indexOfKey(key)
	if !ok {
		return readOnlyContainer{}, false
	}
	// All offsets validated at construction time, safe to ignore the
	// error here.
	// If we had to return an error to Contains(...) and Iterator() then
	// we wouldn't be able to implement the API contract.
	// Today we also have this same issue with existing mmap backed roaring
	// bitmaps from pilosa, so it doesn't reduce or expand our risk exposure.
	container, _ := b.containerAtIndex(index)
	return container, true
}

func (b *ReadOnlyBitmap) containerAtIndex(index uint64) (readOnlyContainer, error) {
	const (
		metaTypeStart = 8
		metaTypeEnd   = 10
		metaCardStart = 10
		metaCardEnd   = 12
		offsetStart   = 0
		offsetEnd     = 4
	)
	metaIdx := headerBaseSize + index*12
	offsetIdx := headerBaseSize + b.keyN*12 + index*4
	size := uint64(len(b.data))
	if size < metaIdx+metaCardEnd {
		return readOnlyContainer{}, fmt.Errorf(
			"data too small: need=%d, actual=%d", metaIdx+metaCardEnd, size)
	}
	if size < offsetIdx+offsetEnd {
		return readOnlyContainer{}, fmt.Errorf(
			"data too small: need=%d, actual=%d", offsetIdx+offsetEnd, size)
	}
	meta := b.data[metaIdx:]
	offsets := b.data[offsetIdx:]
	container := readOnlyContainer{
		data:          b.data,
		key:           b.keyAtIndex(int(index)),
		containerType: containerType(binary.LittleEndian.Uint16(meta[metaTypeStart:metaTypeEnd])),
		cardinality:   uint16(binary.LittleEndian.Uint16(meta[metaCardStart:metaCardEnd])) + 1,
		offset:        binary.LittleEndian.Uint32(offsets[offsetStart:offsetEnd]),
	}
	if err := container.validate(); err != nil {
		return readOnlyContainer{}, err
	}
	return container, nil
}

// Contains returns whether postings ID is contained or not.
func (b *ReadOnlyBitmap) Contains(id postings.ID) bool {
	value := uint64(id)
	container, ok := b.container(highbits(value))
	if !ok {
		return false
	}
	if bitmap, ok := container.bitmap(); ok {
		return bitmap.contains(lowbits(value))
	}
	if array, ok := container.array(); ok {
		return array.contains(lowbits(value))
	}
	if runs, ok := container.runs(); ok {
		return runs.contains(lowbits(value))
	}
	return false
}

// IsEmpty returns true if no results contained by postings.
func (b *ReadOnlyBitmap) IsEmpty() bool {
	return b.count() == 0
}

func (b *ReadOnlyBitmap) count() int {
	l := 0
	for i := uint64(0); i < b.keyN; i++ {
		// All offsets validated at construction time, safe to ignore the
		// error here.
		// If we had to return an error to Contains(...) and Iterator() then
		// we wouldn't be able to implement the API contract.
		// Today we also have this same issue with existing mmap backed roaring
		// bitmaps from pilosa, so it doesn't reduce or expand our risk exposure.
		container, _ := b.containerAtIndex(i)
		l += int(container.cardinality)
	}
	return l
}

// CountFast returns the count of entries in postings, if available, false
// if cannot calculate quickly.
func (b *ReadOnlyBitmap) CountFast() (int, bool) {
	return b.count(), true
}

// CountSlow returns the count of entries in postings.
func (b *ReadOnlyBitmap) CountSlow() int {
	return b.count()
}

// Iterator returns a postings iterator.
func (b *ReadOnlyBitmap) Iterator() postings.Iterator {
	return newReadOnlyBitmapIterator(b)
}

// ContainerIterator returns a container iterator of the postings.
func (b *ReadOnlyBitmap) ContainerIterator() containerIterator {
	return newReadOnlyBitmapContainerIterator(b)
}

// Equal returns whether this postings list matches another.
func (b *ReadOnlyBitmap) Equal(other postings.List) bool {
	return postings.Equal(b, other)
}

// IntersectsAny checks whether other bitmap intersects any values in this one.
func (b *ReadOnlyBitmap) IntersectsAny(other *ReadOnlyBitmap) bool {
	if b.keyN < 1 || other.keyN < 1 {
		return false
	}
	for i, j := uint64(0), uint64(0); i < b.keyN && j < other.keyN; {
		ki, kj := b.keyAtIndex(int(i)), other.keyAtIndex(int(j))
		if ki < kj {
			i++
			continue
		}
		if kj < ki {
			j++
			continue
		}

		// Same key.
		ci, _ := b.containerAtIndex(i)
		cj, _ := other.containerAtIndex(j)
		switch ci.containerType {
		case containerArray:
			left, _ := ci.array()

			switch cj.containerType {
			case containerArray:
				right, _ := cj.array()
				if left.intersectsAnyArray(right) {
					return true
				}
			case containerBitmap:
				right, _ := cj.bitmap()
				if left.intersectsAnyBitmap(right) {
					return true
				}
			case containerRun:
				right, _ := cj.runs()
				if left.intersectsAnyRuns(right) {
					return true
				}
			}
		case containerBitmap:
			left, _ := ci.bitmap()

			switch cj.containerType {
			case containerArray:
				right, _ := cj.array()
				if right.intersectsAnyBitmap(left) {
					return true
				}
			case containerBitmap:
				right, _ := cj.bitmap()
				if left.intersectsAnyBitmap(right) {
					return true
				}
			case containerRun:
				right, _ := cj.runs()
				if right.intersectsAnyBitmap(left) {
					return true
				}
			}
		case containerRun:
			left, _ := ci.runs()

			switch cj.containerType {
			case containerArray:
				right, _ := cj.array()
				if right.intersectsAnyRuns(left) {
					return true
				}
			case containerBitmap:
				right, _ := cj.bitmap()
				if left.intersectsAnyBitmap(right) {
					return true
				}
			case containerRun:
				right, _ := cj.runs()
				if left.intersectsAnyRuns(right) {
					return true
				}
			}
		}

		i++
		j++
	}

	return false
}

func (b *ReadOnlyBitmap) keyAtIndex(index int) uint64 {
	meta := b.data[int(headerBaseSize)+index*12:]
	return binary.LittleEndian.Uint64(meta[0:8])
}

func (b *ReadOnlyBitmap) indexOfKey(value uint64) (uint64, bool) {
	n := int(b.keyN)
	idx := sort.Search(n, func(i int) bool {
		return b.keyAtIndex(i) >= value
	})
	if idx < n && b.keyAtIndex(idx) == value {
		return uint64(idx), true
	}
	return 0, false
}

var _ postings.Iterator = (*readOnlyBitmapIterator)(nil)

type readOnlyBitmapIterator struct {
	b                  *ReadOnlyBitmap
	err                error
	containerIndex     int
	containerExhausted bool
	container          readOnlyContainer
	containerState     readOnlyBitmapIteratorContainerState
	currValue          uint64
}

type readOnlyBitmapIteratorContainerState struct {
	entryIndex       int
	bitmap           []uint64
	bitmapCurr       uint64
	bitmapCurrBase   uint64
	bitmapCurrShifts uint64
	array            []uint16
	runs             []interval16
	runsCurr         interval16
	runsIndex        uint64
}

func newReadOnlyBitmapIterator(
	b *ReadOnlyBitmap,
) *readOnlyBitmapIterator {
	return &readOnlyBitmapIterator{
		b:                  b,
		containerIndex:     -1,
		containerExhausted: true,
	}
}

func (i *readOnlyBitmapIterator) setContainer(c readOnlyContainer) {
	i.container = c

	i.containerState.entryIndex = -1

	bitmap, _ := c.bitmap()
	i.containerState.bitmap = bitmap.values
	i.containerState.bitmapCurr = 0
	i.containerState.bitmapCurrBase = 0
	i.containerState.bitmapCurrShifts = 0

	array, _ := c.array()
	i.containerState.array = array.values

	runs, _ := c.runs()
	i.containerState.runs = runs.values
	i.containerState.runsCurr = interval16{}
	i.containerState.runsIndex = math.MaxUint64
}

func (i *readOnlyBitmapIterator) Next() bool {
	if i.err != nil || i.containerIndex >= int(i.b.keyN) {
		// Already exhausted.
		return false
	}

	if i.containerExhausted {
		// Container exhausted.
		i.containerIndex++
		if i.containerIndex >= int(i.b.keyN) {
			return false
		}

		container, err := i.b.containerAtIndex(uint64(i.containerIndex))
		if err != nil {
			i.err = err
			return false
		}

		i.containerExhausted = false
		i.setContainer(container)
	}

	if i.container.containerType == containerBitmap {
		// Bitmap container.
		for i.containerState.bitmapCurr == 0 {
			// All zero bits, progress to next uint64.
			i.containerState.entryIndex++
			if i.containerState.entryIndex >= len(i.containerState.bitmap) {
				// Move to next container.
				i.containerExhausted = true
				return i.Next()
			}

			i.containerState.bitmapCurr = i.containerState.bitmap[i.containerState.entryIndex]
			i.containerState.bitmapCurrBase = uint64(64 * i.containerState.entryIndex)
			i.containerState.bitmapCurrShifts = 0
		}

		// Non-zero bitmap uint64, work out next bit set and add together with
		// base and current shifts made within this bitmap.
		firstBitSet := uint64(bits.TrailingZeros64(i.containerState.bitmapCurr))
		bitmapValue := i.containerState.bitmapCurrBase +
			i.containerState.bitmapCurrShifts +
			firstBitSet

		// Now shift for the next value.
		shifts := firstBitSet + 1
		i.containerState.bitmapCurr = i.containerState.bitmapCurr >> shifts
		i.containerState.bitmapCurrShifts += shifts

		i.currValue = i.container.key<<16 | bitmapValue
		return true
	}

	if i.container.containerType == containerArray {
		// Array container.
		i.containerState.entryIndex++
		idx := i.containerState.entryIndex
		if idx >= len(i.containerState.array) {
			// Move to next container.
			i.containerExhausted = true
			return i.Next()
		}
		i.currValue = i.container.key<<16 | uint64(i.containerState.array[idx])
		return true
	}

	if i.container.containerType == containerRun {
		// Run container.
		if i.containerState.runsIndex > uint64(i.containerState.runsCurr.last) {
			// No more values left in the run, progress to next run.
			i.containerState.entryIndex++
			idx := i.containerState.entryIndex
			if idx >= len(i.containerState.runs) {
				// Move to next container.
				i.containerExhausted = true
				return i.Next()
			}

			i.containerState.runsCurr = i.containerState.runs[i.containerState.entryIndex]
			i.containerState.runsIndex = uint64(i.containerState.runsCurr.start)
		}

		runValue := i.containerState.runsIndex
		i.containerState.runsIndex++

		i.currValue = i.container.key<<16 | runValue
		return true
	}

	i.containerExhausted = true
	return false
}

func (i *readOnlyBitmapIterator) Current() postings.ID {
	return postings.ID(i.currValue)
}

func (i *readOnlyBitmapIterator) Err() error {
	return nil
}

func (i *readOnlyBitmapIterator) Close() error {
	return nil
}

var _ containerIterator = (*readOnlyBitmapContainerIterator)(nil)

type readOnlyBitmapContainerIterator struct {
	b              *ReadOnlyBitmap
	err            error
	containerIndex int
	container      readOnlyContainer
}

func newReadOnlyBitmapContainerIterator(
	b *ReadOnlyBitmap,
) *readOnlyBitmapContainerIterator {
	return &readOnlyBitmapContainerIterator{
		b:              b,
		containerIndex: -1,
	}
}

func (i *readOnlyBitmapContainerIterator) NextContainer() bool {
	if i.err != nil && i.containerIndex >= int(i.b.keyN) {
		return false
	}

	i.containerIndex++
	if i.containerIndex >= int(i.b.keyN) {
		return false
	}

	container, err := i.b.containerAtIndex(uint64(i.containerIndex))
	if err != nil {
		i.err = err
		return false
	}

	i.container = container
	return true
}

func (i *readOnlyBitmapContainerIterator) ContainerKey() uint64 {
	return i.container.key
}

func (i *readOnlyBitmapContainerIterator) ContainerUnion(
	ctx containerOpContext,
	target *bitmapContainer,
) {
	if bitmap, ok := i.container.bitmap(); ok {
		unionBitmapInPlace(target.bitmap, bitmap.values)
		return
	}

	if array, ok := i.container.array(); ok {
		// Blindly set array values.
		for _, v := range array.values {
			target.bitmap[v>>6] |= (uint64(1) << (v % 64))
		}
		return
	}

	if runs, ok := i.container.runs(); ok {
		// Blindly set run ranges.
		for i := 0; i < len(runs.values); i++ {
			bitmapSetRange(target.bitmap,
				uint64(runs.values[i].start), uint64(runs.values[i].last)+1)
		}
		return
	}
}

func (i *readOnlyBitmapContainerIterator) ContainerIntersect(
	ctx containerOpContext,
	target *bitmapContainer,
) {
	if bitmap, ok := i.container.bitmap(); ok {
		intersectBitmapInPlace(target.bitmap, bitmap.values)
		return
	}

	if array, ok := i.container.array(); ok {
		// Set temp bitmap with the array values then intersect.
		ctx.tempBitmap.Reset(false)
		for _, v := range array.values {
			ctx.tempBitmap.bitmap[v>>6] |= (uint64(1) << (v % 64))
		}

		intersectBitmapInPlace(target.bitmap, ctx.tempBitmap.bitmap)
		return
	}

	if runs, ok := i.container.runs(); ok {
		// Set temp bitmap with the ranges then intersect with temp.
		ctx.tempBitmap.Reset(false)
		for i := 0; i < len(runs.values); i++ {
			bitmapSetRange(ctx.tempBitmap.bitmap,
				uint64(runs.values[i].start), uint64(runs.values[i].last)+1)
		}

		intersectBitmapInPlace(target.bitmap, ctx.tempBitmap.bitmap)
		return
	}
}

func (i *readOnlyBitmapContainerIterator) ContainerNegate(
	ctx containerOpContext,
	target *bitmapContainer,
) {
	if bitmap, ok := i.container.bitmap(); ok {
		differenceBitmapInPlace(target.bitmap, bitmap.values)
		return
	}

	if array, ok := i.container.array(); ok {
		// Set temp bitmap with the array values then intersect.
		ctx.tempBitmap.Reset(false)
		for _, v := range array.values {
			ctx.tempBitmap.bitmap[v>>6] |= (uint64(1) << (v % 64))
		}

		differenceBitmapInPlace(target.bitmap, ctx.tempBitmap.bitmap)
		return
	}

	if runs, ok := i.container.runs(); ok {
		// Set temp bitmap with the ranges then intersect with temp.
		ctx.tempBitmap.Reset(false)
		for i := 0; i < len(runs.values); i++ {
			bitmapSetRange(ctx.tempBitmap.bitmap,
				uint64(runs.values[i].start), uint64(runs.values[i].last)+1)
		}

		differenceBitmapInPlace(target.bitmap, ctx.tempBitmap.bitmap)
		return
	}
}

func (i *readOnlyBitmapContainerIterator) Err() error {
	return i.err
}

func (i *readOnlyBitmapContainerIterator) Close() {
}

// bitmapSetRange sets all bits in [i, j) the same as pilosa's
// bitmapSetRangeIgnoreN.
// pilosa license is included as part of vendor code install.
func bitmapSetRange(bitmap []uint64, i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))

	if x == y {
		bitmap[x] |= (X & Y)
	} else {
		bitmap[x] |= X
		for i := x + 1; i < y; i++ {
			bitmap[i] = maxBitmap
		}
		bitmap[y] |= Y
	}
}

// bitmapContains returns if bitmap includes element the same as pilosa's
// bitmapContains.
// pilosa license is included as part of vendor code install.
func bitmapContains(bitmap []uint64, v uint16) bool {
	return (bitmap[v/64] & (1 << uint64(v%64))) != 0
}

func unionBitmapInPlace(a, b []uint64) {
	// Below is similar to pilosa's unionBitmapInPlace.
	// pilosa license is included as part of vendor code install.
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html
	var (
		ab = a[:bitmapN]
		bb = b[:bitmapN]
	)

	// Manually unroll loop to make it a little faster.
	for i := 0; i < bitmapN; i += 4 {
		ab[i] |= bb[i]
		ab[i+1] |= bb[i+1]
		ab[i+2] |= bb[i+2]
		ab[i+3] |= bb[i+3]
	}
}

func intersectBitmapInPlace(a, b []uint64) {
	// Below is similar to pilosa's unionBitmapInPlace.
	// pilosa license is included as part of vendor code install.
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html
	var (
		ab = a[:bitmapN]
		bb = b[:bitmapN]
	)

	// Manually unroll loop to make it a little faster.
	for i := 0; i < bitmapN; i += 4 {
		ab[i] &= bb[i]
		ab[i+1] &= bb[i+1]
		ab[i+2] &= bb[i+2]
		ab[i+3] &= bb[i+3]
	}
}

func differenceBitmapInPlace(a, b []uint64) {
	// Below is similar to pilosa's unionBitmapInPlace.
	// pilosa license is included as part of vendor code install.
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html
	var (
		ab = a[:bitmapN]
		bb = b[:bitmapN]
	)

	// Manually unroll loop to make it a little faster.
	for i := 0; i < bitmapN; i += 4 {
		ab[i] &= (^bb[i])
		ab[i+1] &= (^bb[i+1])
		ab[i+2] &= (^bb[i+2])
		ab[i+3] &= (^bb[i+3])
	}
}
