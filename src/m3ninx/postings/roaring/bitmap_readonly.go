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

var (
	errNotPilosaRoaring = errors.New("not pilosa roaring format")

	headerBaseSize     = uint64(8)
	magicNumber        = uint32(12348)
	storageVersion     = uint32(0)
	bitmapN            = 1024
	runCountHeaderSize = uint32(2)
)

type containerType byte

const (
	containerUnknown containerType = iota
	containerArray
	containerBitmap
	containerRun
)

var _ postings.List = (*ReadOnlyBitmap)(nil)

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

func (b *ReadOnlyBitmap) Reset(data []byte) error {
	if len(data) == 0 {
		// Reset to nil
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
	keyN := uint64(binary.LittleEndian.Uint32(data[4:8]))

	minBytesN := headerBaseSize + keyN*12 + keyN*4
	if uint64(len(data)) < minBytesN {
		return fmt.Errorf("bitmap too small: need=%d, actual=%d",
			minBytesN, len(data))
	}

	b.data = data
	b.keyN = keyN
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
	return b.containerAtIndex(index), true
}

func (b *ReadOnlyBitmap) containerAtIndex(index uint64) readOnlyContainer {
	meta := b.data[headerBaseSize+index*12:]
	offsets := b.data[headerBaseSize+b.keyN*12+index*4:]
	return readOnlyContainer{
		data:          b.data,
		key:           b.keyAtIndex(int(index)),
		containerType: containerType(binary.LittleEndian.Uint16(meta[8:10])),
		cardinality:   uint16(binary.LittleEndian.Uint16(meta[10:12])) + 1,
		offset:        binary.LittleEndian.Uint32(offsets[0:4]),
	}
}

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

func (b *ReadOnlyBitmap) IsEmpty() bool {
	return b.keyN == 0
}

func (b *ReadOnlyBitmap) Len() int {
	l := 0
	for i := uint64(0); i < b.keyN; i++ {
		l += int(b.containerAtIndex(i).cardinality)
	}
	return l
}

func (b *ReadOnlyBitmap) Iterator() postings.Iterator {
	return newReadOnlyBitmapIterator(b)
}

func (b *ReadOnlyBitmap) Equal(other postings.List) bool {
	if b.Len() != other.Len() {
		return false
	}
	iter := b.Iterator()
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

func highbits(v uint64) uint64 { return v >> 16 }
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }

var _ postings.Iterator = (*readOnlyBitmapIterator)(nil)

type readOnlyBitmapIterator struct {
	b                  *ReadOnlyBitmap
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
	if i.containerIndex >= int(i.b.keyN) {
		// Already exhausted.
		return false
	}

	if i.containerExhausted {
		// Container exhausted.
		i.containerIndex++
		if i.containerIndex >= int(i.b.keyN) {
			return false
		}
		i.containerExhausted = false
		i.setContainer(i.b.containerAtIndex(uint64(i.containerIndex)))
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
