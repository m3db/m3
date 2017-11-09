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

package xsets

import (
	"bytes"
	"encoding/binary"
	"io"
)

const (
	wordSize    = uint(64)
	logWordSize = uint(6) // lg(wordSize)
	wordMask    = wordSize - 1
)

// BitSet is a bitset set membership.
type BitSet struct {
	values []uint64
}

// NewBitSet returns a new bitset that can represent
// a set of n elements exactly.
func NewBitSet(n uint) *BitSet {
	return &BitSet{values: make([]uint64, bitSetIndexOf(n)+1)}
}

// Test returns true if i is within the bit set or false otherwise.
func (b *BitSet) Test(i uint) bool {
	idx := bitSetIndexOf(i)
	if idx >= len(b.values) {
		return false
	}
	return b.values[idx]&(1<<(i&wordMask)) != 0
}

// Set adds i to the membership of the set.
func (b *BitSet) Set(i uint) {
	idx := bitSetIndexOf(i)
	currLen := len(b.values)
	if idx >= currLen {
		newValues := make([]uint64, 2*(idx+1))
		copy(newValues, b.values)
		b.values = newValues
	}
	b.values[idx] |= 1 << (i & wordMask)
}

// ClearAll clears the set.
func (b *BitSet) ClearAll() {
	for i := range b.values {
		b.values[i] = 0
	}
}

// Write writes the bitset values to a stream.
func (b *BitSet) Write(w io.Writer) error {
	var buf [8]byte
	for _, v := range b.values {
		binary.LittleEndian.PutUint64(buf[:], v)
		_, err := w.Write(buf[:])
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadOnly returns a read only bit set created by writing
// the bit set to a buffer.
func (b *BitSet) ReadOnly() (*ReadOnlyBitSet, error) {
	buf := bytes.NewBuffer(make([]byte, 8*len(b.values)))
	buf.Reset()
	if err := b.Write(buf); err != nil {
		return nil, err
	}
	return NewReadOnlyBitSet(buf.Bytes()), nil
}

// ReadOnlyBitSet is a read only bitset set membership.
type ReadOnlyBitSet struct {
	data []byte
}

// NewReadOnlyBitSet returns a new read only bit set backed
// by a byte slice, this means it can be used with a mmap'd bytes ref.
func NewReadOnlyBitSet(data []byte) *ReadOnlyBitSet {
	return &ReadOnlyBitSet{data: data}
}

// Test returns true if i is within the bit set or false otherwise.
func (b *ReadOnlyBitSet) Test(i uint) bool {
	idx := bitSetIndexOf(i)
	values := len(b.data) / 8
	if idx >= values {
		return false
	}
	value := binary.LittleEndian.Uint64(b.data[idx*8 : (idx*8)+8])
	return value&(1<<(i&wordMask)) != 0
}

// Write writes the bitset values to a stream.
func (b *ReadOnlyBitSet) Write(w io.Writer) error {
	_, err := w.Write(b.data)
	return err
}

func bitSetIndexOf(i uint) int {
	return int(i >> logWordSize)
}
