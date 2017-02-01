// Copyright (c) 2016 Uber Technologies, Inc.
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

package commitlog

import (
	"math"

	bset "github.com/willf/bitset"
)

const (
	defaultBitsetLength = 65536
	growthFactor        = 2.0
)

// bitset is a shim for providing a bitset
type bitset interface {
	has(i uint64) bool
	set(i uint64)
	clear(i uint64)
	clearAll()
}

type set struct {
	*bset.BitSet
}

func newBitset() bitset {
	return &set{bset.New(defaultBitsetLength)}
}

func (s *set) has(i uint64) bool {
	return s.Test(uint(i))
}

func (s *set) set(i uint64) {
	// NB(xichen): the bitset implementation keeps track of
	// the maximum value it has seen so far and triggers a
	// reallocation if it needs to set a value larger than
	// the current max. However it only reallocates space large
	// enough to represent the new value, which means in our
	// case each reallocation only grows the internal slice in
	// the bitset implementation by 1 slot and copies the existing data
	// over, causing significant CPU overhead and GC time. Therefore
	// we explicitly controls when the reallocation happens and grows
	// the slice explicitly to amortize the allocation cost.
	if bitsetLen := s.Len(); uint(i) >= bitsetLen {
		newLength := uint(math.Max(float64(i+1), growthFactor*float64(bitsetLen)))
		newBitSet := bset.New(newLength)
		s.BitSet.Copy(newBitSet)
		s.BitSet = newBitSet
	}
	s.Set(uint(i))
}

func (s *set) clear(i uint64) {
	s.Clear(uint(i))
}

func (s *set) clearAll() {
	s.ClearAll()
}
