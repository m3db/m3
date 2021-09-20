// Copyright (c) 2018 Uber Technologies, Inc.
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

package bitset

import (
	"github.com/willf/bitset"
)

// BitSet is a specialized bitset that does not perform heap allocations if the
// range of values in the bitset is small (e.g., [0, 64)). It falls back to
// github.com/willf/bitset for large value ranges.
type BitSet struct {
	bs         *bitset.BitSet
	val        uint64
	smallRange bool
}

// New can be used to keep track of values between [0, maxExclusive).
// Calling APIs with values outside that range may lead to incorrect results.
func New(maxExclusive uint) BitSet {
	if maxExclusive <= 64 {
		return BitSet{smallRange: true}
	}
	return BitSet{bs: bitset.New(maxExclusive)}
}

// Set sets the bit associated with the given value.
func (bs *BitSet) Set(i uint) {
	if bs.smallRange {
		bs.val = bs.val | (1 << i)
		return
	}
	bs.bs.Set(i)
}

// All returns true if all the bits for values between [0, maxExclusive)
// are set.
func (bs *BitSet) All(maxExclusive uint) bool {
	if bs.smallRange {
		mask := uint64((1 << maxExclusive) - 1)
		return (bs.val & mask) == mask
	}
	for i := uint(0); i < maxExclusive; i++ {
		if !bs.bs.Test(i) {
			return false
		}
	}
	return true
}
