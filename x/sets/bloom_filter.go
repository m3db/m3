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
	"math"

	xmurmur3 "github.com/m3db/m3db/x/murmur3"
)

// BloomFilter is a bloom filter set membership.
type BloomFilter struct {
	m   uint64
	k   uint64
	set *BitSet
}

// NewBloomFilter creates a new bloom filter that can represent
// m elements with k hashes.
func NewBloomFilter(m uint, k uint) *BloomFilter {
	if m < 1 {
		m = 1
	}
	if k < 1 {
		k = 1
	}
	return &BloomFilter{
		m:   uint64(m),
		k:   uint64(k),
		set: NewBitSet(m),
	}
}

// BloomFilterEstimate estimates m and k, based on:
// https://stackoverflow.com/a/22467497
func BloomFilterEstimate(n uint, p float64) (m uint, k uint) {
	floatM := (float64(-1) * float64(n) * math.Log(p)) / (math.Pow(math.Log(2), 2))
	floatK := (floatM / float64(n)) * math.Log(2)
	m, k = uint(math.Ceil(floatM)), uint(math.Ceil(floatK))
	return
}

func bloomFilterHashes(data []byte) [4]uint64 {
	hash := xmurmur3.New128()
	hash = hash.Write(data)
	h1, h2 := hash.Sum128()
	// Add more data
	hash = hash.Write([]byte{1})
	h3, h4 := hash.Sum128()
	return [4]uint64{h1, h2, h3, h4}
}

func bloomFilterLocation(h [4]uint64, i, m uint64) uint {
	v := h[i%2] + i*h[2+(((i+(i%2))%4)/2)]
	return uint(v % m)
}

// Add value to the set.
func (b *BloomFilter) Add(value []byte) {
	h := bloomFilterHashes(value)
	for i := uint64(0); i < b.k; i++ {
		b.set.Set(bloomFilterLocation(h, i, b.m))
	}
}

// Test if value is in the set.
func (b *BloomFilter) Test(value []byte) bool {
	h := bloomFilterHashes(value)
	for i := uint64(0); i < b.k; i++ {
		if !b.set.Test(bloomFilterLocation(h, i, b.m)) {
			return false
		}
	}
	return true
}

// M returns the m elements represented.
func (b *BloomFilter) M() uint {
	return uint(b.m)
}

// K returns the k hashes used.
func (b *BloomFilter) K() uint {
	return uint(b.k)
}

// BitSet returns the bitset used.
func (b *BloomFilter) BitSet() *BitSet {
	return b.set
}

// ReadOnlyBloomFilter is a read only bloom filter set membership.
type ReadOnlyBloomFilter struct {
	m   uint64
	k   uint64
	set *ReadOnlyBitSet
}

// NewReadOnlyBloomFilter returns a new read only bloom filter backed
// by a byte slice, this means it can be used with a mmap'd bytes ref.
func NewReadOnlyBloomFilter(m, k uint, data []byte) *ReadOnlyBloomFilter {
	return &ReadOnlyBloomFilter{
		m:   uint64(m),
		k:   uint64(k),
		set: NewReadOnlyBitSet(data),
	}
}

// Test if value is in the set.
func (b *ReadOnlyBloomFilter) Test(value []byte) bool {
	h := bloomFilterHashes(value)
	for i := uint64(0); i < b.k; i++ {
		if !b.set.Test(bloomFilterLocation(h, i, b.m)) {
			return false
		}
	}
	return true
}

// M returns the m elements represented.
func (b *ReadOnlyBloomFilter) M() uint {
	return uint(b.m)
}

// K returns the k hashes used.
func (b *ReadOnlyBloomFilter) K() uint {
	return uint(b.k)
}

// BitSet returns the bitset used.
func (b *ReadOnlyBloomFilter) BitSet() *ReadOnlyBitSet {
	return b.set
}
