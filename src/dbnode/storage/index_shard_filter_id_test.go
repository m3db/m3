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

package storage

import (
	"math/rand"
	"testing"

	"github.com/m3db/bitset"
)

func BenchmarkShardFilterID(b *testing.B) {
	shards := 8192
	shardsActive := int(0.2 * float64(shards))
	nativeMap := make(map[uint32]struct{}, shardsActive)
	nativeBitset := bitset.NewBitSet(uint(shards))

	// Use a random with a fixed seed for repeatable results
	rnd := rand.NewSource(4242)
	for i := 0; i < shards; i++ {
		shardActive := uint32(rnd.Int63() % int64(shards))
		nativeMap[shardActive] = struct{}{}
		nativeBitset.Set(uint(shardActive))
	}

	// Use a random with a fixed seed for repeatable results
	rnd = rand.NewSource(2424)
	accesses := make([]uint32, shards)
	numAccesses := len(accesses)
	for i := 0; i < numAccesses; i++ {
		accesses[i] = uint32(rnd.Int63() % int64(shards))
	}

	b.Run("benchmark lookups for shards in native map", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = nativeMap[accesses[i%numAccesses]]
		}
	})
	b.Run("benchmark lookups for shards in bitset", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = nativeBitset.Test(uint(accesses[i%numAccesses]))
		}
	})
}
