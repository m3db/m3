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

package shard

import (
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkNewShards(b *testing.B) {
	for i := 16; i <= 4096; i *= 4 {
		rndShards := makeTestShards(i)
		b.Run(fmt.Sprintf("%d shards", i), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				res := NewShards(rndShards)
				runtime.KeepAlive(res)
			}
		})
	}
}

func BenchmarkShardsAllShards(b *testing.B) {
	for i := 16; i <= 4096; i *= 4 {
		rndShards := makeTestShards(i)
		shards := NewShards(rndShards)

		b.Run(fmt.Sprintf("%d shards", i), func(b *testing.B) {
			var res []Shard
			for i := 0; i < b.N; i++ {
				res = shards.All()
			}
			runtime.KeepAlive(res)
		})
	}
}

func BenchmarkShardsAllShardIDs(b *testing.B) {
	for i := 16; i <= 4096; i *= 4 {
		rndShards := makeTestShards(i)
		shards := NewShards(rndShards)

		b.Run(fmt.Sprintf("%d shards", i), func(b *testing.B) {
			var res []uint32
			for i := 0; i < b.N; i++ {
				res = shards.AllIDs()
			}
			runtime.KeepAlive(res)
		})
	}
}

func BenchmarkShardsAdd(b *testing.B) {
	for i := 16; i <= 4096; i *= 4 {
		rndShards := makeTestShards(i)
		b.Run(fmt.Sprintf("%d shards", i), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				res := NewShards(nil)
				for j := 0; j < len(rndShards); j++ {
					res.Add(rndShards[j])
				}
				if res.NumShards() != len(rndShards) {
					b.Fail()
				}
			}
		})
	}
}

func BenchmarkShardsEquals(b *testing.B) {
	for i := 16; i <= 4096; i *= 4 {
		rndShards := makeTestShards(i)
		shards := NewShards(rndShards)
		clone := shards.Clone()

		b.Run(fmt.Sprintf("%d shards", i), func(b *testing.B) {
			var res bool
			for i := 0; i < b.N; i++ {
				shards.Equals(clone)
			}
			runtime.KeepAlive(res)
		})
	}
}

func BenchmarkShardsNumShardsForState(b *testing.B) {
	for i := 16; i <= 4096; i *= 4 {
		rndShards := makeTestShards(i)
		shards := NewShards(rndShards)

		b.Run(fmt.Sprintf("%d shards", i), func(b *testing.B) {
			var res int
			for i := 0; i < b.N; i++ {
				res = shards.NumShardsForState(defaultShardState)
			}
			runtime.KeepAlive(res)
		})
	}
}

func BenchmarkShardsShard(b *testing.B) {
	for i := 16; i <= 4096; i *= 4 {
		ids := randomIDs(1, i*2)
		rndShards := makeTestShards(i)
		shards := NewShards(rndShards)

		b.Run(fmt.Sprintf("%d shards", i), func(b *testing.B) {
			var res Shard
			for i := 0; i < b.N; i++ {
				res, _ = shards.Shard(ids[i%len(ids)])
			}
			runtime.KeepAlive(res)
		})
	}
}

func BenchmarkShardsContains(b *testing.B) {
	for i := 16; i <= 4096; i *= 4 {
		ids := randomIDs(1, i*2)
		rndShards := makeTestShards(i)
		shards := NewShards(rndShards)

		b.Run(fmt.Sprintf("%d shards", i), func(b *testing.B) {
			var res bool
			for i := 0; i < b.N; i++ {
				res = shards.Contains(ids[i%len(ids)])
			}
			runtime.KeepAlive(res)
		})
	}
}
