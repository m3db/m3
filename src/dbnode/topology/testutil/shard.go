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

package testutil

import (
	"github.com/m3db/m3/src/cluster/shard"
)

// Uint32Range returns a slice of all values between [from, to].
func Uint32Range(from, to uint32) []uint32 {
	var ids []uint32
	for i := from; i <= to; i++ {
		ids = append(ids, i)
	}
	return ids
}

// Shards returns a slice of shards with provided ids and state `s`.
func Shards(ids []uint32, s shard.State) []shard.Shard {
	var shards []shard.Shard
	for _, id := range ids {
		shards = append(shards, shard.NewShard(id).SetState(s))
	}
	return shards
}

// ShardsRange returns a slice of shards for all ids between [from, to],
// with shard state `s`.
func ShardsRange(from, to uint32, s shard.State) []shard.Shard {
	return Shards(Uint32Range(from, to), s)
}
