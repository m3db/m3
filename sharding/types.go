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

package sharding

// HashFn is a sharding hash function
type HashFn func(identifer string) uint32

// ShardScheme is a sharding scheme
type ShardScheme interface {
	// Shard will return a shard for a given identifer
	Shard(identifer string) uint32

	// CreateSet will return a new shard set from a set of shards
	CreateSet(shards []uint32) (ShardSet, error)

	// All returns a shard set representing all shards
	All() ShardSet
}

// ShardSet is a set of shards, this interface allows for potentially out of order shard sets
type ShardSet interface {
	// Shards returns a slice to the shards in this set
	Shards() []uint32

	// Scheme returns the scheme this shard set belongs to
	Scheme() ShardScheme
}
