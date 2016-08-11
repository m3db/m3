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

import (
	"errors"
)

var (
	// ErrToLessThanFrom returned when to is less than from
	ErrToLessThanFrom = errors.New("to is less than from")
	// ErrNoShards returned when shard set is empty
	ErrNoShards = errors.New("empty shard set")
	// ErrDuplicateShards returned when shard set is empty
	ErrDuplicateShards = errors.New("duplicate shards")
)

type shardScheme struct {
	shards []uint32
	fn     HashFn
}

// NewShardSchemeFromRange creates a new sharding scheme, from and to are inclusive
func NewShardSchemeFromRange(from, to uint32, fn HashFn) (ShardScheme, error) {
	if to < from {
		return nil, ErrToLessThanFrom
	}
	var shards []uint32
	for i := from; i <= to; i++ {
		shards = append(shards, i)
	}
	return NewShardScheme(shards, fn)
}

// NewShardScheme creates a new sharding scheme with a set of shards
func NewShardScheme(shards []uint32, fn HashFn) (ShardScheme, error) {
	if err := validateShards(shards); err != nil {
		return nil, err
	}
	return &shardScheme{shards, fn}, nil
}

func (s *shardScheme) Shard(identifer string) uint32 {
	return s.fn(identifer)
}

func (s *shardScheme) CreateSet(shards []uint32) (ShardSet, error) {
	if err := validateShards(shards); err != nil {
		return nil, err
	}
	return NewShardSet(shards, s), nil
}

func (s *shardScheme) All() ShardSet {
	return NewShardSet(s.shards, s)
}

type shardSet struct {
	shards []uint32
	scheme ShardScheme
}

// NewShardSet creates a new shard set
func NewShardSet(shards []uint32, scheme ShardScheme) ShardSet {
	return &shardSet{shards, scheme}
}

func (s *shardSet) Shards() []uint32 {
	return s.shards[:]
}

func (s *shardSet) Scheme() ShardScheme {
	return s.scheme
}

func validateShards(shards []uint32) error {
	if len(shards) <= 0 {
		return ErrNoShards
	}
	uniqueShards := make(map[uint32]struct{}, len(shards))
	for _, s := range shards {
		if _, exist := uniqueShards[s]; exist {
			return ErrDuplicateShards
		}
		uniqueShards[s] = struct{}{}
	}
	return nil
}
