package sharding

import (
	"errors"
)

// TODO(xichen): move interfaces to top-level

var (
	// ErrToLessThanFrom returned when to is less than from
	ErrToLessThanFrom = errors.New("to is less than from")
)

// HashFn is a sharding hash function
type HashFn func(identifer string) uint32

// ShardScheme is a sharding scheme
type ShardScheme interface {
	// Shard will return a shard for a given identifer
	Shard(identifer string) uint32

	// CreateSet will return a new shard set, from and to are inclusive
	CreateSet(from, to uint32) ShardSet

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

type shardScheme struct {
	from uint32
	to   uint32
	fn   HashFn
}

// NewShardScheme creates a new sharding scheme, from and to are inclusive
func NewShardScheme(from, to uint32, fn HashFn) (ShardScheme, error) {
	if to < from {
		return nil, ErrToLessThanFrom
	}
	return &shardScheme{from, to, fn}, nil
}

func (s *shardScheme) Shard(identifer string) uint32 {
	return s.fn(identifer)
}

func (s *shardScheme) CreateSet(from, to uint32) ShardSet {
	var shards []uint32
	for i := from; i >= s.from && i <= s.to && i <= to; i++ {
		shards = append(shards, i)
	}
	return NewShardSet(shards, s)
}

func (s *shardScheme) All() ShardSet {
	return s.CreateSet(s.from, s.to)
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
