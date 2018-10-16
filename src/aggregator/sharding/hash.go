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

package sharding

import (
	"fmt"
	"strings"

	"github.com/m3db/m3/src/metrics/metric/id"

	"github.com/spaolacci/murmur3"
)

const (
	initialChunkedIDSize = 512
)

// ShardFn maps a id to a shard.
type ShardFn func(id []byte, numShards int) uint32

// AggregatedShardFn maps a chunked id to a shard.
type AggregatedShardFn func(chunkedID id.ChunkedID, numShards int) uint32

// HashType is the hashing type.
type HashType string

// List of supported hashing types.
const (
	// Murmur32Hash represents the murmur3 hash.
	Murmur32Hash HashType = "murmur32"

	// zeroHash always returns 0 as the hash. It is used when sharding is disabled.
	zeroHash HashType = "zero"

	DefaultHash = Murmur32Hash
)

var (
	validHashTypes = []HashType{
		Murmur32Hash,
	}
)

// UnmarshalYAML unmarshals YAML object into a hash type.
func (t *HashType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		*t = DefaultHash
		return nil
	}
	validTypes := make([]string, 0, len(validHashTypes))
	for _, valid := range validHashTypes {
		if str == string(valid) {
			*t = valid
			return nil
		}
		validTypes = append(validTypes, string(valid))
	}
	return fmt.Errorf("invalid hash type '%s' valid types are: %s",
		str, strings.Join(validTypes, ", "))
}

// ShardFn returns the sharding function.
func (t HashType) ShardFn() (ShardFn, error) {
	switch t {
	case Murmur32Hash:
		return func(id []byte, numShards int) uint32 {
			return murmur3.Sum32(id) % uint32(numShards)
		}, nil
	default:
		return nil, fmt.Errorf("unrecognized hashing type %v", t)
	}
}

// MustShardFn returns the sharding function, or panics if an error is encountered.
func (t HashType) MustShardFn() ShardFn {
	fn, err := t.ShardFn()
	if err != nil {
		panic(fmt.Errorf("error creating shard fn: %v", err))
	}
	return fn
}

// AggregatedShardFn returns the sharding function for computing aggregated shards.
func (t HashType) AggregatedShardFn() (AggregatedShardFn, error) {
	switch t {
	case Murmur32Hash:
		// NB(xichen): This function only allocates when the id of the aggregated metric
		// is more than initialChunkedIDSize in size and requires zero allocation otherwise.
		// If this turns out to be still too CPU intensive due to byte copies, can rewrite
		// it to compute murmur3 hashes with zero byte copies.
		return func(chunkedID id.ChunkedID, numShards int) uint32 {
			var b [initialChunkedIDSize]byte
			buf := b[:0]
			buf = append(buf, chunkedID.Prefix...)
			buf = append(buf, chunkedID.Data...)
			buf = append(buf, chunkedID.Suffix...)
			return murmur3.Sum32(buf) % uint32(numShards)
		}, nil
	case zeroHash:
		return func(chunkedID id.ChunkedID, numShards int) uint32 {
			return 0
		}, nil
	default:
		return nil, fmt.Errorf("unrecognized hashing type %v", t)
	}
}
