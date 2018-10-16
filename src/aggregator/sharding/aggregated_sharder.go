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

import "github.com/m3db/m3/src/metrics/metric/id"

var (
	// NoShardingSharderID is the sharder id used where no sharding is applicable.
	// It maps all inputs to a single shard.
	NoShardingSharderID = SharderID{hashType: zeroHash, numShards: 1}
)

// SharderID uniquely identifies a sharder.
type SharderID struct {
	hashType  HashType
	numShards int
}

// NewSharderID creates a new sharder id.
func NewSharderID(hashType HashType, numShards int) SharderID {
	return SharderID{hashType: hashType, numShards: numShards}
}

// NumShards returns the total number of shards.
func (sid SharderID) NumShards() int { return sid.numShards }

// AggregatedSharder maps an aggregated metric to a shard.
type AggregatedSharder struct {
	sharderID SharderID
	shardFn   AggregatedShardFn
}

// NewAggregatedSharder creates a new aggregated sharder.
func NewAggregatedSharder(sharderID SharderID) (AggregatedSharder, error) {
	shardFn, err := sharderID.hashType.AggregatedShardFn()
	if err != nil {
		return AggregatedSharder{}, err
	}
	return AggregatedSharder{
		sharderID: sharderID,
		shardFn:   shardFn,
	}, nil
}

// ID returns the sharder id.
func (s *AggregatedSharder) ID() SharderID { return s.sharderID }

// Shard maps a chunked id to a shard.
func (s *AggregatedSharder) Shard(chunkedID id.ChunkedID) uint32 {
	return s.shardFn(chunkedID, s.sharderID.numShards)
}
