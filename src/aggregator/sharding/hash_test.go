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
	"sync"
	"testing"

	"github.com/m3db/m3metrics/metric/id"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestHashTypeUnmarshalYAML(t *testing.T) {
	inputs := []struct {
		str      string
		expected HashType
	}{
		{str: "murmur32", expected: Murmur32Hash},
	}
	for _, input := range inputs {
		var hashType HashType
		require.NoError(t, yaml.Unmarshal([]byte(input.str), &hashType))
		require.Equal(t, input.expected, hashType)
	}
}

func TestHashTypeUnmarshalYAMLErrors(t *testing.T) {
	inputs := []string{
		"huh",
		"zero",
	}
	for _, input := range inputs {
		var hashType HashType
		err := yaml.Unmarshal([]byte(input), &hashType)
		require.Error(t, err)
		require.Equal(t, "invalid hash type '"+input+"' valid types are: murmur32", err.Error())
	}
}

func TestMurmur32HashShardFn(t *testing.T) {
	hashType := Murmur32Hash
	numShards := 1024
	shardFn, err := hashType.ShardFn()
	require.NoError(t, err)

	inputs := []struct {
		data     []byte
		expected uint32
	}{
		{data: []byte("foo"), expected: 32},
		{data: []byte("bar"), expected: 397},
		{data: []byte("baz"), expected: 234},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, shardFn(input.data, numShards))
	}
}

func TestMurmur32HashAggregatedShardFn(t *testing.T) {
	hashType := Murmur32Hash
	numShards := 1024
	aggregatedShardFn, err := hashType.AggregatedShardFn()
	require.NoError(t, err)

	// Verify the aggregated shard function is thread-safe and the computed
	// shards match expectation.
	var wg sync.WaitGroup
	numWorkers := 100
	inputs := []struct {
		data     id.ChunkedID
		expected uint32
	}{
		{
			data:     id.ChunkedID{Prefix: []byte(""), Data: []byte("bar"), Suffix: []byte("")},
			expected: 397,
		},
		{
			data:     id.ChunkedID{Prefix: []byte("foo"), Data: []byte("bar"), Suffix: []byte("")},
			expected: 189,
		},
		{
			data:     id.ChunkedID{Prefix: []byte(""), Data: []byte("bar"), Suffix: []byte("baz")},
			expected: 350,
		},
		{
			data:     id.ChunkedID{Prefix: []byte("foo"), Data: []byte("bar"), Suffix: []byte("baz")},
			expected: 950,
		},
	}
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for _, input := range inputs {
				require.Equal(t, input.expected, aggregatedShardFn(input.data, numShards))
			}
		}()
	}
	wg.Wait()
}

func TestZeroHashAggregatedShardFn(t *testing.T) {
	hashType := zeroHash
	numShards := 1024
	fn, err := hashType.AggregatedShardFn()
	require.NoError(t, err)

	inputs := []id.ChunkedID{
		{Prefix: []byte(""), Data: []byte("bar"), Suffix: []byte("")},
		{Prefix: []byte("foo"), Data: []byte("bar"), Suffix: []byte("")},
		{Prefix: []byte(""), Data: []byte("bar"), Suffix: []byte("baz")},
		{Prefix: []byte("foo"), Data: []byte("bar"), Suffix: []byte("baz")},
	}
	for _, input := range inputs {
		require.Equal(t, uint32(0), fn(input, numShards))
	}
}
