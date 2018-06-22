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

package handler

import (
	"testing"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestFlushHandlerConfigurationValidate(t *testing.T) {
	var cfg flushHandlerConfiguration

	neitherConfigured := ``
	require.NoError(t, yaml.Unmarshal([]byte(neitherConfigured), &cfg))
	err := cfg.Validate()
	require.Error(t, err)
	require.Equal(t, errNoDynamicOrStaticBackendConfiguration, err)

	bothConfigured := `
staticBackend:
  type: blackhole
dynamicBackend:
  name: test
`
	require.NoError(t, yaml.Unmarshal([]byte(bothConfigured), &cfg))
	err = cfg.Validate()
	require.Error(t, err)
	require.Equal(t, errBothDynamicAndStaticBackendConfiguration, err)
}

func TestBackendConfigurationValidate(t *testing.T) {
	nonSharded := `
name: backend1
servers:
  - server1
  - server2
`

	sharded := `
name: backend2
sharded:
  hashType: murmur32
  totalShards: 128
  shards:
    - name: shard0
      shardSet: 0..63
      servers:
        - server1
        - server2
    - name: shard1
      shardSet: 64..127
      servers:
        - server3
        - server4
`

	for _, input := range []string{nonSharded, sharded} {
		var cfg staticBackendConfiguration
		require.NoError(t, yaml.Unmarshal([]byte(input), &cfg))
		require.NoError(t, cfg.Validate())
	}
}

func TestBackendConfigurationValidateErrors(t *testing.T) {
	bothShardedAndNonSharded := `
name: backend1
servers: [foo-1, foo-2, foo-3, foo-1]
sharded:
  hashType: murmur32
  totalShards: 128
  shards:
    - name: shard0
      shardSet: 0..63
      servers:
        - server1
        - server2
    - name: shard1
      shardSet: 64..127
      servers:
        - server3
        - server4
`

	neitherShardedNorNonSharded := `
name: backend1
`

	nonShardedMultipleServers := `
name: backend1
servers: [foo-1, foo-2, foo-3, foo-1]
`

	shardedShardOverlap := `
name: backend1
sharded:
  hashType: murmur32
  totalShards: 128
  shards:
    - name: shard0
      shardSet: 0..63
      servers:
        - server1
        - server2
    - name: shard1
      shardSet: 63..127
      servers:
        - server3
        - server4
`

	shardedExceedsTotal := `
name: backend1
sharded:
  hashType: murmur32
  totalShards: 128
  shards:
    - name: shard0
      shardSet: 0..63
      servers:
        - server1
        - server2
    - name: shard1
      shardSet: 64..128
      servers:
        - server3
        - server4
`

	shardedServerOverlap := `
name: backend1
sharded:
  hashType: murmur32
  totalShards: 128
  shards:
    - name: shard0
      shardSet: 0..63
      servers:
        - server1
        - server2
    - name: shard1
      shardSet: 64..127
      servers:
        - server1
        - server4
`

	shardedInsufficientShards := `
name: backend1
sharded:
  hashType: murmur32
  totalShards: 128
  shards:
    - name: shard0
      shardSet: 0..63
      servers:
        - server1
        - server2
`

	tests := []struct {
		config      string
		expectedErr string
	}{
		{bothShardedAndNonSharded, "backend backend1 configuration has both servers and shards"},
		{neitherShardedNorNonSharded, "backend backend1 configuration has neither servers no shards"},
		{nonShardedMultipleServers, "server foo-1 specified more than once"},
		{shardedShardOverlap, "shard 63 is present in multiple ranges"},
		{shardedExceedsTotal, "shard 128 exceeds total available shards 128"},
		{shardedServerOverlap, "server server1 is present in multiple ranges"},
		{shardedInsufficientShards, "missing shards; expected 128 total received 64"},
	}

	for _, test := range tests {
		var cfg staticBackendConfiguration
		require.NoError(t, yaml.Unmarshal([]byte(test.config), &cfg), "invalid config %s", test.config)

		err := cfg.Validate()
		require.Error(t, err)
		require.Equal(t, test.expectedErr, err.Error())
	}
}
