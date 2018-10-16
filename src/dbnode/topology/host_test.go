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

package topology

import (
	"testing"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/assert"
)

func TestNewHostShardSetFromServiceInstance(t *testing.T) {
	i1 := services.NewServiceInstance().
		SetInstanceID("h1").
		SetEndpoint("h1:9000").
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(1),
			shard.NewShard(2),
			shard.NewShard(3),
		}))
	hash := sharding.DefaultHashFn(3)
	host, err := NewHostShardSetFromServiceInstance(i1, hash)
	assert.NoError(t, err)
	assert.Equal(t, "h1:9000", host.Host().Address())
	assert.Equal(t, "h1", host.Host().ID())
	assert.Equal(t, 3, len(host.ShardSet().AllIDs()))
	assert.Equal(t, uint32(1), host.ShardSet().Min())
	assert.Equal(t, uint32(3), host.ShardSet().Max())

	id := ident.StringID("id")
	assert.Equal(t, host.ShardSet().Lookup(id), hash(id))
}
