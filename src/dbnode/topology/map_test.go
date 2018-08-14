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

	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestShardSet(
	t *testing.T,
	shards []uint32,
	hashFn sharding.HashFn,
) sharding.ShardSet {
	values := sharding.NewShards(shards, shard.Available)
	shardSet, err := sharding.NewShardSet(values, hashFn)
	require.NoError(t, err)
	return shardSet
}

func TestStaticMap(t *testing.T) {
	hashFn := func(id ident.ID) uint32 {
		switch id.String() {
		case "foo":
			return 0
		case "bar":
			return 1
		case "unowned":
			return 999
		default:
			return 2
		}
	}

	hosts := []struct {
		id     string
		addr   string
		shards []uint32
	}{
		{"h1", "h1:9000", []uint32{0}},
		{"h2", "h2:9000", []uint32{1}},
		{"h3", "h3:9000", []uint32{0}},
		{"h4", "h4:9000", []uint32{1}},
	}

	var hostShardSets []HostShardSet
	for _, h := range hosts {
		hostShardSets = append(hostShardSets,
			NewHostShardSet(
				NewHost(h.id, h.addr),
				newTestShardSet(t, h.shards, hashFn)))
	}

	opts := NewStaticOptions().
		SetShardSet(newTestShardSet(t, []uint32{0, 1}, hashFn)).
		SetReplicas(2).
		SetHostShardSets(hostShardSets)

	m := NewStaticMap(opts)

	require.Equal(t, 4, len(m.Hosts()))
	require.Equal(t, 4, m.HostsLen())
	for i, h := range hosts {
		assert.Equal(t, h.id, m.Hosts()[i].ID())
		assert.Equal(t, h.addr, m.Hosts()[i].Address())
	}

	require.Equal(t, 4, len(m.HostShardSets()))
	for i, h := range hosts {
		assert.Equal(t, h.id, m.HostShardSets()[i].Host().ID())
		assert.Equal(t, h.addr, m.HostShardSets()[i].Host().Address())
		assert.Equal(t, h.shards, m.HostShardSets()[i].ShardSet().AllIDs())
	}

	shard, targetHosts, err := m.Route(ident.StringID("foo"))
	require.NoError(t, err)
	assert.Equal(t, uint32(0), shard)
	require.Equal(t, 2, len(targetHosts))
	assert.Equal(t, "h1", targetHosts[0].ID())
	assert.Equal(t, "h3", targetHosts[1].ID())

	_, _, err = m.Route(ident.StringID("unowned"))
	require.Error(t, err)
	assert.Equal(t, errUnownedShard, err)

	targetHosts, err = m.RouteShard(1)
	require.NoError(t, err)
	require.Equal(t, 2, len(targetHosts))
	assert.Equal(t, "h2", targetHosts[0].ID())
	assert.Equal(t, "h4", targetHosts[1].ID())

	_, err = m.RouteShard(999)
	require.Error(t, err)
	assert.Equal(t, errUnownedShard, err)

	err = m.RouteForEach(ident.StringID("bar"), func(idx int, h Host) {
		switch idx {
		case 1:
			assert.Equal(t, "h2", h.ID())
		case 3:
			assert.Equal(t, "h4", h.ID())
		default:
			assert.Fail(t, "routed to wrong host")
		}
	})
	assert.NoError(t, err)

	err = m.RouteForEach(ident.StringID("unowned"), func(idx int, h Host) {})
	require.Error(t, err)
	assert.Equal(t, errUnownedShard, err)

	assert.Equal(t, 2, m.Replicas())
	assert.Equal(t, 2, m.MajorityReplicas())
}
