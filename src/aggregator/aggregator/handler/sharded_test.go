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

	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3aggregator/aggregator/handler/writer"
	"github.com/m3db/m3aggregator/sharding"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestNewShardedHandlerOneNonSharded(t *testing.T) {
	router := &mockRouter{id: 1}
	srs := []SharderRouter{
		{
			SharderID: sharding.NoShardingSharderID,
			Router:    router,
		},
	}
	handler := NewShardedHandler(srs, nil).(*shardedHandler)
	expected := []SharderRouter{
		{
			SharderID: sharding.NoShardingSharderID,
			Router:    router,
		},
	}
	require.Equal(t, expected, handler.routersBySharderID)
}

func TestNewShardedHandlerMultipleNonSharded(t *testing.T) {
	routers := []common.Router{&mockRouter{id: 1}, &mockRouter{id: 2}}
	srs := []SharderRouter{
		{
			SharderID: sharding.NoShardingSharderID,
			Router:    routers[0],
		},
		{
			SharderID: sharding.NoShardingSharderID,
			Router:    routers[1],
		},
	}
	handler := NewShardedHandler(srs, nil).(*shardedHandler)
	expected := []SharderRouter{
		{
			SharderID: sharding.NoShardingSharderID,
			Router:    common.NewBroadcastRouter(routers),
		},
	}
	require.Equal(t, expected, handler.routersBySharderID)
}

func TestNewShardedHandlerOneSharded(t *testing.T) {
	router := &mockRouter{id: 1}
	srs := []SharderRouter{
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 1024),
			Router:    router,
		},
	}
	handler := NewShardedHandler(srs, nil).(*shardedHandler)
	expected := []SharderRouter{
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 1024),
			Router:    router,
		},
	}
	require.Equal(t, expected, handler.routersBySharderID)
}

func TestNewShardedHandlerMultipleShardedSameSharderID(t *testing.T) {
	routers := []common.Router{&mockRouter{id: 1}, &mockRouter{id: 2}}
	sharderID := sharding.NewSharderID(sharding.Murmur32Hash, 1024)
	srs := []SharderRouter{
		{
			SharderID: sharderID,
			Router:    routers[0],
		},
		{
			SharderID: sharderID,
			Router:    routers[1],
		},
	}
	handler := NewShardedHandler(srs, nil).(*shardedHandler)
	expected := []SharderRouter{
		{
			SharderID: sharderID,
			Router:    common.NewBroadcastRouter(routers),
		},
	}
	require.Equal(t, expected, handler.routersBySharderID)
}

func TestNewShardedHandlerMultipleShardedDifferentSharderIDs(t *testing.T) {
	routers := []common.Router{&mockRouter{id: 1}, &mockRouter{id: 2}, &mockRouter{id: 3}}
	srs := []SharderRouter{
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 1024),
			Router:    routers[0],
		},
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 128),
			Router:    routers[1],
		},
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 1024),
			Router:    routers[2],
		},
	}
	handler := NewShardedHandler(srs, nil).(*shardedHandler)
	expected := []SharderRouter{
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 1024),
			Router:    common.NewBroadcastRouter([]common.Router{routers[0], routers[2]}),
		},
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 128),
			Router:    routers[1],
		},
	}
	require.Equal(t, expected, handler.routersBySharderID)
}

func TestNewShardedHandlerBothShardedAndNonSharded(t *testing.T) {
	routers := []common.Router{
		&mockRouter{id: 1},
		&mockRouter{id: 2},
		&mockRouter{id: 3},
		&mockRouter{id: 4},
		&mockRouter{id: 5},
	}
	srs := []SharderRouter{
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 1024),
			Router:    routers[0],
		},
		{
			SharderID: sharding.NoShardingSharderID,
			Router:    routers[1],
		},
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 128),
			Router:    routers[2],
		},
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 1024),
			Router:    routers[3],
		},
		{
			SharderID: sharding.NoShardingSharderID,
			Router:    routers[4],
		},
	}
	handler := NewShardedHandler(srs, nil).(*shardedHandler)
	expected := []SharderRouter{
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 1024),
			Router:    common.NewBroadcastRouter([]common.Router{routers[0], routers[3], routers[1], routers[4]}),
		},
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 128),
			Router:    routers[2],
		},
	}
	require.Equal(t, expected, handler.routersBySharderID)
}

func TestShardedHandlerNewWriterSingleSharder(t *testing.T) {
	sharderID := sharding.NewSharderID(sharding.Murmur32Hash, 1024)
	router := &mockRouter{id: 1}
	opts := writer.NewOptions()
	handler := NewShardedHandler(nil, opts).(*shardedHandler)
	handler.routersBySharderID = []SharderRouter{
		{
			SharderID: sharderID,
			Router:    router,
		},
	}
	_, err := handler.NewWriter(tally.NoopScope)
	require.NoError(t, err)
}

func TestShardedHandlerNewWriterMultipleSharder(t *testing.T) {
	routers := []common.Router{
		&mockRouter{id: 1},
		&mockRouter{id: 2},
	}
	opts := writer.NewOptions()
	handler := NewShardedHandler(nil, opts).(*shardedHandler)
	handler.routersBySharderID = []SharderRouter{
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 1024),
			Router:    routers[0],
		},
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 128),
			Router:    routers[1],
		},
	}
	_, err := handler.NewWriter(tally.NoopScope)
	require.NoError(t, err)
}

func TestShardedHandlerNewWriterError(t *testing.T) {
	routers := []common.Router{
		&mockRouter{id: 1},
		&mockRouter{id: 2},
	}
	opts := writer.NewOptions()
	handler := NewShardedHandler(nil, opts).(*shardedHandler)
	handler.routersBySharderID = []SharderRouter{
		{
			SharderID: sharding.NewSharderID(sharding.HashType("abcd"), 1024),
			Router:    routers[0],
		},
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 128),
			Router:    routers[1],
		},
	}
	_, err := handler.NewWriter(tally.NoopScope)
	require.Error(t, err)
}

type routeFn func(shard uint32, buf *common.RefCountedBuffer) error

type mockRouter struct {
	id      int
	routeFn routeFn
}

func (r *mockRouter) Route(shard uint32, buf *common.RefCountedBuffer) error {
	return r.routeFn(shard, buf)
}

func (r *mockRouter) Close() {}
