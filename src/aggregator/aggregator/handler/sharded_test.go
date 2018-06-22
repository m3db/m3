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

	"github.com/m3db/m3aggregator/aggregator/handler/router"
	"github.com/m3db/m3aggregator/aggregator/handler/writer"
	"github.com/m3db/m3aggregator/sharding"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestNewShardedHandlerOneNonSharded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	router := router.NewMockRouter(ctrl)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routers := []router.Router{router.NewMockRouter(ctrl), router.NewMockRouter(ctrl)}
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
			Router:    router.NewBroadcastRouter(routers),
		},
	}
	require.Equal(t, expected, handler.routersBySharderID)
}

func TestNewShardedHandlerOneSharded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	router := router.NewMockRouter(ctrl)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routers := []router.Router{router.NewMockRouter(ctrl), router.NewMockRouter(ctrl)}
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
			Router:    router.NewBroadcastRouter(routers),
		},
	}
	require.Equal(t, expected, handler.routersBySharderID)
}

func TestNewShardedHandlerMultipleShardedDifferentSharderIDs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routers := []router.Router{
		router.NewMockRouter(ctrl),
		router.NewMockRouter(ctrl),
		router.NewMockRouter(ctrl),
	}
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
			Router:    router.NewBroadcastRouter([]router.Router{routers[0], routers[2]}),
		},
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 128),
			Router:    routers[1],
		},
	}
	require.Equal(t, expected, handler.routersBySharderID)
}

func TestNewShardedHandlerBothShardedAndNonSharded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routers := []router.Router{
		router.NewMockRouter(ctrl),
		router.NewMockRouter(ctrl),
		router.NewMockRouter(ctrl),
		router.NewMockRouter(ctrl),
		router.NewMockRouter(ctrl),
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
			Router:    router.NewBroadcastRouter([]router.Router{routers[0], routers[3], routers[1], routers[4]}),
		},
		{
			SharderID: sharding.NewSharderID(sharding.Murmur32Hash, 128),
			Router:    routers[2],
		},
	}
	require.Equal(t, expected, handler.routersBySharderID)
}

func TestShardedHandlerNewWriterSingleSharder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sharderID := sharding.NewSharderID(sharding.Murmur32Hash, 1024)
	router := router.NewMockRouter(ctrl)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routers := []router.Router{
		router.NewMockRouter(ctrl),
		router.NewMockRouter(ctrl),
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routers := []router.Router{
		router.NewMockRouter(ctrl),
		router.NewMockRouter(ctrl),
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
