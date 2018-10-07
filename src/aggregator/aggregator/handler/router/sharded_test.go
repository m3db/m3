// Copyright (c) 2018 Uber Technologies, Inc.
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

package router

import (
	"testing"

	"github.com/m3db/m3/src/aggregator/aggregator/handler/common"
	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3metrics/encoding/msgpack"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestShardedRouterRoute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		enqueued      [3][]*common.RefCountedBuffer
		shardedQueues []ShardedQueue
		totalShards   = 1024
	)
	ranges := []string{"10..30", "60..80", "95"}
	for i, rng := range ranges {
		i := i
		q := common.NewMockQueue(ctrl)
		q.EXPECT().
			Enqueue(gomock.Any()).
			Return(nil).
			Do(func(b *common.RefCountedBuffer) {
				enqueued[i] = append(enqueued[i], b)
			}).
			AnyTimes()
		sq := ShardedQueue{
			ShardSet: sharding.MustParseShardSet(rng),
			Queue:    q,
		}
		shardedQueues = append(shardedQueues, sq)
	}
	router := NewShardedRouter(shardedQueues, totalShards, tally.NoopScope)

	inputs := []struct {
		shard uint32
		buf   *common.RefCountedBuffer
	}{
		{shard: 12, buf: common.NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: 67, buf: common.NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: 95, buf: common.NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: 24, buf: common.NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: 70, buf: common.NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
	}
	for _, input := range inputs {
		require.NoError(t, router.Route(input.shard, input.buf))
	}
	expected := [3][]*common.RefCountedBuffer{
		[]*common.RefCountedBuffer{inputs[0].buf, inputs[3].buf},
		[]*common.RefCountedBuffer{inputs[1].buf, inputs[4].buf},
		[]*common.RefCountedBuffer{inputs[2].buf},
	}
	require.Equal(t, expected, enqueued)
}

func TestShardedRouterRouteErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		enqueued      [3][]*common.RefCountedBuffer
		shardedQueues []ShardedQueue
		totalShards   = 1024
	)
	ranges := []string{"10..30", "60..80", "95"}
	for i, rng := range ranges {
		i := i
		q := common.NewMockQueue(ctrl)
		q.EXPECT().
			Enqueue(gomock.Any()).
			Return(nil).
			Do(func(b *common.RefCountedBuffer) {
				enqueued[i] = append(enqueued[i], b)
			}).
			AnyTimes()
		sq := ShardedQueue{
			ShardSet: sharding.MustParseShardSet(rng),
			Queue:    q,
		}
		shardedQueues = append(shardedQueues, sq)
	}
	router := NewShardedRouter(shardedQueues, totalShards, tally.NoopScope)

	inputs := []struct {
		shard uint32
		buf   *common.RefCountedBuffer
	}{
		{shard: 0, buf: common.NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: uint32(totalShards + 100), buf: common.NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: 88, buf: common.NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
	}
	for _, input := range inputs {
		require.Error(t, router.Route(input.shard, input.buf))
		require.Panics(t, func() { input.buf.DecRef() })
	}
	expected := [3][]*common.RefCountedBuffer{nil, nil, nil}
	require.Equal(t, expected, enqueued)
}

func TestShardedRouterPartialShardSetClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		shardedQueues []ShardedQueue
		totalShards   = 1024
	)
	ranges := []string{"10..30"}
	for _, rng := range ranges {
		q := common.NewMockQueue(ctrl)
		q.EXPECT().Close().Times(21)
		sq := ShardedQueue{
			ShardSet: sharding.MustParseShardSet(rng),
			Queue:    q,
		}
		shardedQueues = append(shardedQueues, sq)
	}
	router := NewShardedRouter(shardedQueues, totalShards, tally.NoopScope)
	require.NotPanics(t, func() { router.Close() })
}
