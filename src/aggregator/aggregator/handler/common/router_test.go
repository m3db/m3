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

package common

import (
	"testing"

	"github.com/m3db/m3aggregator/sharding"
	"github.com/m3db/m3metrics/protocol/msgpack"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestShardedRouterRoute(t *testing.T) {
	var (
		enqueued      [3][]*RefCountedBuffer
		shardedQueues []ShardedQueue
		totalShards   = 1024
	)
	ranges := []string{"10..30", "60..80", "95"}
	for i, rng := range ranges {
		i := i
		sq := ShardedQueue{
			ShardSet: sharding.MustParseShardSet(rng),
			Queue: &mockQueue{
				enqueueFn: func(b *RefCountedBuffer) error {
					enqueued[i] = append(enqueued[i], b)
					return nil
				},
			},
		}
		shardedQueues = append(shardedQueues, sq)
	}
	router := NewShardedRouter(shardedQueues, totalShards, tally.NoopScope)

	inputs := []struct {
		shard uint32
		buf   *RefCountedBuffer
	}{
		{shard: 12, buf: NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: 67, buf: NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: 95, buf: NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: 24, buf: NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: 70, buf: NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
	}
	for _, input := range inputs {
		require.NoError(t, router.Route(input.shard, input.buf))
	}
	expected := [3][]*RefCountedBuffer{
		[]*RefCountedBuffer{inputs[0].buf, inputs[3].buf},
		[]*RefCountedBuffer{inputs[1].buf, inputs[4].buf},
		[]*RefCountedBuffer{inputs[2].buf},
	}
	require.Equal(t, expected, enqueued)
}

func TestShardedRouterRouteErrors(t *testing.T) {
	var (
		enqueued      [3][]*RefCountedBuffer
		shardedQueues []ShardedQueue
		totalShards   = 1024
	)
	ranges := []string{"10..30", "60..80", "95"}
	for i, rng := range ranges {
		sq := ShardedQueue{
			ShardSet: sharding.MustParseShardSet(rng),
			Queue: &mockQueue{
				enqueueFn: func(b *RefCountedBuffer) error {
					enqueued[i] = append(enqueued[i], b)
					return nil
				},
			},
		}
		shardedQueues = append(shardedQueues, sq)
	}
	router := NewShardedRouter(shardedQueues, totalShards, tally.NoopScope)

	inputs := []struct {
		shard uint32
		buf   *RefCountedBuffer
	}{
		{shard: 0, buf: NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: uint32(totalShards + 100), buf: NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
		{shard: 88, buf: NewRefCountedBuffer(msgpack.NewBufferedEncoder())},
	}
	for _, input := range inputs {
		require.Error(t, router.Route(input.shard, input.buf))
		require.Panics(t, func() { input.buf.DecRef() })
	}
	expected := [3][]*RefCountedBuffer{nil, nil, nil}
	require.Equal(t, expected, enqueued)

}
