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

package writer

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/protocol/proto"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSharedShardWriter(t *testing.T) {
	defer leaktest.Check(t)()

	a := newAckRouter(2)
	opts := testOptions()
	sw := newSharedShardWriter(1, a, newMessagePool(), opts, testMessageWriterMetrics())
	defer sw.Close()

	cw1 := newConsumerWriter("i1", a, opts, testConsumerWriterMetrics())
	cw1.Init()
	defer cw1.Close()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	addr2 := lis.Addr().String()
	cw2 := newConsumerWriter(addr2, a, opts, testConsumerWriterMetrics())
	cw2.Init()
	defer cw2.Close()

	cws := make(map[string]consumerWriter)
	cws["i1"] = cw1
	cws[addr2] = cw2

	i1 := placement.NewInstance().SetEndpoint("i1")
	i2 := placement.NewInstance().SetEndpoint(addr2)

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()

	sw.UpdateInstances(
		[]placement.Instance{i1},
		cws,
	)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Bytes().Return([]byte("foo"))
	mm.EXPECT().Finalize(producer.Consumed)
	mm.EXPECT().Size().Return(3)

	sw.Write(producer.NewRefCountedMessage(mm, nil))

	mw := sw.(*sharedShardWriter).mw.(*messageWriterImpl)
	mw.RLock()
	require.Equal(t, 1, len(mw.consumerWriters))
	require.Equal(t, 1, mw.queue.Len())
	mw.RUnlock()

	sw.UpdateInstances(
		[]placement.Instance{i1, i2},
		cws,
	)
	mw.RLock()
	require.Equal(t, 2, len(mw.consumerWriters))
	mw.RUnlock()
	for {
		mw.RLock()
		l := mw.queue.Len()
		mw.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestReplicatedShardWriter(t *testing.T) {
	defer leaktest.Check(t)()

	a := newAckRouter(3)
	opts := testOptions()
	sw := newReplicatedShardWriter(1, 200, a, newMessagePool(), opts, testMessageWriterMetrics()).(*replicatedShardWriter)
	defer sw.Close()

	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis1.Close()

	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis2.Close()

	addr1 := lis1.Addr().String()
	cw1 := newConsumerWriter(addr1, a, opts, testConsumerWriterMetrics())
	cw1.Init()
	defer cw1.Close()

	addr2 := lis2.Addr().String()
	cw2 := newConsumerWriter(addr2, a, opts, testConsumerWriterMetrics())
	cw2.Init()
	defer cw2.Close()

	cw3 := newConsumerWriter("i3", a, opts, testConsumerWriterMetrics())
	cw3.Init()
	defer cw3.Close()

	cws := make(map[string]consumerWriter)
	cws[addr1] = cw1
	cws[addr2] = cw2
	cws["i3"] = cw3
	i1 := placement.NewInstance().
		SetEndpoint(addr1).
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)}))
	i2 := placement.NewInstance().
		SetEndpoint(addr2).
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)}))
	i3 := placement.NewInstance().
		SetEndpoint("i3").
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)}))

	sw.UpdateInstances(
		[]placement.Instance{i1, i3},
		cws,
	)
	require.Equal(t, 2, len(sw.messageWriters))

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(3)
	mm.EXPECT().Bytes().Return([]byte("foo")).Times(2)

	sw.Write(producer.NewRefCountedMessage(mm, nil))

	mw1 := sw.messageWriters[i1.Endpoint()].(*messageWriterImpl)
	require.Equal(t, 1, mw1.queue.Len())
	mw3 := sw.messageWriters[i3.Endpoint()].(*messageWriterImpl)
	require.Equal(t, 1, mw3.queue.Len())

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis1, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()

	for {
		mw1.RLock()
		l := mw1.queue.Len()
		mw1.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, 1, mw3.queue.Len())

	mm.EXPECT().Finalize(producer.Consumed)
	sw.UpdateInstances(
		[]placement.Instance{i1, i2},
		cws,
	)

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis2, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()

	for {
		mw3.RLock()
		l := mw3.queue.Len()
		mw3.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mw2 := sw.messageWriters[i2.Endpoint()].(*messageWriterImpl)
	require.Equal(t, mw3, mw2)
	_, ok := sw.messageWriters[i3.Endpoint()]
	require.False(t, ok)
}

func TestReplicatedShardWriterRemoveMessageWriter(t *testing.T) {
	defer leaktest.Check(t)()

	router := newAckRouter(2).(*router)
	opts := testOptions()
	sw := newReplicatedShardWriter(
		1, 200, router, newMessagePool(), opts, testMessageWriterMetrics(),
	).(*replicatedShardWriter)

	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis1.Close()

	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis2.Close()

	addr1 := lis1.Addr().String()
	cw1 := newConsumerWriter(addr1, router, opts, testConsumerWriterMetrics())
	cw1.Init()
	defer cw1.Close()

	addr2 := lis2.Addr().String()
	cw2 := newConsumerWriter(addr2, router, opts, testConsumerWriterMetrics())
	cw2.Init()
	defer cw2.Close()

	cws := make(map[string]consumerWriter)
	cws[addr1] = cw1
	cws[addr2] = cw2
	i1 := placement.NewInstance().
		SetEndpoint(addr1).
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)}))
	i2 := placement.NewInstance().
		SetEndpoint(addr2).
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)}))

	sw.UpdateInstances(
		[]placement.Instance{i1, i2},
		cws,
	)

	require.Equal(t, 2, len(sw.messageWriters))

	mw1 := sw.messageWriters[i1.Endpoint()].(*messageWriterImpl)
	mw2 := sw.messageWriters[i2.Endpoint()].(*messageWriterImpl)
	require.Equal(t, 0, mw1.queue.Len())
	require.Equal(t, 0, mw2.queue.Len())

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(3)
	mm.EXPECT().Bytes().Return([]byte("foo")).Times(2)

	sw.Write(producer.NewRefCountedMessage(mm, nil))
	require.Equal(t, 1, mw1.queue.Len())
	require.Equal(t, 1, mw2.queue.Len())

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis1, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()

	for {
		mw1.RLock()
		l := mw1.queue.Len()
		mw1.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	require.Equal(t, 1, mw2.queue.Len())

	conn, err := lis2.Accept()
	require.NoError(t, err)
	defer conn.Close()

	serverEncoder := proto.NewEncoder(opts.EncoderOptions())
	serverDecoder := proto.NewDecoder(conn, opts.DecoderOptions(), 10)

	var msg msgpb.Message
	require.NoError(t, serverDecoder.Decode(&msg))
	sw.UpdateInstances(
		[]placement.Instance{i1},
		cws,
	)

	require.Equal(t, 1, len(sw.messageWriters))

	mm.EXPECT().Finalize(producer.Consumed)
	require.NoError(t, serverEncoder.Encode(&msgpb.Ack{Metadata: []msgpb.Metadata{msg.Metadata}}))
	_, err = conn.Write(serverEncoder.Bytes())
	require.NoError(t, err)
	// Make sure mw2 is closed and removed from router.
	for {
		router.RLock()
		l := len(router.messageWriters)
		router.RUnlock()
		if l == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	mw2.RLock()
	require.Equal(t, 0, mw2.queue.Len())
	mw2.RUnlock()

	sw.Close()
}

func TestReplicatedShardWriterUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	a := newAckRouter(4)
	opts := testOptions()
	sw := newReplicatedShardWriter(1, 200, a, newMessagePool(), opts, testMessageWriterMetrics()).(*replicatedShardWriter)
	defer sw.Close()

	cw1 := newConsumerWriter("i1", a, opts, testConsumerWriterMetrics())
	cw2 := newConsumerWriter("i2", a, opts, testConsumerWriterMetrics())
	cw3 := newConsumerWriter("i3", a, opts, testConsumerWriterMetrics())
	cw4 := newConsumerWriter("i4", a, opts, testConsumerWriterMetrics())
	cws := make(map[string]consumerWriter)
	cws["i1"] = cw1
	cws["i2"] = cw2
	cws["i3"] = cw3
	cws["i4"] = cw4

	i1 := placement.NewInstance().
		SetEndpoint("i1").
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetCutoffNanos(801).SetCutoverNanos(401)}))
	i2 := placement.NewInstance().
		SetEndpoint("i2").
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetCutoffNanos(802).SetCutoverNanos(402)}))
	i3 := placement.NewInstance().
		SetEndpoint("i3").
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetCutoffNanos(803).SetCutoverNanos(403)}))
	i4 := placement.NewInstance().
		SetEndpoint("i4").
		SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetCutoffNanos(804).SetCutoverNanos(404)}))

	sw.UpdateInstances([]placement.Instance{i1, i2}, cws)
	require.Equal(t, 2, int(sw.replicaID))
	require.Equal(t, 2, len(sw.messageWriters))
	mw1 := sw.messageWriters[i1.Endpoint()]
	require.NotNil(t, mw1)
	require.Equal(t, 801, int(mw1.CutoffNanos()))
	require.Equal(t, 401, int(mw1.CutoverNanos()))
	require.NotNil(t, sw.messageWriters[i2.Endpoint()])
	require.Equal(t, 0, int(mw1.MessageTTLNanos()))

	sw.SetMessageTTLNanos(500)
	require.Equal(t, 500, int(mw1.MessageTTLNanos()))

	sw.UpdateInstances([]placement.Instance{i2, i3}, cws)
	require.Equal(t, 2, int(sw.replicaID))
	require.Equal(t, 2, len(sw.messageWriters))
	mw2 := sw.messageWriters[i2.Endpoint()]
	require.NotNil(t, mw2)
	mw3 := sw.messageWriters[i3.Endpoint()]
	require.NotNil(t, mw3)
	require.Equal(t, mw1, mw3)
	require.Equal(t, 803, int(mw3.CutoffNanos()))
	require.Equal(t, 403, int(mw3.CutoverNanos()))
	m := make(map[uint64]int, 2)
	m[mw2.ReplicatedShardID()] = 1
	m[mw3.ReplicatedShardID()] = 1
	require.Equal(t, map[uint64]int{1: 1, 201: 1}, m)
	require.Equal(t, 500, int(mw2.MessageTTLNanos()))
	require.Equal(t, 500, int(mw3.MessageTTLNanos()))

	sw.UpdateInstances([]placement.Instance{i3}, cws)
	require.Equal(t, 2, int(sw.replicaID))
	require.Equal(t, 1, len(sw.messageWriters))
	require.NotNil(t, sw.messageWriters[i3.Endpoint()])
	require.Equal(t, 500, int(mw3.MessageTTLNanos()))
	for {
		mw2.(*messageWriterImpl).RLock()
		isClosed := mw2.(*messageWriterImpl).isClosed
		mw2.(*messageWriterImpl).RUnlock()
		if isClosed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	sw.SetMessageTTLNanos(800)
	require.Equal(t, 800, int(mw3.MessageTTLNanos()))
	sw.UpdateInstances([]placement.Instance{i1, i2, i3}, cws)
	require.Equal(t, 4, int(sw.replicaID))
	require.Equal(t, 3, len(sw.messageWriters))
	newmw1 := sw.messageWriters[i1.Endpoint()]
	require.NotNil(t, newmw1)
	require.NotEqual(t, &mw1, &newmw1)
	newmw2 := sw.messageWriters[i2.Endpoint()]
	require.NotNil(t, newmw2)
	require.NotEqual(t, &mw2, &newmw2)
	newmw3 := sw.messageWriters[i3.Endpoint()]
	require.NotNil(t, newmw3)
	require.Equal(t, &mw3, &newmw3)
	m = make(map[uint64]int, 3)
	m[newmw1.ReplicatedShardID()] = 1
	m[newmw2.ReplicatedShardID()] = 1
	m[newmw3.ReplicatedShardID()] = 1
	require.Equal(t, map[uint64]int{601: 1, 401: 1, mw3.ReplicatedShardID(): 1}, m)
	require.Equal(t, 800, int(newmw1.MessageTTLNanos()))
	require.Equal(t, 800, int(newmw2.MessageTTLNanos()))
	require.Equal(t, 800, int(newmw3.MessageTTLNanos()))

	sw.UpdateInstances([]placement.Instance{i2, i4}, cws)
	require.Equal(t, 4, int(sw.replicaID))
	require.Equal(t, 2, len(sw.messageWriters))
	require.NotNil(t, sw.messageWriters[i2.Endpoint()])
	require.NotNil(t, sw.messageWriters[i4.Endpoint()])

	sw.UpdateInstances([]placement.Instance{i1}, cws)
	require.Equal(t, 4, int(sw.replicaID))
	require.Equal(t, 1, len(sw.messageWriters))
	require.NotNil(t, sw.messageWriters[i1.Endpoint()])
}
