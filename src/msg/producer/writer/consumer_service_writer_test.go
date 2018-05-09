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
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/placement/service"
	"github.com/m3db/m3cluster/placement/storage"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/producer/data"
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3msg/topic"
	"github.com/m3db/m3x/watch"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestConsumerServiceWriterWithSharedConsumer(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sid := services.NewServiceID().SetName("foo")
	cs := topic.NewConsumerService().SetServiceID(sid).SetConsumptionType(topic.Shared)
	sd := services.NewMockServices(ctrl)
	ps := testPlacementService(mem.NewStore(), sid)
	sd.EXPECT().PlacementService(sid, gomock.Any()).Return(ps, nil)

	opts := testOptions().SetServiceDiscovery(sd)
	w, err := newConsumerServiceWriter(cs, 3, testMessagePool(opts), opts)
	require.NoError(t, err)

	csw := w.(*consumerServiceWriterImpl)

	var (
		lock               sync.Mutex
		numConsumerWriters int
	)
	csw.processFn = func(p interface{}) error {
		err := csw.process(p)
		lock.Lock()
		numConsumerWriters = len(csw.consumerWriters)
		lock.Unlock()
		return err
	}

	require.NoError(t, csw.Init(allowInitValueError))
	lock.Lock()
	require.Equal(t, 0, numConsumerWriters)
	lock.Unlock()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	p1 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint(lis.Addr().String()).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(1).SetState(shard.Available),
					shard.NewShard(2).SetState(shard.Available),
				})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("addr2").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
					shard.NewShard(2).SetState(shard.Available),
				})),
			placement.NewInstance().
				SetID("i3").
				SetEndpoint("addr3").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
					shard.NewShard(1).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0, 1, 2}).
		SetReplicaFactor(2).
		SetIsSharded(true)
	require.NoError(t, ps.Set(p1))

	for {
		lock.Lock()
		l := numConsumerWriters
		lock.Unlock()
		if l == 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncodeDecoderOptions())
		wg.Done()
	}()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Shard().Return(uint32(1))
	md.EXPECT().Bytes().Return([]byte("foo"))
	md.EXPECT().Finalize(producer.Consumed)

	rd := data.NewRefCountedData(md, nil)
	csw.Write(rd)
	for {
		if rd.IsDroppedOrConsumed() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	p2 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint(lis.Addr().String()).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(1).SetState(shard.Available),
				})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("addr2").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
					shard.NewShard(2).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0, 1, 2}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	require.NoError(t, ps.Set(p2))

	for {
		lock.Lock()
		l := numConsumerWriters
		lock.Unlock()
		if l == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	csw.Close()
	csw.Close()
}

func TestConsumerServiceWriterWithReplicatedConsumer(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sid := services.NewServiceID().SetName("foo")
	cs := topic.NewConsumerService().SetServiceID(sid).SetConsumptionType(topic.Replicated)
	sd := services.NewMockServices(ctrl)
	ps := testPlacementService(mem.NewStore(), sid)
	sd.EXPECT().PlacementService(sid, gomock.Any()).Return(ps, nil)

	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis1.Close()

	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis2.Close()

	p1 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint(lis1.Addr().String()).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
					shard.NewShard(1).SetState(shard.Available),
				})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint(lis2.Addr().String()).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(1).SetState(shard.Available),
				})),
			placement.NewInstance().
				SetID("i3").
				SetEndpoint("addr3").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0, 1}).
		SetReplicaFactor(2).
		SetIsSharded(true)
	require.NoError(t, ps.Set(p1))

	opts := testOptions().SetServiceDiscovery(sd)
	w, err := newConsumerServiceWriter(cs, 2, testMessagePool(opts), opts)
	csw := w.(*consumerServiceWriterImpl)
	require.NoError(t, err)
	require.NotNil(t, csw)

	var (
		lock               sync.Mutex
		numConsumerWriters int
	)
	csw.processFn = func(p interface{}) error {
		err := csw.process(p)
		lock.Lock()
		numConsumerWriters = len(csw.consumerWriters)
		lock.Unlock()
		return err
	}
	require.NoError(t, csw.Init(allowInitValueError))

	for {
		lock.Lock()
		l := numConsumerWriters
		lock.Unlock()
		if l == 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	md := producer.NewMockData(ctrl)
	md.EXPECT().Shard().Return(uint32(1)).AnyTimes()
	md.EXPECT().Bytes().Return([]byte("foo")).AnyTimes()
	md.EXPECT().Finalize(producer.Consumed)

	rd := data.NewRefCountedData(md, nil)
	csw.Write(rd)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis1, opts.EncodeDecoderOptions())
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis2, opts.EncodeDecoderOptions())
		wg.Done()
	}()
	wg.Wait()

	for {
		if rd.IsDroppedOrConsumed() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	p2 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint(lis1.Addr().String()).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint(lis2.Addr().String()).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(1).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0, 1}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	require.NoError(t, ps.Set(p2))

	for {
		lock.Lock()
		l := numConsumerWriters
		lock.Unlock()
		if l == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	go func() {
		for {
			conn, err := lis2.Accept()
			if err != nil {
				return
			}

			server := proto.NewEncodeDecoder(
				conn,
				opts.EncodeDecoderOptions(),
			)

			var msg msgpb.Message
			err = server.Decode(&msg)
			if err != nil {
				conn.Close()
				continue
			}
			server.Encode(&msgpb.Ack{
				Metadata: []msgpb.Metadata{
					msg.Metadata,
				},
			})
			conn.Close()
		}
	}()

	md.EXPECT().Finalize(producer.Consumed)
	rd = data.NewRefCountedData(md, nil)
	csw.Write(rd)
	for {
		if rd.IsDroppedOrConsumed() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	csw.Close()
	csw.Close()
}

func TestConsumerServiceWriterFilter(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sid := services.NewServiceID().SetName("foo")
	cs := topic.NewConsumerService().SetServiceID(sid).SetConsumptionType(topic.Replicated)
	sd := services.NewMockServices(ctrl)
	ps := testPlacementService(mem.NewStore(), sid)
	sd.EXPECT().PlacementService(sid, gomock.Any()).Return(ps, nil)

	opts := testOptions().SetServiceDiscovery(sd)
	csw, err := newConsumerServiceWriter(cs, 3, testMessagePool(opts), opts)
	require.NoError(t, err)

	sw0 := NewMockshardWriter(ctrl)
	sw1 := NewMockshardWriter(ctrl)
	csw.(*consumerServiceWriterImpl).shardWriters[0] = sw0
	csw.(*consumerServiceWriterImpl).shardWriters[1] = sw1

	md0 := producer.NewMockData(ctrl)
	md0.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	md1 := producer.NewMockData(ctrl)
	md1.EXPECT().Shard().Return(uint32(1)).AnyTimes()

	sw0.EXPECT().Write(gomock.Any())
	csw.Write(data.NewRefCountedData(md0, nil))
	sw1.EXPECT().Write(gomock.Any())
	csw.Write(data.NewRefCountedData(md1, nil))

	csw.RegisterFilter(func(data producer.Data) bool { return data.Shard() == uint32(0) })
	csw.Write(data.NewRefCountedData(md1, nil))

	sw0.EXPECT().Write(gomock.Any())
	csw.Write(data.NewRefCountedData(md0, nil))

	csw.UnregisterFilter()
	sw1.EXPECT().Write(gomock.Any())
	csw.Write(data.NewRefCountedData(md1, nil))
}

func TestConsumerServiceWriterAllowInitValueErrorWithCreateWatchError(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sid := services.NewServiceID().SetName("foo")
	cs := topic.NewConsumerService().SetServiceID(sid).SetConsumptionType(topic.Shared)

	ps := placement.NewMockService(ctrl)
	ps.EXPECT().Watch().Return(nil, errors.New("mock err")).AnyTimes()

	sd := services.NewMockServices(ctrl)
	sd.EXPECT().PlacementService(sid, gomock.Any()).Return(ps, nil)

	opts := testOptions().SetServiceDiscovery(sd)
	w, err := newConsumerServiceWriter(cs, 3, testMessagePool(opts), opts)
	require.NoError(t, err)
	defer w.Close()

	require.Error(t, w.Init(allowInitValueError))
}

func TestConsumerServiceWriterAllowInitValueErrorWithInitValueError(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sid := services.NewServiceID().SetName("foo")
	cs := topic.NewConsumerService().SetServiceID(sid).SetConsumptionType(topic.Shared)

	ps := testPlacementService(mem.NewStore(), sid)
	sd := services.NewMockServices(ctrl)
	sd.EXPECT().PlacementService(sid, gomock.Any()).Return(ps, nil)

	opts := testOptions().SetServiceDiscovery(sd)
	w, err := newConsumerServiceWriter(cs, 3, testMessagePool(opts), opts)
	require.NoError(t, err)
	defer w.Close()

	require.NoError(t, w.Init(allowInitValueError))
}

func TestConsumerServiceWriterInitFailOnErrorWithCreateWatchError(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sid := services.NewServiceID().SetName("foo")
	cs := topic.NewConsumerService().SetServiceID(sid).SetConsumptionType(topic.Shared)

	ps := placement.NewMockService(ctrl)
	ps.EXPECT().Watch().Return(nil, errors.New("mock err")).AnyTimes()

	sd := services.NewMockServices(ctrl)
	sd.EXPECT().PlacementService(sid, gomock.Any()).Return(ps, nil)

	opts := testOptions().SetServiceDiscovery(sd)
	w, err := newConsumerServiceWriter(cs, 3, testMessagePool(opts), opts)
	require.NoError(t, err)
	defer w.Close()

	err = w.Init(failOnError)
	require.Error(t, err)
	_, ok := err.(watch.CreateWatchError)
	require.True(t, ok)
}

func TestConsumerServiceWriterInitFailOnErrorWithInitValueError(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sid := services.NewServiceID().SetName("foo")
	cs := topic.NewConsumerService().SetServiceID(sid).SetConsumptionType(topic.Shared)

	ps := testPlacementService(mem.NewStore(), sid)
	sd := services.NewMockServices(ctrl)
	sd.EXPECT().PlacementService(sid, gomock.Any()).Return(ps, nil)

	opts := testOptions().SetServiceDiscovery(sd)
	w, err := newConsumerServiceWriter(cs, 3, testMessagePool(opts), opts)
	require.NoError(t, err)
	defer w.Close()

	err = w.Init(failOnError)
	require.Error(t, err)
	_, ok := err.(watch.InitValueError)
	require.True(t, ok)
}

func testPlacementService(store kv.Store, sid services.ServiceID) placement.Service {
	return service.NewPlacementService(storage.NewPlacementStorage(store, sid.String(), placement.NewOptions()), placement.NewOptions())
}
