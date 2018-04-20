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

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/producer/data"
	"github.com/m3db/m3msg/topic"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestWriterInitError(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	opts := testOptions().SetTopicService(ts)
	w := NewWriter(opts)
	require.Error(t, w.Init())
	w.Close()
}

func TestWriterWriteAfterClosed(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	opts := testOptions().SetTopicService(ts)
	w := NewWriter(opts)
	w.Init()
	w.Close()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Finalize(producer.Dropped)
	rd := data.NewRefCountedData(md, nil)
	err = w.Write(rd)
	require.Error(t, err)
	require.Equal(t, errWriterClosed, err)
}

func TestWriterWriteWithInvalidShard(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	opts := testOptions().SetTopicService(ts)
	w := NewWriter(opts).(*writer)
	w.numShards = 2

	md := producer.NewMockData(ctrl)
	md.EXPECT().Shard().Return(uint32(2))
	md.EXPECT().Finalize(producer.Dropped)
	rd := data.NewRefCountedData(md, nil)
	err = w.Write(rd)
	require.Error(t, err)

	md.EXPECT().Shard().Return(uint32(100))
	md.EXPECT().Finalize(producer.Dropped)
	rd = data.NewRefCountedData(md, nil)
	err = w.Write(rd)
	require.Error(t, err)
}

func TestWriterInvalidTopicUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	opts := testOptions().SetTopicService(ts)
	sid1 := services.NewServiceID().SetName("s1")
	cs1 := topic.NewConsumerService().SetConsumptionType(topic.Replicated).SetServiceID(sid1)
	testTopic := topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(2).
		SetConsumerServices([]topic.ConsumerService{cs1})
	pb, err := topic.ToProto(testTopic)
	require.NoError(t, err)
	store.Set(opts.TopicName(), pb)

	sd := services.NewMockServices(ctrl)
	opts = opts.SetServiceDiscovery(sd)
	ps1 := testPlacementService(store, sid1)
	sd.EXPECT().PlacementService(sid1, gomock.Any()).Return(ps1, nil)

	p1 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("addr1").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
					shard.NewShard(1).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0, 1}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	require.NoError(t, ps1.Set(p1))

	w := NewWriter(opts).(*writer)
	var wg sync.WaitGroup
	w.processFn = func(i interface{}) error {
		defer wg.Done()
		return w.process(i)
	}
	wg.Add(1)
	require.NoError(t, w.Init())
	wg.Wait()
	defer w.Close()

	require.Equal(t, 2, int(w.numShards))
	testTopic = topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(3).
		SetConsumerServices([]topic.ConsumerService{cs1})
	pb, err = topic.ToProto(testTopic)
	require.NoError(t, err)
	wg.Add(1)
	store.Set(opts.TopicName(), pb)
	wg.Wait()

	require.Equal(t, 2, int(w.numShards))
}

func TestWriterWriteRegisterFilter(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	opts := testOptions().SetTopicService(ts)

	sid1 := services.NewServiceID().SetName("s1")
	cs1 := topic.NewConsumerService().SetConsumptionType(topic.Replicated).SetServiceID(sid1)
	csw1 := NewMockconsumerServiceWriter(ctrl)

	sid2 := services.NewServiceID().SetName("s2")
	filter := func(producer.Data) bool { return false }

	w := NewWriter(opts).(*writer)
	w.consumerServiceWriters[cs1.ServiceID().String()] = csw1

	csw1.EXPECT().UnregisterFilter()
	w.UnregisterFilter(sid1)

	// Wrong service id triggers nothing.
	w.RegisterFilter(sid2, filter)

	csw1.EXPECT().RegisterFilter(gomock.Any())
	w.RegisterFilter(sid1, filter)

	csw1.EXPECT().UnregisterFilter()
	w.UnregisterFilter(sid1)

	csw1.EXPECT().RegisterFilter(gomock.Any())
	w.RegisterFilter(sid1, filter)

	csw1.EXPECT().RegisterFilter(gomock.Any())
	testTopic := topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(6).
		SetConsumerServices([]topic.ConsumerService{cs1})
	w.process(testTopic)
}

func TestWriterTopicUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	opts := testOptions().SetTopicService(ts)

	sid1 := services.NewServiceID().SetName("s1")
	cs1 := topic.NewConsumerService().SetConsumptionType(topic.Replicated).SetServiceID(sid1)
	testTopic := topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(2).
		SetConsumerServices([]topic.ConsumerService{cs1})
	pb, err := topic.ToProto(testTopic)
	require.NoError(t, err)
	store.Set(opts.TopicName(), pb)

	sd := services.NewMockServices(ctrl)
	opts = opts.SetServiceDiscovery(sd)
	ps1 := testPlacementService(store, sid1)
	sd.EXPECT().PlacementService(sid1, gomock.Any()).Return(ps1, nil)

	p1 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("addr1").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
					shard.NewShard(1).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0, 1}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	require.NoError(t, ps1.Set(p1))

	w := NewWriter(opts).(*writer)
	require.NoError(t, w.Init())
	defer w.Close()

	require.Equal(t, 1, len(w.consumerServiceWriters))

	sid2 := services.NewServiceID().SetName("s2")
	cs2 := topic.NewConsumerService().SetConsumptionType(topic.Shared).SetServiceID(sid2)
	ps2 := testPlacementService(store, sid2)
	sd.EXPECT().PlacementService(sid2, gomock.Any()).Return(ps2, nil)

	sid3 := services.NewServiceID().SetName("s3")
	cs3 := topic.NewConsumerService().SetConsumptionType(topic.Replicated).SetServiceID(sid3)
	sd.EXPECT().PlacementService(sid3, gomock.Any()).Return(nil, errors.New("test error"))

	sid4 := services.NewServiceID().SetName("s4")
	cs4 := topic.NewConsumerService().SetConsumptionType(topic.Shared).SetServiceID(sid4)
	ps4 := placement.NewMockService(ctrl)
	sd.EXPECT().PlacementService(sid4, gomock.Any()).Return(ps4, nil)
	ps4.EXPECT().Watch().Return(nil, errors.New("watch error"))

	testTopic = testTopic.SetConsumerServices([]topic.ConsumerService{
		cs1, cs2,
		cs3, // Could not create consumer service write for cs3.
		cs4, // Could not init cs4.
	})
	pb, err = topic.ToProto(testTopic)
	require.NoError(t, err)
	store.Set(opts.TopicName(), pb)

	for {
		w.RLock()
		l := len(w.consumerServiceWriters)
		w.RUnlock()
		if l == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	store.Delete(opts.TopicName())
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 2, len(w.consumerServiceWriters))

	testTopic = testTopic.SetConsumerServices([]topic.ConsumerService{cs2})
	pb, err = topic.ToProto(testTopic)
	require.NoError(t, err)
	store.Set(opts.TopicName(), pb)

	for {
		w.RLock()
		l := len(w.consumerServiceWriters)
		w.RUnlock()
		if l == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	w.Close()

	testTopic = testTopic.SetConsumerServices([]topic.ConsumerService{cs1, cs2})
	pb, err = topic.ToProto(testTopic)
	require.NoError(t, err)
	store.Set(opts.TopicName(), pb)

	// Not going to process topic update anymore.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, len(w.consumerServiceWriters))
}

func TestWriterWrite(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	opts := testOptions().SetTopicService(ts)

	sid1 := services.NewServiceID().SetName("s1")
	cs1 := topic.NewConsumerService().SetConsumptionType(topic.Replicated).SetServiceID(sid1)
	sid2 := services.NewServiceID().SetName("s2")
	cs2 := topic.NewConsumerService().SetConsumptionType(topic.Shared).SetServiceID(sid2)
	testTopic := topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(1).
		SetConsumerServices([]topic.ConsumerService{cs1, cs2})
	pb, err := topic.ToProto(testTopic)
	require.NoError(t, err)
	store.Set(opts.TopicName(), pb)

	sd := services.NewMockServices(ctrl)
	opts = opts.SetServiceDiscovery(sd)
	ps1 := testPlacementService(store, sid1)
	sd.EXPECT().PlacementService(sid1, gomock.Any()).Return(ps1, nil)
	ps2 := testPlacementService(store, sid2)
	sd.EXPECT().PlacementService(sid2, gomock.Any()).Return(ps2, nil)

	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis1.Close()

	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis2.Close()

	lis3, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis3.Close()

	p1 := placement.NewPlacement().
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
					shard.NewShard(0).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(2).
		SetIsSharded(true)
	require.NoError(t, ps1.Set(p1))

	p2 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i13").
				SetEndpoint(lis3.Addr().String()).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
			placement.NewInstance().
				SetID("i4").
				SetEndpoint("addr4").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(2).
		SetIsSharded(true)
	require.NoError(t, ps2.Set(p2))

	w := NewWriter(opts).(*writer)
	require.NoError(t, w.Init())
	defer w.Close()

	require.Equal(t, 2, len(w.consumerServiceWriters))

	var wg sync.WaitGroup
	md := producer.NewMockData(ctrl)
	md.EXPECT().Shard().Return(uint32(0)).Times(3)
	md.EXPECT().Bytes().Return([]byte("foo")).Times(3)
	md.EXPECT().Finalize(producer.Consumed).Do(func(interface{}) { wg.Done() })
	rd := data.NewRefCountedData(md, nil)
	wg.Add(1)
	require.NoError(t, w.Write(rd))

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

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis3, opts.EncodeDecoderOptions())
		wg.Done()
	}()

	wg.Wait()
	w.Close()
}

func TestWriterCloseBlocking(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	opts := testOptions().SetTopicService(ts)

	sid1 := services.NewServiceID().SetName("s1")
	cs1 := topic.NewConsumerService().SetConsumptionType(topic.Shared).SetServiceID(sid1)
	testTopic := topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(1).
		SetConsumerServices([]topic.ConsumerService{cs1})
	pb, err := topic.ToProto(testTopic)
	require.NoError(t, err)
	store.Set(opts.TopicName(), pb)

	sd := services.NewMockServices(ctrl)
	opts = opts.SetServiceDiscovery(sd)
	ps1 := testPlacementService(store, sid1)
	sd.EXPECT().PlacementService(sid1, gomock.Any()).Return(ps1, nil)

	p1 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("addr1").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	require.NoError(t, ps1.Set(p1))

	w := NewWriter(opts).(*writer)
	require.NoError(t, w.Init())
	require.Equal(t, 1, len(w.consumerServiceWriters))

	md := producer.NewMockData(ctrl)
	md.EXPECT().Shard().Return(uint32(0)).Times(2)
	md.EXPECT().Bytes().Return([]byte("foo")).Times(1)
	md.EXPECT().Finalize(producer.Dropped)
	rd := data.NewRefCountedData(md, nil)
	require.NoError(t, w.Write(rd))

	doneCh := make(chan struct{})
	go func() {
		w.Close()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		require.FailNow(t, "writer.Close() should block until all messages dropped or consumed")
	default:
	}

	rd.Drop()
	<-doneCh
}
