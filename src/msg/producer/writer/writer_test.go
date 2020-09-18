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

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/topic"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestWriterInitErrorNoTopic(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
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

	ctrl := xtest.NewController(t)
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

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Finalize(producer.Dropped)
	mm.EXPECT().Size().Return(3)
	rm := producer.NewRefCountedMessage(mm, nil)
	err = w.Write(rm)
	require.Error(t, err)
	require.Equal(t, errWriterClosed, err)
}

func TestWriterWriteWithInvalidShard(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	opts := testOptions().SetTopicService(ts)
	w := NewWriter(opts).(*writer)
	w.numShards = 2

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Shard().Return(uint32(2))
	mm.EXPECT().Finalize(producer.Dropped)
	mm.EXPECT().Size().Return(3)
	rm := producer.NewRefCountedMessage(mm, nil)
	err = w.Write(rm)
	require.Error(t, err)

	mm.EXPECT().Shard().Return(uint32(100))
	mm.EXPECT().Finalize(producer.Dropped)
	mm.EXPECT().Size().Return(3)
	rm = producer.NewRefCountedMessage(mm, nil)
	err = w.Write(rm)
	require.Error(t, err)
}

func TestWriterInvalidTopicUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
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
	_, err = ts.CheckAndSet(testTopic, kv.UninitializedVersion)
	require.NoError(t, err)

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
	_, err = ps1.Set(p1)
	require.NoError(t, err)

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
		SetConsumerServices([]topic.ConsumerService{cs1}).
		SetVersion(1)
	wg.Add(1)
	_, err = ts.CheckAndSet(testTopic, 1)
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, 2, int(w.numShards))
}

func TestWriterRegisterFilter(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
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
	filter := func(producer.Message) bool { return false }

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
	csw1.EXPECT().SetMessageTTLNanos(int64(0))
	testTopic := topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(6).
		SetConsumerServices([]topic.ConsumerService{cs1})
	w.process(testTopic)
}

func TestWriterTopicUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
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
	_, err = ts.CheckAndSet(testTopic, kv.UninitializedVersion)
	require.NoError(t, err)

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
	_, err = ps1.Set(p1)
	require.NoError(t, err)

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

	testTopic = testTopic.
		SetConsumerServices([]topic.ConsumerService{
			cs1, cs2,
			cs3, // Could not create consumer service write for cs3.
			cs4, // Could not init cs4.
		}).
		SetVersion(1)
	_, err = ts.CheckAndSet(testTopic, 1)
	require.NoError(t, err)

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

	testTopic = testTopic.
		SetConsumerServices([]topic.ConsumerService{cs2}).
		SetVersion(0)
	_, err = ts.CheckAndSet(testTopic, kv.UninitializedVersion)
	require.NoError(t, err)

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

	testTopic = testTopic.
		SetConsumerServices([]topic.ConsumerService{cs1, cs2}).
		SetVersion(1)
	_, err = ts.CheckAndSet(testTopic, 1)
	require.NoError(t, err)

	// Not going to process topic update anymore.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, len(w.consumerServiceWriters))
}

func TestTopicUpdateWithSameConsumerServicesButDifferentOrder(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
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
	cs2 := topic.NewConsumerService().SetConsumptionType(topic.Shared).SetServiceID(sid2).SetMessageTTLNanos(500)
	testTopic := topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(1).
		SetConsumerServices([]topic.ConsumerService{cs1, cs2})
	_, err = ts.CheckAndSet(testTopic, kv.UninitializedVersion)
	require.NoError(t, err)

	sd := services.NewMockServices(ctrl)
	opts = opts.SetServiceDiscovery(sd)
	ps1 := testPlacementService(store, sid1)
	sd.EXPECT().PlacementService(sid1, gomock.Any()).Return(ps1, nil)
	ps2 := testPlacementService(store, sid2)
	sd.EXPECT().PlacementService(sid2, gomock.Any()).Return(ps2, nil)

	p1 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("i1").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	_, err = ps1.Set(p1)
	require.NoError(t, err)

	p2 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("i2").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	_, err = ps2.Set(p2)
	require.NoError(t, err)

	w := NewWriter(opts).(*writer)

	called := atomic.NewInt32(0)
	w.processFn = func(update interface{}) error {
		called.Inc()
		return w.process(update)
	}
	require.NoError(t, w.Init())
	require.Equal(t, 1, int(called.Load()))
	require.Equal(t, 2, len(w.consumerServiceWriters))
	csw, ok := w.consumerServiceWriters[cs1.ServiceID().String()]
	require.True(t, ok)
	cswMock1 := NewMockconsumerServiceWriter(ctrl)
	w.consumerServiceWriters[cs1.ServiceID().String()] = cswMock1
	defer csw.Close()

	csw, ok = w.consumerServiceWriters[cs2.ServiceID().String()]
	require.True(t, ok)
	cswMock2 := NewMockconsumerServiceWriter(ctrl)
	w.consumerServiceWriters[cs2.ServiceID().String()] = cswMock2
	defer csw.Close()

	cswMock1.EXPECT().SetMessageTTLNanos(int64(0))
	cswMock2.EXPECT().SetMessageTTLNanos(int64(500))
	testTopic = testTopic.
		SetConsumerServices([]topic.ConsumerService{cs2, cs1}).
		SetVersion(1)
	_, err = ts.CheckAndSet(testTopic, 1)
	require.NoError(t, err)

	// The update will be processed, but nothing will be called on any of the mock writers.
	for called.Load() != 2 {
		time.Sleep(50 * time.Millisecond)
	}
	cswMock1.EXPECT().Close()
	cswMock2.EXPECT().Close()
	w.Close()
}

func TestWriterWrite(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
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
	_, err = ts.CheckAndSet(testTopic, kv.UninitializedVersion)
	require.NoError(t, err)

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
	_, err = ps1.Set(p1)
	require.NoError(t, err)

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
	_, err = ps2.Set(p2)
	require.NoError(t, err)

	w := NewWriter(opts).(*writer)
	require.NoError(t, w.Init())
	defer w.Close()

	require.Equal(t, 2, len(w.consumerServiceWriters))

	var wg sync.WaitGroup
	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Shard().Return(uint32(0)).Times(3)
	mm.EXPECT().Size().Return(3)
	mm.EXPECT().Bytes().Return([]byte("foo")).Times(3)
	mm.EXPECT().Finalize(producer.Consumed).Do(func(interface{}) { wg.Done() })
	rm := producer.NewRefCountedMessage(mm, nil)
	wg.Add(1)
	require.NoError(t, w.Write(rm))

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis1, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis2, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis3, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()

	wg.Wait()
	w.Close()
}

func TestWriterCloseBlocking(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
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
	_, err = ts.CheckAndSet(testTopic, kv.UninitializedVersion)
	require.NoError(t, err)

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
	_, err = ps1.Set(p1)
	require.NoError(t, err)

	w := NewWriter(opts).(*writer)
	require.NoError(t, w.Init())
	require.Equal(t, 1, len(w.consumerServiceWriters))

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(3)
	mm.EXPECT().Shard().Return(uint32(0)).Times(2)
	mm.EXPECT().Bytes().Return([]byte("foo")).Times(1)
	mm.EXPECT().Finalize(producer.Dropped)
	rm := producer.NewRefCountedMessage(mm, nil)
	require.NoError(t, w.Write(rm))

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

	rm.Drop()
	<-doneCh
}

func TestWriterSetMessageTTLNanosDropMetric(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
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
	_, err = ts.CheckAndSet(testTopic, kv.UninitializedVersion)
	require.NoError(t, err)

	sd := services.NewMockServices(ctrl)
	opts = opts.SetServiceDiscovery(sd)
	ps1 := testPlacementService(store, sid1)
	sd.EXPECT().PlacementService(sid1, gomock.Any()).Return(ps1, nil)
	ps2 := testPlacementService(store, sid2)
	sd.EXPECT().PlacementService(sid2, gomock.Any()).Return(ps2, nil)

	lis1, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis1.Close()

	p1 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint(lis1.Addr().String()).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	_, err = ps1.Set(p1)
	require.NoError(t, err)

	p2 := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("i2").
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	_, err = ps2.Set(p2)
	require.NoError(t, err)

	w := NewWriter(opts).(*writer)
	require.NoError(t, w.Init())
	defer w.Close()

	require.Equal(t, 2, len(w.consumerServiceWriters))

	var called int
	var wg sync.WaitGroup
	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	mm.EXPECT().Size().Return(3).AnyTimes()
	mm.EXPECT().Bytes().Return([]byte("foo")).AnyTimes()
	mm.EXPECT().Finalize(producer.Consumed).Do(func(interface{}) { called++; wg.Done() })
	require.NoError(t, w.Write(producer.NewRefCountedMessage(mm, nil)))

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis1, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()
	wg.Wait()
	require.Equal(t, 0, called)

	// Wait for the message ttl update to trigger finalize.
	wg.Add(1)
	testTopic = topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(1).
		SetConsumerServices([]topic.ConsumerService{cs1, cs2.SetMessageTTLNanos(int64(50 * time.Millisecond))})
	_, err = ts.CheckAndSet(testTopic, 1)
	require.NoError(t, err)
	wg.Wait()
	require.Equal(t, 1, called)

	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis2.Close()

	p2 = placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i2").
				SetEndpoint(lis2.Addr().String()).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)
	_, err = ps2.Set(p2)
	require.NoError(t, err)

	testTopic = topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(1).
		SetConsumerServices([]topic.ConsumerService{cs1, cs2.SetMessageTTLNanos(0)})
	_, err = ts.CheckAndSet(testTopic, 2)
	require.NoError(t, err)

	require.NoError(t, w.Write(producer.NewRefCountedMessage(mm, nil)))

	called = 0
	mm.EXPECT().Finalize(producer.Consumed).Do(func(interface{}) { called++; wg.Done() })
	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis1, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()
	wg.Wait()
	require.Equal(t, 0, called)

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 0, called)

	// Wait for the consumer to trigger finalize because there is no more message ttl.
	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis2, opts.EncoderOptions(), opts.DecoderOptions())
	}()
	wg.Wait()
	require.Equal(t, 1, called)

	w.Close()
}

func TestWriterNumShards(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	ts, err := topic.NewService(topic.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	opts := testOptions().SetTopicService(ts)

	testTopic := topic.NewTopic().
		SetName(opts.TopicName()).
		SetNumberOfShards(2)
	_, err = ts.CheckAndSet(testTopic, kv.UninitializedVersion)
	require.NoError(t, err)

	w := NewWriter(opts).(*writer)
	defer w.Close()

	require.Equal(t, 0, int(w.NumShards()))

	require.NoError(t, w.Init())
	require.Equal(t, 2, int(w.NumShards()))
}
