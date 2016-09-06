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
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/watch"
	"github.com/stretchr/testify/assert"
)

func testSetup(ctrl *gomock.Controller) (DynamicOptions, xwatch.Watch) {
	opts := NewDynamicOptions()
	opts = opts.RetryOptions(xretry.NewOptions().InitialBackoff(time.Millisecond))

	watch := newTestWatch(ctrl, time.Millisecond, time.Millisecond, 10, 10)
	mockCSServices := services.NewMockServices(ctrl)
	mockCSServices.EXPECT().Watch(opts.GetService(), opts.GetQueryOptions()).Return(watch, nil)

	mockCSClient := client.NewMockClient(ctrl)
	mockCSClient.EXPECT().Services().Return(mockCSServices)
	opts = opts.ConfigServiceClient(mockCSClient)
	return opts, watch
}

func TestInitTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts, _ := testSetup(ctrl)
	topo, err := newDynamicTopology(opts.InitTimeout(10 * time.Millisecond))
	assert.Equal(t, errInitTimeOut, err)
	assert.Nil(t, topo)
}

func TestInitNoTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts, watch := testSetup(ctrl)
	go watch.(*testWatch).run()
	topo, err := newDynamicTopology(opts)

	assert.NoError(t, err)
	assert.NotNil(t, topo)
	topo.Close()
	// safe to close again
	topo.Close()
}

func TestGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts, watch := testSetup(ctrl)
	go watch.(*testWatch).run()
	topo, err := newDynamicTopology(opts)
	assert.NoError(t, err)

	m := topo.Get()
	assert.Equal(t, 2, m.Replicas())
}

func TestWatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts, watch := testSetup(ctrl)
	go watch.(*testWatch).run()
	topo, err := newDynamicTopology(opts)
	assert.NoError(t, err)

	w, err := topo.Watch()
	<-w.C()
	m := w.Get()
	assert.Equal(t, 2, m.Replicas())
	assert.Equal(t, 2, w.Get().Replicas())

	for _ = range w.C() {
		assert.Equal(t, 2, w.Get().Replicas())
	}
}

func TestGetUniqueShardsAndReplicas(t *testing.T) {
	shards, err := validateInstances(goodInstances, 2, 3)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(shards))

	goodInstances[0].SetShards(nil)
	shards, err = validateInstances(goodInstances, 2, 3)
	assert.Equal(t, errInstanceHasNoShardsAssignment, err)

	goodInstances[0].SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(0),
			shard.NewShard(1),
			shard.NewShard(3),
		}))
	shards, err = validateInstances(goodInstances, 2, 3)
	assert.Equal(t, errUnexpectedShard, err)

	// got h1: 1, h2: 1, 2, h3 0,2, missing a replica for 1
	goodInstances[0].SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(1),
		}))
	shards, err = validateInstances(goodInstances, 2, 3)
	assert.Equal(t, errNotEnoughReplicasForShard, err)

	goodInstances[0].SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(0),
		}))
	goodInstances[1].SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(2),
		}))
	shards, err = validateInstances(goodInstances, 2, 3)
	// got h1:0, h2: 2, h3 0,2, missing 1
	assert.Equal(t, errMissingShard, err)
}

type testWatch struct {
	sync.RWMutex

	ctrl                  *gomock.Controller
	data                  interface{}
	firstDelay, nextDelay time.Duration
	errAfter, closeAfter  int
	currentCalled         int
	ch                    chan struct{}
	closed                bool
}

func newTestWatch(ctrl *gomock.Controller, firstDelay, nextDelay time.Duration, errAfter, closeAfter int) xwatch.Watch {
	w := testWatch{ctrl: ctrl, firstDelay: firstDelay, nextDelay: nextDelay, errAfter: errAfter, closeAfter: closeAfter}
	w.ch = make(chan struct{})
	return &w
}

func (w *testWatch) run() {
	time.Sleep(w.firstDelay)
	w.update()
	for w.currentCalled < w.closeAfter {
		time.Sleep(w.nextDelay)
		w.update()
	}
	close(w.ch)
}

func (w *testWatch) update() {
	w.Lock()
	defer w.Unlock()
	if w.currentCalled < w.errAfter {
		w.data = getMockService(w.ctrl)
	} else {
		w.data = nil
	}
	w.ch <- struct{}{}
	w.currentCalled++
}

func (w *testWatch) Close() {
}

func (w *testWatch) Get() interface{} {
	w.RLock()
	defer w.RUnlock()
	return w.data
}

func (w *testWatch) C() <-chan struct{} {
	return w.ch
}

func getMockService(ctrl *gomock.Controller) services.Service {
	mockService := services.NewMockService(ctrl)

	mockReplication := services.NewMockServiceReplication(ctrl)
	mockReplication.EXPECT().Replicas().Return(2).AnyTimes()
	mockService.EXPECT().Replication().Return(mockReplication).AnyTimes()

	mockSharding := services.NewMockServiceSharding(ctrl)
	mockSharding.EXPECT().NumShards().Return(3).AnyTimes()
	mockService.EXPECT().Sharding().Return(mockSharding).AnyTimes()

	mockService.EXPECT().Instances().Return(goodInstances).AnyTimes()

	return mockService
}

var (
	i1 = services.NewServiceInstance().SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(0),
			shard.NewShard(1),
		})).SetID("h1").SetEndpoint("h1:9000")

	i2 = services.NewServiceInstance().SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(1),
			shard.NewShard(2),
		})).SetID("h2").SetEndpoint("h2:9000")

	i3 = services.NewServiceInstance().SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(2),
			shard.NewShard(0),
		})).SetID("h3").SetEndpoint("h3:9000")

	goodInstances = []services.ServiceInstance{i1, i2, i3}
)
