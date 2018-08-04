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

package client

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/integration/fake"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

type testHostQueues struct {
	sync.RWMutex
	queues map[string][]*MockhostQueue
}

func newTestHostQueues() *testHostQueues {
	return &testHostQueues{
		queues: make(map[string][]*MockhostQueue),
	}
}

func (q *testHostQueues) add(id string, value *MockhostQueue) {
	q.Lock()
	defer q.Unlock()
	q.queues[id] = append(q.queues[id], value)
}

func (q *testHostQueues) get(id string) []*MockhostQueue {
	q.RLock()
	defer q.RUnlock()
	return q.queues[id]
}

func (q *testHostQueues) numUnique() int {
	q.RLock()
	defer q.RUnlock()
	return len(q.queues)
}

func TestSessionTopologyChangeCreatesNewClosesOldHostQueues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	avail := shard.Available

	node := func(id string, shards []uint32) services.ServiceInstance {
		result := services.NewServiceInstance().SetInstanceID(id)
		resultShards := make([]shard.Shard, len(shards))
		for i, id := range shards {
			resultShards[i] = shard.NewShard(id).SetState(avail)
		}
		return result.SetShards(shard.NewShards(resultShards))
	}

	svc := fake.NewM3ClusterService().
		SetInstances([]services.ServiceInstance{
			node("testhost0", []uint32{0, 1}),
			node("testhost1", []uint32{2, 3}),
		}).
		SetReplication(services.NewServiceReplication().SetReplicas(1)).
		SetSharding(services.NewServiceSharding().SetNumShards(4))

	svcs := fake.NewM3ClusterServices()
	svcs.RegisterService("m3db", svc)

	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(fake.NewM3ClusterClient(svcs, nil))
	topoInit := topology.NewDynamicInitializer(topoOpts)

	var testScopeTags map[string]string
	scope := tally.NewTestScope("", testScopeTags)

	opts := newSessionTestOptions().
		SetTopologyInitializer(topoInit)
	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().
		SetMetricsScope(scope))

	s, err := newSession(opts)
	require.NoError(t, err)

	createdQueues := newTestHostQueues()
	closedQueues := newTestHostQueues()

	session := s.(*session)
	session.newHostQueueFn = func(
		host topology.Host,
		opts hostQueueOpts,
	) hostQueue {
		queue := NewMockhostQueue(ctrl)
		createdQueues.add(host.ID(), queue)

		queue.EXPECT().Open()
		queue.EXPECT().Host().Return(host).AnyTimes()
		queue.EXPECT().ConnectionCount().Return(opts.opts.MinConnectionCount()).AnyTimes()
		queue.EXPECT().Close().Do(func() {
			closedQueues.add(host.ID(), queue)
		})
		return queue
	}

	require.NoError(t, session.Open())
	defer func() {
		assert.NoError(t, session.Close())
	}()

	// Assert created two
	require.Equal(t, 2, createdQueues.numUnique())
	require.Equal(t, 0, closedQueues.numUnique())
	require.Equal(t, 1, len(createdQueues.get("testhost0")))
	require.Equal(t, 1, len(createdQueues.get("testhost1")))

	// Change topology with one new instance and close one instance
	svc.SetInstances([]services.ServiceInstance{
		node("testhost1", []uint32{2, 3}),
		node("testhost2", []uint32{0, 1}),
	})
	svcs.NotifyServiceUpdate("m3db")

	// Wait for topology to be processed
	testScopeCounterKey := tally.KeyForPrefixedStringMap("topology.updated-success", testScopeTags)
	for {
		updated, ok := scope.Snapshot().Counters()[testScopeCounterKey]
		if ok && updated.Value() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for the close to occur
	for closedQueues.numUnique() < 1 {
		time.Sleep(10 * time.Millisecond)
	}

	// Assert create third and closed first
	require.Equal(t, 3, createdQueues.numUnique())
	require.Equal(t, 1, closedQueues.numUnique())
	require.Equal(t, 1, len(createdQueues.get("testhost0")))
	require.Equal(t, 1, len(createdQueues.get("testhost1")))
	require.Equal(t, 1, len(createdQueues.get("testhost2")))
	require.Equal(t, 1, len(closedQueues.get("testhost0")))
}
