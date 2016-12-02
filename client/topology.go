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

package client

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/topology"

	"github.com/m3db/m3x/pool"

	"github.com/uber-go/tally"
)

func (s *session) initHostQueues(topologyMap topology.Map) ([]hostQueue, int, int, error) {
	// NB(r): we leave existing writes in the host queues to finish
	// as they are already enroute to their destination, this is ok
	// as part of adding a host is to add another replica for the
	// shard set and only once bootstrapped decomission the old node
	start := s.nowFn()

	hosts := topologyMap.Hosts()
	queues := make([]hostQueue, len(hosts))
	for i := range queues {
		queues[i] = s.newHostQueue(hosts[i], topologyMap)
	}

	shards := topologyMap.ShardSet().Shards()
	minConnectionCount := s.opts.MinConnectionCount()
	replicas := topologyMap.Replicas()
	majority := topologyMap.MajorityReplicas()

	firstConnectConsistencyLevel := s.opts.ClusterConnectConsistencyLevel()
	if firstConnectConsistencyLevel == ConnectConsistencyLevelNone {
		// Return immediately if no connect consistency required
		return queues, replicas, majority, nil
	}

	connectConsistencyLevel := firstConnectConsistencyLevel
	if connectConsistencyLevel == ConnectConsistencyLevelAny {
		// If level any specified, first attempt all then proceed lowering requirement
		connectConsistencyLevel = ConnectConsistencyLevelAll
	}

	// Abort if we do not connect
	connected := false
	defer func() {
		if !connected {
			for _, q := range queues {
				q.Close()
			}
		}
	}()

	for {
		if now := s.nowFn(); now.Sub(start) >= s.opts.ClusterConnectTimeout() {
			switch firstConnectConsistencyLevel {
			case ConnectConsistencyLevelAny:
				// If connecting with connect any strategy then keep
				// trying but lower consistency requirement
				start = now
				connectConsistencyLevel--
				if connectConsistencyLevel == ConnectConsistencyLevelNone {
					// Already tried to resolve all consistency requirements, just
					// return successfully at this point
					return queues, replicas, majority, nil
				}
			default:
				// Timed out connecting to a specific consistency requirement
				return nil, 0, 0, ErrClusterConnectTimeout
			}
		}
		// Be optimistic
		clusterAvailable := true
		for _, shard := range shards {
			shardReplicasAvailable := 0
			routeErr := topologyMap.RouteShardForEach(shard, func(idx int, host topology.Host) {
				if queues[idx].ConnectionCount() >= minConnectionCount {
					shardReplicasAvailable++
				}
			})
			if routeErr != nil {
				return nil, 0, 0, routeErr
			}
			var clusterAvailableForShard bool
			switch connectConsistencyLevel {
			case ConnectConsistencyLevelAll:
				clusterAvailableForShard = shardReplicasAvailable == replicas
			case ConnectConsistencyLevelMajority:
				clusterAvailableForShard = shardReplicasAvailable >= majority
			case ConnectConsistencyLevelOne:
				clusterAvailableForShard = shardReplicasAvailable > 0
			default:
				return nil, 0, 0, errSessionInvalidConnectClusterConnectConsistencyLevel
			}
			if !clusterAvailableForShard {
				clusterAvailable = false
				break
			}
		}
		if clusterAvailable {
			// All done
			break
		}
		time.Sleep(clusterConnectWaitInterval)
	}

	connected = true
	return queues, replicas, majority, nil
}

func (s *session) setTopologyWithLock(topologyMap topology.Map, hasLock bool) error {
	queues, replicas, majority, err := s.initHostQueues(topologyMap)
	if err != nil {
		return err
	}

	if !hasLock { // avoids code duplication. todo@bl: revisit this
		s.Lock()
		defer s.Unlock()
	}

	prev := s.queues
	s.queues = queues
	s.topoMap = topologyMap

	prevReplicas := atomic.LoadInt32(&s.replicas)
	atomic.StoreInt32(&s.replicas, int32(replicas))
	atomic.StoreInt32(&s.majority, int32(majority))

	if s.fetchBatchOpArrayArrayPool == nil ||
		s.fetchBatchOpArrayArrayPool.Entries() != len(queues) {
		poolOpts := pool.NewObjectPoolOptions().
			SetSize(s.opts.FetchBatchOpPoolSize()).
			SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
				s.scope.SubScope("fetch-batch-op-array-array-pool"),
			))
		s.fetchBatchOpArrayArrayPool = newFetchBatchOpArrayArrayPool(
			poolOpts,
			len(queues),
			s.opts.FetchBatchOpPoolSize()/len(queues))
		s.fetchBatchOpArrayArrayPool.Init()
	}
	if s.iteratorArrayPool == nil ||
		prevReplicas != s.replicas {
		s.iteratorArrayPool = encoding.NewIteratorArrayPool([]pool.Bucket{
			pool.Bucket{
				Capacity: replicas,
				Count:    s.opts.SeriesIteratorPoolSize(),
			},
		})
		s.iteratorArrayPool.Init()
	}
	if s.readerSliceOfSlicesIteratorPool == nil ||
		prevReplicas != s.replicas {
		size := replicas * s.opts.SeriesIteratorPoolSize()
		poolOpts := pool.NewObjectPoolOptions().
			SetSize(size).
			SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
				s.scope.SubScope("reader-slice-of-slices-iterator-pool"),
			))
		s.readerSliceOfSlicesIteratorPool = newReaderSliceOfSlicesIteratorPool(poolOpts)
		s.readerSliceOfSlicesIteratorPool.Init()
	}
	if s.multiReaderIteratorPool == nil ||
		prevReplicas != s.replicas {
		size := replicas * s.opts.SeriesIteratorPoolSize()
		poolOpts := pool.NewObjectPoolOptions().
			SetSize(size).
			SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
				s.scope.SubScope("multi-reader-iterator-pool"),
			))
		s.multiReaderIteratorPool = encoding.NewMultiReaderIteratorPool(poolOpts)
		s.multiReaderIteratorPool.Init(s.opts.ReaderIteratorAllocate())
	}

	for i := len(s.metrics.writeNodesRespondingErrors); i < replicas; i++ {
		counter := makeCounter(s.scope, i, "write.nodes-responding-error")
		s.metrics.writeNodesRespondingErrors = append(s.metrics.writeNodesRespondingErrors, counter)
	}

	for i := len(s.metrics.fetchNodesRespondingErrors); i < replicas; i++ {
		counter := makeCounter(s.scope, i, "fetch.nodes-responding-error")
		s.metrics.fetchNodesRespondingErrors = append(s.metrics.fetchNodesRespondingErrors, counter)
	}

	// Asynchronously close the previous set of host queues
	go func() {
		for _, p := range prev {
			p.Close()
		}
	}()

	s.log.Infof("successfully updated topology to %d hosts", topologyMap.HostsLen())
	return nil
}

func makeCounter(scope tally.Scope, i int, name string) tally.Counter {
	tags := map[string]string{"nodes": fmt.Sprintf("%d", i+1)}
	return scope.Tagged(tags).Counter(name)
}

func (s *session) updateTopology() {
	for range s.topoWatch.C() {
		s.log.Info("received update for topology")
		if err := s.setTopologyWithLock(s.topoWatch.Get(), false); err != nil {
			s.log.Errorf("could not update topology map: %v", err)
		}
	}
}
