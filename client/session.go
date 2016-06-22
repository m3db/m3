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
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	xtime "github.com/m3db/m3db/x/time"
)

const (
	clusterConnectWaitInterval = 10 * time.Millisecond
)

var (
	// ErrClusterConnectTimeout is raised when connecting to the cluster and
	// ensuring at least each partition has an up node with a connection to it
	ErrClusterConnectTimeout = errors.New("timed out establishing min connections to cluster")

	errSessionAlreadyOpen = errors.New("session already open")
)

type clientSession interface {
	m3db.Session

	// Open the client session
	Open() error
}

type session struct {
	sync.RWMutex

	opts           m3db.ClientOptions
	nowFn          m3db.NowFn
	newHostQueueFn newHostQueueFn
	topo           m3db.Topology
	topoMap        m3db.TopologyMap
	topoMapCh      chan m3db.TopologyMap
	queues         []hostQueue
	opened         bool
	closed         bool

	// NB(r): We make sets of pools, one for each shard, to reduce
	// contention on the pool when retrieving ops
	writeOpPools []writeOpPool
}

type newHostQueueFn func(
	host m3db.Host,
	writeBatchRequestPool writeBatchRequestPool,
	writeRequestArrayPool writeRequestArrayPool,
	opts m3db.ClientOptions,
) hostQueue

func newSession(opts m3db.ClientOptions) (clientSession, error) {
	topology, err := opts.GetTopologyType().Create()
	if err != nil {
		return nil, err
	}

	return &session{
		opts:           opts,
		nowFn:          opts.GetNowFn(),
		newHostQueueFn: newHostQueue,
		topo:           topology,
		topoMapCh:      make(chan m3db.TopologyMap),
	}, nil
}

func (s *session) Open() error {
	s.Lock()
	if s.opened || s.closed {
		s.Unlock()
		return errSessionAlreadyOpen
	}
	s.opened = true
	s.Unlock()

	currTopologyMap := s.topo.GetAndSubscribe(s.topoMapCh)
	if err := s.setTopologyMap(currTopologyMap); err != nil {
		s.Lock()
		s.opened = false
		s.Unlock()
		return err
	}

	go func() {
		log := s.opts.GetLogger()
		for value := range s.topoMapCh {
			if err := s.setTopologyMap(value); err != nil {
				log.Errorf("could not update topology map: %v", err)
			}
		}
	}()

	return nil
}

func (s *session) setTopologyMap(topologyMap m3db.TopologyMap) error {
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

	shards := topologyMap.ShardScheme().All().Shards()
	minConnectionCount := s.opts.GetMinConnectionCount()
	for {
		if s.nowFn().Sub(start) >= s.opts.GetClusterConnectTimeout() {
			for i := range queues {
				queues[i].Close()
			}
			return ErrClusterConnectTimeout
		}
		// Be optimistic
		clusterHasMinConnectionsToAllShards := true
		for _, shard := range shards {
			// Be optimistic
			shardHasMinConnectionsToOneHost := true
			routeErr := topologyMap.RouteShardForEach(shard, func(idx int, host m3db.Host) {
				if queues[idx].GetConnectionCount() < minConnectionCount {
					shardHasMinConnectionsToOneHost = false
				}
			})
			if routeErr != nil {
				return routeErr
			}
			if !shardHasMinConnectionsToOneHost {
				clusterHasMinConnectionsToAllShards = false
				break
			}
		}
		if clusterHasMinConnectionsToAllShards {
			// All done
			break
		}
		time.Sleep(clusterConnectWaitInterval)
	}

	s.Lock()
	prev := s.queues
	s.queues = queues
	s.setWriteOpPoolsWithLock(topologyMap)
	s.topoMap = topologyMap
	s.Unlock()

	// Asynchronously close the previous set of host queues
	go func() {
		for i := range prev {
			prev[i].Close()
		}
	}()

	return nil
}

func (s *session) newHostQueue(host m3db.Host, topologyMap m3db.TopologyMap) hostQueue {
	// NB(r): Due to hosts being replicas we have:
	// = replica * numWrites
	// = total writes to all hosts
	// We need to pool:
	// = replica * (numWrites / writeBatchSize)
	// = number of batch request structs to pool
	// For purposes of simplifying the options for pooling the write op pool size
	// represents the number of ops to pool not including replication, this is due
	// to the fact that the ops are shared between the different host queue replicas.
	totalBatches := topologyMap.Replicas() *
		int(math.Ceil(float64(s.opts.GetWriteOpPoolSize())/float64(s.opts.GetWriteBatchSize())))
	hostBatches := int(math.Ceil(float64(totalBatches) / float64(topologyMap.HostsLen())))
	writeBatchRequestPool := newWriteBatchRequestPool(hostBatches)
	writeRequestArrayPool := newWriteRequestArrayPool(hostBatches, s.opts.GetWriteBatchSize())
	return s.newHostQueueFn(host, writeBatchRequestPool, writeRequestArrayPool, s.opts)
}

func (s *session) setWriteOpPoolsWithLock(topologyMap m3db.TopologyMap) {
	totalShards := len(topologyMap.ShardScheme().All().Shards())
	if len(s.writeOpPools) == totalShards {
		// Already created all pools
		return
	}

	s.writeOpPools = make([]writeOpPool, totalShards)
	totalSize := s.opts.GetWriteOpPoolSize()
	eachSize := totalSize / totalShards
	for i := range s.writeOpPools {
		s.writeOpPools[i] = newWriteOpPool(eachSize)
	}
}

func (s *session) Write(id string, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	var (
		w             *writeOp
		pool          writeOpPool
		wg            sync.WaitGroup
		enqueueErr    error
		enqueued      int
		resultErrLock sync.Mutex
		resultErr     error
		resultErrs    int
		quorum        int
	)

	timeType, timeTypeErr := convert.UnitToTimeType(unit)
	if timeTypeErr != nil {
		return timeTypeErr
	}

	ts, tsErr := convert.TimeToValue(t, timeType)
	if tsErr != nil {
		return tsErr
	}

	s.RLock()
	routeErr := s.topoMap.RouteForEach(id, func(shard uint32) {
		pool = s.writeOpPools[shard]
		w = pool.Get()
		w.request.ID = id
		w.datapoint.Value = value
		w.datapoint.Timestamp = ts
		w.datapoint.TimestampType = timeType
		w.datapoint.Annotation = annotation
		w.completionFn = func(result interface{}, err error) {
			if err != nil {
				resultErrLock.Lock()
				if resultErr == nil {
					resultErr = err
				}
				resultErrs++
				resultErrLock.Unlock()
			}
			wg.Done()
		}
	}, func(idx int, host m3db.Host) {
		wg.Add(1)
		enqueued++
		if err := s.queues[idx].Enqueue(w); err != nil && enqueueErr == nil {
			// NB(r): if this ever happens we have a code bug, once we are in the read lock
			// the current queues we are using should never be closed
			enqueueErr = err
		}
	})
	quorum = s.topoMap.QuorumReplicas()
	s.RUnlock()

	if routeErr != nil {
		return routeErr
	}

	if enqueueErr != nil {
		s.opts.GetLogger().Errorf("failed to enqueue write: %v", enqueueErr)
		return enqueueErr
	}

	// Wait for writes to complete
	wg.Wait()

	// Return write to pool
	pool.Put(w)

	if resultErrs > 0 {
		// Check consistency level satisfied
		level := s.opts.GetConsistencyLevel()
		success := enqueued - resultErrs
		reportErr := func() error {
			return fmt.Errorf(
				"failed to meet %s with %d/%d success, error[0]: %v",
				level.String(), success, enqueued, resultErr)
		}
		switch level {
		case m3db.ConsistencyLevelAll:
			return reportErr()
		case m3db.ConsistencyLevelQuorum:
			if success >= quorum {
				// Meets quorum
				break
			}
			return reportErr()
		case m3db.ConsistencyLevelOne:
			if success > 0 {
				// Meets one
				break
			}
			return reportErr()
		}
	}

	return nil
}

func (s *session) Close() error {
	s.Lock()
	if !s.opened || s.closed {
		s.Unlock()
		return nil
	}
	s.closed = true
	s.Unlock()

	for _, q := range s.queues {
		q.Close()
	}

	close(s.topoMapCh)
	return s.topo.Close()
}
