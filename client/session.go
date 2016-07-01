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
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	"github.com/m3db/m3db/pool"
	xtime "github.com/m3db/m3db/x/time"
)

const (
	clusterConnectWaitInterval = 10 * time.Millisecond
)

var (
	// ErrClusterConnectTimeout is raised when connecting to the cluster and
	// ensuring at least each partition has an up node with a connection to it
	ErrClusterConnectTimeout = errors.New("timed out establishing min connections to cluster")

	errSessionStateNotInitial = errors.New("session not in initial state")
	errSessionStateNotOpen    = errors.New("session not in open state")
)

type clientSession interface {
	m3db.Session

	// Open the client session
	Open() error
}

type session struct {
	sync.RWMutex

	opts                     m3db.ClientOptions
	level                    m3db.ConsistencyLevel
	nowFn                    m3db.NowFn
	newHostQueueFn           newHostQueueFn
	topo                     m3db.Topology
	topoMap                  m3db.TopologyMap
	topoMapCh                chan m3db.TopologyMap
	replicas                 int
	quorum                   int
	queues                   []hostQueue
	state                    state
	writeOpPool              writeOpPool
	fetchOpPool              fetchOpPool
	fetchOpArrayArrayPool    fetchOpArrayArrayPool
	iteratorArrayPool        m3db.IteratorArrayPool
	mixedReadersIteratorPool m3db.MixedReadersIteratorPool
	seriesIteratorPool       m3db.SeriesIteratorPool
	seriesIteratorArrayPool  m3db.SeriesIteratorArrayPool
	fetchBatchSize           int
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
		level:          opts.GetConsistencyLevel(),
		nowFn:          opts.GetNowFn(),
		newHostQueueFn: newHostQueue,
		topo:           topology,
		topoMapCh:      make(chan m3db.TopologyMap),
		fetchBatchSize: opts.GetFetchBatchSize(),
	}, nil
}

func (s *session) Open() error {
	s.Lock()
	if s.state != stateNotOpen {
		s.Unlock()
		return errSessionStateNotInitial
	}
	s.state = stateOpen
	// NB(r): Alloc pools that can take some time in Open, expectation is already
	// that Open will take some time.
	s.writeOpPool = newWriteOpPool(s.opts.GetWriteOpPoolSize())
	s.fetchOpPool = newFetchOpPool(s.opts.GetFetchOpPoolSize(), s.fetchBatchSize)
	s.seriesIteratorPool = pool.NewSeriesIteratorPool(s.opts.GetSeriesIteratorPoolSize())
	s.seriesIteratorPool.Init()
	s.seriesIteratorArrayPool = pool.NewSeriesIteratorArrayPool(s.opts.GetSeriesIteratorArrayPoolBuckets())
	s.seriesIteratorArrayPool.Init()
	s.Unlock()

	currTopologyMap := s.topo.GetAndSubscribe(s.topoMapCh)
	if err := s.setTopologyMap(currTopologyMap); err != nil {
		s.Lock()
		s.state = stateNotOpen
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
	s.topoMap = topologyMap
	prevReplicas := s.replicas
	s.replicas = topologyMap.Replicas()
	s.quorum = topologyMap.QuorumReplicas()
	if s.fetchOpArrayArrayPool == nil ||
		s.fetchOpArrayArrayPool.Entries() != len(queues) {
		s.fetchOpArrayArrayPool = newFetchOpArrayArrayPool(
			s.opts.GetFetchOpPoolSize(),
			len(queues),
			s.opts.GetFetchOpPoolSize()/s.opts.GetFetchBatchSize())
	}
	if s.iteratorArrayPool == nil ||
		prevReplicas != s.replicas {
		s.iteratorArrayPool = pool.NewIteratorArrayPool([]m3db.PoolBucket{
			m3db.PoolBucket{
				Capacity: s.replicas,
				Count:    s.opts.GetSeriesIteratorPoolSize(),
			},
		})
		s.iteratorArrayPool.Init()
	}
	if s.mixedReadersIteratorPool == nil ||
		prevReplicas != s.replicas {
		size := s.replicas * s.opts.GetSeriesIteratorPoolSize()
		s.mixedReadersIteratorPool = pool.NewMixedReadersIteratorPool(size)
		s.mixedReadersIteratorPool.Init(s.opts.GetMixedReadersIteratorAlloc())
	}
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
	hostQueue := s.newHostQueueFn(host, writeBatchRequestPool, writeRequestArrayPool, s.opts)
	hostQueue.Open()
	return hostQueue
}

func (s *session) Write(id string, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	var (
		wg            sync.WaitGroup
		enqueueErr    error
		enqueued      int
		resultErrLock sync.Mutex
		resultErr     error
		resultErrs    int
		quorum        int
		w             = s.writeOpPool.Get()
	)

	timeType, timeTypeErr := convert.UnitToTimeType(unit)
	if timeTypeErr != nil {
		return timeTypeErr
	}

	ts, tsErr := convert.TimeToValue(t, timeType)
	if tsErr != nil {
		return tsErr
	}

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

	s.RLock()
	quorum = s.quorum
	routeErr := s.topoMap.RouteForEach(id, func(idx int, host m3db.Host) {
		wg.Add(1)
		enqueued++
		if err := s.queues[idx].Enqueue(w); err != nil && enqueueErr == nil {
			// NB(r): if this ever happens we have a code bug, once we are in the read lock
			// the current queues we are using should never be closed
			enqueueErr = err
		}
	})
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
	s.writeOpPool.Put(w)

	return s.consistencyResult(quorum, enqueued, resultErrs, resultErr)
}

func (s *session) Fetch(id string, startInclusive, endExclusive time.Time) (m3db.SeriesIterator, error) {
	results, err := s.FetchAll([]string{id}, startInclusive, endExclusive)
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

func (s *session) FetchAll(ids []string, startInclusive, endExclusive time.Time) ([]m3db.SeriesIterator, error) {
	var (
		wg                sync.WaitGroup
		routeErr          error
		enqueueErr        error
		resultErrLock     sync.Mutex
		resultErr         error
		resultErrs        int
		replicas          int
		quorum            int
		fetchOpsByHostIdx [][]*fetchOp
	)

	rangeStart, tsErr := convert.TimeToValue(startInclusive, rpc.TimeType_UNIX_NANOSECONDS)
	if tsErr != nil {
		return nil, tsErr
	}

	rangeEnd, tsErr := convert.TimeToValue(endExclusive, rpc.TimeType_UNIX_NANOSECONDS)
	if tsErr != nil {
		return nil, tsErr
	}

	iters := s.seriesIteratorArrayPool.Get(len(ids))

	fetchOpsByHostIdx = s.fetchOpArrayArrayPool.Get()

	s.RLock()
	replicas = s.replicas
	quorum = s.quorum
	for idx := range ids {
		idx := idx

		var firstErr error
		enqueued := 0
		pending := int32(0)
		errs := int32(0)
		results := s.iteratorArrayPool.Get(replicas)

		wg.Add(1)
		completionFn := func(result interface{}, err error) {
			resultsIdx := atomic.AddInt32(&pending, -1)
			if err != nil {
				n := atomic.AddInt32(&errs, 1)
				if n == 1 {
					firstErr = err
				}
			} else {
				r := result.([]*rpc.Segments)
				// TODO(r): pool the slicesIter
				slicesIter := newReaderSliceOfSlicesIterator(r)
				mixedIter := s.mixedReadersIteratorPool.Get()
				mixedIter.Reset(slicesIter)
				results[resultsIdx] = mixedIter
			}
			if resultsIdx != 0 {
				// Requests still pending
				return
			}
			if err := s.consistencyResult(quorum, enqueued, int(errs), firstErr); err != nil {
				resultErrLock.Lock()
				if resultErr == nil {
					resultErr = err
				}
				resultErrs++
				resultErrLock.Unlock()
			} else {
				iters[idx] = s.seriesIteratorPool.Get()
				iters[idx].Reset(ids[idx], startInclusive, endExclusive, results)
			}
			wg.Done()
		}

		if err := s.topoMap.RouteForEach(ids[idx], func(idx int, host m3db.Host) {
			// Inc safely as this for each is sequential
			enqueued++
			pending++

			ops := fetchOpsByHostIdx[idx]

			var f *fetchOp
			if len(ops) > 0 {
				// Find the last and potentially current fetch op for this host
				f = ops[len(ops)-1]
			}
			if f == nil || f.Size() >= s.fetchBatchSize {
				// If no current fetch op or existing one is at batch capacity add one
				f = s.fetchOpPool.Get()
				ops = append(ops, f)
				f.request.RangeStart = rangeStart
				f.request.RangeEnd = rangeEnd
				f.request.RangeType = rpc.TimeType_UNIX_NANOSECONDS
			}

			// Append ID to this request
			f.append(ids[idx], completionFn)
		}); err != nil {
			routeErr = err
			break
		}
	}

	if routeErr != nil {
		s.RUnlock()
		return nil, routeErr
	}

	// Enqueue fetch ops
	for idx := range fetchOpsByHostIdx {
		for _, f := range fetchOpsByHostIdx[idx] {
			if err := s.queues[idx].Enqueue(f); err != nil && enqueueErr == nil {
				enqueueErr = err
			}
		}
	}
	s.RUnlock()

	if enqueueErr != nil {
		s.opts.GetLogger().Errorf("failed to enqueue fetch: %v", enqueueErr)
		return nil, enqueueErr
	}

	wg.Wait()

	// Return fetch ops to pool
	for _, ops := range fetchOpsByHostIdx {
		for _, op := range ops {
			s.fetchOpPool.Put(op)
		}
	}

	// Return fetch ops array array to pool
	s.fetchOpArrayArrayPool.Put(fetchOpsByHostIdx)

	if resultErr != nil {
		return nil, resultErr
	}
	return iters, nil
}

func (s *session) consistencyResult(quorum, enqueued, resultErrs int, resultErr error) error {
	if resultErrs == 0 {
		return nil
	}

	// Check consistency level satisfied
	success := enqueued - resultErrs
	reportErr := func() error {
		return fmt.Errorf(
			"failed to meet %s with %d/%d success, error[0]: %v",
			s.level.String(), success, enqueued, resultErr)
	}

	switch s.level {
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

	return nil
}

func (s *session) Close() error {
	s.Lock()
	if s.state != stateOpen {
		s.Unlock()
		return errSessionStateNotOpen
	}
	s.state = stateClosed
	s.Unlock()

	for _, q := range s.queues {
		q.Close()
	}

	close(s.topoMapCh)
	return s.topo.Close()
}
