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
	xerrors "github.com/m3db/m3db/x/errors"
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

// clientSession adds methods not exposed to users of the client
type clientSession interface {
	m3db.Session

	// Open the client session
	Open() error
}

type session struct {
	sync.RWMutex

	opts                            m3db.ClientOptions
	level                           m3db.ConsistencyLevel
	nowFn                           m3db.NowFn
	newHostQueueFn                  newHostQueueFn
	topo                            m3db.Topology
	topoMap                         m3db.TopologyMap
	topoMapCh                       chan m3db.TopologyMap
	replicas                        int
	majority                        int
	queues                          []hostQueue
	state                           state
	writeOpPool                     writeOpPool
	fetchBatchOpPool                fetchBatchOpPool
	fetchBatchOpArrayArrayPool      fetchBatchOpArrayArrayPool
	iteratorArrayPool               m3db.IteratorArrayPool
	readerSliceOfSlicesIteratorPool readerSliceOfSlicesIteratorPool
	multiReaderIteratorPool         m3db.MultiReaderIteratorPool
	seriesIteratorPool              m3db.SeriesIteratorPool
	seriesIteratorsPool             m3db.MutableSeriesIteratorsPool
	fetchBatchSize                  int
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
	// NB(r): Alloc pools that can take some time in Open, expectation
	// is already that Open will take some time
	s.writeOpPool = newWriteOpPool(s.opts.GetWriteOpPoolSize())
	s.writeOpPool.Init()
	s.fetchBatchOpPool = newFetchBatchOpPool(s.opts.GetFetchBatchOpPoolSize(), s.fetchBatchSize)
	s.fetchBatchOpPool.Init()
	s.seriesIteratorPool = pool.NewSeriesIteratorPool(s.opts.GetSeriesIteratorPoolSize())
	s.seriesIteratorPool.Init()
	s.seriesIteratorsPool = pool.NewMutableSeriesIteratorsPool(s.opts.GetSeriesIteratorArrayPoolBuckets())
	s.seriesIteratorsPool.Init()
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
	replicas := topologyMap.Replicas()
	majority := topologyMap.MajorityReplicas()
	connectConsistencyLevel := s.opts.GetClusterConnectConsistencyLevel()
	for {
		if s.nowFn().Sub(start) >= s.opts.GetClusterConnectTimeout() {
			for i := range queues {
				queues[i].Close()
			}
			return ErrClusterConnectTimeout
		}
		// Be optimistic
		clusterAvailable := true
		for _, shard := range shards {
			shardReplicasAvailable := 0
			routeErr := topologyMap.RouteShardForEach(shard, func(idx int, host m3db.Host) {
				if queues[idx].GetConnectionCount() >= minConnectionCount {
					shardReplicasAvailable++
				}
			})
			if routeErr != nil {
				return routeErr
			}
			var clusterAvailableForShard bool
			switch connectConsistencyLevel {
			case m3db.ConsistencyLevelAll:
				clusterAvailableForShard = shardReplicasAvailable == replicas
			case m3db.ConsistencyLevelMajority:
				clusterAvailableForShard = shardReplicasAvailable >= majority
			case m3db.ConsistencyLevelOne:
				clusterAvailableForShard = shardReplicasAvailable > 0
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

	s.Lock()
	prev := s.queues
	s.queues = queues
	s.topoMap = topologyMap
	prevReplicas := s.replicas
	s.replicas = replicas
	s.majority = majority
	if s.fetchBatchOpArrayArrayPool == nil ||
		s.fetchBatchOpArrayArrayPool.Entries() != len(queues) {
		s.fetchBatchOpArrayArrayPool = newFetchBatchOpArrayArrayPool(
			s.opts.GetFetchBatchOpPoolSize(),
			len(queues),
			s.opts.GetFetchBatchOpPoolSize()/len(queues))
		s.fetchBatchOpArrayArrayPool.Init()
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
	if s.readerSliceOfSlicesIteratorPool == nil ||
		prevReplicas != s.replicas {
		size := s.replicas * s.opts.GetSeriesIteratorPoolSize()
		s.readerSliceOfSlicesIteratorPool = newReaderSliceOfSlicesIteratorPool(size)
		s.readerSliceOfSlicesIteratorPool.Init()
	}
	if s.multiReaderIteratorPool == nil ||
		prevReplicas != s.replicas {
		size := s.replicas * s.opts.GetSeriesIteratorPoolSize()
		s.multiReaderIteratorPool = pool.NewMultiReaderIteratorPool(size)
		s.multiReaderIteratorPool.Init(s.opts.GetReaderIteratorAllocate())
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
	writeBatchRequestPool.Init()
	writeRequestArrayPool := newWriteRequestArrayPool(hostBatches, s.opts.GetWriteBatchSize())
	writeRequestArrayPool.Init()
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
		majority      int
	)

	timeType, timeTypeErr := convert.UnitToTimeType(unit)
	if timeTypeErr != nil {
		return timeTypeErr
	}

	ts, tsErr := convert.TimeToValue(t, timeType)
	if tsErr != nil {
		return tsErr
	}

	w := s.writeOpPool.Get()
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
	majority = s.majority
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

	return s.consistencyResult(majority, enqueued, resultErrs, resultErr)
}

func (s *session) Fetch(id string, startInclusive, endExclusive time.Time) (m3db.SeriesIterator, error) {
	results, err := s.FetchAll([]string{id}, startInclusive, endExclusive)
	if err != nil {
		return nil, err
	}
	mutableResults := results.(m3db.MutableSeriesIterators)
	iters := mutableResults.Iters()
	iter := iters[0]
	// Reset to zero so that when we close this results set the iter doesn't get closed
	mutableResults.Reset(0)
	mutableResults.Close()
	return iter, nil
}

func (s *session) FetchAll(ids []string, startInclusive, endExclusive time.Time) (m3db.SeriesIterators, error) {
	var (
		wg                     sync.WaitGroup
		routeErr               error
		enqueueErr             error
		resultErrLock          sync.Mutex
		resultErr              error
		resultErrs             int
		majority               int
		fetchBatchOpsByHostIdx [][]*fetchBatchOp
	)

	rangeStart, tsErr := convert.TimeToValue(startInclusive, rpc.TimeType_UNIX_NANOSECONDS)
	if tsErr != nil {
		return nil, tsErr
	}

	rangeEnd, tsErr := convert.TimeToValue(endExclusive, rpc.TimeType_UNIX_NANOSECONDS)
	if tsErr != nil {
		return nil, tsErr
	}

	iters := s.seriesIteratorsPool.Get(len(ids))
	iters.Reset(len(ids))

	fetchBatchOpsByHostIdx = s.fetchBatchOpArrayArrayPool.Get()

	// Defer cleanup
	defer func() {
		// Return fetch ops to pool
		for _, ops := range fetchBatchOpsByHostIdx {
			for _, op := range ops {
				s.fetchBatchOpPool.Put(op)
			}
		}

		// Return fetch ops array array to pool
		s.fetchBatchOpArrayArrayPool.Put(fetchBatchOpsByHostIdx)
	}()

	s.RLock()
	majority = s.majority
	for idx := range ids {
		idx := idx

		var (
			results  []m3db.Iterator
			enqueued int
			pending  int32
			success  int32
			firstErr error
			errs     int32
		)

		wg.Add(1)
		completionFn := func(result interface{}, err error) {
			if err != nil {
				n := atomic.AddInt32(&errs, 1)
				if n == 1 {
					firstErr = err
				}
			} else {
				slicesIter := s.readerSliceOfSlicesIteratorPool.Get()
				slicesIter.Reset(result.([]*rpc.Segments))
				multiIter := s.multiReaderIteratorPool.Get()
				multiIter.ResetSliceOfSlices(slicesIter)
				// Results is pre-allocated after creating fetch ops for this ID below
				iterIdx := atomic.AddInt32(&success, 1) - 1
				results[iterIdx] = multiIter
			}
			// NB(xichen): decrementing pending and checking remaining against zero must
			// come after incrementing success, otherwise we might end up passing results[:success]
			// to iter.Reset down below before setting the iterator in the results array,
			// which would cause a nil pointer exception.
			if remaining := atomic.AddInt32(&pending, -1); remaining != 0 {
				// Requests still pending
				return
			}
			if err := s.consistencyResult(majority, enqueued, int(errs), firstErr); err != nil {
				resultErrLock.Lock()
				if resultErr == nil {
					resultErr = err
				}
				resultErrs++
				resultErrLock.Unlock()
			} else {
				// This is the final result, its safe to load "success" now without atomic operation
				iter := s.seriesIteratorPool.Get()
				iter.Reset(ids[idx], startInclusive, endExclusive, results[:success])
				iters.SetAt(idx, iter)
			}
			wg.Done()

			// SeriesIterator has taken its own references to the results array after Reset
			s.iteratorArrayPool.Put(results)
		}

		if err := s.topoMap.RouteForEach(ids[idx], func(hostIdx int, host m3db.Host) {
			// Inc safely as this for each is sequential
			enqueued++
			pending++

			ops := fetchBatchOpsByHostIdx[hostIdx]

			var f *fetchBatchOp
			if len(ops) > 0 {
				// Find the last and potentially current fetch op for this host
				f = ops[len(ops)-1]
			}
			if f == nil || f.Size() >= s.fetchBatchSize {
				// If no current fetch op or existing one is at batch capacity add one
				f = s.fetchBatchOpPool.Get()
				fetchBatchOpsByHostIdx[hostIdx] = append(fetchBatchOpsByHostIdx[hostIdx], f)
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

		// Once we've enqueued we know how many to expect so retrieve and set length
		results = s.iteratorArrayPool.Get(enqueued)
		results = results[:enqueued]
	}

	if routeErr != nil {
		s.RUnlock()
		return nil, routeErr
	}

	// Enqueue fetch ops
	for idx := range fetchBatchOpsByHostIdx {
		for _, f := range fetchBatchOpsByHostIdx[idx] {
			if err := s.queues[idx].Enqueue(f); err != nil && enqueueErr == nil {
				enqueueErr = err
				break
			}
		}
		if enqueueErr != nil {
			break
		}
	}
	s.RUnlock()

	if enqueueErr != nil {
		s.opts.GetLogger().Errorf("failed to enqueue fetch: %v", enqueueErr)
		return nil, enqueueErr
	}

	wg.Wait()

	if resultErr != nil {
		return nil, resultErr
	}
	return iters, nil
}

func (s *session) consistencyResult(majority, enqueued, resultErrs int, resultErr error) error {
	if resultErrs == 0 {
		return nil
	}

	// Check consistency level satisfied
	success := enqueued - resultErrs
	reportErr := func() error {
		return xerrors.NewRenamedError(resultErr, fmt.Errorf(
			"failed to meet %s with %d/%d success, first error: %v",
			s.level.String(), success, enqueued, resultErr))
	}

	switch s.level {
	case m3db.ConsistencyLevelAll:
		return reportErr()
	case m3db.ConsistencyLevelMajority:
		if success >= majority {
			// Meets majority
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
