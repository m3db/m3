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
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"
)

const (
	clusterConnectWaitInterval           = 10 * time.Millisecond
	blocksMetadataInitialCapacity        = 64
	blocksMetadataChannelInitialCapacity = 4096
	gaugeReportInterval                  = 500 * time.Millisecond
)

var (
	// ErrClusterConnectTimeout is raised when connecting to the cluster and
	// ensuring at least each partition has an up node with a connection to it
	ErrClusterConnectTimeout = errors.New("timed out establishing min connections to cluster")
	// errSessionStateNotInitial is raised when trying to open a session and
	// its not in the initial clean state
	errSessionStateNotInitial = errors.New("session not in initial state")
	// errSessionStateNotOpen is raised when operations are requested when the
	// session is not in the open state
	errSessionStateNotOpen = errors.New("session not in open state")
	// errSessionBadBlockResultFromPeer is raised when there is a bad block
	// return from a peer when fetching blocks from peers
	errSessionBadBlockResultFromPeer = errors.New("session fetched bad block result from peer")
	// errSessionInvalidConnectClusterConnectConsistencyLevel is raised when
	// the connect consistency level specified is not recognized
	errSessionInvalidConnectClusterConnectConsistencyLevel = errors.New("session has invalid connect consistency level specified")
)

type session struct {
	sync.RWMutex

	opts                             Options
	scope                            tally.Scope
	nowFn                            clock.NowFn
	log                              xlog.Logger
	level                            topology.ConsistencyLevel
	newHostQueueFn                   newHostQueueFn
	topo                             topology.Topology
	topoMap                          topology.Map
	topoWatch                        topology.MapWatch
	replicas                         int32
	majority                         int32
	queues                           []hostQueue
	state                            state
	writeOpPool                      writeOpPool
	fetchBatchOpPool                 fetchBatchOpPool
	fetchBatchOpArrayArrayPool       fetchBatchOpArrayArrayPool
	iteratorArrayPool                encoding.IteratorArrayPool
	readerSliceOfSlicesIteratorPool  readerSliceOfSlicesIteratorPool
	multiReaderIteratorPool          encoding.MultiReaderIteratorPool
	seriesIteratorPool               encoding.SeriesIteratorPool
	seriesIteratorsPool              encoding.MutableSeriesIteratorsPool
	fetchBatchSize                   int
	newPeerBlocksQueueFn             newPeerBlocksQueueFn
	origin                           topology.Host
	streamBlocksWorkers              pool.WorkerPool
	streamBlocksReattemptWorkers     pool.WorkerPool
	streamBlocksResultsProcessors    chan struct{}
	streamBlocksBatchSize            int
	streamBlocksMetadataBatchTimeout time.Duration
	streamBlocksBatchTimeout         time.Duration
	metrics                          sessionMetrics
}

type sessionMetrics struct {
	sync.RWMutex
	streamFromPeersMetrics map[uint32]streamFromPeersMetrics
}

type streamFromPeersMetrics struct {
	fetchBlocksFromPeers      tally.Gauge
	metadataFetches           tally.Gauge
	metadataFetchBatchCall    tally.Counter
	metadataFetchBatchSuccess tally.Counter
	metadataFetchBatchError   tally.Counter
	metadataReceived          tally.Counter
	blocksEnqueueChannel      tally.Gauge
}

type newHostQueueFn func(
	host topology.Host,
	writeBatchRawRequestPool writeBatchRawRequestPool,
	writeBatchRawRequestElementArrayPool writeBatchRawRequestElementArrayPool,
	opts Options,
) hostQueue

func newSession(opts Options) (clientSession, error) {
	topo, err := opts.TopologyInitializer().Init()
	if err != nil {
		return nil, err
	}

	s := &session{
		opts:                 opts,
		scope:                opts.InstrumentOptions().MetricsScope(),
		nowFn:                opts.ClockOptions().NowFn(),
		log:                  opts.InstrumentOptions().Logger(),
		level:                opts.ConsistencyLevel(),
		newHostQueueFn:       newHostQueue,
		topo:                 topo,
		fetchBatchSize:       opts.FetchBatchSize(),
		newPeerBlocksQueueFn: newPeerBlocksQueue,
		metrics: sessionMetrics{
			streamFromPeersMetrics: make(map[uint32]streamFromPeersMetrics),
		},
	}

	if opts, ok := opts.(AdminOptions); ok {
		s.origin = opts.Origin()
		s.streamBlocksWorkers = pool.NewWorkerPool(opts.FetchSeriesBlocksBatchConcurrency())
		s.streamBlocksWorkers.Init()
		s.streamBlocksReattemptWorkers = pool.NewWorkerPool(opts.FetchSeriesBlocksBatchConcurrency())
		s.streamBlocksReattemptWorkers.Init()
		processors := opts.FetchSeriesBlocksResultsProcessors()
		// NB(r): We use a list of tokens here instead of a worker pool for bounding
		// the stream blocks results processors as we require the processing of the
		// response to be synchronous in relation to the caller, which is the peer
		// queue. This is required because after executing the stream blocks batch request,
		// which is triggered from the peer queue, we decrement the outstanding work
		// and we cannot let this fall to zero until there is absolutely no work left.
		// This is because we sample the queue length and if it's ever zero we consider
		// streaming blocks to be finished.
		s.streamBlocksResultsProcessors = make(chan struct{}, processors)
		for i := 0; i < processors; i++ {
			s.streamBlocksResultsProcessors <- struct{}{}
		}
		s.streamBlocksBatchSize = opts.FetchSeriesBlocksBatchSize()
		s.streamBlocksMetadataBatchTimeout = opts.FetchSeriesBlocksMetadataBatchTimeout()
		s.streamBlocksBatchTimeout = opts.FetchSeriesBlocksBatchTimeout()
	}

	return s, nil
}

func (s *session) streamFromPeersMetricsForShard(shard uint32) *streamFromPeersMetrics {
	s.metrics.RLock()
	m, ok := s.metrics.streamFromPeersMetrics[shard]
	s.metrics.RUnlock()

	if ok {
		return &m
	}

	scope := s.opts.InstrumentOptions().MetricsScope()

	s.metrics.Lock()
	m, ok = s.metrics.streamFromPeersMetrics[shard]
	if ok {
		s.metrics.Unlock()
		return &m
	}
	scope = scope.SubScope("stream-from-peers").Tagged(map[string]string{
		"shard": fmt.Sprintf("%d", shard),
	})
	m = streamFromPeersMetrics{
		fetchBlocksFromPeers:      scope.Gauge("fetch-blocks-inprogress"),
		metadataFetches:           scope.Gauge("fetch-metadata-peers-inprogress"),
		metadataFetchBatchCall:    scope.Counter("fetch-metadata-peers-batch-call"),
		metadataFetchBatchSuccess: scope.Counter("fetch-metadata-peers-batch-success"),
		metadataFetchBatchError:   scope.Counter("fetch-metadata-peers-batch-error"),
		metadataReceived:          scope.Counter("fetch-metadata-peers-received"),
		blocksEnqueueChannel:      scope.Gauge("fetch-blocks-enqueue-channel-length"),
	}
	s.metrics.streamFromPeersMetrics[shard] = m
	s.metrics.Unlock()
	return &m
}

func (s *session) Open() error {
	s.Lock()
	if s.state != stateNotOpen {
		s.Unlock()
		return errSessionStateNotInitial
	}

	topologyWatch, err := s.topo.Watch()
	if err != nil {
		s.Unlock()
		return err
	}

	// Wait for the topology to be available
	<-topologyWatch.C()

	topologyMap := topologyWatch.Get()

	queues, replicas, majority, err := s.initHostQueues(topologyMap)
	if err != nil {
		s.Unlock()
		return err
	}
	s.setTopologyWithLock(topologyMap, queues, replicas, majority)
	s.topoWatch = topologyWatch

	// NB(r): Alloc pools that can take some time in Open, expectation
	// is already that Open will take some time
	writeOpPoolOpts := pool.NewObjectPoolOptions().
		SetSize(s.opts.WriteOpPoolSize()).
		SetMetricsScope(s.scope.SubScope("write-op-pool"))
	s.writeOpPool = newWriteOpPool(writeOpPoolOpts)
	s.writeOpPool.Init()
	fetchBatchOpPoolOpts := pool.NewObjectPoolOptions().
		SetSize(s.opts.FetchBatchOpPoolSize()).
		SetMetricsScope(s.scope.SubScope("fetch-batch-op-pool"))
	s.fetchBatchOpPool = newFetchBatchOpPool(fetchBatchOpPoolOpts, s.fetchBatchSize)
	s.fetchBatchOpPool.Init()
	seriesIteratorPoolOpts := pool.NewObjectPoolOptions().
		SetSize(s.opts.SeriesIteratorPoolSize()).
		SetMetricsScope(s.scope.SubScope("series-iterator-pool"))
	s.seriesIteratorPool = encoding.NewSeriesIteratorPool(seriesIteratorPoolOpts)
	s.seriesIteratorPool.Init()
	s.seriesIteratorsPool = encoding.NewMutableSeriesIteratorsPool(s.opts.SeriesIteratorArrayPoolBuckets())
	s.seriesIteratorsPool.Init()
	s.state = stateOpen
	s.Unlock()

	go func() {
		for range s.topoWatch.C() {
			s.log.Info("received update for topology")
			topologyMap := s.topoWatch.Get()
			queues, replicas, majority, err := s.initHostQueues(topologyMap)
			if err != nil {
				s.log.Errorf("could not update topology map: %v", err)
				continue
			}
			s.Lock()
			s.setTopologyWithLock(topologyMap, queues, replicas, majority)
			s.Unlock()
		}
	}()

	return nil
}

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
			for i := range queues {
				queues[i].Close()
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

func (s *session) setTopologyWithLock(topologyMap topology.Map, queues []hostQueue, replicas, majority int) {
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
			SetMetricsScope(s.scope.SubScope("fetch-batch-op-array-array-pool"))
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
			SetMetricsScope(s.scope.SubScope("reader-slice-of-slices-iterator-pool"))
		s.readerSliceOfSlicesIteratorPool = newReaderSliceOfSlicesIteratorPool(poolOpts)
		s.readerSliceOfSlicesIteratorPool.Init()
	}
	if s.multiReaderIteratorPool == nil ||
		prevReplicas != s.replicas {
		size := replicas * s.opts.SeriesIteratorPoolSize()
		poolOpts := pool.NewObjectPoolOptions().
			SetSize(size).
			SetMetricsScope(s.scope.SubScope("multi-reader-iterator-pool"))
		s.multiReaderIteratorPool = encoding.NewMultiReaderIteratorPool(poolOpts)
		s.multiReaderIteratorPool.Init(s.opts.ReaderIteratorAllocate())
	}

	// Asynchronously close the previous set of host queues
	go func() {
		for i := range prev {
			prev[i].Close()
		}
	}()

	s.log.Infof("successfully updated topology to %d hosts", topologyMap.HostsLen())
}

func (s *session) newHostQueue(host topology.Host, topologyMap topology.Map) hostQueue {
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
		int(math.Ceil(float64(s.opts.WriteOpPoolSize())/float64(s.opts.WriteBatchSize())))
	hostBatches := int(math.Ceil(float64(totalBatches) / float64(topologyMap.HostsLen())))
	writeBatchRequestPoolOpts := pool.NewObjectPoolOptions().
		SetSize(hostBatches).
		SetMetricsScope(s.scope.SubScope("write-batch-request-pool"))
	writeBatchRequestPool := newWriteBatchRawRequestPool(writeBatchRequestPoolOpts)
	writeBatchRequestPool.Init()
	writeBatchRawRequestElementArrayPoolOpts := pool.NewObjectPoolOptions().
		SetSize(hostBatches).
		SetMetricsScope(s.scope.SubScope("id-datapoint-array-pool"))
	writeBatchRawRequestElementArrayPool := newWriteBatchRawRequestElementArrayPool(
		writeBatchRawRequestElementArrayPoolOpts, s.opts.WriteBatchSize())
	writeBatchRawRequestElementArrayPool.Init()
	hostQueue := s.newHostQueueFn(host, writeBatchRequestPool, writeBatchRawRequestElementArrayPool, s.opts)
	hostQueue.Open()
	return hostQueue
}

func (s *session) Write(namespace, id string, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	var (
		wg            sync.WaitGroup
		wgIsDone      int32
		enqueueErr    error
		enqueued      int32
		pending       int32
		resultErrLock sync.RWMutex
		resultErrs    int32
		errors        []error
		majority      int32
		success       int32
		tsID          = ts.StringID(id)
	)

	timeType, timeTypeErr := convert.ToTimeType(unit)
	if timeTypeErr != nil {
		return timeTypeErr
	}

	timestamp, timestampErr := convert.ToValue(t, timeType)
	if timestampErr != nil {
		return timestampErr
	}

	wg.Add(1)

	majority = atomic.LoadInt32(&s.majority)

	w := s.writeOpPool.Get()
	w.namespace = ts.StringID(namespace)
	w.request.ID = tsID.Data()
	w.request.Datapoint.Value = value
	w.request.Datapoint.Timestamp = timestamp
	w.request.Datapoint.TimestampType = timeType
	w.request.Datapoint.Annotation = annotation
	w.completionFn = func(result interface{}, err error) {
		var snapshotSuccess int32
		if err != nil {
			atomic.AddInt32(&resultErrs, 1)
			resultErrLock.Lock()
			errors = append(errors, err)
			resultErrLock.Unlock()
		} else {
			snapshotSuccess = atomic.AddInt32(&success, 1)
		}
		remaining := atomic.AddInt32(&pending, -1)
		doneAll := remaining == 0
		switch s.level {
		case topology.ConsistencyLevelOne:
			complete := snapshotSuccess > 0 || doneAll
			if complete && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
				wg.Done()
			}
		case topology.ConsistencyLevelMajority:
			complete := snapshotSuccess >= majority || doneAll
			if complete && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
				wg.Done()
			}
		case topology.ConsistencyLevelAll:
			if doneAll && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
				wg.Done()
			}
		}
		// Finalize resources that can be returned
		if doneAll {
			// Return write to pool
			s.writeOpPool.Put(w)
		}
	}

	s.RLock()
	routeErr := s.topoMap.RouteForEach(tsID, func(idx int, host topology.Host) {
		// First count all the pending write requests to ensure
		// we count the amount we're going to be waiting for
		// before we enqueue the completion fns that rely on having
		// an accurate number of the pending requests when they execute
		pending++
	})
	if routeErr == nil {
		// Now enqueue the write requests
		routeErr = s.topoMap.RouteForEach(tsID, func(idx int, host topology.Host) {
			enqueued++
			if err := s.queues[idx].Enqueue(w); err != nil && enqueueErr == nil {
				// NB(r): if this ever happens we have a code bug, once we
				// are in the read lock the current queues we are using should
				// never be closed
				enqueueErr = err
			}
		})
	}
	s.RUnlock()

	if routeErr != nil {
		return routeErr
	}

	if enqueueErr != nil {
		s.opts.InstrumentOptions().Logger().Errorf("failed to enqueue write: %v", enqueueErr)
		return enqueueErr
	}

	// Wait for writes to complete
	wg.Wait()

	var reportErrors []error
	errsLen := atomic.LoadInt32(&resultErrs)
	if errsLen > 0 {
		resultErrLock.RLock()
		reportErrors = errors[:]
		resultErrLock.RUnlock()
	}
	responded := enqueued - atomic.LoadInt32(&pending)
	return s.consistencyResult(majority, enqueued, responded, errsLen, reportErrors)
}

func (s *session) Fetch(namespace string, id string, startInclusive, endExclusive time.Time) (encoding.SeriesIterator, error) {
	results, err := s.FetchAll(namespace, []string{id}, startInclusive, endExclusive)
	if err != nil {
		return nil, err
	}
	mutableResults := results.(encoding.MutableSeriesIterators)
	iters := mutableResults.Iters()
	iter := iters[0]
	// Reset to zero so that when we close this results set the iter doesn't get closed
	mutableResults.Reset(0)
	mutableResults.Close()
	return iter, nil
}

func (s *session) FetchAll(namespace string, ids []string, startInclusive, endExclusive time.Time) (encoding.SeriesIterators, error) {
	var (
		wg                     sync.WaitGroup
		allPending             int32
		routeErr               error
		enqueueErr             error
		resultErrLock          sync.RWMutex
		resultErr              error
		resultErrs             int32
		majority               int32
		fetchBatchOpsByHostIdx [][]*fetchBatchOp
		nsID                   = []byte(namespace)
	)

	rangeStart, tsErr := convert.ToValue(startInclusive, rpc.TimeType_UNIX_NANOSECONDS)
	if tsErr != nil {
		return nil, tsErr
	}

	rangeEnd, tsErr := convert.ToValue(endExclusive, rpc.TimeType_UNIX_NANOSECONDS)
	if tsErr != nil {
		return nil, tsErr
	}

	iters := s.seriesIteratorsPool.Get(len(ids))
	iters.Reset(len(ids))

	fetchBatchOpsByHostIdx = s.fetchBatchOpArrayArrayPool.Get()

	majority = atomic.LoadInt32(&s.majority)

	s.RLock()
	for idx := range ids {
		idx := idx

		var (
			wgIsDone int32
			// NB(xichen): resultsAccessors gets initialized to number of replicas + 1 before enqueuing
			// (incremented when iterating over the replicas for this ID), and gets decremented for each
			// replica as well as inside the allCompletionFn so we know when resultsAccessors is 0,
			// results are no longer accessed and it's safe to return results to the pool.
			resultsAccessors int32 = 1
			resultsLock      sync.RWMutex
			results          []encoding.Iterator
			enqueued         int32
			pending          int32
			success          int32
			errors           []error
			errs             int32
		)

		wg.Add(1)
		allCompletionFn := func() {
			var reportErrors []error
			errsLen := atomic.LoadInt32(&errs)
			if errsLen > 0 {
				resultErrLock.RLock()
				reportErrors = errors[:]
				resultErrLock.RUnlock()
			}
			responded := enqueued - atomic.LoadInt32(&pending)
			if err := s.consistencyResult(majority, enqueued, responded, errsLen, reportErrors); err != nil {
				resultErrLock.Lock()
				if resultErr == nil {
					resultErr = err
				}
				resultErrs++
				resultErrLock.Unlock()
			} else {
				resultsLock.RLock()
				successIters := results[:success]
				resultsLock.RUnlock()
				iter := s.seriesIteratorPool.Get()
				iter.Reset(ids[idx], startInclusive, endExclusive, successIters)
				iters.SetAt(idx, iter)
			}
			if atomic.AddInt32(&resultsAccessors, -1) == 0 {
				s.iteratorArrayPool.Put(results)
			}
			wg.Done()
		}
		completionFn := func(result interface{}, err error) {
			var snapshotSuccess int32
			if err != nil {
				n := atomic.AddInt32(&errs, 1)
				if n == 1 {
					// NB(r): reuse the error lock here as we do not want to create
					// a whole lot of locks for every single ID fetched due to size
					// of mutex being non-trivial and likely to cause more stack growth
					// or GC pressure if ends up on heap which is likely due to naive
					// escape analysis.
					resultErrLock.Lock()
					errors = append(errors, err)
					resultErrLock.Unlock()
				}
			} else {
				slicesIter := s.readerSliceOfSlicesIteratorPool.Get()
				slicesIter.Reset(result.([]*rpc.Segments))
				multiIter := s.multiReaderIteratorPool.Get()
				multiIter.ResetSliceOfSlices(slicesIter)
				// Results is pre-allocated after creating fetch ops for this ID below
				resultsLock.Lock()
				results[success] = multiIter
				success++
				snapshotSuccess = success
				resultsLock.Unlock()
			}
			// NB(xichen): decrementing pending and checking remaining against zero must
			// come after incrementing success, otherwise we might end up passing results[:success]
			// to iter.Reset down below before setting the iterator in the results array,
			// which would cause a nil pointer exception.
			remaining := atomic.AddInt32(&pending, -1)
			doneAll := remaining == 0
			switch s.level {
			case topology.ConsistencyLevelOne:
				complete := snapshotSuccess > 0 || doneAll
				if complete && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
					allCompletionFn()
				}
			case topology.ConsistencyLevelMajority:
				complete := snapshotSuccess >= majority || doneAll
				if complete && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
					allCompletionFn()
				}
			case topology.ConsistencyLevelAll:
				if doneAll && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
					allCompletionFn()
				}
			}

			if atomic.AddInt32(&resultsAccessors, -1) == 0 {
				s.iteratorArrayPool.Put(results)
			}

			allRemaining := atomic.AddInt32(&allPending, -1)
			if allRemaining == 0 {
				// Return fetch ops to pool
				for _, ops := range fetchBatchOpsByHostIdx {
					for _, op := range ops {
						s.fetchBatchOpPool.Put(op)
					}
				}

				// Return fetch ops array array to pool
				s.fetchBatchOpArrayArrayPool.Put(fetchBatchOpsByHostIdx)
			}
		}

		tsID := ts.StringID(ids[idx])

		if err := s.topoMap.RouteForEach(tsID, func(hostIdx int, host topology.Host) {
			// Inc safely as this for each is sequential
			enqueued++
			pending++
			allPending++
			resultsAccessors++

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

			// Append IDWithNamespace to this request
			f.append(nsID, tsID.Data(), completionFn)
		}); err != nil {
			routeErr = err
			break
		}

		// Once we've enqueued we know how many to expect so retrieve and set length
		results = s.iteratorArrayPool.Get(int(enqueued))
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
		s.log.Errorf("failed to enqueue fetch: %v", enqueueErr)
		return nil, enqueueErr
	}

	wg.Wait()

	resultErrLock.RLock()
	retErr := resultErr
	resultErrLock.RUnlock()
	if retErr != nil {
		return nil, retErr
	}
	return iters, nil
}

func (s *session) consistencyResult(
	majority, enqueued, responded, resultErrs int32,
	errors []error,
) error {
	if resultErrs == 0 {
		return nil
	}

	// Check consistency level satisfied
	success := enqueued - resultErrs
	reportErr := func() error {
		// NB(r): if any errors are bad request errors, encapsulate that error
		// to ensure the error itself is wholly classified as a bad request error.
		topLevelErr := errors[0]
		for i := 1; i < len(errors); i++ {
			if IsBadRequestError(errors[i]) {
				topLevelErr = errors[i]
				break
			}
		}
		return xerrors.NewRenamedError(topLevelErr, fmt.Errorf(
			"failed to meet %s with %d/%d success, %d nodes responded, errors: %v",
			s.level.String(), success, enqueued, responded, errors))
	}

	switch s.level {
	case topology.ConsistencyLevelAll:
		return reportErr()
	case topology.ConsistencyLevelMajority:
		if success >= majority {
			// Meets majority
			break
		}
		return reportErr()
	case topology.ConsistencyLevelOne:
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

	s.topoWatch.Close()
	s.topo.Close()
	return nil
}

func (s *session) Origin() topology.Host {
	return s.origin
}

func (s *session) Replicas() int {
	return int(atomic.LoadInt32(&s.replicas))
}

func (s *session) Truncate(namespace ts.ID) (int64, error) {
	var (
		wg            sync.WaitGroup
		enqueueErr    xerrors.MultiError
		resultErrLock sync.Mutex
		resultErr     xerrors.MultiError
		truncated     int64
	)

	t := &truncateOp{}
	t.request.NameSpace = namespace.Data()
	t.completionFn = func(result interface{}, err error) {
		if err != nil {
			resultErrLock.Lock()
			resultErr = resultErr.Add(err)
			resultErrLock.Unlock()
		} else {
			res := result.(*rpc.TruncateResult_)
			atomic.AddInt64(&truncated, res.NumSeries)
		}
		wg.Done()
	}

	s.RLock()
	for idx := range s.queues {
		wg.Add(1)
		if err := s.queues[idx].Enqueue(t); err != nil {
			wg.Done()
			enqueueErr = enqueueErr.Add(err)
		}
	}
	s.RUnlock()

	if err := enqueueErr.FinalError(); err != nil {
		s.log.Errorf("failed to enqueue request: %v", err)
		return 0, err
	}

	// Wait for namespace to be truncated on all replicas
	wg.Wait()

	return truncated, resultErr.FinalError()
}

func (s *session) peersForShard(shard uint32) ([]hostQueue, error) {
	s.RLock()
	peers := make([]hostQueue, 0, s.topoMap.Replicas()-1)
	err := s.topoMap.RouteShardForEach(shard, func(idx int, host topology.Host) {
		if s.origin != nil && s.origin.ID() == host.ID() {
			// Don't include the origin host
			return
		}
		peers = append(peers, s.queues[idx])
	})
	s.RUnlock()
	if err != nil {
		return nil, err
	}
	return peers, nil
}

func (s *session) FetchBlocksMetadataFromPeers(
	namespace ts.ID,
	shard uint32,
	start, end time.Time,
) (PeerBlocksMetadataIter, error) {
	peers, err := s.peersForShard(shard)
	if err != nil {
		return nil, err
	}

	var (
		metadataCh = make(chan blocksMetadata, blocksMetadataChannelInitialCapacity)
		errCh      = make(chan error, 1)
		m          = s.streamFromPeersMetricsForShard(shard)
	)

	go func() {
		errCh <- s.streamBlocksMetadataFromPeers(namespace, shard, peers,
			start, end, metadataCh, m)
		close(metadataCh)
		close(errCh)
	}()

	return newMetadataIter(metadataCh, errCh), nil
}

func (s *session) FetchBootstrapBlocksFromPeers(
	namespace ts.ID,
	shard uint32,
	start, end time.Time,
	opts bootstrap.Options,
) (bootstrap.ShardResult, error) {
	var (
		result   = newBlocksResult(s.opts, opts, s.multiReaderIteratorPool)
		complete = int64(0)
		doneCh   = make(chan error, 1)
		onDone   = func(err error) {
			atomic.StoreInt64(&complete, 1)
			select {
			case doneCh <- err:
			default:
			}
		}
		waitDone = func() error {
			return <-doneCh
		}
		m = s.streamFromPeersMetricsForShard(shard)
	)

	peers, err := s.peersForShard(shard)
	if err != nil {
		return nil, err
	}

	go func() {
		for atomic.LoadInt64(&complete) == 0 {
			m.fetchBlocksFromPeers.Update(1)
			time.Sleep(gaugeReportInterval)
		}
		m.fetchBlocksFromPeers.Update(0)
	}()

	// Begin pulling metadata, if one or multiple peers fail no error will
	// be returned from this routine as long as one peer succeeds completely
	metadataCh := make(chan blocksMetadata, 4096)
	go func() {
		err := s.streamBlocksMetadataFromPeers(namespace, shard, peers,
			start, end, metadataCh, m)

		close(metadataCh)
		if err != nil {
			// Bail early
			onDone(err)
		}
	}()

	// Begin consuming metadata and making requests
	go func() {
		err := s.streamBlocksFromPeers(namespace, shard, peers,
			metadataCh, opts, result, m)
		onDone(err)
	}()

	if err := waitDone(); err != nil {
		return nil, err
	}
	return result.result, nil
}

func (s *session) streamBlocksMetadataFromPeers(
	namespace ts.ID,
	shard uint32,
	peers []hostQueue,
	start, end time.Time,
	ch chan<- blocksMetadata,
	m *streamFromPeersMetrics,
) error {
	var (
		wg       sync.WaitGroup
		errLock  sync.Mutex
		errLen   int
		pending  int64
		multiErr = xerrors.NewMultiError()
	)

	pending = int64(len(peers))
	m.metadataFetches.Update(pending)
	for _, peer := range peers {
		peer := peer

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.streamBlocksMetadataFromPeer(namespace, shard, peer,
				start, end, ch, m)
			if err != nil {
				errLock.Lock()
				defer errLock.Unlock()
				errLen++
				multiErr = multiErr.Add(err)
			}
			m.metadataFetches.Update(atomic.AddInt64(&pending, -1))
		}()
	}

	wg.Wait()

	if errLen == len(peers) {
		return multiErr.FinalError()
	}
	return nil
}

func (s *session) streamBlocksMetadataFromPeer(
	namespace ts.ID,
	shard uint32,
	peer hostQueue,
	start, end time.Time,
	ch chan<- blocksMetadata,
	m *streamFromPeersMetrics,
) error {
	var (
		pageToken *int64
		retrier   = xretry.NewRetrier(xretry.NewOptions().
				SetBackoffFactor(2).
				SetMax(3).
				SetInitialBackoff(time.Second).
				SetJitter(true))
		optionIncludeSizes     = true
		optionIncludeChecksums = true
		moreResults            = true
	)
	// Declare before loop to avoid redeclaring each iteration
	attemptFn := func() error {
		client, err := peer.ConnectionPool().NextClient()
		if err != nil {
			return err
		}

		tctx, _ := thrift.NewContext(s.streamBlocksMetadataBatchTimeout)
		req := rpc.NewFetchBlocksMetadataRawRequest()
		req.NameSpace = namespace.Data()
		req.Shard = int32(shard)
		req.RangeStart = start.UnixNano()
		req.RangeEnd = end.UnixNano()
		req.Limit = int64(s.streamBlocksBatchSize)
		req.PageToken = pageToken
		req.IncludeSizes = &optionIncludeSizes
		req.IncludeChecksums = &optionIncludeChecksums

		m.metadataFetchBatchCall.Inc(1)
		result, err := client.FetchBlocksMetadataRaw(tctx, req)
		if err != nil {
			m.metadataFetchBatchError.Inc(1)
			return err
		}

		m.metadataFetchBatchSuccess.Inc(1)
		m.metadataReceived.Inc(int64(len(result.Elements)))

		if result.NextPageToken != nil {
			// Create space on the heap for the page token and take it's
			// address to avoid having to keep the entire result around just
			// for the page token
			resultPageToken := *result.NextPageToken
			pageToken = &resultPageToken
		} else {
			// No further results
			moreResults = false
		}

		for _, elem := range result.Elements {
			blockMetas := make([]blockMetadata, 0, len(elem.Blocks))
			for _, b := range elem.Blocks {
				blockStart := time.Unix(0, b.Start)

				if b.Err != nil {
					// Error occurred retrieving block metadata, use default values
					blockMetas = append(blockMetas, blockMetadata{
						start: blockStart,
					})
					continue
				}

				if b.Size == nil {
					s.log.WithFields(
						xlog.NewLogField("id", elem.ID),
						xlog.NewLogField("start", blockStart),
					).Warnf("stream blocks metadata requested block size and not returned")
					continue
				}

				var pChecksum *uint32
				if b.Checksum != nil {
					value := uint32(*b.Checksum)
					pChecksum = &value
				}

				blockMetas = append(blockMetas, blockMetadata{
					start:    blockStart,
					size:     *b.Size,
					checksum: pChecksum,
				})
			}
			ch <- blocksMetadata{
				peer:   peer,
				id:     ts.BinaryID(elem.ID),
				blocks: blockMetas,
			}
		}

		return nil
	}
	for moreResults {
		if err := retrier.Attempt(attemptFn); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) streamBlocksFromPeers(
	namespace ts.ID,
	shard uint32,
	peers []hostQueue,
	ch <-chan blocksMetadata,
	opts bootstrap.Options,
	result *blocksResult,
	m *streamFromPeersMetrics,
) error {
	var (
		retrier = xretry.NewRetrier(xretry.NewOptions().
			SetBackoffFactor(2).
			SetMax(3).
			SetInitialBackoff(time.Second).
			SetJitter(true))
		enqueueCh           = newEnqueueChannel(m)
		peerBlocksBatchSize = s.streamBlocksBatchSize
	)

	// Consume the incoming metadata and enqueue to the ready channel
	go func() {
		s.streamCollectedBlocksMetadata(len(peers), ch, enqueueCh)
		// Begin assessing the queue and how much is processed, once queue
		// is entirely processed then we can close the enqueue channel
		enqueueCh.closeOnAllProcessed()
	}()

	// Fetch blocks from peers as results become ready
	peerQueues := make(peerBlocksQueues, 0, len(peers))
	for _, peer := range peers {
		peer := peer
		size := peerBlocksBatchSize
		workers := s.streamBlocksWorkers
		drainEvery := 100 * time.Millisecond
		processFn := func(batch []*blocksMetadata) {
			s.streamBlocksBatchFromPeer(namespace, shard, peer, batch, opts,
				result, enqueueCh, retrier)
		}
		queue := s.newPeerBlocksQueueFn(peer, size, drainEvery, workers, processFn)
		peerQueues = append(peerQueues, queue)
	}

	var (
		currStart, currEligible []*blocksMetadata
		blocksMetadataQueues    []blocksMetadataQueue
	)
	for perPeerBlocksMetadata := range enqueueCh.get() {
		// Filter and select which blocks to retrieve from which peers
		s.selectBlocksForSeriesFromPeerBlocksMetadata(
			perPeerBlocksMetadata, peerQueues,
			currStart, currEligible, blocksMetadataQueues)

		// Insert work into peer queues
		queues := uint32(blocksMetadatas(perPeerBlocksMetadata).hasBlocksLen())
		if queues == 0 {
			// No blocks at all available from any peers, series may have just expired
			enqueueCh.trackProcessed(1)
			continue
		}

		completed := uint32(0)
		onDone := func() {
			// Mark completion of work from the enqueue channel when all queues drained
			if atomic.AddUint32(&completed, 1) != queues {
				return
			}
			enqueueCh.trackProcessed(1)
		}

		for _, peerBlocksMetadata := range perPeerBlocksMetadata {
			if len(peerBlocksMetadata.blocks) == 0 {
				continue // No blocks to enqueue
			}
			queue := peerQueues.findQueue(peerBlocksMetadata.peer)
			queue.enqueue(peerBlocksMetadata, onDone)
		}
	}

	// Close all queues
	peerQueues.closeAll()

	return nil
}

func (s *session) streamCollectedBlocksMetadata(
	peersLen int,
	ch <-chan blocksMetadata,
	enqueueCh *enqueueChannel,
) {
	metadata := make(map[ts.Hash]*receivedBlocks)

	// Receive off of metadata channel
	for {
		m, ok := <-ch
		if !ok {
			break
		}

		received, ok := metadata[m.id.Hash()]
		if !ok {
			received = &receivedBlocks{
				results: make([]*blocksMetadata, 0, peersLen),
			}
			metadata[m.id.Hash()] = received
		}
		if received.submitted {
			// Already submitted to enqueue channel
			continue
		}

		received.results = append(received.results, &m)

		if len(received.results) == peersLen {
			enqueueCh.enqueue(received.results)
			received.submitted = true
		}
	}

	// Enqueue all unsubmitted received metadata
	for _, received := range metadata {
		if received.submitted {
			continue
		}
		enqueueCh.enqueue(received.results)
	}
}

func (s *session) selectBlocksForSeriesFromPeerBlocksMetadata(
	perPeerBlocksMetadata []*blocksMetadata,
	peerQueues peerBlocksQueues,
	pooledCurrStart, pooledCurrEligible []*blocksMetadata,
	pooledBlocksMetadataQueues []blocksMetadataQueue,
) {
	// Use pooled arrays
	var (
		currStart            = pooledCurrStart[:0]
		currEligible         = pooledCurrEligible[:0]
		blocksMetadataQueues = pooledBlocksMetadataQueues[:0]
	)

	// Sort the per peer metadatas by peer ID for consistent results
	sort.Sort(peerBlocksMetadataByID(perPeerBlocksMetadata))

	// Sort the metadatas per peer by time and reset the selection index
	for _, blocksMetadata := range perPeerBlocksMetadata {
		sort.Sort(blockMetadatasByTime(blocksMetadata.blocks))
		// Reset the selection index
		blocksMetadata.idx = 0
	}

	// Select blocks from peers
	for {
		// Find the earliest start time
		var earliestStart time.Time
		for _, blocksMetadata := range perPeerBlocksMetadata {
			if len(blocksMetadata.unselectedBlocks()) == 0 {
				// No unselected blocks
				continue
			}
			unselected := blocksMetadata.unselectedBlocks()
			if earliestStart.IsZero() ||
				unselected[0].start.Before(earliestStart) {
				earliestStart = unselected[0].start
			}
		}

		// Find all with the earliest start time,
		// ordered by time so must be first of each
		currStart = currStart[:0]
		for _, blocksMetadata := range perPeerBlocksMetadata {
			if len(blocksMetadata.unselectedBlocks()) == 0 {
				// No unselected blocks
				continue
			}

			unselected := blocksMetadata.unselectedBlocks()
			if !unselected[0].start.Equal(earliestStart) {
				// Not the same block
				continue
			}

			currStart = append(currStart, blocksMetadata)
		}

		if len(currStart) == 0 {
			// No more blocks to select from any peers
			break
		}

		// Only select from peers not already attempted
		currEligible = currStart[:]
		currID := currStart[0].id
		for i := len(currEligible) - 1; i >= 0; i-- {
			unselected := currEligible[i].unselectedBlocks()
			if unselected[0].reattempt.attempt == 0 {
				// Not attempted yet
				continue
			}

			// Check if eligible
			if unselected[0].reattempt.hasAttemptedPeer(currEligible[i].peer) {
				// Swap current entry to tail
				blocksMetadatas(currEligible).swap(i, len(currEligible)-1)
				// Trim newly last entry
				currEligible = currEligible[:len(currEligible)-1]
				continue
			}
		}

		if len(currEligible) == 0 {
			// No current eligible peers to select from
			s.log.WithFields(
				xlog.NewLogField("id", currID.String()),
				xlog.NewLogField("start", earliestStart),
				xlog.NewLogField("attempted", currStart[0].unselectedBlocks()[0].reattempt.attempt),
			).Error("retries failed for streaming blocks from peers")

			// Remove the block from all peers
			for i := range currStart {
				blocksLen := len(currStart[i].blocks)
				idx := currStart[i].idx
				tailIdx := blocksLen - 1
				currStart[i].blocks[idx], currStart[i].blocks[tailIdx] = currStart[i].blocks[tailIdx], currStart[i].blocks[idx]
				currStart[i].blocks = currStart[i].blocks[:blocksLen-1]
			}
			continue
		}

		// Prepare the reattempt peers metadata
		peersMetadata := make([]blockMetadataReattemptPeerMetadata, 0, len(currStart))
		for i := range currStart {
			unselected := currStart[i].unselectedBlocks()
			metadata := blockMetadataReattemptPeerMetadata{
				peer:     currStart[i].peer,
				start:    unselected[0].start,
				size:     unselected[0].size,
				checksum: unselected[0].checksum,
			}
			peersMetadata = append(peersMetadata, metadata)
		}

		var (
			sameNonNilChecksum = true
			curChecksum        *uint32
		)

		for i := 0; i < len(currEligible); i++ {
			unselected := currEligible[i].unselectedBlocks()
			// If any peer has a nil checksum, this might be the most recent block
			// and therefore not sealed so we want to merge from all peers
			if unselected[0].checksum == nil {
				sameNonNilChecksum = false
				break
			}
			if curChecksum == nil {
				curChecksum = unselected[0].checksum
			} else if *curChecksum != *unselected[0].checksum {
				sameNonNilChecksum = false
				break
			}
		}

		// If all the peers have the same non-nil checksum, we pick the peer with the
		// fewest outstanding requests
		if sameNonNilChecksum {
			// Order by least outstanding blocks being fetched
			blocksMetadataQueues = blocksMetadataQueues[:0]
			for i := range currEligible {
				insert := blocksMetadataQueue{
					blocksMetadata: currEligible[i],
					queue:          peerQueues.findQueue(currEligible[i].peer),
				}
				blocksMetadataQueues = append(blocksMetadataQueues, insert)
			}
			sort.Stable(blocksMetadatasQueuesByOutstandingAsc(blocksMetadataQueues))

			// Select the best peer
			bestPeerBlocksQueue := blocksMetadataQueues[0].queue

			// Remove the block from all other peers and increment index for selected peer
			for i := range currStart {
				peer := currStart[i].peer
				if peer == bestPeerBlocksQueue.peer {
					// Select this block
					idx := currStart[i].idx
					currStart[i].idx = idx + 1

					// Set the reattempt metadata
					currStart[i].blocks[idx].reattempt.id = currID
					currStart[i].blocks[idx].reattempt.attempt++
					currStart[i].blocks[idx].reattempt.attempted =
						append(currStart[i].blocks[idx].reattempt.attempted, peer)
					currStart[i].blocks[idx].reattempt.peersMetadata = peersMetadata

					// Leave the block in the current peers blocks list
					continue
				}

				// Removing this block
				blocksLen := len(currStart[i].blocks)
				idx := currStart[i].idx
				tailIdx := blocksLen - 1
				currStart[i].blocks[idx], currStart[i].blocks[tailIdx] = currStart[i].blocks[tailIdx], currStart[i].blocks[idx]
				currStart[i].blocks = currStart[i].blocks[:blocksLen-1]
			}
		} else {
			for i := range currStart {
				// Select this block
				idx := currStart[i].idx
				currStart[i].idx = idx + 1

				// Set the reattempt metadata
				currStart[i].blocks[idx].reattempt.id = currID
				currStart[i].blocks[idx].reattempt.peersMetadata = peersMetadata

				// Each block now has attempted multiple peers
				for j := range currStart {
					currStart[i].blocks[idx].reattempt.attempt++
					currStart[i].blocks[idx].reattempt.attempted =
						append(currStart[i].blocks[idx].reattempt.attempted, currStart[j].peer)
				}
			}
		}
	}
}

func (s *session) streamBlocksBatchFromPeer(
	namespace ts.ID,
	shard uint32,
	peer hostQueue,
	batch []*blocksMetadata,
	opts bootstrap.Options,
	blocksResult *blocksResult,
	enqueueCh *enqueueChannel,
	retrier xretry.Retrier,
) {
	// Prepare request
	var (
		req    = rpc.NewFetchBlocksRawRequest()
		result *rpc.FetchBlocksRawResult_

		nowFn              = opts.ClockOptions().NowFn()
		ropts              = opts.RetentionOptions()
		blockSize          = ropts.BlockSize()
		retention          = ropts.RetentionPeriod()
		earliestBlockStart = nowFn().Add(-retention).Truncate(blockSize)
	)
	req.NameSpace = namespace.Data()
	req.Shard = int32(shard)
	req.Elements = make([]*rpc.FetchBlocksRawRequestElement, len(batch))
	for i := range batch {
		starts := make([]int64, 0, len(batch[i].blocks))
		sort.Sort(blockMetadatasByTime(batch[i].blocks))
		for j := range batch[i].blocks {
			blockStart := batch[i].blocks[j].start
			if blockStart.Before(earliestBlockStart) {
				continue // Fell out of retention while we were streaming blocks
			}
			starts = append(starts, blockStart.UnixNano())
		}
		req.Elements[i] = &rpc.FetchBlocksRawRequestElement{
			ID:     batch[i].id.Data(),
			Starts: starts,
		}
	}

	// Attempt request
	if err := retrier.Attempt(func() error {
		client, err := peer.ConnectionPool().NextClient()
		if err != nil {
			return err
		}

		tctx, _ := thrift.NewContext(s.streamBlocksBatchTimeout)
		result, err = client.FetchBlocksRaw(tctx, req)
		return err
	}); err != nil {
		s.log.Errorf("stream blocks response from peer %s returned error: %v", peer.Host().String(), err)
		for i := range batch {
			s.streamBlocksReattemptFromPeers(batch[i].blocks, enqueueCh)
		}
		return
	}

	// Calculate earliest block start as of end of request
	earliestBlockStart = nowFn().Add(-retention).Truncate(blockSize)

	// NB(r): As discussed in the new session constructor this processing needs to happen
	// synchronously so that our outstanding work is calculated correctly, however we do
	// not want to steal all the CPUs on the machine to avoid dropping incoming writes so
	// we reduce the amount of processors in this critical section by checking out a token.
	// Wait for token to process the results to bound the amount of CPU of collecting results.
	token := <-s.streamBlocksResultsProcessors

	// Parse and act on result
	for i := range result.Elements {
		if i >= len(batch) {
			s.log.Errorf("stream blocks response from peer %s returned more IDs than expected", peer.Host().String())
			break
		}

		id := ts.BinaryID(result.Elements[i].ID)

		if !batch[i].id.Equal(id) {
			s.streamBlocksReattemptFromPeers(batch[i].blocks, enqueueCh)
			s.log.WithFields(
				xlog.NewLogField("expectedID", batch[i].id),
				xlog.NewLogField("actualID", id.String()),
				xlog.NewLogField("indexID", i),
			).Errorf("stream blocks response from peer %s returned mismatched ID", peer.Host().String())
			continue
		}

		missed := 0
		for j := range result.Elements[i].Blocks {
			if j >= len(batch[i].blocks) {
				s.log.WithFields(
					xlog.NewLogField("id", id.String()),
					xlog.NewLogField("expectedStarts", newTimesByUnixNanos(req.Elements[i].Starts)),
					xlog.NewLogField("actualStarts", newTimesByRPCBlocks(result.Elements[i].Blocks)),
				).Errorf("stream blocks response from peer %s returned more blocks than expected", peer.Host().String())
				break
			}

			// Index of the received block could be offset by missed blocks
			block := result.Elements[i].Blocks[j-missed]

			if block.Start != batch[i].blocks[j].start.UnixNano() {
				missed++

				// If fell out of retention during request this is healthy, otherwise an error
				if !time.Unix(0, block.Start).Before(earliestBlockStart) {
					failed := []blockMetadata{batch[i].blocks[j]}
					s.streamBlocksReattemptFromPeers(failed, enqueueCh)
					s.log.WithFields(
						xlog.NewLogField("id", id.String()),
						xlog.NewLogField("expectedStart", batch[i].blocks[j].start.UnixNano()),
						xlog.NewLogField("actualStart", block.Start),
						xlog.NewLogField("expectedStarts", newTimesByUnixNanos(req.Elements[i].Starts)),
						xlog.NewLogField("actualStarts", newTimesByRPCBlocks(result.Elements[i].Blocks)),
						xlog.NewLogField("indexID", i),
						xlog.NewLogField("indexBlock", j),
					).Errorf("stream blocks response from peer %s returned mismatched block start", peer.Host().String())
				}
				continue
			}

			if block.Err != nil {
				failed := []blockMetadata{batch[i].blocks[j]}
				s.streamBlocksReattemptFromPeers(failed, enqueueCh)
				s.log.WithFields(
					xlog.NewLogField("id", id.String()),
					xlog.NewLogField("start", block.Start),
					xlog.NewLogField("errorType", block.Err.Type),
					xlog.NewLogField("errorMessage", block.Err.Message),
					xlog.NewLogField("indexID", i),
					xlog.NewLogField("indexBlock", j),
				).Errorf("stream blocks response from peer %s returned block error", peer.Host().String())
				continue
			}

			err := blocksResult.addBlockFromPeer(id, block)
			if err != nil {
				failed := []blockMetadata{batch[i].blocks[j]}
				s.streamBlocksReattemptFromPeers(failed, enqueueCh)
				s.log.WithFields(
					xlog.NewLogField("id", id.String()),
					xlog.NewLogField("start", block.Start),
					xlog.NewLogField("error", err),
					xlog.NewLogField("indexID", i),
					xlog.NewLogField("indexBlock", j),
				).Errorf("stream blocks response from peer %s bad block response", peer.Host().String())
			}
		}
	}

	// Return token to continue collecting results in other go routines
	s.streamBlocksResultsProcessors <- token
}

func (s *session) streamBlocksReattemptFromPeers(
	blocks []blockMetadata,
	enqueueCh *enqueueChannel,
) {
	// Must do this asynchronously or else could get into a deadlock scenario
	// where cannot enqueue into the reattempt channel because no more work is
	// getting done because new attempts are blocked on existing attempts completing
	// and existing attempts are trying to enqueue into a full reattempt channel
	enqueue := enqueueCh.enqueueDelayed()
	s.streamBlocksReattemptWorkers.Go(func() {
		for i := range blocks {
			// Reconstruct peers metadata for reattempt
			reattemptBlocksMetadata :=
				make([]*blocksMetadata, len(blocks[i].reattempt.peersMetadata))
			for j := range reattemptBlocksMetadata {
				reattemptBlocksMetadata[j] = &blocksMetadata{
					peer: blocks[i].reattempt.peersMetadata[j].peer,
					id:   blocks[i].reattempt.id,
					blocks: []blockMetadata{blockMetadata{
						start:     blocks[i].reattempt.peersMetadata[j].start,
						size:      blocks[i].reattempt.peersMetadata[j].size,
						checksum:  blocks[i].reattempt.peersMetadata[j].checksum,
						reattempt: blocks[i].reattempt,
					}},
				}
			}
			// Re-enqueue the block to be fetched
			enqueue(reattemptBlocksMetadata)
		}
	})
}

type blocksResult struct {
	sync.RWMutex
	opts                    Options
	blockOpts               block.Options
	blockAllocSize          int
	contextPool             context.Pool
	encoderPool             encoding.EncoderPool
	multiReaderIteratorPool encoding.MultiReaderIteratorPool
	bytesPool               pool.BytesPool
	result                  bootstrap.ShardResult
}

func newBlocksResult(
	opts Options,
	bootstrapOpts bootstrap.Options,
	multiReaderIteratorPool encoding.MultiReaderIteratorPool,
) *blocksResult {
	blockOpts := bootstrapOpts.DatabaseBlockOptions()
	return &blocksResult{
		opts:                    opts,
		blockOpts:               blockOpts,
		blockAllocSize:          blockOpts.DatabaseBlockAllocSize(),
		contextPool:             opts.ContextPool(),
		encoderPool:             blockOpts.EncoderPool(),
		multiReaderIteratorPool: multiReaderIteratorPool,
		bytesPool:               blockOpts.BytesPool(),
		result:                  bootstrap.NewShardResult(4096, bootstrapOpts),
	}
}

func (r *blocksResult) addBlockFromPeer(id ts.ID, block *rpc.Block) error {
	var (
		start    = time.Unix(0, block.Start)
		segments = block.Segments
		result   = r.blockOpts.DatabaseBlockPool().Get()
	)

	if segments == nil {
		return errSessionBadBlockResultFromPeer
	}

	switch {
	case segments.Merged != nil:
		// Unmerged, can insert directly into a single block
		size := len(segments.Merged.Head) + len(segments.Merged.Tail)
		data := r.bytesPool.Get(size)[:size]
		n := copy(data, segments.Merged.Head)
		copy(data[n:], segments.Merged.Tail)

		encoder := r.encoderPool.Get()
		encoder.ResetSetData(start, data, false)

		result.Reset(start, encoder)

	case segments.Unmerged != nil:
		// Must merge to provide a single block
		readers := make([]io.Reader, len(segments.Unmerged))
		for i := range segments.Unmerged {
			readers[i] = xio.NewSegmentReader(ts.Segment{
				Head: segments.Unmerged[i].Head,
				Tail: segments.Unmerged[i].Tail,
			})
		}
		encoder, err := r.mergeReaders(start, readers)
		if err != nil {
			return err
		}
		result.Reset(start, encoder)

	default:
		return errSessionBadBlockResultFromPeer

	}

	// No longer need the encoder, seal the block
	result.Seal()

	resultCtx := r.contextPool.Get()
	defer resultCtx.Close()

	resultReader, err := result.Stream(resultCtx)
	if err != nil {
		return err
	}
	if resultReader == nil {
		return nil
	}

	var tmpCtx context.Context

	for {
		r.Lock()
		currBlock, exists := r.result.BlockAt(id, start)
		if !exists {
			r.result.AddBlock(id, result)
			r.Unlock()
			break
		}

		// Remove the existing block from the result so it doesn't get
		// merged again
		r.result.RemoveBlockAt(id, start)
		r.Unlock()

		// If we've already received data for this block, merge them
		// with the new block if possible
		if tmpCtx == nil {
			tmpCtx = r.contextPool.Get()
		} else {
			tmpCtx.Reset()
		}

		currReader, err := currBlock.Stream(tmpCtx)
		if err != nil {
			return err
		}

		// If there are no data in the current block, there is no
		// need to merge
		if currReader == nil {
			continue
		}

		readers := []io.Reader{currReader, resultReader}
		encoder, err := r.mergeReaders(start, readers)
		tmpCtx.BlockingClose()

		if err != nil {
			return err
		}

		result.Reset(start, encoder)
		result.Seal()
	}

	return nil
}

func (r *blocksResult) mergeReaders(start time.Time, readers []io.Reader) (encoding.Encoder, error) {
	iter := r.multiReaderIteratorPool.Get()
	iter.Reset(readers)
	defer iter.Close()

	encoder := r.encoderPool.Get()
	encoder.Reset(start, r.blockAllocSize)

	for iter.Next() {
		dp, unit, annotation := iter.Current()
		if err := encoder.Encode(dp, unit, annotation); err != nil {
			encoder.Close()
			return nil, err
		}
	}
	if err := iter.Err(); err != nil {
		encoder.Close()
		return nil, err
	}

	return encoder, nil
}

type enqueueChannel struct {
	enqueued        uint64
	processed       uint64
	peersMetadataCh chan []*blocksMetadata
	closed          int64
	metrics         *streamFromPeersMetrics
}

func newEnqueueChannel(m *streamFromPeersMetrics) *enqueueChannel {
	c := &enqueueChannel{
		peersMetadataCh: make(chan []*blocksMetadata, 4096),
		closed:          0,
		metrics:         m,
	}
	go func() {
		for atomic.LoadInt64(&c.closed) == 0 {
			m.blocksEnqueueChannel.Update(int64(len(c.peersMetadataCh)))
			time.Sleep(gaugeReportInterval)
		}
	}()
	return c
}

func (c *enqueueChannel) enqueue(peersMetadata []*blocksMetadata) {
	atomic.AddUint64(&c.enqueued, 1)
	c.peersMetadataCh <- peersMetadata
}

func (c *enqueueChannel) enqueueDelayed() func([]*blocksMetadata) {
	atomic.AddUint64(&c.enqueued, 1)
	return func(peersMetadata []*blocksMetadata) {
		c.peersMetadataCh <- peersMetadata
	}
}

func (c *enqueueChannel) get() <-chan []*blocksMetadata {
	return c.peersMetadataCh
}

func (c *enqueueChannel) trackProcessed(amount int) {
	atomic.AddUint64(&c.processed, uint64(amount))
}

func (c *enqueueChannel) unprocessedLen() int {
	return int(atomic.LoadUint64(&c.enqueued) - atomic.LoadUint64(&c.processed))
}

func (c *enqueueChannel) closeOnAllProcessed() {
	defer func() {
		atomic.StoreInt64(&c.closed, 1)
	}()
	for {
		if c.unprocessedLen() == 0 {
			// Will only ever be zero after all is processed if called
			// after enqueueing the desired set of entries as long as
			// the guarentee that reattempts are enqueued before the
			// failed attempt is marked as processed is upheld
			close(c.peersMetadataCh)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type receivedBlocks struct {
	submitted bool
	results   []*blocksMetadata
}

type processFn func(batch []*blocksMetadata)

type peerBlocksQueue struct {
	sync.RWMutex
	closed       bool
	peer         hostQueue
	queue        []*blocksMetadata
	doneFns      []func()
	assigned     uint64
	completed    uint64
	maxQueueSize int
	workers      pool.WorkerPool
	processFn    processFn
}

type newPeerBlocksQueueFn func(
	peer hostQueue,
	maxQueueSize int,
	interval time.Duration,
	workers pool.WorkerPool,
	processFn processFn,
) *peerBlocksQueue

func newPeerBlocksQueue(
	peer hostQueue,
	maxQueueSize int,
	interval time.Duration,
	workers pool.WorkerPool,
	processFn processFn,
) *peerBlocksQueue {
	q := &peerBlocksQueue{
		peer:         peer,
		maxQueueSize: maxQueueSize,
		workers:      workers,
		processFn:    processFn,
	}
	if interval > 0 {
		go q.drainEvery(interval)
	}
	return q
}

func (q *peerBlocksQueue) drainEvery(interval time.Duration) {
	for {
		q.Lock()
		if q.closed {
			q.Unlock()
			return
		}
		q.drainWithLock()
		q.Unlock()
		time.Sleep(interval)
	}
}

func (q *peerBlocksQueue) close() {
	q.Lock()
	defer q.Unlock()
	q.closed = true
}

func (q *peerBlocksQueue) trackAssigned(amount int) {
	atomic.AddUint64(&q.assigned, uint64(amount))
}

func (q *peerBlocksQueue) trackCompleted(amount int) {
	atomic.AddUint64(&q.completed, uint64(amount))
}

func (q *peerBlocksQueue) enqueue(bm *blocksMetadata, doneFn func()) {
	q.Lock()

	if len(q.queue) == 0 && cap(q.queue) < q.maxQueueSize {
		// Lazy initialize queue
		q.queue = make([]*blocksMetadata, 0, q.maxQueueSize)
	}
	if len(q.doneFns) == 0 && cap(q.doneFns) < q.maxQueueSize {
		// Lazy initialize doneFns
		q.doneFns = make([]func(), 0, q.maxQueueSize)
	}
	q.queue = append(q.queue, bm)
	if doneFn != nil {
		q.doneFns = append(q.doneFns, doneFn)
	}
	q.trackAssigned(len(bm.blocks))

	// Determine if should drain immediately
	if len(q.queue) < q.maxQueueSize {
		// Require more to fill up block
		q.Unlock()
		return
	}
	q.drainWithLock()

	q.Unlock()
}

func (q *peerBlocksQueue) drain() {
	q.Lock()
	q.drainWithLock()
	q.Unlock()
}

func (q *peerBlocksQueue) drainWithLock() {
	if len(q.queue) == 0 {
		// None to drain
		return
	}
	enqueued := q.queue
	doneFns := q.doneFns
	q.queue = nil
	q.doneFns = nil
	q.workers.Go(func() {
		q.processFn(enqueued)
		// Call done callbacks
		for i := range doneFns {
			doneFns[i]()
		}
		// Track completed blocks
		completed := 0
		for i := range enqueued {
			completed += len(enqueued[i].blocks)
		}
		q.trackCompleted(completed)
	})
}

type peerBlocksQueues []*peerBlocksQueue

func (qs peerBlocksQueues) findQueue(peer hostQueue) *peerBlocksQueue {
	for _, q := range qs {
		if q.peer == peer {
			return q
		}
	}
	return nil
}

func (qs peerBlocksQueues) closeAll() {
	for _, q := range qs {
		q.close()
	}
}

type blocksMetadata struct {
	peer   hostQueue
	id     ts.ID
	blocks []blockMetadata
	idx    int
}

func (b blocksMetadata) unselectedBlocks() []blockMetadata {
	if b.idx == len(b.blocks) {
		return nil
	}
	return b.blocks[b.idx:]
}

type blocksMetadatas []*blocksMetadata

func (arr blocksMetadatas) swap(i, j int) { arr[i], arr[j] = arr[j], arr[i] }
func (arr blocksMetadatas) hasBlocksLen() int {
	count := 0
	for i := range arr {
		if arr[i] != nil && len(arr[i].blocks) > 0 {
			count++
		}
	}
	return count
}

type peerBlocksMetadataByID []*blocksMetadata

func (arr peerBlocksMetadataByID) Len() int      { return len(arr) }
func (arr peerBlocksMetadataByID) Swap(i, j int) { arr[i], arr[j] = arr[j], arr[i] }
func (arr peerBlocksMetadataByID) Less(i, j int) bool {
	return strings.Compare(arr[i].peer.Host().ID(), arr[j].peer.Host().ID()) < 0
}

type blocksMetadataQueue struct {
	blocksMetadata *blocksMetadata
	queue          *peerBlocksQueue
}

type blocksMetadatasQueuesByOutstandingAsc []blocksMetadataQueue

func (arr blocksMetadatasQueuesByOutstandingAsc) Len() int      { return len(arr) }
func (arr blocksMetadatasQueuesByOutstandingAsc) Swap(i, j int) { arr[i], arr[j] = arr[j], arr[i] }
func (arr blocksMetadatasQueuesByOutstandingAsc) Less(i, j int) bool {
	outstandingFirst := atomic.LoadUint64(&arr[i].queue.assigned) - atomic.LoadUint64(&arr[i].queue.completed)
	outstandingSecond := atomic.LoadUint64(&arr[j].queue.assigned) - atomic.LoadUint64(&arr[j].queue.completed)
	return outstandingFirst < outstandingSecond
}

type blockMetadata struct {
	start     time.Time
	size      int64
	checksum  *uint32
	reattempt blockMetadataReattempt
}

type blockMetadataReattempt struct {
	attempt       int
	id            ts.ID
	attempted     []hostQueue
	peersMetadata []blockMetadataReattemptPeerMetadata
}

type blockMetadataReattemptPeerMetadata struct {
	peer     hostQueue
	start    time.Time
	size     int64
	checksum *uint32
}

func (b blockMetadataReattempt) hasAttemptedPeer(peer hostQueue) bool {
	for i := range b.attempted {
		if b.attempted[i] == peer {
			return true
		}
	}
	return false
}

type blockMetadatasByTime []blockMetadata

func (b blockMetadatasByTime) Len() int      { return len(b) }
func (b blockMetadatasByTime) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b blockMetadatasByTime) Less(i, j int) bool {
	return b[i].start.Before(b[j].start)
}

func newTimesByUnixNanos(values []int64) []time.Time {
	result := make([]time.Time, len(values))
	for i := range values {
		result[i] = time.Unix(0, values[i])
	}
	return result
}

func newTimesByRPCBlocks(values []*rpc.Block) []time.Time {
	result := make([]time.Time, len(values))
	for i := range values {
		result[i] = time.Unix(0, values[i].Start)
	}
	return result
}

type metadataIter struct {
	inputCh  <-chan blocksMetadata
	errCh    <-chan error
	host     topology.Host
	blocks   []block.Metadata
	metadata block.BlocksMetadata
	done     bool
	err      error
}

func newMetadataIter(inputCh <-chan blocksMetadata, errCh <-chan error) PeerBlocksMetadataIter {
	return &metadataIter{
		inputCh: inputCh,
		errCh:   errCh,
		blocks:  make([]block.Metadata, 0, blocksMetadataInitialCapacity),
	}
}

func (it *metadataIter) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	m, more := <-it.inputCh
	if !more {
		it.err = <-it.errCh
		it.done = true
		return false
	}
	it.host = m.peer.Host()
	it.blocks = it.blocks[:0]
	for _, b := range m.blocks {
		bm := block.NewMetadata(b.start, b.size, b.checksum)
		it.blocks = append(it.blocks, bm)
	}
	it.metadata = block.NewBlocksMetadata(m.id, it.blocks)
	return true
}

func (it *metadataIter) Current() (topology.Host, block.BlocksMetadata) {
	return it.host, it.metadata
}

func (it *metadataIter) Err() error {
	return it.err
}
