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

	"github.com/uber/tchannel-go/thrift"
)

const (
	clusterConnectWaitInterval = 10 * time.Millisecond
)

var (
	// ErrClusterConnectTimeout is raised when connecting to the cluster and
	// ensuring at least each partition has an up node with a connection to it
	ErrClusterConnectTimeout = errors.New("timed out establishing min connections to cluster")

	errSessionStateNotInitial        = errors.New("session not in initial state")
	errSessionStateNotOpen           = errors.New("session not in open state")
	errSessionBadBlockResultFromPeer = errors.New("session fetched bad block result from peer")
)

type session struct {
	sync.RWMutex

	opts                             Options
	nowFn                            clock.NowFn
	log                              xlog.Logger
	level                            topology.ConsistencyLevel
	newHostQueueFn                   newHostQueueFn
	topo                             topology.Topology
	topoMap                          topology.Map
	topoWatch                        topology.MapWatch
	replicas                         int
	majority                         int
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
	streamBlocksBatchSize            int
	streamBlocksMetadataBatchTimeout time.Duration
	streamBlocksBatchTimeout         time.Duration
}

type newHostQueueFn func(
	host topology.Host,
	writeBatchRequestPool writeBatchRequestPool,
	idDatapointArrayPool idDatapointArrayPool,
	opts Options,
) hostQueue

func newSession(opts Options) (clientSession, error) {
	topo, err := opts.TopologyInitializer().Init()
	if err != nil {
		return nil, err
	}

	s := &session{
		opts:                 opts,
		nowFn:                opts.ClockOptions().NowFn(),
		log:                  opts.InstrumentOptions().Logger(),
		level:                opts.ConsistencyLevel(),
		newHostQueueFn:       newHostQueue,
		topo:                 topo,
		fetchBatchSize:       opts.FetchBatchSize(),
		newPeerBlocksQueueFn: newPeerBlocksQueue,
	}

	if opts, ok := opts.(AdminOptions); ok {
		s.origin = opts.Origin()
		s.streamBlocksWorkers = pool.NewWorkerPool(opts.FetchSeriesBlocksBatchConcurrency())
		s.streamBlocksWorkers.Init()
		s.streamBlocksReattemptWorkers = pool.NewWorkerPool(opts.FetchSeriesBlocksBatchConcurrency())
		s.streamBlocksReattemptWorkers.Init()
		s.streamBlocksBatchSize = opts.FetchSeriesBlocksBatchSize()
		s.streamBlocksMetadataBatchTimeout = opts.FetchSeriesBlocksMetadataBatchTimeout()
		s.streamBlocksBatchTimeout = opts.FetchSeriesBlocksBatchTimeout()
	}

	return s, nil
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
	s.writeOpPool = newWriteOpPool(s.opts.WriteOpPoolSize())
	s.writeOpPool.Init()
	s.fetchBatchOpPool = newFetchBatchOpPool(s.opts.FetchBatchOpPoolSize(), s.fetchBatchSize)
	s.fetchBatchOpPool.Init()
	s.seriesIteratorPool = encoding.NewSeriesIteratorPool(s.opts.SeriesIteratorPoolSize())
	s.seriesIteratorPool.Init()
	s.seriesIteratorsPool = encoding.NewMutableSeriesIteratorsPool(s.opts.SeriesIteratorArrayPoolBuckets())
	s.seriesIteratorsPool.Init()
	s.state = stateOpen
	s.Unlock()

	go func() {
		for range s.topoWatch.C() {
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
	connectConsistencyLevel := s.opts.ClusterConnectConsistencyLevel()
	for {
		if s.nowFn().Sub(start) >= s.opts.ClusterConnectTimeout() {
			for i := range queues {
				queues[i].Close()
			}
			return nil, 0, 0, ErrClusterConnectTimeout
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
			case topology.ConsistencyLevelAll:
				clusterAvailableForShard = shardReplicasAvailable == replicas
			case topology.ConsistencyLevelMajority:
				clusterAvailableForShard = shardReplicasAvailable >= majority
			case topology.ConsistencyLevelOne:
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

	return queues, replicas, majority, nil
}

func (s *session) setTopologyWithLock(topologyMap topology.Map, queues []hostQueue, replicas, majority int) {
	prev := s.queues
	s.queues = queues
	s.topoMap = topologyMap
	prevReplicas := s.replicas
	s.replicas = replicas
	s.majority = majority
	if s.fetchBatchOpArrayArrayPool == nil ||
		s.fetchBatchOpArrayArrayPool.Entries() != len(queues) {
		s.fetchBatchOpArrayArrayPool = newFetchBatchOpArrayArrayPool(
			s.opts.FetchBatchOpPoolSize(),
			len(queues),
			s.opts.FetchBatchOpPoolSize()/len(queues))
		s.fetchBatchOpArrayArrayPool.Init()
	}
	if s.iteratorArrayPool == nil ||
		prevReplicas != s.replicas {
		s.iteratorArrayPool = encoding.NewIteratorArrayPool([]pool.Bucket{
			pool.Bucket{
				Capacity: s.replicas,
				Count:    s.opts.SeriesIteratorPoolSize(),
			},
		})
		s.iteratorArrayPool.Init()
	}
	if s.readerSliceOfSlicesIteratorPool == nil ||
		prevReplicas != s.replicas {
		size := s.replicas * s.opts.SeriesIteratorPoolSize()
		s.readerSliceOfSlicesIteratorPool = newReaderSliceOfSlicesIteratorPool(size)
		s.readerSliceOfSlicesIteratorPool.Init()
	}
	if s.multiReaderIteratorPool == nil ||
		prevReplicas != s.replicas {
		size := s.replicas * s.opts.SeriesIteratorPoolSize()
		s.multiReaderIteratorPool = encoding.NewMultiReaderIteratorPool(size)
		s.multiReaderIteratorPool.Init(s.opts.ReaderIteratorAllocate())
	}

	// Asynchronously close the previous set of host queues
	go func() {
		for i := range prev {
			prev[i].Close()
		}
	}()
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
	writeBatchRequestPool := newWriteBatchRequestPool(hostBatches)
	writeBatchRequestPool.Init()
	idDatapointArrayPool := newIDDatapointArrayPool(hostBatches, s.opts.WriteBatchSize())
	idDatapointArrayPool.Init()
	hostQueue := s.newHostQueueFn(host, writeBatchRequestPool, idDatapointArrayPool, s.opts)
	hostQueue.Open()
	return hostQueue
}

func (s *session) Write(namespace string, id string, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	var (
		wg            sync.WaitGroup
		enqueueErr    error
		enqueued      int
		resultErrLock sync.Mutex
		resultErr     error
		resultErrs    int
		majority      int
	)

	timeType, timeTypeErr := convert.ToTimeType(unit)
	if timeTypeErr != nil {
		return timeTypeErr
	}

	ts, tsErr := convert.ToValue(t, timeType)
	if tsErr != nil {
		return tsErr
	}

	w := s.writeOpPool.Get()
	w.request.NameSpace = namespace
	w.idDatapoint.ID = id
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
	routeErr := s.topoMap.RouteForEach(id, func(idx int, host topology.Host) {
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
		s.opts.InstrumentOptions().Logger().Errorf("failed to enqueue write: %v", enqueueErr)
		return enqueueErr
	}

	// Wait for writes to complete
	wg.Wait()

	// Return write to pool
	s.writeOpPool.Put(w)

	return s.consistencyResult(majority, enqueued, resultErrs, resultErr)
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
		routeErr               error
		enqueueErr             error
		resultErrLock          sync.Mutex
		resultErr              error
		resultErrs             int
		majority               int
		fetchBatchOpsByHostIdx [][]*fetchBatchOp
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
			results  []encoding.Iterator
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

		if err := s.topoMap.RouteForEach(ids[idx], func(hostIdx int, host topology.Host) {
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

			// Append IDWithNamespace to this request
			f.append(namespace, ids[idx], completionFn)
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
		s.log.Errorf("failed to enqueue fetch: %v", enqueueErr)
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

func (s *session) Truncate(namespace string) (int64, error) {
	var (
		wg            sync.WaitGroup
		enqueueErr    xerrors.MultiError
		resultErrLock sync.Mutex
		resultErr     xerrors.MultiError
		truncated     int64
	)

	t := &truncateOp{}
	t.request.NameSpace = namespace
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

func (s *session) FetchBootstrapBlocksFromPeers(
	namespace string,
	shard uint32,
	start, end time.Time,
	opts bootstrap.Options,
) (bootstrap.ShardResult, error) {
	var (
		result = newBlocksResult(s.opts, opts)
		doneCh = make(chan error, 1)
		onDone = func(err error) {
			select {
			case doneCh <- err:
			default:
			}
		}
		waitDone = func() error {
			return <-doneCh
		}
	)

	s.RLock()
	peers := make([]hostQueue, 0, s.topoMap.Replicas())
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

	// Begin pulling metadata, if one or multiple peers fail no error will
	// be returned from this routine as long as one peer succeeds completely
	metadataCh := make(chan blocksMetadata, 4096)
	go func() {
		blockSize := opts.RetentionOptions().BlockSize()
		err := s.streamBlocksMetadataFromPeers(namespace, shard, peers, start, end, blockSize, metadataCh)

		close(metadataCh)
		if err != nil {
			// Bail early
			onDone(err)
		}
	}()

	// Begin consuming metadata and making requests
	go func() {
		err := s.streamBlocksFromPeers(namespace, shard, peers, metadataCh, result)
		onDone(err)
	}()

	if err := waitDone(); err != nil {
		return nil, err
	}
	return result.result, nil
}

func (s *session) streamBlocksMetadataFromPeers(
	namespace string,
	shard uint32,
	peers []hostQueue,
	start, end time.Time,
	blockSize time.Duration,
	ch chan<- blocksMetadata,
) error {
	var (
		wg       sync.WaitGroup
		errLock  sync.Mutex
		errLen   int
		multiErr = xerrors.NewMultiError()
	)
	for _, peer := range peers {
		peer := peer

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.streamBlocksMetadataFromPeer(namespace, shard, peer, start, end, blockSize, ch)
			if err != nil {
				s.log.Warnf("failed to stream blocks metadata from peer %s for namespace %s shard %d: %v", peer.Host().String(), namespace, shard, err)
				errLock.Lock()
				defer errLock.Unlock()
				errLen++
				multiErr.Add(err)
			}
		}()
	}

	wg.Wait()

	if errLen == len(peers) {
		s.log.Errorf("failed to complete streaming blocks from all peers for namespace %s shard %d", namespace, shard)
		return multiErr
	}
	return nil
}

func (s *session) streamBlocksMetadataFromPeer(
	namespace string,
	shard uint32,
	peer hostQueue,
	start, end time.Time,
	blockSize time.Duration,
	ch chan<- blocksMetadata,
) error {
	var (
		pageToken *int64
		retrier   = xretry.NewRetrier(xretry.NewOptions().
				SetBackoffFactor(2).
				SetMax(3).
				SetInitialBackoff(time.Second).
				SetJitter(true))
		optionIncludeSizes = true
		moreResults        = true
	)
	// Declare before loop to avoid redeclaring each iteration
	attemptFn := func() error {
		client, err := peer.ConnectionPool().NextClient()
		if err != nil {
			return err
		}

		tctx, _ := thrift.NewContext(s.streamBlocksMetadataBatchTimeout)
		req := rpc.NewFetchBlocksMetadataRequest()
		req.NameSpace = namespace
		req.Shard = int32(shard)
		req.Limit = int64(s.streamBlocksBatchSize)
		req.PageToken = pageToken
		req.IncludeSizes = &optionIncludeSizes

		result, err := client.FetchBlocksMetadata(tctx, req)
		if err != nil {
			return err
		}

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
				var (
					blockStart = time.Unix(0, b.Start)
					blockEnd   = blockStart.Add(blockSize)
				)
				if !start.Before(blockEnd) || !blockStart.Before(end) {
					continue
				}

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

				blockMetas = append(blockMetas, blockMetadata{
					start: blockStart,
					size:  *b.Size,
				})
			}
			ch <- blocksMetadata{
				peer:   peer,
				id:     elem.ID,
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
	namespace string,
	shard uint32,
	peers []hostQueue,
	ch <-chan blocksMetadata,
	result *blocksResult,
) error {
	var (
		retrier = xretry.NewRetrier(xretry.NewOptions().
			SetBackoffFactor(2).
			SetMax(3).
			SetInitialBackoff(time.Second).
			SetJitter(true))
		enqueueCh           = newEnqueueChannel()
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
		queue := s.newPeerBlocksQueueFn(peer, size, drainEvery, workers, func(batch []*blocksMetadata) {
			s.streamBlocksBatchFromPeer(namespace, shard, peer, batch, result, enqueueCh, retrier)
		})
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
		var (
			completed = uint32(0)
			queues    = uint32(blocksMetadatas(perPeerBlocksMetadata).hasBlocksLen())
			onDone    = func() {
				// Mark completion of work from the enqueue channel when all queues drained
				if atomic.AddUint32(&completed, 1) != queues {
					return
				}
				enqueueCh.trackProcessed(1)
			}
		)
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
	metadata := make(map[string]*receivedBlocks)

	// Receive off of metadata channel
	for {
		m, ok := <-ch
		if !ok {
			break
		}

		received, ok := metadata[m.id]
		if !ok {
			received = &receivedBlocks{
				results: make([]*blocksMetadata, 0, peersLen),
			}
			metadata[m.id] = received
		}
		if received.submitted {
			// Already submitted to enqueue channel
			s.log.WithFields(xlog.NewLogField("id", m.id)).
				Warnf("received blocks metadata for already collected blocks metadata")
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
				xlog.NewLogField("id", currID),
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

		// Determine maximum size
		currMaxSize := int64(math.MinInt64)
		for i := 0; i < len(currEligible); i++ {
			unselected := currEligible[i].unselectedBlocks()
			// Track maximum size
			if unselected[0].size > currMaxSize {
				currMaxSize = unselected[0].size
			}
		}

		// Only select from those with the maximum size
		for i := len(currEligible) - 1; i >= 0; i-- {
			unselected := currEligible[i].unselectedBlocks()
			if unselected[0].size < currMaxSize {
				// Swap current entry to tail
				blocksMetadatas(currEligible).swap(i, len(currEligible)-1)
				// Trim newly last entry
				currEligible = currEligible[:len(currEligible)-1]
			}
		}

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

		// Prepare the reattempt peers metadata
		peersMetadata := make([]blockMetadataReattemptPeerMetadata, 0, len(currStart))
		for i := range currStart {
			unselected := currStart[i].unselectedBlocks()
			metadata := blockMetadataReattemptPeerMetadata{
				peer:  currStart[i].peer,
				start: unselected[0].start,
				size:  unselected[0].size,
			}
			peersMetadata = append(peersMetadata, metadata)
		}

		// Remove the block from all other peers and increment index for selected peer
		for i := range currStart {
			peer := currStart[i].peer
			if peer == bestPeerBlocksQueue.peer {
				// Select this block
				idx := currStart[i].idx
				currStart[i].idx = idx + 1

				// Set the reattempt metadata
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
	}
}

func (s *session) streamBlocksBatchFromPeer(
	namespace string,
	shard uint32,
	peer hostQueue,
	batch []*blocksMetadata,
	blocksResult *blocksResult,
	enqueueCh *enqueueChannel,
	retrier xretry.Retrier,
) {
	// Prepare request
	var (
		req    = rpc.NewFetchBlocksRequest()
		result *rpc.FetchBlocksResult_
	)
	req.NameSpace = namespace
	req.Shard = int32(shard)
	req.Elements = make([]*rpc.FetchBlocksParam, len(batch))
	for i := range batch {
		starts := make([]int64, len(batch[i].blocks))
		for j := range batch[i].blocks {
			starts[j] = batch[i].blocks[j].start.UnixNano()
		}
		req.Elements[i] = &rpc.FetchBlocksParam{
			ID:     batch[i].id,
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
		result, err = client.FetchBlocks(tctx, req)
		return err
	}); err != nil {
		for i := range batch {
			s.streamBlocksReattemptFromPeers(batch[i].blocks, enqueueCh)
		}
		return
	}

	// Parse and act on result
	for i := range result.Elements {
		if i >= len(batch) {
			s.log.Errorf("stream blocks response from peer %s returned more IDs than expected", peer.Host().String())
			break
		}

		id := result.Elements[i].ID
		if id != batch[i].id {
			s.streamBlocksReattemptFromPeers(batch[i].blocks, enqueueCh)
			s.log.WithFields(
				xlog.NewLogField("expectedID", batch[i].id),
				xlog.NewLogField("actualID", id),
				xlog.NewLogField("indexID", i),
			).Errorf("stream blocks response from peer %s returned mismatched ID", peer.Host().String())
			continue
		}

		for j := range result.Elements[i].Blocks {
			if j >= len(batch[i].blocks) {
				s.log.Errorf("stream blocks response from peer %s returned more blocks than expected", peer.Host().String())
				break
			}

			block := result.Elements[i].Blocks[j]
			if block.Start != batch[i].blocks[j].start.UnixNano() {
				failed := []blockMetadata{batch[i].blocks[j]}
				s.streamBlocksReattemptFromPeers(failed, enqueueCh)
				s.log.WithFields(
					xlog.NewLogField("id", id),
					xlog.NewLogField("expectedStart", batch[i].blocks[j].start.UnixNano()),
					xlog.NewLogField("actualStart", block.Start),
					xlog.NewLogField("indexID", i),
					xlog.NewLogField("indexBlock", j),
				).Errorf("stream blocks response from peer %s returned mismatched block start", peer.Host().String())
				continue
			}

			if block.Err != nil {
				failed := []blockMetadata{batch[i].blocks[j]}
				s.streamBlocksReattemptFromPeers(failed, enqueueCh)
				s.log.WithFields(
					xlog.NewLogField("id", id),
					xlog.NewLogField("start", block.Start),
					xlog.NewLogField("errorType", block.Err.Type),
					xlog.NewLogField("errorMessage", block.Err.Message),
					xlog.NewLogField("indexID", i),
					xlog.NewLogField("indexBlock", j),
				).Errorf("stream blocks response from peer %s returned block error", peer.Host().String())
				continue
			}

			blocksResult.addBlockFromPeer(id, block)
		}
	}
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
	opts           Options
	blockOpts      block.Options
	blockAllocSize int
	encoderPool    encoding.EncoderPool
	bytesPool      pool.BytesPool
	result         bootstrap.ShardResult
}

func newBlocksResult(opts Options, bootstrapOpts bootstrap.Options) *blocksResult {
	blockOpts := bootstrapOpts.DatabaseBlockOptions()
	return &blocksResult{
		opts:           opts,
		blockOpts:      blockOpts,
		blockAllocSize: blockOpts.DatabaseBlockAllocSize(),
		encoderPool:    blockOpts.EncoderPool(),
		bytesPool:      blockOpts.BytesPool(),
		result:         bootstrap.NewShardResult(bootstrapOpts),
	}
}

func (r *blocksResult) addBlockFromPeer(id string, block *rpc.Block) error {
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
		size := len(segments.Merged.Head) + len(segments.Merged.Tail)
		data := r.bytesPool.Get(size)[:size]
		n := copy(data, segments.Merged.Head)
		copy(data[n:], segments.Merged.Tail)
		encoder := r.encoderPool.Get()
		encoder.ResetSetData(start, data, false)
		if err := encoder.Unseal(); err != nil {
			return err
		}
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

		alloc := r.opts.ReaderIteratorAllocate()
		iter := encoding.NewMultiReaderIterator(alloc, nil)
		iter.Reset(readers)
		defer iter.Close()

		encoder := r.encoderPool.Get()
		encoder.Reset(start, r.blockAllocSize)

		for iter.Next() {
			dp, unit, annotation := iter.Current()
			encoder.Encode(dp, unit, annotation)
		}
		if err := iter.Err(); err != nil {
			return err
		}

		result.Reset(start, encoder)
	default:
		return errSessionBadBlockResultFromPeer
	}

	r.Lock()
	r.result.AddBlock(id, result)
	r.Unlock()

	return nil
}

type enqueueChannel struct {
	enqueued        uint64
	processed       uint64
	peersMetadataCh chan []*blocksMetadata
}

func newEnqueueChannel() *enqueueChannel {
	return &enqueueChannel{
		peersMetadataCh: make(chan []*blocksMetadata, 4096),
	}
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
	for {
		if c.unprocessedLen() == 0 {
			// Will only ever be zero after all is processed if called
			// after enqueueing the desired set of entries as long as
			// the guarentee that reattempts are enqueued before the
			// failed attempt is marked as processed is upheld
			close(c.peersMetadataCh)
			return
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
	id     string
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
	reattempt blockMetadataReattempt
}

type blockMetadataReattempt struct {
	attempt       int
	id            string
	attempted     []hostQueue
	peersMetadata []blockMetadataReattemptPeerMetadata
}

type blockMetadataReattemptPeerMetadata struct {
	peer  hostQueue
	start time.Time
	size  int64
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
