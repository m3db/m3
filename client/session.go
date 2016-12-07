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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"

	xerrors "github.com/m3db/m3x/errors"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xretry "github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/sync"
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

type session struct {
	sync.RWMutex

	opts                             Options
	scope                            tally.Scope
	nowFn                            clock.NowFn
	log                              xlog.Logger
	writeLevel                       topology.ConsistencyLevel
	readLevel                        ReadConsistencyLevel
	topo                             topology.Topology
	topoMap                          topology.Map
	topoWatch                        topology.MapWatch
	origin                           topology.Host
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
	writeStatePool                   sync.Pool
	fetchBatchSize                   int
	streamBlocksWorkers              xsync.WorkerPool
	streamBlocksResultsProcessors    chan struct{}
	streamBlocksBatchSize            int
	streamBlocksMetadataBatchTimeout time.Duration
	streamBlocksBatchTimeout         time.Duration
	metrics                          sessionMetrics

	// for testing
	newPeerBlocksQueueFn newPeerBlocksQueueFn
	newHostQueueFn       newHostQueueFn
}

func newSession(opts Options) (clientSession, error) {
	topo, err := opts.TopologyInitializer().Init()
	if err != nil {
		return nil, err
	}

	scope := opts.InstrumentOptions().MetricsScope()

	s := &session{
		opts:                 opts,
		scope:                scope,
		nowFn:                opts.ClockOptions().NowFn(),
		log:                  opts.InstrumentOptions().Logger(),
		writeLevel:           opts.WriteConsistencyLevel(),
		readLevel:            opts.ReadConsistencyLevel(),
		newHostQueueFn:       newHostQueue,
		topo:                 topo,
		fetchBatchSize:       opts.FetchBatchSize(),
		newPeerBlocksQueueFn: newPeerBlocksQueue,
		metrics:              newSessionMetrics(scope),
	}
	s.writeStatePool = sync.Pool{New: func() interface{} {
		w := &writeState{session: s}
		w.reset()
		return w
	}}

	if opts, ok := opts.(AdminOptions); ok {
		s.origin = opts.Origin()
		s.streamBlocksWorkers = xsync.NewWorkerPool(opts.FetchSeriesBlocksBatchConcurrency())
		s.streamBlocksWorkers.Init()
		processors := opts.FetchSeriesBlocksResultsProcessors()
		// NB(r): We use a semaphore here instead of a worker pool for bounding
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

func (s *session) ShardID(id string) (uint32, error) {
	s.RLock()
	if s.state != stateOpen {
		s.RUnlock()
		return 0, errSessionStateNotOpen
	}
	value := s.topoMap.ShardSet().Lookup(ts.StringID(id))
	s.RUnlock()
	return value, nil
}

func (s *session) Open() error {
	s.Lock()
	if s.state != stateNotOpen {
		s.Unlock()
		return errSessionStateNotInitial
	}

	watch, err := s.topo.Watch()
	if err != nil {
		s.Unlock()
		return err
	}

	// Wait for the topology to be available
	<-watch.C()

	if err := s.setTopologyWithLock(watch.Get(), true); err != nil {
		s.Unlock()
		return err
	}

	s.topoWatch = watch

	// NB(r): Alloc pools that can take some time in Open, expectation
	// is that Open will already take some time
	writeOpPoolOpts := pool.NewObjectPoolOptions().
		SetSize(s.opts.WriteOpPoolSize()).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("write-op-pool"),
		))
	s.writeOpPool = newWriteOpPool(writeOpPoolOpts)
	s.writeOpPool.Init()
	fetchBatchOpPoolOpts := pool.NewObjectPoolOptions().
		SetSize(s.opts.FetchBatchOpPoolSize()).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("fetch-batch-op-pool"),
		))
	s.fetchBatchOpPool = newFetchBatchOpPool(fetchBatchOpPoolOpts, s.fetchBatchSize)
	s.fetchBatchOpPool.Init()
	seriesIteratorPoolOpts := pool.NewObjectPoolOptions().
		SetSize(s.opts.SeriesIteratorPoolSize()).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("series-iterator-pool"),
		))
	s.seriesIteratorPool = encoding.NewSeriesIteratorPool(seriesIteratorPoolOpts)
	s.seriesIteratorPool.Init()
	s.seriesIteratorsPool = encoding.NewMutableSeriesIteratorsPool(s.opts.SeriesIteratorArrayPoolBuckets())
	s.seriesIteratorsPool.Init()
	s.state = stateOpen
	s.Unlock()

	go s.updateTopology()

	return nil
}

func (s *session) Write(namespace, id string, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	var (
		enqueued int32
		majority = atomic.LoadInt32(&s.majority)
		tsID     = ts.StringID(id)
	)

	timeType, timeTypeErr := convert.ToTimeType(unit)
	if timeTypeErr != nil {
		return timeTypeErr
	}

	timestamp, timestampErr := convert.ToValue(t, timeType)
	if timestampErr != nil {
		return timestampErr
	}

	if s.RLock(); s.state != stateOpen {
		s.RUnlock()
		return errSessionStateNotOpen
	}

	state := s.writeStatePool.Get().(*writeState)
	state.incRef()

	state.op, state.majority = s.writeOpPool.Get(), majority

	state.op.namespace = ts.StringID(namespace)
	state.op.request.ID = tsID.Data()
	state.op.request.Datapoint.Value = value
	state.op.request.Datapoint.Timestamp = timestamp
	state.op.request.Datapoint.TimestampType = timeType
	state.op.request.Datapoint.Annotation = annotation
	state.op.completionFn = state.completionFn

	if err := s.topoMap.RouteForEach(tsID, func(idx int, host topology.Host) {
		// First count all the pending write requests to ensure
		// we count the amount we're going to be waiting for
		// before we enqueue the completion fns that rely on having
		// an accurate number of the pending requests when they execute
		state.pending++
		state.queues = append(state.queues, s.queues[idx])
	}); err != nil {
		state.decRef()
		s.RUnlock()
		return err
	}

	state.Lock()

	for i := range state.queues {
		if err := state.queues[i].Enqueue(state.op); err != nil {
			msg := fmt.Sprintf("failed to enqueue write: %v", err)
			s.opts.InstrumentOptions().Logger().Error(msg)
			panic(msg)
			// NB(r): if this happens we have a bug, once we are in the read
			// lock the current queues should never be closed
		}

		state.incRef()
		enqueued++
	}

	// Wait for writes to complete. We don't need to loop over Wait() since there
	// are no spurious wakeups in Golang and the condition is one-way and binary.
	s.RUnlock()
	state.Wait()

	err := s.writeConsistencyResult(majority, enqueued,
		enqueued-atomic.LoadInt32(&state.pending), int32(len(state.errors)), state.errors)
	s.incWriteMetrics(err, int32(len(state.errors)))

	state.Unlock()
	state.decRef()

	return err
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
		success                = false
	)

	rangeStart, tsErr := convert.ToValue(startInclusive, rpc.TimeType_UNIX_NANOSECONDS)
	if tsErr != nil {
		return nil, tsErr
	}

	rangeEnd, tsErr := convert.ToValue(endExclusive, rpc.TimeType_UNIX_NANOSECONDS)
	if tsErr != nil {
		return nil, tsErr
	}

	s.RLock()
	if s.state != stateOpen {
		s.RUnlock()
		return nil, errSessionStateNotOpen
	}

	iters := s.seriesIteratorsPool.Get(len(ids))
	iters.Reset(len(ids))

	defer func() {
		// NB(r): Ensure we close iters if an error is returned
		if !success {
			iters.Close()
		}
	}()

	fetchBatchOpsByHostIdx = s.fetchBatchOpArrayArrayPool.Get()

	majority = atomic.LoadInt32(&s.majority)

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
			err := s.readConsistencyResult(majority, enqueued, responded, errsLen, reportErrors)
			s.incFetchMetrics(err, errsLen)
			if err != nil {
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
			// to iter. Reset down below before setting the iterator in the results array,
			// which would cause a nil pointer exception.
			remaining := atomic.AddInt32(&pending, -1)
			var complete bool

			switch s.readLevel {
			case ReadConsistencyLevelOne:
				complete = remaining == 0 || snapshotSuccess > 0
			case ReadConsistencyLevelMajority, ReadConsistencyLevelUnstrictMajority:
				complete = remaining == 0 || snapshotSuccess >= majority
			case ReadConsistencyLevelAll:
				complete = remaining == 0
			}

			if complete && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
				allCompletionFn()
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
	success = true
	return iters, nil
}

func (s *session) writeConsistencyResult(
	majority, enqueued, responded, resultErrs int32,
	errs []error,
) error {
	if resultErrs == 0 {
		return nil
	}

	// Check consistency level satisfied
	success := enqueued - resultErrs
	switch s.writeLevel {
	case topology.ConsistencyLevelAll:
		return newConsistencyResultError(s.writeLevel, int(enqueued), int(responded), errs)
	case topology.ConsistencyLevelMajority:
		if success >= majority {
			// Meets majority
			break
		}
		return newConsistencyResultError(s.writeLevel, int(enqueued), int(responded), errs)
	case topology.ConsistencyLevelOne:
		if success > 0 {
			// Meets one
			break
		}
		return newConsistencyResultError(s.writeLevel, int(enqueued), int(responded), errs)
	}

	return nil
}

func (s *session) readConsistencyResult(
	majority, enqueued, responded, resultErrs int32,
	errs []error,
) error {
	if resultErrs == 0 {
		return nil
	}

	// Check consistency level satisfied
	success := enqueued - resultErrs
	switch s.readLevel {
	case ReadConsistencyLevelAll:
		return newConsistencyResultError(s.readLevel, int(enqueued), int(responded), errs)
	case ReadConsistencyLevelMajority:
		if success >= majority {
			// Meets majority
			break
		}
		return newConsistencyResultError(s.readLevel, int(enqueued), int(responded), errs)
	case ReadConsistencyLevelOne, ReadConsistencyLevelUnstrictMajority:
		if success > 0 {
			// Meets one
			break
		}
		return newConsistencyResultError(s.readLevel, int(enqueued), int(responded), errs)
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

func (s *session) Replicas() int { return int(atomic.LoadInt32(&s.replicas)) }

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

func (s *session) FetchBootstrapBlocksFromPeers(
	namespace ts.ID,
	shard uint32,
	start, end time.Time,
	opts bootstrap.Options,
) (bootstrap.ShardResult, error) {
	var (
		result = newBlocksResult(s.opts, opts, s.multiReaderIteratorPool)
		doneCh = make(chan error, 1)
		onDone = func(err error) {
			select {
			case doneCh <- err:
			default:
			}
		}
		m = s.streamFromPeersMetricsForShard(shard)
	)

	peers, err := s.peersForShard(shard)
	if err != nil {
		return nil, err
	}

	m.fetchBlocksFromPeers.Update(1)

	// Begin pulling metadata, if one or multiple peers fail no error will
	// be returned from this routine as long as one peer succeeds completely
	metadataCh := make(chan blocksMetadata, 4096)
	go func() {
		err := s.streamBlocksMetadataFromPeers(namespace, shard, peers, start, end, metadataCh, m)

		close(metadataCh)
		if err != nil { // Bail early
			onDone(err)
		}
	}()

	// Begin consuming metadata and making requests
	go func() {
		s.streamBlocksFromPeers(namespace, shard, peers, metadataCh, opts, result, m)
		onDone(nil)
	}()

	err = <-doneCh
	m.fetchBlocksFromPeers.Update(0)

	if err != nil {
		return nil, err
	}
	return result.result, nil
}

func (s *session) streamBlocksFromPeers(
	namespace ts.ID,
	shard uint32,
	peers []hostQueue,
	ch <-chan blocksMetadata,
	opts bootstrap.Options,
	result *blocksResult,
	m *streamFromPeersMetrics,
) {
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
		enqueueCh.maybeDestruct()
		// prevents deadlock in case nothing is ever enqueued
		// todo@bl: this is awful, improve this
	}()

	// Fetch blocks from peers as results become ready
	peerQueues := make(peerBlocksQueues, len(peers))
	for i, peer := range peers {
		peer := peer
		workers := s.streamBlocksWorkers
		drainEvery := 100 * time.Millisecond
		processFn := func(batch []*blocksMetadata) {
			s.streamBlocksBatchFromPeer(namespace, shard, peer, batch, opts, result, enqueueCh, retrier)
		}
		peerQueues[i] = s.newPeerBlocksQueueFn(peer, peerBlocksBatchSize, drainEvery, workers, processFn)
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
			enqueueCh.done()
			continue
		}

		completed := uint32(0)
		onDone := func() {
			// Mark completion when all queues drained
			if atomic.AddUint32(&completed, 1) == queues {
				enqueueCh.done()
			}
		}

		for _, peerBlocksMetadata := range perPeerBlocksMetadata {
			if len(peerBlocksMetadata.blocks) > 0 {
				peerQueues.findQueue(peerBlocksMetadata.peer).enqueue(peerBlocksMetadata, onDone)
			}
		}
	}

	peerQueues.closeAll()
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
		req    = newFetchBlocksRawRequest(namespace, shard, batch)
		result *rpc.FetchBlocksRawResult_

		nowFn              = opts.ClockOptions().NowFn()
		ropts              = opts.RetentionOptions()
		blockSize          = ropts.BlockSize()
		retention          = ropts.RetentionPeriod()
		earliestBlockStart = nowFn().Add(-retention).Truncate(blockSize)
	)

	for i, b := range batch {
		starts := make([]int64, 0, len(b.blocks))
		sort.Sort(blockMetadatasByTime(b.blocks))
		for _, block := range b.blocks {
			if block.start.Before(earliestBlockStart) {
				continue // Fell out of retention while we were streaming blocks
			}
			starts = append(starts, block.start.UnixNano())
		}
		req.Elements[i] = &rpc.FetchBlocksRawRequestElement{ID: b.id.Data(), Starts: starts}
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
		for _, b := range batch {
			s.streamBlocksReattemptFromPeers(b.blocks, enqueueCh)
		}
		return
	}

	if len(result.Elements) > len(batch) {
		s.log.Errorf("stream blocks response from peer %s returned too many IDs", peer.Host().String())
		result.Elements = result.Elements[:len(batch)]
	}

	// Calculate earliest block start as of end of request
	earliestBlockStart = nowFn().Add(-retention).Truncate(blockSize)

	// NB(r): As discussed in the new session constructor this processing needs to happen
	// synchronously so that our outstanding work is calculated correctly, however we do
	// not want to steal all the CPUs on the machine to avoid dropping incoming writes so
	// we reduce the amount of processors in this critical section with a semaphore.
	token := <-s.streamBlocksResultsProcessors

	// Parse and act on result

	for i := range result.Elements {
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
				).Errorf("stream blocks response from peer %s returned too many blocks", peer.Host().String())
				break
			}

			if err := validateBlock(
				result.Elements[i].Blocks[j-missed], // Index of the received block could be offset by missed blocks
				batch[i].blocks[j],
				blocksResult,
				earliestBlockStart,
				id,
				peer.Host().String(),
				s.log); err != nil {
				if err == errBlockStartMismatch {
					missed++
				}
				s.streamBlocksReattemptFromPeers([]blockMetadata{batch[i].blocks[j]}, enqueueCh)
			}
		}
	}

	// Return token to continue collecting results in other go routines
	s.streamBlocksResultsProcessors <- token
}

func (s *session) streamBlocksReattemptFromPeers(blocks []blockMetadata, enqueueCh *enqueueChannel) {
	for i := range blocks { // Reconstruct peers metadata for reattempt
		reattemptBlocksMetadata := make([]*blocksMetadata, len(blocks[i].reattempt.peersMetadata))
		for j := range reattemptBlocksMetadata {
			reattemptBlocksMetadata[j] = copyBlocksMetadata(blocks, i, j)
		}

		enqueueCh.enqueue(reattemptBlocksMetadata) // Re-enqueue the block to be fetched
	}
}
