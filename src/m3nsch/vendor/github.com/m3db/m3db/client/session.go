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
	"bytes"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/network/server/tchannelthrift/convert"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
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

type resultTypeEnum string

const (
	resultTypeMetadata  resultTypeEnum = "metadata"
	resultTypeBootstrap                = "bootstrap"
	resultTypeRaw                      = "raw"
	resultTypeTest                     = "test"
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
	// errSessionHasNoHostQueueForHost is raised when host queue requested for a missing host
	errSessionHasNoHostQueueForHost = errors.New("session has no host queue for host")
)

type session struct {
	sync.RWMutex

	opts                             Options
	scope                            tally.Scope
	nowFn                            clock.NowFn
	log                              xlog.Logger
	writeLevel                       topology.ConsistencyLevel
	readLevel                        ReadConsistencyLevel
	newHostQueueFn                   newHostQueueFn
	topo                             topology.Topology
	topoMap                          topology.Map
	topoWatch                        topology.MapWatch
	replicas                         int32
	majority                         int32
	queues                           []hostQueue
	queuesByHostID                   map[string]hostQueue
	state                            state
	writeRetrier                     xretry.Retrier
	fetchRetrier                     xretry.Retrier
	contextPool                      context.Pool
	idPool                           ts.IdentifierPool
	writeOpPool                      *writeOpPool
	fetchBatchOpPool                 *fetchBatchOpPool
	fetchBatchOpArrayArrayPool       *fetchBatchOpArrayArrayPool
	iteratorArrayPool                encoding.IteratorArrayPool
	readerSliceOfSlicesIteratorPool  *readerSliceOfSlicesIteratorPool
	multiReaderIteratorPool          encoding.MultiReaderIteratorPool
	seriesIteratorPool               encoding.SeriesIteratorPool
	seriesIteratorsPool              encoding.MutableSeriesIteratorsPool
	writeAttemptPool                 *writeAttemptPool
	writeStatePool                   *writeStatePool
	digestPool                       sync.Pool
	fetchAttemptPool                 *fetchAttemptPool
	fetchBatchSize                   int
	newPeerBlocksQueueFn             newPeerBlocksQueueFn
	reattemptStreamBlocksFromPeersFn reattemptStreamBlocksFromPeersFn
	origin                           topology.Host
	streamBlocksMaxBlockRetries      int
	streamBlocksWorkers              xsync.WorkerPool
	streamBlocksBatchSize            int
	streamBlocksMetadataBatchTimeout time.Duration
	streamBlocksBatchTimeout         time.Duration
	metrics                          sessionMetrics
}

type shardMetricsKey struct {
	shardID    uint32
	resultType resultTypeEnum
}

type sessionMetrics struct {
	sync.RWMutex
	writeSuccess               tally.Counter
	writeErrors                tally.Counter
	writeNodesRespondingErrors []tally.Counter
	fetchSuccess               tally.Counter
	fetchErrors                tally.Counter
	fetchNodesRespondingErrors []tally.Counter
	topologyUpdatedSuccess     tally.Counter
	topologyUpdatedError       tally.Counter
	streamFromPeersMetrics     map[shardMetricsKey]streamFromPeersMetrics
}

func newSessionMetrics(scope tally.Scope) sessionMetrics {
	return sessionMetrics{
		writeSuccess:           scope.Counter("write.success"),
		writeErrors:            scope.Counter("write.errors"),
		fetchSuccess:           scope.Counter("fetch.success"),
		fetchErrors:            scope.Counter("fetch.errors"),
		topologyUpdatedSuccess: scope.Counter("topology.updated-success"),
		topologyUpdatedError:   scope.Counter("topology.updated-error"),
		streamFromPeersMetrics: make(map[shardMetricsKey]streamFromPeersMetrics),
	}
}

type streamFromPeersMetrics struct {
	fetchBlocksFromPeers       tally.Gauge
	metadataFetches            tally.Gauge
	metadataFetchBatchCall     tally.Counter
	metadataFetchBatchSuccess  tally.Counter
	metadataFetchBatchError    tally.Counter
	metadataReceived           tally.Counter
	fetchBlockSuccess          tally.Counter
	fetchBlockError            tally.Counter
	fetchBlockFinalError       tally.Counter
	fetchBlockRetriesReqError  tally.Counter
	fetchBlockRetriesRespError tally.Counter
	blocksEnqueueChannel       tally.Gauge
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

	scope := opts.InstrumentOptions().MetricsScope()

	s := &session{
		opts:                 opts,
		scope:                scope,
		nowFn:                opts.ClockOptions().NowFn(),
		log:                  opts.InstrumentOptions().Logger(),
		writeLevel:           opts.WriteConsistencyLevel(),
		readLevel:            opts.ReadConsistencyLevel(),
		newHostQueueFn:       newHostQueue,
		queuesByHostID:       make(map[string]hostQueue),
		topo:                 topo,
		fetchBatchSize:       opts.FetchBatchSize(),
		newPeerBlocksQueueFn: newPeerBlocksQueue,
		writeRetrier:         opts.WriteRetrier(),
		fetchRetrier:         opts.FetchRetrier(),
		contextPool:          opts.ContextPool(),
		idPool:               opts.IdentifierPool(),
		metrics:              newSessionMetrics(scope),
	}
	s.reattemptStreamBlocksFromPeersFn = s.streamBlocksReattemptFromPeers
	writeAttemptPoolOpts := pool.NewObjectPoolOptions().
		SetSize(opts.WriteOpPoolSize()).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(
			scope.SubScope("write-attempt-pool"),
		))
	s.writeAttemptPool = newWriteAttemptPool(s, writeAttemptPoolOpts)
	s.writeAttemptPool.Init()
	fetchAttemptPoolOpts := pool.NewObjectPoolOptions().
		SetSize(opts.FetchBatchOpPoolSize()).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(
			scope.SubScope("fetch-attempt-pool"),
		))
	s.fetchAttemptPool = newFetchAttemptPool(s, fetchAttemptPoolOpts)
	s.fetchAttemptPool.Init()
	s.digestPool = sync.Pool{New: func() interface{} {
		return digest.NewDigest()
	}}

	if opts, ok := opts.(AdminOptions); ok {
		s.origin = opts.Origin()
		s.streamBlocksMaxBlockRetries = opts.FetchSeriesBlocksMaxBlockRetries()
		s.streamBlocksWorkers = xsync.NewWorkerPool(opts.FetchSeriesBlocksBatchConcurrency())
		s.streamBlocksWorkers.Init()
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

func (s *session) streamFromPeersMetricsForShard(
	shard uint32,
	resultType resultTypeEnum,
) *streamFromPeersMetrics {
	mKey := shardMetricsKey{shardID: shard, resultType: resultType}
	s.metrics.RLock()
	m, ok := s.metrics.streamFromPeersMetrics[mKey]
	s.metrics.RUnlock()

	if ok {
		return &m
	}

	scope := s.opts.InstrumentOptions().MetricsScope()

	s.metrics.Lock()
	m, ok = s.metrics.streamFromPeersMetrics[mKey]
	if ok {
		s.metrics.Unlock()
		return &m
	}
	scope = scope.SubScope("stream-from-peers").Tagged(map[string]string{
		"shard":      fmt.Sprintf("%d", shard),
		"resultType": string(resultType),
	})
	m = streamFromPeersMetrics{
		fetchBlocksFromPeers:      scope.Gauge("fetch-blocks-inprogress"),
		metadataFetches:           scope.Gauge("fetch-metadata-peers-inprogress"),
		metadataFetchBatchCall:    scope.Counter("fetch-metadata-peers-batch-call"),
		metadataFetchBatchSuccess: scope.Counter("fetch-metadata-peers-batch-success"),
		metadataFetchBatchError:   scope.Counter("fetch-metadata-peers-batch-error"),
		metadataReceived:          scope.Counter("fetch-metadata-peers-received"),
		fetchBlockSuccess:         scope.Counter("fetch-block-success"),
		fetchBlockError:           scope.Counter("fetch-block-error"),
		fetchBlockFinalError:      scope.Counter("fetch-block-final-error"),
		fetchBlockRetriesReqError: scope.Tagged(map[string]string{
			"reason": "request-error",
		}).Counter("fetch-block-retries"),
		fetchBlockRetriesRespError: scope.Tagged(map[string]string{
			"reason": "response-error",
		}).Counter("fetch-block-retries"),
		blocksEnqueueChannel: scope.Gauge("fetch-blocks-enqueue-channel-length"),
	}
	s.metrics.streamFromPeersMetrics[mKey] = m
	s.metrics.Unlock()
	return &m
}

func (s *session) incWriteMetrics(consistencyResultErr error, respErrs int32) {
	if idx := s.nodesRespondingErrorsMetricIndex(respErrs); idx >= 0 {
		s.metrics.writeNodesRespondingErrors[idx].Inc(1)
	}
	if consistencyResultErr == nil {
		s.metrics.writeSuccess.Inc(1)
	} else {
		s.metrics.writeErrors.Inc(1)
	}
}

func (s *session) incFetchMetrics(consistencyResultErr error, respErrs int32) {
	if idx := s.nodesRespondingErrorsMetricIndex(respErrs); idx >= 0 {
		s.metrics.fetchNodesRespondingErrors[idx].Inc(1)
	}
	if consistencyResultErr == nil {
		s.metrics.fetchSuccess.Inc(1)
	} else {
		s.metrics.fetchErrors.Inc(1)
	}
}

func (s *session) nodesRespondingErrorsMetricIndex(respErrs int32) int32 {
	idx := respErrs - 1
	replicas := int32(s.Replicas())
	if respErrs > replicas {
		// Cap to the max replicas, we might get more errors
		// when a node is initializing a shard causing replicas + 1
		// nodes to respond to operations
		idx = replicas - 1
	}
	return idx
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

	topoMap := watch.Get()

	queues, replicas, majority, err := s.hostQueues(topoMap, nil)
	if err != nil {
		s.Unlock()
		return err
	}
	s.setTopologyWithLock(topoMap, queues, replicas, majority)
	s.topoWatch = watch

	// NB(r): Alloc pools that can take some time in Open, expectation
	// is already that Open will take some time
	writeOpPoolOpts := pool.NewObjectPoolOptions().
		SetSize(s.opts.WriteOpPoolSize()).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("write-op-pool"),
		))
	s.writeOpPool = newWriteOpPool(writeOpPoolOpts)
	s.writeOpPool.Init()
	writeStatePoolOpts := pool.NewObjectPoolOptions().
		SetSize(s.opts.WriteOpPoolSize()).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("write-state-pool"),
		))
	s.writeStatePool = newWriteStatePool(s, writeStatePoolOpts)
	s.writeStatePool.Init()
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

	go func() {
		for range watch.C() {
			s.log.Info("received update for topology")
			topoMap := watch.Get()

			s.RLock()
			existingQueues := s.queues
			s.RUnlock()

			queues, replicas, majority, err := s.hostQueues(topoMap, existingQueues)
			if err != nil {
				s.log.Errorf("could not update topology map: %v", err)
				s.metrics.topologyUpdatedError.Inc(1)
				continue
			}
			s.Lock()
			s.setTopologyWithLock(topoMap, queues, replicas, majority)
			s.Unlock()
			s.metrics.topologyUpdatedSuccess.Inc(1)
		}
	}()

	return nil
}

func (s *session) BorrowConnection(hostID string, fn withConnectionFn) error {
	s.RLock()
	unlocked := false
	queue, ok := s.queuesByHostID[hostID]
	if !ok {
		s.RUnlock()
		return errSessionHasNoHostQueueForHost
	}
	err := queue.BorrowConnection(func(c rpc.TChanNode) {
		// Unlock early on success
		s.RUnlock()
		unlocked = true

		// Execute function with borrowed connection
		fn(c)
	})
	if !unlocked {
		s.RUnlock()
	}
	return err
}

func (s *session) hostQueues(
	topoMap topology.Map,
	existing []hostQueue,
) ([]hostQueue, int, int, error) {
	// NB(r): we leave existing writes in the host queues to finish
	// as they are already enroute to their destination. This is an edge case
	// that might result in leaving nodes counting towards quorum, but fixing it
	// would result in additional chatter.

	start := s.nowFn()

	existingByHostID := make(map[string]hostQueue, len(existing))
	for _, queue := range existing {
		existingByHostID[queue.Host().ID()] = queue
	}

	hosts := topoMap.Hosts()
	queues := make([]hostQueue, 0, len(hosts))
	newQueues := make([]hostQueue, 0, len(hosts))
	for _, host := range hosts {
		if existingQueue, ok := existingByHostID[host.ID()]; ok {
			queues = append(queues, existingQueue)
			continue
		}
		newQueue := s.newHostQueue(host, topoMap)
		queues = append(queues, newQueue)
		newQueues = append(newQueues, newQueue)
	}

	shards := topoMap.ShardSet().AllIDs()
	minConnectionCount := s.opts.MinConnectionCount()
	replicas := topoMap.Replicas()
	majority := topoMap.MajorityReplicas()

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
			for _, queue := range newQueues {
				queue.Close()
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
					err := fmt.Errorf("timed out connecting, returning success")
					s.log.Warnf("cluster connect with consistency any: %v", err)
					connected = true
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
			routeErr := topoMap.RouteShardForEach(shard, func(idx int, _ topology.Host) {
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
		if clusterAvailable { // All done
			break
		}
		time.Sleep(clusterConnectWaitInterval)
	}

	connected = true
	return queues, replicas, majority, nil
}

func (s *session) setTopologyWithLock(topoMap topology.Map, queues []hostQueue, replicas, majority int) {
	prevQueues := s.queues

	newQueuesByHostID := make(map[string]hostQueue, len(queues))
	for _, queue := range queues {
		newQueuesByHostID[queue.Host().ID()] = queue
	}

	s.queues = queues
	s.queuesByHostID = newQueuesByHostID

	s.topoMap = topoMap

	atomic.StoreInt32(&s.replicas, int32(replicas))
	atomic.StoreInt32(&s.majority, int32(majority))

	// NB(r): Always recreate the fetch batch op array array pool as it must be
	// the exact length of the queues as we index directly into the return array in
	// in fetch calls
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

	if s.iteratorArrayPool == nil {
		s.iteratorArrayPool = encoding.NewIteratorArrayPool([]pool.Bucket{
			pool.Bucket{
				Capacity: replicas,
				Count:    s.opts.SeriesIteratorPoolSize(),
			},
		})
		s.iteratorArrayPool.Init()
	}
	if s.readerSliceOfSlicesIteratorPool == nil {
		size := replicas * s.opts.SeriesIteratorPoolSize()
		poolOpts := pool.NewObjectPoolOptions().
			SetSize(size).
			SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
				s.scope.SubScope("reader-slice-of-slices-iterator-pool"),
			))
		s.readerSliceOfSlicesIteratorPool = newReaderSliceOfSlicesIteratorPool(poolOpts)
		s.readerSliceOfSlicesIteratorPool.Init()
	}
	if s.multiReaderIteratorPool == nil {
		size := replicas * s.opts.SeriesIteratorPoolSize()
		poolOpts := pool.NewObjectPoolOptions().
			SetSize(size).
			SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
				s.scope.SubScope("multi-reader-iterator-pool"),
			))
		s.multiReaderIteratorPool = encoding.NewMultiReaderIteratorPool(poolOpts)
		s.multiReaderIteratorPool.Init(s.opts.ReaderIteratorAllocate())
	}
	if replicas > len(s.metrics.writeNodesRespondingErrors) {
		curr := len(s.metrics.writeNodesRespondingErrors)
		for i := curr; i < replicas; i++ {
			tags := map[string]string{"nodes": fmt.Sprintf("%d", i+1)}
			counter := s.scope.Tagged(tags).Counter("write.nodes-responding-error")
			s.metrics.writeNodesRespondingErrors =
				append(s.metrics.writeNodesRespondingErrors, counter)
		}
	}
	if replicas > len(s.metrics.fetchNodesRespondingErrors) {
		curr := len(s.metrics.fetchNodesRespondingErrors)
		for i := curr; i < replicas; i++ {
			tags := map[string]string{"nodes": fmt.Sprintf("%d", i+1)}
			counter := s.scope.Tagged(tags).Counter("fetch.nodes-responding-error")
			s.metrics.fetchNodesRespondingErrors =
				append(s.metrics.fetchNodesRespondingErrors, counter)
		}
	}

	// Asynchronously close the set of host queues no longer in use
	go func() {
		for _, queue := range prevQueues {
			newQueue, ok := newQueuesByHostID[queue.Host().ID()]
			if !ok || newQueue != queue {
				queue.Close()
			}
		}
	}()

	s.log.Infof("successfully updated topology to %d hosts", topoMap.HostsLen())
}

func (s *session) newHostQueue(host topology.Host, topoMap topology.Map) hostQueue {
	// NB(r): Due to hosts being replicas we have:
	// = replica * numWrites
	// = total writes to all hosts
	// We need to pool:
	// = replica * (numWrites / writeBatchSize)
	// = number of batch request structs to pool
	// For purposes of simplifying the options for pooling the write op pool size
	// represents the number of ops to pool not including replication, this is due
	// to the fact that the ops are shared between the different host queue replicas.
	totalBatches := topoMap.Replicas() *
		int(math.Ceil(float64(s.opts.WriteOpPoolSize())/float64(s.opts.WriteBatchSize())))
	hostBatches := int(math.Ceil(float64(totalBatches) / float64(topoMap.HostsLen())))
	writeBatchRequestPoolOpts := pool.NewObjectPoolOptions().
		SetSize(hostBatches).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("write-batch-request-pool"),
		))
	writeBatchRequestPool := newWriteBatchRawRequestPool(writeBatchRequestPoolOpts)
	writeBatchRequestPool.Init()
	writeBatchRawRequestElementArrayPoolOpts := pool.NewObjectPoolOptions().
		SetSize(hostBatches).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("id-datapoint-array-pool"),
		))
	writeBatchRawRequestElementArrayPool := newWriteBatchRawRequestElementArrayPool(
		writeBatchRawRequestElementArrayPoolOpts, s.opts.WriteBatchSize())
	writeBatchRawRequestElementArrayPool.Init()
	hostQueue := s.newHostQueueFn(host, writeBatchRequestPool, writeBatchRawRequestElementArrayPool, s.opts)
	hostQueue.Open()
	return hostQueue
}

func (s *session) Write(
	namespace, id string,
	t time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	w := s.writeAttemptPool.Get()
	w.args.namespace, w.args.id =
		namespace, id
	w.args.t, w.args.value, w.args.unit, w.args.annotation =
		t, value, unit, annotation
	err := s.writeRetrier.Attempt(w.attemptFn)
	s.writeAttemptPool.Put(w)
	return err
}

func (s *session) writeAttempt(
	namespace, id string,
	t time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	var (
		enqueued int32
		majority = atomic.LoadInt32(&s.majority)
		ctx      = s.contextPool.Get()
		nsID     = s.idPool.GetStringID(ctx, namespace)
		tsID     = s.idPool.GetStringID(ctx, id)
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

	state := s.writeStatePool.Get()
	state.topoMap = s.topoMap
	state.incRef()

	// todo@bl: Can we combine the writeOpPool and the writeStatePool?
	state.op, state.majority = s.writeOpPool.Get(), majority
	state.ctx, state.nsID, state.tsID = ctx, nsID, tsID

	state.op.namespace = nsID
	state.op.request.ID = tsID.Data().Get()
	state.op.shardID = s.topoMap.ShardSet().Lookup(tsID)
	state.op.request.ID = tsID.Data().Get()
	state.op.request.Datapoint.Value = value
	state.op.request.Datapoint.Timestamp = timestamp
	state.op.request.Datapoint.TimestampType = timeType
	state.op.request.Datapoint.Annotation = annotation
	state.op.completionFn = state.completionFn

	if err := s.topoMap.RouteForEach(tsID, func(idx int, host topology.Host) {
		// Count pending write requests before we enqueue the completion fns,
		// which rely on the count when executing
		state.pending++
		state.queues = append(state.queues, s.queues[idx])
	}); err != nil {
		state.decRef()
		s.RUnlock()
		return err
	}

	state.Lock()

	for i := range state.queues {
		state.incRef()
		if err := state.queues[i].Enqueue(state.op); err != nil {
			state.Unlock()
			state.decRef()

			// NB(r): if this happens we have a bug, once we are in the read
			// lock the current queues should never be closed
			s.RUnlock()
			s.opts.InstrumentOptions().Logger().Errorf("failed to enqueue write: %v", err)
			return err
		}

		enqueued++
	}

	s.RUnlock()
	state.Wait()

	err := s.writeConsistencyResult(majority, enqueued, enqueued-state.pending, int32(len(state.errors)), state.errors)
	s.incWriteMetrics(err, int32(len(state.errors)))

	state.Unlock()
	state.decRef()

	return err
}

func (s *session) Fetch(
	namespace string,
	id string,
	startInclusive, endExclusive time.Time,
) (encoding.SeriesIterator, error) {
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

func (s *session) FetchAll(
	namespace string,
	ids []string,
	startInclusive, endExclusive time.Time,
) (encoding.SeriesIterators, error) {
	f := s.fetchAttemptPool.Get()
	f.args.namespace, f.args.ids, f.args.start, f.args.end =
		namespace, ids, startInclusive, endExclusive
	err := s.fetchRetrier.Attempt(f.attemptFn)
	result := f.result
	s.fetchAttemptPool.Put(f)
	return result, err
}

func (s *session) fetchAllAttempt(
	namespace string,
	ids []string,
	startInclusive, endExclusive time.Time,
) (encoding.SeriesIterators, error) {
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
		// NB(r): Ensure we cover all edge cases and close the iters in any case
		// of an error being returned
		if !success {
			iters.Close()
		}
	}()

	// NB(r): We must take and return pooled items in the session read lock for the
	// pools that change during a topology update.
	// This is due to when a queue is re-initialized it enqueues a fixed number
	// of entries into the backing channel for the pool and will forever stall
	// on the last few puts if any unexpected entries find their way there
	// while it is filling.
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
				atomic.AddInt32(&errs, 1)
				// NB(r): reuse the error lock here as we do not want to create
				// a whole lot of locks for every single ID fetched due to size
				// of mutex being non-trivial and likely to cause more stack growth
				// or GC pressure if ends up on heap which is likely due to naive
				// escape analysis.
				resultErrLock.Lock()
				errors = append(errors, err)
				resultErrLock.Unlock()
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
			switch s.readLevel {
			case ReadConsistencyLevelOne:
				complete := snapshotSuccess > 0 || doneAll
				if complete && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
					allCompletionFn()
				}
			case ReadConsistencyLevelMajority, ReadConsistencyLevelUnstrictMajority:
				complete := snapshotSuccess >= majority || doneAll
				if complete && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
					allCompletionFn()
				}
			case ReadConsistencyLevelAll:
				if doneAll && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
					allCompletionFn()
				}
			}

			if atomic.AddInt32(&resultsAccessors, -1) == 0 {
				s.iteratorArrayPool.Put(results)
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
				// NB(r): Note that we defer to the host queue to take ownership
				// of these ops and for returning the ops to the pool when done as
				// they know when their use is complete.
				f = s.fetchBatchOpPool.Get()
				f.IncRef()
				fetchBatchOpsByHostIdx[hostIdx] = append(fetchBatchOpsByHostIdx[hostIdx], f)
				f.request.RangeStart = rangeStart
				f.request.RangeEnd = rangeEnd
				f.request.RangeType = rpc.TimeType_UNIX_NANOSECONDS
			}

			// Append IDWithNamespace to this request
			f.append(nsID, tsID.Data().Get(), completionFn)
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
			// Passing ownership of the op itself to the host queue
			f.DecRef()
			if err := s.queues[idx].Enqueue(f); err != nil && enqueueErr == nil {
				enqueueErr = err
				break
			}
		}
		if enqueueErr != nil {
			break
		}
	}
	s.fetchBatchOpArrayArrayPool.Put(fetchBatchOpsByHostIdx)
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
		if success >= majority { // Meets majority
			break
		}
		return newConsistencyResultError(s.writeLevel, int(enqueued), int(responded), errs)
	case topology.ConsistencyLevelOne:
		if success > 0 { // Meets one
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
	t.request.NameSpace = namespace.Data().Get()
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

func (s *session) peersForShard(shard uint32) ([]peer, error) {
	s.RLock()
	peers := make([]peer, 0, s.topoMap.Replicas())
	err := s.topoMap.RouteShardForEach(shard, func(idx int, host topology.Host) {
		if s.origin != nil && s.origin.ID() == host.ID() {
			// Don't include the origin host
			return
		}
		peers = append(peers, newPeer(s, host))
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
		m          = s.streamFromPeersMetricsForShard(shard, resultTypeMetadata)
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
	opts result.Options,
) (result.ShardResult, error) {
	var (
		result   = newBulkBlocksResult(s.opts, opts)
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
		m = s.streamFromPeersMetricsForShard(shard, resultTypeBootstrap)
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
			metadataCh, opts, result, m, s.streamAndGroupCollectedBlocksMetadata)
		onDone(err)
	}()

	if err := waitDone(); err != nil {
		return nil, err
	}
	return result.result, nil
}

func (s *session) FetchBlocksFromPeers(
	namespace ts.ID,
	shard uint32,
	metadatas []block.ReplicaMetadata,
	opts result.Options,
) (PeerBlocksIter, error) {
	var (
		logger   = opts.InstrumentOptions().Logger()
		complete = int64(0)
		doneCh   = make(chan error, 1)
		outputCh = make(chan peerBlocksDatapoint, 4096)
		result   = newStreamBlocksResult(s.opts, opts, outputCh)
		onDone   = func(err error) {
			atomic.StoreInt64(&complete, 1)
			select {
			case doneCh <- err:
			default:
			}
		}
		m = s.streamFromPeersMetricsForShard(shard, resultTypeRaw)
	)

	peers, err := s.peersForShard(shard)
	if err != nil {
		return nil, err
	}
	peersByHost := make(map[string]peer, len(peers))
	for _, peer := range peers {
		peersByHost[peer.Host().ID()] = peer
	}

	go func() {
		for atomic.LoadInt64(&complete) == 0 {
			m.fetchBlocksFromPeers.Update(1)
			time.Sleep(gaugeReportInterval)
		}
		m.fetchBlocksFromPeers.Update(0)
	}()

	metadataCh := make(chan blocksMetadata, 4096)
	go func() {
		for _, rb := range metadatas {
			peer, ok := peersByHost[rb.Host.ID()]
			if !ok {
				logger.WithFields(
					xlog.NewLogField("peer", rb.Host.String()),
					xlog.NewLogField("id", rb.ID.String()),
					xlog.NewLogField("start", rb.Start.String()),
				).Warnf("replica requested from unknown peer, skipping")
				continue
			}
			metadataCh <- blocksMetadata{
				id:   rb.ID,
				peer: peer,
				blocks: []blockMetadata{
					blockMetadata{
						start:    rb.Start,
						size:     rb.Size,
						checksum: rb.Checksum,
					},
				},
			}
		}
		close(metadataCh)
	}()

	// Begin consuming metadata and making requests
	go func() {
		err := s.streamBlocksFromPeers(namespace, shard, peers,
			metadataCh, opts, result, m, s.passThruBlocksMetadata)
		close(outputCh)
		onDone(err)
	}()

	pbi := newPeerBlocksIter(outputCh, doneCh)
	return pbi, nil
}

func (s *session) streamBlocksMetadataFromPeers(
	namespace ts.ID,
	shard uint32,
	peers []peer,
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
	m.metadataFetches.Update(float64(pending))
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
			m.metadataFetches.Update(float64(atomic.AddInt64(&pending, -1)))
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
	peer peer,
	start, end time.Time,
	ch chan<- blocksMetadata,
	m *streamFromPeersMetrics,
) error {
	var (
		pageToken *int64
		retrier   = xretry.NewRetrier(xretry.NewOptions().
				SetBackoffFactor(2).
				SetMaxRetries(3).
				SetInitialBackoff(time.Second).
				SetJitter(true))
		optionIncludeSizes     = true
		optionIncludeChecksums = true
		moreResults            = true
	)
	// Declare before loop to avoid redeclaring each iteration
	attemptFn := func(client rpc.TChanNode) error {
		tctx, _ := thrift.NewContext(s.streamBlocksMetadataBatchTimeout)
		req := rpc.NewFetchBlocksMetadataRawRequest()
		req.NameSpace = namespace.Data().Get()
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
				id:     ts.BinaryID(checked.NewBytes(elem.ID, nil)),
				blocks: blockMetas,
			}
		}

		return nil
	}

	// NB(r): split the following methods up so they don't allocate
	// a closure per fetch blocks call
	var attemptErr error
	checkedAttemptFn := func(client rpc.TChanNode) {
		attemptErr = attemptFn(client)
	}

	fetchFn := func() error {
		borrowErr := peer.BorrowConnection(checkedAttemptFn)
		return xerrors.FirstError(borrowErr, attemptErr)
	}

	for moreResults {
		if err := retrier.Attempt(fetchFn); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) streamBlocksFromPeers(
	namespace ts.ID,
	shard uint32,
	peers []peer,
	ch <-chan blocksMetadata,
	opts result.Options,
	result blocksResult,
	m *streamFromPeersMetrics,
	streamMetadataFn streamBlocksMetadataFn,
) error {
	var (
		retrier = xretry.NewRetrier(xretry.NewOptions().
			SetBackoffFactor(2).
			SetMaxRetries(3).
			SetInitialBackoff(time.Second).
			SetJitter(true))
		enqueueCh           = newEnqueueChannel(m)
		peerBlocksBatchSize = s.streamBlocksBatchSize
	)

	// Consume the incoming metadata and enqueue to the ready channel
	go func() {
		streamMetadataFn(len(peers), ch, enqueueCh)
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
				result, enqueueCh, retrier, m)
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
			currStart, currEligible, blocksMetadataQueues, m)

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

type streamBlocksMetadataFn func(
	peersLen int,
	ch <-chan blocksMetadata,
	enqueueCh *enqueueChannel,
)

func (s *session) passThruBlocksMetadata(
	peersLen int,
	ch <-chan blocksMetadata,
	enqueueCh *enqueueChannel,
) {
	// Receive off of metadata channel
	for {
		m, ok := <-ch
		if !ok {
			break
		}
		res := []*blocksMetadata{&m}
		enqueueCh.enqueue(res)
	}
}

func (s *session) streamAndGroupCollectedBlocksMetadata(
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
		if !ok || received.submitted {
			received = &receivedBlocks{
				results: make([]*blocksMetadata, 0, peersLen),
			}
			metadata[m.id.Hash()] = received
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
	m *streamFromPeersMetrics,
) {
	// Free any references the pool still has
	for i := range pooledCurrStart {
		pooledCurrStart[i] = nil
	}
	for i := range pooledCurrEligible {
		pooledCurrEligible[i] = nil
	}
	var zeroed blocksMetadataQueue
	for i := range pooledCurrEligible {
		pooledBlocksMetadataQueues[i] = zeroed
	}

	// Get references to pooled arrays
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
		currUnselected := currStart[0].unselectedBlocks()[0]
		for i := len(currEligible) - 1; i >= 0; i-- {
			unselected := currEligible[i].unselectedBlocks()
			if unselected[0].reattempt.attempt == 0 {
				// Not attempted yet
				continue
			}

			// Check if eligible
			n := s.streamBlocksMaxBlockRetries
			if unselected[0].reattempt.peerAttempts(currEligible[i].peer) >= n {
				// Remove this block
				currEligible[i].removeFirstUnselected()
				// Swap current entry to tail
				blocksMetadatas(currEligible).swap(i, len(currEligible)-1)
				// Trim newly last entry
				currEligible = currEligible[:len(currEligible)-1]
				continue
			}
		}

		if len(currEligible) == 0 {
			// No current eligible peers to select from
			finalError := true
			if currUnselected.reattempt.failAllowed != nil && atomic.AddInt32(currUnselected.reattempt.failAllowed, -1) > 0 {
				// Some peers may still return results so we don't report error here
				finalError = false
			}

			if finalError {
				m.fetchBlockFinalError.Inc(1)
				s.log.WithFields(
					xlog.NewLogField("id", currID.String()),
					xlog.NewLogField("start", earliestStart),
					xlog.NewLogField("attempted", currUnselected.reattempt.attempt),
					xlog.NewLogField("attemptErrs", xerrors.Errors(currUnselected.reattempt.errs).Error()),
				).Error("retries failed for streaming blocks from peers")
			}

			continue
		}

		var (
			singlePeer         = len(currEligible) == 1
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
		// fewest attempts and fewest outstanding requests
		if singlePeer || sameNonNilChecksum {
			// Prepare the reattempt peers metadata so we can retry from any of the peers on failure
			peersMetadata := make([]blockMetadataReattemptPeerMetadata, 0, len(currEligible))
			for i := range currEligible {
				unselected := currEligible[i].unselectedBlocks()
				metadata := blockMetadataReattemptPeerMetadata{
					peer:     currEligible[i].peer,
					start:    unselected[0].start,
					size:     unselected[0].size,
					checksum: unselected[0].checksum,
				}
				peersMetadata = append(peersMetadata, metadata)
			}

			var bestPeer peer
			if singlePeer {
				bestPeer = currEligible[0].peer
			} else {
				// Order by least attempts then by least outstanding blocks being fetched
				blocksMetadataQueues = blocksMetadataQueues[:0]
				for i := range currEligible {
					insert := blocksMetadataQueue{
						blocksMetadata: currEligible[i],
						queue:          peerQueues.findQueue(currEligible[i].peer),
					}
					blocksMetadataQueues = append(blocksMetadataQueues, insert)
				}
				sort.Stable(blocksMetadatasQueuesByAttemptsAscOutstandingAsc(blocksMetadataQueues))

				// Select the best peer
				bestPeer = blocksMetadataQueues[0].queue.peer
			}

			// Remove the block from all other peers and increment index for selected peer
			for i := range currEligible {
				peer := currEligible[i].peer
				if peer != bestPeer {
					currEligible[i].removeFirstUnselected()
				} else {
					// Select this block
					idx := currEligible[i].idx
					currEligible[i].idx = idx + 1

					// Set the reattempt metadata
					currEligible[i].blocks[idx].reattempt.id = currID
					currEligible[i].blocks[idx].reattempt.attempt++
					currEligible[i].blocks[idx].reattempt.attempted =
						append(currEligible[i].blocks[idx].reattempt.attempted, peer)
					currEligible[i].blocks[idx].reattempt.peersMetadata = peersMetadata
				}
			}
		} else {
			numPeers := int32(len(currEligible))
			for i := range currEligible {
				// Select this block
				idx := currEligible[i].idx
				currEligible[i].idx = idx + 1

				// Set the reattempt metadata
				// NB(xichen): each block will only be retried on the same peer because we
				// already fan out the request to all peers. This means we merge data on
				// a best-effort basis and only fail if we failed to read data from all peers.
				peer := currEligible[i].peer
				block := currEligible[i].blocks[idx]
				currEligible[i].blocks[idx].reattempt.id = currID
				currEligible[i].blocks[idx].reattempt.attempt++
				currEligible[i].blocks[idx].reattempt.attempted =
					append(currEligible[i].blocks[idx].reattempt.attempted, peer)
				currEligible[i].blocks[idx].reattempt.failAllowed = &numPeers
				currEligible[i].blocks[idx].reattempt.peersMetadata = []blockMetadataReattemptPeerMetadata{
					{
						peer:     peer,
						start:    block.start,
						size:     block.size,
						checksum: block.checksum,
					},
				}
			}
		}
	}
}

func (s *session) streamBlocksBatchFromPeer(
	namespace ts.ID,
	shard uint32,
	peer peer,
	batch []*blocksMetadata,
	opts result.Options,
	blocksResult blocksResult,
	enqueueCh *enqueueChannel,
	retrier xretry.Retrier,
	m *streamFromPeersMetrics,
) {
	// Prepare request
	var (
		req          = rpc.NewFetchBlocksRawRequest()
		result       *rpc.FetchBlocksRawResult_
		reqBlocksLen int64

		nowFn              = opts.ClockOptions().NowFn()
		ropts              = opts.RetentionOptions()
		blockSize          = ropts.BlockSize()
		retention          = ropts.RetentionPeriod()
		earliestBlockStart = nowFn().Add(-retention).Truncate(blockSize)
	)
	req.NameSpace = namespace.Data().Get()
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
		reqBlocksLen += int64(len(starts))
		req.Elements[i] = &rpc.FetchBlocksRawRequestElement{
			ID:     batch[i].id.Data().Get(),
			Starts: starts,
		}
	}

	// Attempt request
	if err := retrier.Attempt(func() error {
		var attemptErr error
		borrowErr := peer.BorrowConnection(func(client rpc.TChanNode) {
			tctx, _ := thrift.NewContext(s.streamBlocksBatchTimeout)
			result, attemptErr = client.FetchBlocksRaw(tctx, req)
		})
		err := xerrors.FirstError(borrowErr, attemptErr)
		// Do not retry if cannot borrow the connection or
		// if the connection pool has no connections
		switch err {
		case errSessionHasNoHostQueueForHost,
			errConnectionPoolHasNoConnections:
			err = xerrors.NewNonRetryableError(err)
		}
		return err
	}); err != nil {
		blocksErr := fmt.Errorf(
			"stream blocks request error: error=%s, peer=%s",
			err.Error(), peer.Host().String(),
		)
		for i := range batch {
			b := batch[i].blocks
			s.reattemptStreamBlocksFromPeersFn(b, enqueueCh, blocksErr, reqErrReason, m)
		}
		m.fetchBlockError.Inc(reqBlocksLen)
		s.log.Debugf(blocksErr.Error())
		return
	}

	// Parse and act on result
	tooManyIDsLogged := false
	for i := range result.Elements {
		if i >= len(batch) {
			m.fetchBlockError.Inc(int64(len(req.Elements[i].Starts)))
			m.fetchBlockFinalError.Inc(int64(len(req.Elements[i].Starts)))
			if !tooManyIDsLogged {
				tooManyIDsLogged = true
				s.log.WithFields(
					xlog.NewLogField("peer", peer.Host().String()),
				).Errorf("stream blocks more IDs than expected")
			}
			continue
		}

		id := batch[i].id
		if !bytes.Equal(id.Data().Get(), result.Elements[i].ID) {
			b := batch[i].blocks
			blocksErr := fmt.Errorf(
				"stream blocks mismatched ID: expectedID=%s, actualID=%s, indexID=%d, peer=%s",
				batch[i].id.String(), id.String(), i, peer.Host().String(),
			)
			s.reattemptStreamBlocksFromPeersFn(b, enqueueCh, blocksErr, respErrReason, m)
			m.fetchBlockError.Inc(int64(len(req.Elements[i].Starts)))
			s.log.Debugf(blocksErr.Error())
			continue
		}

		missed := 0
		tooManyBlocksLogged := false
		for j := range result.Elements[i].Blocks {
			if j >= len(req.Elements[i].Starts) {
				m.fetchBlockError.Inc(int64(len(req.Elements[i].Starts)))
				m.fetchBlockFinalError.Inc(int64(len(req.Elements[i].Starts)))
				if !tooManyBlocksLogged {
					tooManyBlocksLogged = true
					s.log.WithFields(
						xlog.NewLogField("id", id.String()),
						xlog.NewLogField("expectedStarts", newTimesByUnixNanos(req.Elements[i].Starts)),
						xlog.NewLogField("actualStarts", newTimesByRPCBlocks(result.Elements[i].Blocks)),
						xlog.NewLogField("peer", peer.Host().String()),
					).Errorf("stream blocks returned more blocks than expected")
				}
				continue
			}

			// Index of the received block could be offset by missed blocks
			block := result.Elements[i].Blocks[j-missed]

			if block.Start != batch[i].blocks[j].start.UnixNano() {
				// If fell out of retention during request this is healthy, otherwise
				// missing blocks will be repaired during an active repair
				missed++
				continue
			}

			// Verify and if verify succeeds add the block from the peer
			err := s.verifyFetchedBlock(block)
			if err == nil {
				err = blocksResult.addBlockFromPeer(id, peer.Host(), block)
			}

			if err != nil {
				failed := []blockMetadata{batch[i].blocks[j]}
				blocksErr := fmt.Errorf(
					"stream blocks bad block: id=%s, start=%d, error=%s, indexID=%d, indexBlock=%d, peer=%s",
					id.String(), block.Start, err.Error(), i, j, peer.Host().String(),
				)
				s.reattemptStreamBlocksFromPeersFn(failed, enqueueCh, blocksErr, respErrReason, m)
				m.fetchBlockError.Inc(1)
				s.log.Debugf(blocksErr.Error())
				continue
			}

			m.fetchBlockSuccess.Inc(1)
		}
	}
}

func (s *session) verifyFetchedBlock(block *rpc.Block) error {
	if block.Err != nil {
		return fmt.Errorf("block error from peer: %s %s", block.Err.Type.String(), block.Err.Message)
	}
	if block.Segments == nil {
		return fmt.Errorf("block segments is bad: segments is nil")
	}
	if block.Segments.Merged == nil && len(block.Segments.Unmerged) == 0 {
		return fmt.Errorf("block segments is bad: merged and unmerged not set")
	}

	if block.Checksum != nil {
		expected := uint32(*block.Checksum)

		digest := s.digestPool.Get().(hash.Hash32)
		digest.Reset()
		defer s.digestPool.Put(digest)

		if block.Segments.Merged != nil {
			digest.Write(block.Segments.Merged.Head)
			digest.Write(block.Segments.Merged.Tail)
		} else {
			for _, segment := range block.Segments.Unmerged {
				digest.Write(segment.Head)
				digest.Write(segment.Tail)
			}
		}
		actual := digest.Sum32()
		if actual != expected {
			return fmt.Errorf("block checksum is bad: expected=%d, actual=%d", expected, actual)
		}
	}

	return nil
}

type reason int

const (
	reqErrReason reason = iota
	respErrReason
)

type reattemptStreamBlocksFromPeersFn func(
	[]blockMetadata,
	*enqueueChannel,
	error,
	reason,
	*streamFromPeersMetrics,
)

func (s *session) streamBlocksReattemptFromPeers(
	blocks []blockMetadata,
	enqueueCh *enqueueChannel,
	attemptErr error,
	reason reason,
	m *streamFromPeersMetrics,
) {
	switch reason {
	case reqErrReason:
		m.fetchBlockRetriesReqError.Inc(int64(len(blocks)))
	case respErrReason:
		m.fetchBlockRetriesRespError.Inc(int64(len(blocks)))
	}

	// Must do this asynchronously or else could get into a deadlock scenario
	// where cannot enqueue into the reattempt channel because no more work is
	// getting done because new attempts are blocked on existing attempts completing
	// and existing attempts are trying to enqueue into a full reattempt channel
	enqueue := enqueueCh.enqueueDelayed(len(blocks))
	go s.streamBlocksReattemptFromPeersEnqueue(blocks, attemptErr, enqueue)
}

func (s *session) streamBlocksReattemptFromPeersEnqueue(
	blocks []blockMetadata,
	attemptErr error,
	enqueueFn func([]*blocksMetadata),
) {
	for i := range blocks {
		// Reconstruct peers metadata for reattempt
		reattemptBlocksMetadata :=
			make([]*blocksMetadata, len(blocks[i].reattempt.peersMetadata))
		for j := range reattemptBlocksMetadata {
			reattempt := blocks[i].reattempt

			// Copy the errors for every peer so they don't shard the same error
			// slice and therefore are not subject to race conditions when the
			// error slice is modified
			reattemptErrs := make([]error, len(reattempt.errs)+1)
			n := copy(reattemptErrs, reattempt.errs)
			reattemptErrs[n] = attemptErr
			reattempt.errs = reattemptErrs

			reattemptBlocksMetadata[j] = &blocksMetadata{
				peer: reattempt.peersMetadata[j].peer,
				id:   reattempt.id,
				blocks: []blockMetadata{blockMetadata{
					start:     reattempt.peersMetadata[j].start,
					size:      reattempt.peersMetadata[j].size,
					checksum:  reattempt.peersMetadata[j].checksum,
					reattempt: reattempt,
				}},
			}

		}
		// Re-enqueue the block to be fetched
		enqueueFn(reattemptBlocksMetadata)
	}
}

type blocksResult interface {
	addBlockFromPeer(id ts.ID, peer topology.Host, block *rpc.Block) error
}

type baseBlocksResult struct {
	blockOpts               block.Options
	blockAllocSize          int
	contextPool             context.Pool
	encoderPool             encoding.EncoderPool
	multiReaderIteratorPool encoding.MultiReaderIteratorPool
}

func newBaseBlocksResult(
	opts Options,
	resultOpts result.Options,
) baseBlocksResult {
	blockOpts := resultOpts.DatabaseBlockOptions()
	return baseBlocksResult{
		blockOpts:               blockOpts,
		blockAllocSize:          blockOpts.DatabaseBlockAllocSize(),
		contextPool:             opts.ContextPool(),
		encoderPool:             blockOpts.EncoderPool(),
		multiReaderIteratorPool: blockOpts.MultiReaderIteratorPool(),
	}
}

func (b *baseBlocksResult) segmentForBlock(seg *rpc.Segment) ts.Segment {
	var (
		bytesPool  = b.blockOpts.BytesPool()
		head, tail checked.Bytes
	)
	if len(seg.Head) > 0 {
		head = bytesPool.Get(len(seg.Head))
		head.IncRef()
		head.AppendAll(seg.Head)
		head.DecRef()
	}
	if len(seg.Tail) > 0 {
		tail = bytesPool.Get(len(seg.Tail))
		tail.IncRef()
		tail.AppendAll(seg.Tail)
		tail.DecRef()
	}
	return ts.NewSegment(head, tail, ts.FinalizeHead&ts.FinalizeTail)
}

func (b *baseBlocksResult) mergeReaders(start time.Time, readers []io.Reader) (encoding.Encoder, error) {
	iter := b.multiReaderIteratorPool.Get()
	iter.Reset(readers)
	defer iter.Close()

	encoder := b.encoderPool.Get()
	encoder.Reset(start, b.blockAllocSize)

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

func (b *baseBlocksResult) newDatabaseBlock(block *rpc.Block) (block.DatabaseBlock, error) {
	var (
		start    = time.Unix(0, block.Start)
		segments = block.Segments
		result   = b.blockOpts.DatabaseBlockPool().Get()
	)

	if segments == nil {
		result.Close() // return block to pool
		return nil, errSessionBadBlockResultFromPeer
	}

	switch {
	case segments.Merged != nil:
		// Unmerged, can insert directly into a single block
		result.Reset(start, b.segmentForBlock(segments.Merged))

	case segments.Unmerged != nil:
		// Must merge to provide a single block
		segmentReaderPool := b.blockOpts.SegmentReaderPool()
		readers := make([]io.Reader, len(segments.Unmerged))
		for i := range segments.Unmerged {
			segmentReader := segmentReaderPool.Get()
			segmentReader.Reset(b.segmentForBlock(segments.Unmerged[i]))
			readers[i] = segmentReader
		}

		encoder, err := b.mergeReaders(start, readers)
		for _, reader := range readers {
			// Close each reader
			segmentReader := reader.(xio.SegmentReader)
			segmentReader.Finalize()
		}

		if err != nil {
			// mergeReaders(...) already calls encoder.Close() upon error
			result.Close() // return block to pool
			return nil, err
		}

		// Set the block data
		result.Reset(start, encoder.Discard())

	default:
		result.Close() // return block to pool
		return nil, errSessionBadBlockResultFromPeer
	}

	return result, nil
}

type streamBlocksResult struct {
	baseBlocksResult
	outputCh chan<- peerBlocksDatapoint
}

func newStreamBlocksResult(
	opts Options,
	resultOpts result.Options,
	outputCh chan<- peerBlocksDatapoint,
) *streamBlocksResult {
	return &streamBlocksResult{
		baseBlocksResult: newBaseBlocksResult(opts, resultOpts),
		outputCh:         outputCh,
	}
}

type peerBlocksDatapoint struct {
	id    ts.ID
	peer  topology.Host
	block block.DatabaseBlock
}

func (s *streamBlocksResult) addBlockFromPeer(id ts.ID, peer topology.Host, block *rpc.Block) error {
	result, err := s.newDatabaseBlock(block)
	if err != nil {
		return err
	}
	s.outputCh <- peerBlocksDatapoint{
		id:    id,
		peer:  peer,
		block: result,
	}
	return nil
}

type peerBlocksIter struct {
	inputCh <-chan peerBlocksDatapoint
	errCh   <-chan error
	current peerBlocksDatapoint
	err     error
	done    bool
}

func newPeerBlocksIter(
	inputC <-chan peerBlocksDatapoint,
	errC <-chan error,
) *peerBlocksIter {
	return &peerBlocksIter{
		inputCh: inputC,
		errCh:   errC,
	}
}

func (it *peerBlocksIter) Current() (topology.Host, ts.ID, block.DatabaseBlock) {
	return it.current.peer, it.current.id, it.current.block
}

func (it *peerBlocksIter) Err() error {
	return it.err
}

func (it *peerBlocksIter) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	m, more := <-it.inputCh

	if !more {
		it.err = <-it.errCh
		it.done = true
		return false
	}

	it.current = m
	return true
}

type bulkBlocksResult struct {
	sync.RWMutex
	baseBlocksResult
	result result.ShardResult
}

func newBulkBlocksResult(
	opts Options,
	resultOpts result.Options,
) *bulkBlocksResult {
	return &bulkBlocksResult{
		baseBlocksResult: newBaseBlocksResult(opts, resultOpts),
		result:           result.NewShardResult(4096, resultOpts),
	}
}

func (r *bulkBlocksResult) addBlockFromPeer(id ts.ID, peer topology.Host, block *rpc.Block) error {
	start := time.Unix(0, block.Start)
	result, err := r.newDatabaseBlock(block)
	if err != nil {
		return err
	}

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
		tmpCtx := r.contextPool.Get()
		currReader, err := currBlock.Stream(tmpCtx)
		if err != nil {
			return err
		}

		// If there are no data in the current block, there is no
		// need to merge
		if currReader == nil {
			continue
		}

		resultReader, err := result.Stream(tmpCtx)
		if err != nil {
			return err
		}
		if resultReader == nil {
			return nil
		}

		readers := []io.Reader{currReader, resultReader}
		encoder, err := r.mergeReaders(start, readers)

		if err != nil {
			return err
		}

		result.Close()

		result = r.blockOpts.DatabaseBlockPool().Get()
		result.Reset(start, encoder.Discard())

		tmpCtx.Close()
	}

	return nil
}

type enqueueChannel struct {
	enqueued        uint64
	processed       uint64
	peersMetadataCh chan []*blocksMetadata
	closed          int64
	metrics         *streamFromPeersMetrics
}

const enqueueChannelDefaultLen = 32768

func newEnqueueChannel(m *streamFromPeersMetrics) *enqueueChannel {
	c := &enqueueChannel{
		peersMetadataCh: make(chan []*blocksMetadata, enqueueChannelDefaultLen),
		closed:          0,
		metrics:         m,
	}
	go func() {
		for atomic.LoadInt64(&c.closed) == 0 {
			m.blocksEnqueueChannel.Update(float64(len(c.peersMetadataCh)))
			time.Sleep(gaugeReportInterval)
		}
	}()
	return c
}

func (c *enqueueChannel) enqueue(peersMetadata []*blocksMetadata) {
	atomic.AddUint64(&c.enqueued, 1)
	c.peersMetadataCh <- peersMetadata
}

func (c *enqueueChannel) enqueueDelayed(numToEnqueue int) func([]*blocksMetadata) {
	atomic.AddUint64(&c.enqueued, uint64(numToEnqueue))
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

// peerBlocksQueue is a per peer queue of blocks to be retrieved from a peer
type peerBlocksQueue struct {
	sync.RWMutex
	closed       bool
	peer         peer
	queue        []*blocksMetadata
	doneFns      []func()
	assigned     uint64
	completed    uint64
	maxQueueSize int
	workers      xsync.WorkerPool
	processFn    processFn
}

type newPeerBlocksQueueFn func(
	peer peer,
	maxQueueSize int,
	interval time.Duration,
	workers xsync.WorkerPool,
	processFn processFn,
) *peerBlocksQueue

func newPeerBlocksQueue(
	peer peer,
	maxQueueSize int,
	interval time.Duration,
	workers xsync.WorkerPool,
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

func (qs peerBlocksQueues) findQueue(peer peer) *peerBlocksQueue {
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
	peer   peer
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

// removeFirstUnselected removes the first unselected block while maintaining
// the original block order by shifting the blocks and overriding the block to
// be removed
func (b *blocksMetadata) removeFirstUnselected() {
	blocksLen := len(b.blocks)
	idx := b.idx
	copy(b.blocks[idx:], b.blocks[idx+1:])
	b.blocks = b.blocks[:blocksLen-1]
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

type blocksMetadatasQueuesByAttemptsAscOutstandingAsc []blocksMetadataQueue

func (arr blocksMetadatasQueuesByAttemptsAscOutstandingAsc) Len() int {
	return len(arr)
}
func (arr blocksMetadatasQueuesByAttemptsAscOutstandingAsc) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}
func (arr blocksMetadatasQueuesByAttemptsAscOutstandingAsc) Less(i, j int) bool {
	peerI := arr[i].queue.peer
	peerJ := arr[j].queue.peer
	blocksI := arr[i].blocksMetadata.unselectedBlocks()
	blocksJ := arr[j].blocksMetadata.unselectedBlocks()
	attemptsI := blocksI[0].reattempt.peerAttempts(peerI)
	attemptsJ := blocksJ[0].reattempt.peerAttempts(peerJ)
	if attemptsI != attemptsJ {
		return attemptsI < attemptsJ
	}

	outstandingI :=
		atomic.LoadUint64(&arr[i].queue.assigned) -
			atomic.LoadUint64(&arr[i].queue.completed)
	outstandingJ :=
		atomic.LoadUint64(&arr[j].queue.assigned) -
			atomic.LoadUint64(&arr[j].queue.completed)
	return outstandingI < outstandingJ
}

type blockMetadata struct {
	start     time.Time
	size      int64
	checksum  *uint32
	reattempt blockMetadataReattempt
}

type blockMetadataReattempt struct {
	attempt       int
	failAllowed   *int32
	id            ts.ID
	attempted     []peer
	errs          []error
	peersMetadata []blockMetadataReattemptPeerMetadata
}

type blockMetadataReattemptPeerMetadata struct {
	peer     peer
	start    time.Time
	size     int64
	checksum *uint32
}

func (b blockMetadataReattempt) peerAttempts(p peer) int {
	r := 0
	for i := range b.attempted {
		if b.attempted[i] == p {
			r++
		}
	}
	return r
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
	var zeroed block.Metadata
	for i := range it.blocks {
		it.blocks[i] = zeroed
	}
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
