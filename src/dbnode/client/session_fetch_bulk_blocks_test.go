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
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xretry "github.com/m3db/m3/src/x/retry"
	"github.com/m3db/m3/src/x/serialize"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	blockSize       = 2 * time.Hour
	nsID            = ident.StringID("testNs1")
	nsRetentionOpts = retention.NewOptions().
			SetBlockSize(blockSize).
			SetRetentionPeriod(48 * blockSize)
	testTagDecodingPool = serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{}),
		pool.NewObjectPoolOptions().SetSize(1))
	testTagEncodingPool = serialize.NewTagEncoderPool(
		serialize.NewTagEncoderOptions(),
		pool.NewObjectPoolOptions().SetSize(1))
	testIDPool     = newSessionTestOptions().IdentifierPool()
	fooID          = ident.StringID("foo")
	fooTags        checked.Bytes
	fooDecodedTags = ident.NewTags(ident.StringTag("aaa", "bbb"))
	barID          = ident.StringID("bar")
	bazID          = ident.StringID("baz")
	testHost       = topology.NewHost("testhost", "testhost:9000")
)

func init() {
	testTagDecodingPool.Init()
	testTagEncodingPool.Init()
	tagEncoder := testTagEncodingPool.Get()
	err := tagEncoder.Encode(ident.NewTagsIterator(fooDecodedTags))
	if err != nil {
		panic(err)
	}
	var ok bool
	fooTags, ok = tagEncoder.Data()
	if !ok {
		panic(fmt.Errorf("encode tags failed"))
	}
}

func testsNsMetadata(t *testing.T) namespace.Metadata {
	md, err := namespace.NewMetadata(nsID, namespace.NewOptions().SetRetentionOptions(nsRetentionOpts))
	require.NoError(t, err)
	return md
}

func newSessionTestMultiReaderIteratorPool() encoding.MultiReaderIteratorPool {
	p := encoding.NewMultiReaderIteratorPool(nil)
	p.Init(func(r io.Reader, _ namespace.SchemaDescr) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	})
	return p
}

func newSessionTestAdminOptions() AdminOptions {
	opts := newSessionTestOptions().(AdminOptions)
	hostShardSets := sessionTestHostAndShards(sessionTestShardSet())
	host := hostShardSets[0].Host()
	return opts.
		SetOrigin(host).
		SetFetchSeriesBlocksBatchSize(2).
		SetFetchSeriesBlocksMetadataBatchTimeout(time.Second).
		SetFetchSeriesBlocksBatchTimeout(time.Second).
		SetFetchSeriesBlocksBatchConcurrency(4)
}

func newResultTestOptions() result.Options {
	opts := result.NewOptions()
	encoderPool := encoding.NewEncoderPool(nil)
	encoderPool.Init(encoding.NewNullEncoder)
	return opts.SetDatabaseBlockOptions(opts.DatabaseBlockOptions().
		SetEncoderPool(encoderPool))
}

func testPeers(v []peer) peers {
	return peers{peers: v, majorityReplicas: topology.Majority(len(v))}
}

func newRoundRobinPickBestPeerFn() pickBestPeerFn {
	calls := int32(0)
	return func(
		perPeerBlocksMetadata []receivedBlockMetadata,
		peerQueues peerBlocksQueues,
		resources pickBestPeerPooledResources,
	) (int, pickBestPeerPooledResources) {
		numCalled := atomic.AddInt32(&calls, 1)
		callIdx := numCalled - 1
		idx := callIdx % int32(len(perPeerBlocksMetadata))
		return int(idx), resources
	}
}

func newTestBlocks(start time.Time) []testBlocks {
	return []testBlocks{
		{
			id: fooID,
			blocks: []testBlock{
				{
					start: start.Add(blockSize * 1),
					segments: &testBlockSegments{merged: &testBlockSegment{
						head: []byte{1, 2},
						tail: []byte{3},
					}},
				},
			},
		},
		{
			id: barID,
			blocks: []testBlock{
				{
					start: start.Add(blockSize * 2),
					segments: &testBlockSegments{merged: &testBlockSegment{
						head: []byte{4, 5},
						tail: []byte{6},
					}},
				},
			},
		},
		{
			id: bazID,
			blocks: []testBlock{
				{
					start: start.Add(blockSize * 3),
					segments: &testBlockSegments{merged: &testBlockSegment{
						head: []byte{7, 8},
						tail: []byte{9},
					}},
				},
			},
		},
	}
}
func TestFetchBootstrapBlocksAllPeersSucceedV2(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)

	mockHostQueues, mockClients := mockHostQueuesAndClientsForFetchBootstrapBlocks(ctrl, opts)
	session.newHostQueueFn = mockHostQueues.newHostQueueFn()

	// Don't drain the peer blocks queue, explicitly drain ourselves to
	// avoid unpredictable batches being retrieved from peers
	var (
		qs      []*peerBlocksQueue
		qsMutex sync.RWMutex
	)
	session.newPeerBlocksQueueFn = func(
		peer peer,
		maxQueueSize int,
		_ time.Duration,
		workers xsync.WorkerPool,
		processFn processFn,
	) *peerBlocksQueue {
		qsMutex.Lock()
		defer qsMutex.Unlock()
		q := newPeerBlocksQueue(peer, maxQueueSize, 0, workers, processFn)
		qs = append(qs, q)
		return q
	}

	require.NoError(t, session.Open())

	var (
		batchSize = opts.FetchSeriesBlocksBatchSize()
		start     = time.Now().Truncate(blockSize).Add(blockSize * -(24 - 1))
		blocks    = newTestBlocks(start)
	)

	// Expect the fetch metadata calls
	metadataResult := resultMetadataFromBlocks(blocks)
	// Skip the first client which is the client for the origin
	mockClients[1:].expectFetchMetadataAndReturn(metadataResult, opts)

	// Expect the fetch blocks calls
	participating := len(mockClients) - 1
	blocksExpectedReqs, blocksResult := expectedReqsAndResultFromBlocks(t,
		blocks, batchSize, participating,
		func(_ ident.ID, blockIdx int) (clientIdx int) {
			// Round robin to match the best peer selection algorithm
			return blockIdx % participating
		})
	// Skip the first client which is the client for the origin
	for i, client := range mockClients[1:] {
		expectFetchBlocksAndReturn(client, blocksExpectedReqs[i], blocksResult[i])
	}

	// Make sure peer selection is round robin to match our expected
	// peer fetch calls
	session.pickBestPeerFn = newRoundRobinPickBestPeerFn()

	// Fetch blocks
	go func() {
		// Trigger peer queues to drain explicitly when all work enqueued
		for {
			qsMutex.RLock()
			assigned := 0
			for _, q := range qs {
				assigned += int(atomic.LoadUint64(&q.assigned))
			}
			qsMutex.RUnlock()
			if assigned == len(blocks) {
				qsMutex.Lock()
				defer qsMutex.Unlock()
				for _, q := range qs {
					q.drain()
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	rangeStart := start
	rangeEnd := start.Add(blockSize * (24 - 1))
	bootstrapOpts := newResultTestOptions()
	result, err := session.FetchBootstrapBlocksFromPeers(
		testsNsMetadata(t), 0, rangeStart, rangeEnd, bootstrapOpts)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Assert result
	assertFetchBootstrapBlocksResult(t, blocks, result)

	assert.NoError(t, session.Close())
}

// TestFetchBootstrapBlocksDontRetryHostNotAvailableInRetrier was added as a regression test
// to ensure that in the scenario where a peer is not available (hard down) but the others are
// available the streamBlocksMetadataFromPeers does not wait for all of the exponential retries
// to the downed host to fail before continuing. This is important because if the client waits for
// all the retries to the downed host to complete it can block each metadata fetch for up to 30 seconds
// due to the exponential backoff logic in the retrier.
func TestFetchBootstrapBlocksDontRetryHostNotAvailableInRetrier(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions().
		// Set bootstrap consistency level to unstrict majority because there are only 3 nodes in the
		// cluster. The first one will not return data because it is the origin and the last node will
		// return an error.
		SetBootstrapConsistencyLevel(topology.ReadConsistencyLevelUnstrictMajority).
		// Configure the stream blocks retrier such that if the short-circuit logic did not work the
		// test would timeout.
		SetStreamBlocksRetrier(xretry.NewRetrier(
			xretry.NewOptions().
				SetBackoffFactor(10).
				SetMaxRetries(10).
				SetInitialBackoff(30 * time.Second).
				SetJitter(true),
		)).
		// Ensure that the batch size is configured such that all of the blocks could
		// be retrieved from a single peer in a single request. This makes mocking
		// expected calls and responses significantly easier since which batch call a
		// given block will fall into is non-deterministic (depends on the result of
		// concurrent execution).
		SetFetchSeriesBlocksBatchSize(len(newTestBlocks(time.Now())))
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)

	var (
		mockHostQueues MockHostQueues
		mockClients    MockTChanNodes
		hostShardSets  = sessionTestHostAndShards(sessionTestShardSet())
	)
	// Skip the last one because it is going to be manually configured to return an error.
	for i := 0; i < len(hostShardSets)-1; i++ {
		host := hostShardSets[i].Host()
		hostQueue, client := defaultHostAndClientWithExpect(ctrl, host, opts)
		mockHostQueues = append(mockHostQueues, hostQueue)

		if i != 0 {
			// Skip creating a client for the origin because it will never be called so we want
			// to avoid setting up expectations for it.
			mockClients = append(mockClients, client)
		}
	}

	// Construct the last hostQueue with a connection pool that will error out.
	host := hostShardSets[len(hostShardSets)-1].Host()
	connectionPool := NewMockconnectionPool(ctrl)
	connectionPool.EXPECT().
		NextClient().
		Return(nil, nil, errConnectionPoolHasNoConnections).
		AnyTimes()
	hostQueue := NewMockhostQueue(ctrl)
	hostQueue.EXPECT().Open()
	hostQueue.EXPECT().Host().Return(host).AnyTimes()
	hostQueue.EXPECT().
		ConnectionCount().
		Return(opts.MinConnectionCount()).
		Times(sessionTestShards)
	hostQueue.EXPECT().
		ConnectionPool().
		Return(connectionPool).
		AnyTimes()
	hostQueue.EXPECT().
		BorrowConnection(gomock.Any()).
		Return(errConnectionPoolHasNoConnections).
		AnyTimes()
	hostQueue.EXPECT().Close()
	mockHostQueues = append(mockHostQueues, hostQueue)

	session.newHostQueueFn = mockHostQueues.newHostQueueFn()

	// Don't drain the peer blocks queue, explicitly drain ourselves to
	// avoid unpredictable batches being retrieved from peers
	var (
		qs      []*peerBlocksQueue
		qsMutex sync.RWMutex
	)
	session.newPeerBlocksQueueFn = func(
		peer peer,
		maxQueueSize int,
		_ time.Duration,
		workers xsync.WorkerPool,
		processFn processFn,
	) *peerBlocksQueue {
		qsMutex.Lock()
		defer qsMutex.Unlock()
		q := newPeerBlocksQueue(peer, maxQueueSize, 0, workers, processFn)
		qs = append(qs, q)
		return q
	}
	require.NoError(t, session.Open())

	var (
		batchSize = opts.FetchSeriesBlocksBatchSize()
		start     = time.Now().Truncate(blockSize).Add(blockSize * -(24 - 1))
		blocks    = newTestBlocks(start)

		// Expect the fetch metadata calls.
		metadataResult = resultMetadataFromBlocks(blocks)
	)
	mockClients.expectFetchMetadataAndReturn(metadataResult, opts)

	// Expect the fetch blocks calls.
	participating := len(mockClients)
	blocksExpectedReqs, blocksResult := expectedReqsAndResultFromBlocks(t,
		blocks, batchSize, participating,
		func(id ident.ID, blockIdx int) (clientIdx int) {
			// Only one host to pull data from.
			return 0
		})
	// Skip the first client which is the client for the origin.
	for i, client := range mockClients {
		expectFetchBlocksAndReturn(client, blocksExpectedReqs[i], blocksResult[i])
	}

	// Make sure peer selection is round robin to match our expected
	// peer fetch calls.
	session.pickBestPeerFn = func(
		perPeerBlocksMetadata []receivedBlockMetadata,
		peerQueues peerBlocksQueues,
		resources pickBestPeerPooledResources,
	) (int, pickBestPeerPooledResources) {
		// Only one host to pull data from.
		return 0, resources
	}

	// Fetch blocks.
	go func() {
		// Trigger peer queues to drain explicitly when all work enqueued.
		for {
			qsMutex.RLock()
			assigned := 0
			for _, q := range qs {
				assigned += int(atomic.LoadUint64(&q.assigned))
			}
			qsMutex.RUnlock()
			if assigned == len(blocks) {
				qsMutex.Lock()
				defer qsMutex.Unlock()
				for _, q := range qs {
					q.drain()
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	rangeStart := start
	rangeEnd := start.Add(blockSize * (24 - 1))
	bootstrapOpts := newResultTestOptions()
	result, err := session.FetchBootstrapBlocksFromPeers(
		testsNsMetadata(t), 0, rangeStart, rangeEnd, bootstrapOpts)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Assert result.
	assertFetchBootstrapBlocksResult(t, blocks, result)

	require.NoError(t, session.Close())
}

type fetchBlocksFromPeersTestScenarioGenerator func(
	peerIdx int,
	numPeers int,
	start time.Time,
) []testBlocks

func fetchBlocksFromPeersTestsHelper(
	t *testing.T,
	peerScenarioFn fetchBlocksFromPeersTestScenarioGenerator,
) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)

	mockHostQueues, mockClients := mockHostQueuesAndClientsForFetchBootstrapBlocks(ctrl, opts)
	session.newHostQueueFn = mockHostQueues.newHostQueueFn()

	// Don't drain the peer blocks queue, explicitly drain ourselves to
	// avoid unpredictable batches being retrieved from peers
	var (
		qs      []*peerBlocksQueue
		qsMutex sync.RWMutex
	)
	session.newPeerBlocksQueueFn = func(
		peer peer,
		maxQueueSize int,
		_ time.Duration,
		workers xsync.WorkerPool,
		processFn processFn,
	) *peerBlocksQueue {
		qsMutex.Lock()
		defer qsMutex.Unlock()
		q := newPeerBlocksQueue(peer, maxQueueSize, 0, workers, processFn)
		qs = append(qs, q)
		return q
	}

	require.NoError(t, session.Open())

	batchSize := opts.FetchSeriesBlocksBatchSize()
	start := time.Now().Truncate(blockSize).Add(blockSize * -(24 - 1))

	allBlocks := make([][]testBlocks, 0, len(mockHostQueues))
	peerBlocks := make([][]testBlocks, 0, len(mockHostQueues))
	numBlocks := 0
	for idx := 0; idx < len(mockHostQueues); idx++ {
		blocks := peerScenarioFn(idx, len(mockHostQueues), start)

		// Add to the expected list
		allBlocks = append(allBlocks, blocks)

		if idx == 0 {
			continue // i.e. skip the first host. used as local m3dbnode
		}

		// Expect the fetch blocks calls
		blocksExpectedReqs, blocksResult := expectedRepairFetchRequestsAndResponses(blocks, batchSize)
		expectFetchBlocksAndReturn(mockClients[idx], blocksExpectedReqs, blocksResult)

		// Track number of blocks to be used to drain the work queue
		for _, blk := range blocks {
			numBlocks = numBlocks + len(blk.blocks)
		}
		peerBlocks = append(peerBlocks, blocks)
	}

	// Fetch blocks
	go func() {
		// Trigger peer queues to drain explicitly when all work enqueued
		for {
			qsMutex.RLock()
			assigned := 0
			for _, q := range qs {
				assigned += int(atomic.LoadUint64(&q.assigned))
			}
			qsMutex.RUnlock()
			if assigned == numBlocks {
				qsMutex.Lock()
				defer qsMutex.Unlock()
				for _, q := range qs {
					q.drain()
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	blockReplicasMetadata := testBlocksToBlockReplicasMetadata(t, peerBlocks, mockHostQueues[1:])
	bootstrapOpts := newResultTestOptions()
	result, err := session.FetchBlocksFromPeers(testsNsMetadata(t), 0, topology.ReadConsistencyLevelAll,
		blockReplicasMetadata, bootstrapOpts)
	require.NoError(t, err)
	require.NotNil(t, result)

	assertFetchBlocksFromPeersResult(t, peerBlocks, mockHostQueues[1:], result)
	require.NoError(t, session.Close())
}

func TestFetchBlocksFromPeersSingleNonIdenticalBlockReplica(t *testing.T) {
	peerScenarioGeneratorFn := func(peerIdx int, numPeers int, start time.Time) []testBlocks {
		if peerIdx == 0 {
			return []testBlocks{}
		}
		return []testBlocks{
			{
				id: fooID,
				blocks: []testBlock{
					{
						start: start.Add(blockSize * 1),
						segments: &testBlockSegments{merged: &testBlockSegment{
							head: []byte{byte(1 + 10*peerIdx), byte(2 + 10*peerIdx)},
							tail: []byte{byte(3 + 10*peerIdx)},
						}},
					},
				},
			},
		}
	}
	fetchBlocksFromPeersTestsHelper(t, peerScenarioGeneratorFn)
}

func TestFetchRepairBlocksMultipleDifferentBlocks(t *testing.T) {
	peerScenarioGeneratorFn := func(peerIdx int, numPeers int, start time.Time) []testBlocks {
		return []testBlocks{
			{
				id: fooID,
				blocks: []testBlock{
					{
						start: start.Add(blockSize * 1),
						segments: &testBlockSegments{merged: &testBlockSegment{
							head: []byte{byte(1 + 10*peerIdx), byte(2 + 10*peerIdx)},
							tail: []byte{byte(3 + 10*peerIdx)},
						}},
					},
				},
			},
			{
				id: barID,
				blocks: []testBlock{
					{
						start: start.Add(blockSize * 2),
						segments: &testBlockSegments{merged: &testBlockSegment{
							head: []byte{byte(4 + 10*peerIdx), byte(5 + 10*peerIdx)},
							tail: []byte{byte(6 + 10*peerIdx)},
						}},
					},
				},
			},
			{
				id: bazID,
				blocks: []testBlock{
					{
						start: start.Add(blockSize * 3),
						segments: &testBlockSegments{merged: &testBlockSegment{
							head: []byte{byte(7 + 10*peerIdx), byte(8 + 10*peerIdx)},
							tail: []byte{byte(9 + 10*peerIdx)},
						}},
					},
				},
			},
		}
	}
	fetchBlocksFromPeersTestsHelper(t, peerScenarioGeneratorFn)
}

func TestFetchRepairBlocksMultipleBlocksSameIDAndPeer(t *testing.T) {
	peerScenarioGeneratorFn := func(peerIdx int, numPeers int, start time.Time) []testBlocks {
		return []testBlocks{
			{
				id: fooID,
				blocks: []testBlock{
					{
						start: start.Add(blockSize * 1),
						segments: &testBlockSegments{merged: &testBlockSegment{
							head: []byte{byte(1 + 10*peerIdx), byte(2 + 10*peerIdx)},
							tail: []byte{byte(3 + 10*peerIdx)},
						}},
					},
				},
			},
			{
				id: barID,
				blocks: []testBlock{
					{
						start: start.Add(blockSize * 2),
						segments: &testBlockSegments{merged: &testBlockSegment{
							head: []byte{byte(4 + 10*peerIdx), byte(5 + 10*peerIdx)},
							tail: []byte{byte(6 + 10*peerIdx)},
						}},
					},
				},
			},
			{
				id: bazID,
				blocks: []testBlock{
					{
						start: start.Add(blockSize * 3),
						segments: &testBlockSegments{merged: &testBlockSegment{
							head: []byte{byte(7 + 10*peerIdx), byte(8 + 10*peerIdx)},
							tail: []byte{byte(9 + 10*peerIdx)},
						}},
					},
				},
			},
			{
				id: bazID,
				blocks: []testBlock{
					{
						start: start.Add(blockSize * 4),
						segments: &testBlockSegments{merged: &testBlockSegment{
							head: []byte{byte(8 + 10*peerIdx), byte(9 + 10*peerIdx)},
							tail: []byte{byte(1 + 10*peerIdx)},
						}},
					},
				},
			},
		}
	}
	fetchBlocksFromPeersTestsHelper(t, peerScenarioGeneratorFn)
}

func assertFetchBlocksFromPeersResult(
	t *testing.T,
	expectedBlocks [][]testBlocks,
	peers MockHostQueues,
	observedBlocksIter PeerBlocksIter,
) {
	matchedBlocks := make([][][]bool, 0, len(expectedBlocks))
	for _, blocks := range expectedBlocks {
		unsetBlocks := make([][]bool, len(blocks))
		matchedBlocks = append(matchedBlocks, unsetBlocks)
	}
	extraBlocks := []peerBlocksDatapoint{}
	for observedBlocksIter.Next() {
		observedHost, observedID, observedBlock := observedBlocksIter.Current()

		// find which peer the current datapoint is for
		peerIdx := -1
		for idx, mockPeer := range peers {
			if observedHost.String() == mockPeer.Host().String() {
				peerIdx = idx
				break
			}
		}

		// unknown peer, marking extra block
		if peerIdx == -1 {
			extraBlocks = append(extraBlocks, peerBlocksDatapoint{
				id:    observedID,
				peer:  observedHost,
				block: observedBlock,
			})
			continue
		}

		// find blockIdx
		blockIdx := -1
		subBlockIdx := -1
		for i, blocks := range expectedBlocks[peerIdx] {
			if !blocks.id.Equal(observedID) {
				continue
			}
			for j, expectedBlock := range blocks.blocks {
				if observedBlock.StartTime().Equal(expectedBlock.start) {
					blockIdx = i
					subBlockIdx = j
					break
				}

			}
		}

		// unknown block, marking extra
		if blockIdx == -1 || subBlockIdx == -1 {
			extraBlocks = append(extraBlocks, peerBlocksDatapoint{
				id:    observedID,
				peer:  observedHost,
				block: observedBlock,
			})
			continue
		}

		// lazily construct matchedBlocks inner most array
		if matchedBlocks[peerIdx][blockIdx] == nil {
			matchedBlocks[peerIdx][blockIdx] = make([]bool, len(expectedBlocks[peerIdx][blockIdx].blocks))
		}

		expectedBlock := expectedBlocks[peerIdx][blockIdx].blocks[subBlockIdx]
		expectedData := append(expectedBlock.segments.merged.head, expectedBlock.segments.merged.tail...)
		ctx := context.NewContext()
		defer ctx.Close()
		stream, err := observedBlock.Stream(ctx)
		require.NoError(t, err)
		seg, err := stream.Segment()
		require.NoError(t, err)

		actualData := append(bytesFor(seg.Head), bytesFor(seg.Tail)...)

		// compare actual v expected data
		if len(expectedData) != len(actualData) {
			continue
		}
		for i := range expectedData {
			if expectedData[i] != actualData[i] {
				continue
			}
		}

		// data is the same, mark match
		matchedBlocks[peerIdx][blockIdx][subBlockIdx] = true
	}

	for _, extraBlock := range extraBlocks {
		assert.Fail(t, "received extra block: %v", extraBlock)
	}

	for i, peerMatches := range matchedBlocks {
		for j, blockMatches := range peerMatches {
			if len(blockMatches) == 0 {
				assert.Fail(t,
					"un-matched block [ peer=%d, block=%d, expected=%v ]",
					i, j, expectedBlocks[i][j])
			}
			for k, blockMatch := range blockMatches {
				if !blockMatch {
					assert.Fail(t,
						"un-matched block [ peer=%d, block=%d, sub-block=%d, expected=%v ]",
						i, j, k, expectedBlocks[i][j].blocks[k])
				}
			}
		}
	}
}

func testBlocksToBlockReplicasMetadata(
	t *testing.T,
	peerBlocks [][]testBlocks,
	peers MockHostQueues,
) []block.ReplicaMetadata {
	assert.True(t, len(peerBlocks) == len(peers))
	blockReplicas := make([]block.ReplicaMetadata, 0, len(peers))
	for idx, blocks := range peerBlocks {
		blocksMetadata := resultMetadataFromBlocks(blocks)
		peerHost := peers[idx].Host()
		for _, bm := range blocksMetadata {
			for _, b := range bm.blocks {
				blockReplicas = append(blockReplicas, block.ReplicaMetadata{
					Metadata: block.Metadata{
						ID:       bm.id,
						Start:    b.start,
						Size:     *(b.size),
						Checksum: b.checksum,
					},
					Host: peerHost,
				})
			}
		}
	}
	return blockReplicas
}

func TestSelectPeersFromPerPeerBlockMetadatasAllPeersSucceed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)

	var (
		metrics          = session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw)
		peerA            = NewMockpeer(ctrl)
		peerB            = NewMockpeer(ctrl)
		peers            = preparedMockPeers(peerA, peerB)
		enqueueCh        = NewMockenqueueChannel(ctrl)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		start    = timeZero
		checksum = uint32(1)
		perPeer  = []receivedBlockMetadata{
			{
				peer: peerA,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum,
				},
			},
			{
				peer: peerB,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum,
				},
			},
		}
		pooled = selectPeersFromPerPeerBlockMetadatasPooledResources{}
	)

	// Perform selection
	selected, _ := session.selectPeersFromPerPeerBlockMetadatas(
		perPeer, peerBlocksQueues, enqueueCh,
		newStaticRuntimeReadConsistencyLevel(opts.BootstrapConsistencyLevel()),
		testPeers(peers), pooled, metrics)

	// Assert selection first peer
	require.Equal(t, 1, len(selected))

	assert.Equal(t, start, selected[0].block.start)
	assert.Equal(t, int64(2), selected[0].block.size)
	assert.Equal(t, &checksum, selected[0].block.checksum)

	assert.Equal(t, 1, selected[0].block.reattempt.attempt)
	assert.Equal(t, []peer{peerA}, selected[0].block.reattempt.attempted)
}

func TestSelectPeersFromPerPeerBlockMetadatasSelectAllOnDifferingChecksums(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)

	var (
		metrics          = session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw)
		peerA            = NewMockpeer(ctrl)
		peerB            = NewMockpeer(ctrl)
		peerC            = NewMockpeer(ctrl)
		peers            = preparedMockPeers(peerA, peerB, peerC)
		enqueueCh        = NewMockenqueueChannel(ctrl)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		start     = timeZero
		checksums = []uint32{1, 2}
		perPeer   = []receivedBlockMetadata{
			{
				peer: peerA,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksums[0],
				},
			},
			{
				peer: peerB,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksums[1],
				},
			},
			{
				peer: peerC,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksums[1],
				},
			},
		}
		pooled = selectPeersFromPerPeerBlockMetadatasPooledResources{}
	)

	// Perform selection
	selected, _ := session.selectPeersFromPerPeerBlockMetadatas(
		perPeer, peerBlocksQueues, enqueueCh,
		newStaticRuntimeReadConsistencyLevel(opts.BootstrapConsistencyLevel()),
		testPeers(peers), pooled, metrics)

	// Assert selection all peers
	require.Equal(t, 3, len(selected))

	for i, metadata := range perPeer {
		assert.Equal(t, metadata.peer, selected[i].peer)
		assert.True(t, metadata.block.start.Equal(selected[i].block.start))
		assert.Equal(t, metadata.block.size, selected[i].block.size)
		assert.Equal(t, metadata.block.checksum, selected[i].block.checksum)
	}
}

func TestSelectPeersFromPerPeerBlockMetadatasTakeSinglePeer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)

	var (
		metrics          = session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw)
		peerA            = NewMockpeer(ctrl)
		peerB            = NewMockpeer(ctrl)
		peerC            = NewMockpeer(ctrl)
		peers            = preparedMockPeers(peerA, peerB, peerC)
		enqueueCh        = NewMockenqueueChannel(ctrl)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		start    = timeZero
		checksum = uint32(2)
		perPeer  = []receivedBlockMetadata{
			{
				peer: peerA,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum,
				},
			},
		}
		pooled = selectPeersFromPerPeerBlockMetadatasPooledResources{}
	)

	// Perform selection
	selected, _ := session.selectPeersFromPerPeerBlockMetadatas(
		perPeer, peerBlocksQueues, enqueueCh,
		newStaticRuntimeReadConsistencyLevel(opts.BootstrapConsistencyLevel()),
		testPeers(peers), pooled, metrics)

	// Assert selection first peer
	require.Equal(t, 1, len(selected))

	assert.Equal(t, start, selected[0].block.start)
	assert.Equal(t, int64(2), selected[0].block.size)
	assert.Equal(t, &checksum, selected[0].block.checksum)

	assert.Equal(t, 1, selected[0].block.reattempt.attempt)
	assert.Equal(t, []peer{peerA}, selected[0].block.reattempt.attempted)
}

func TestSelectPeersFromPerPeerBlockMetadatasAvoidsReattemptingFromAttemptedPeers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)

	var (
		metrics          = session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw)
		peerA            = NewMockpeer(ctrl)
		peerB            = NewMockpeer(ctrl)
		peerC            = NewMockpeer(ctrl)
		peers            = preparedMockPeers(peerA, peerB, peerC)
		enqueueCh        = NewMockenqueueChannel(ctrl)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		start     = timeZero
		checksum  = uint32(2)
		reattempt = blockMetadataReattempt{
			attempt:   1,
			attempted: []peer{peerA},
		}
		perPeer = []receivedBlockMetadata{
			{
				peer: peerA,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum, reattempt: reattempt,
				},
			},
			{
				peer: peerB,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum, reattempt: reattempt,
				},
			},
			{
				peer: peerC,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum, reattempt: reattempt,
				},
			},
		}
		pooled = selectPeersFromPerPeerBlockMetadatasPooledResources{}
	)

	// Track peer C as having an assigned block to ensure block ends up
	// under peer B which is just as eligible as peer C to receive the block
	// assignment
	peerBlocksQueues.findQueue(peerC).trackAssigned(1)

	// Perform selection
	selected, _ := session.selectPeersFromPerPeerBlockMetadatas(
		perPeer, peerBlocksQueues, enqueueCh,
		newStaticRuntimeReadConsistencyLevel(opts.BootstrapConsistencyLevel()),
		testPeers(peers), pooled, metrics)

	// Assert selection length
	require.Equal(t, 1, len(selected))

	// Assert selection second peer
	assert.Equal(t, peerB, selected[0].peer)
	assert.Equal(t, start, selected[0].block.start)
	assert.Equal(t, int64(2), selected[0].block.size)
	assert.Equal(t, &checksum, selected[0].block.checksum)

	assert.Equal(t, 2, selected[0].block.reattempt.attempt)
	assert.Equal(t, []peer{
		peerA, peerB,
	}, selected[0].block.reattempt.attempted)
}

func TestSelectPeersFromPerPeerBlockMetadatasAvoidRetryWithLevelNone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions().
		SetFetchSeriesBlocksMaxBlockRetries(0).
		SetBootstrapConsistencyLevel(topology.ReadConsistencyLevelNone)
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)

	var (
		metrics          = session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw)
		peerA            = NewMockpeer(ctrl)
		peerB            = NewMockpeer(ctrl)
		peerC            = NewMockpeer(ctrl)
		peers            = preparedMockPeers(peerA, peerB, peerC)
		enqueueCh        = NewMockenqueueChannel(ctrl)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		start    = timeZero
		checksum = uint32(2)
		// Block should be avoided being fetched at all as all peers already
		// attempted
		reattempt = blockMetadataReattempt{
			attempt:   3,
			attempted: []peer{peerA, peerB, peerC},
			errs:      []error{fmt.Errorf("errA"), fmt.Errorf("errB"), fmt.Errorf("errC")},
		}
		perPeer = []receivedBlockMetadata{
			{
				peer: peerA,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum, reattempt: reattempt,
				},
			},
			{
				peer: peerB,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum, reattempt: reattempt,
				},
			},
			{
				peer: peerC,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum, reattempt: reattempt,
				},
			},
		}
		pooled = selectPeersFromPerPeerBlockMetadatasPooledResources{}
	)

	// Perform selection
	selected, _ := session.selectPeersFromPerPeerBlockMetadatas(
		perPeer, peerBlocksQueues, enqueueCh,
		newStaticRuntimeReadConsistencyLevel(opts.BootstrapConsistencyLevel()),
		testPeers(peers), pooled, metrics)

	// Assert no selection
	require.Equal(t, 0, len(selected))
}

func TestSelectPeersFromPerPeerBlockMetadatasPerformsRetries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions().
		SetFetchSeriesBlocksMaxBlockRetries(2)
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)

	var (
		metrics          = session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw)
		peerA            = NewMockpeer(ctrl)
		peerB            = NewMockpeer(ctrl)
		peers            = preparedMockPeers(peerA, peerB)
		enqueueCh        = NewMockenqueueChannel(ctrl)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	atomic.StoreUint64(&peerBlocksQueues[0].assigned, 16)
	atomic.StoreUint64(&peerBlocksQueues[1].assigned, 0)
	defer peerBlocksQueues.closeAll()

	var (
		start    = timeZero
		checksum = uint32(2)
		// Peer A has 2 attempts, peer B has 1 attempt, peer B should be selected
		// for the second block as it has one retry remaining.  The first block
		// should select peer B to retrieve as we synthetically set the assigned
		// blocks count for peer A much higher than peer B.
		reattempt = blockMetadataReattempt{
			attempt:   3,
			attempted: []peer{peerA, peerB, peerA},
		}
		perPeer = []receivedBlockMetadata{
			{
				peer: peerA,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum, reattempt: reattempt,
				},
			},
			{
				peer: peerB,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksum, reattempt: reattempt,
				},
			},
		}
		pooled = selectPeersFromPerPeerBlockMetadatasPooledResources{}
	)

	// Perform selection
	selected, _ := session.selectPeersFromPerPeerBlockMetadatas(
		perPeer, peerBlocksQueues, enqueueCh,
		newStaticRuntimeReadConsistencyLevel(opts.BootstrapConsistencyLevel()),
		testPeers(peers), pooled, metrics)

	// Assert selection
	require.Equal(t, 1, len(selected))

	// Assert block selected for second peer
	assert.True(t, start.Equal(selected[0].block.start))
	assert.Equal(t, int64(2), selected[0].block.size)
	assert.Equal(t, &checksum, selected[0].block.checksum)

	assert.Equal(t, 4, selected[0].block.reattempt.attempt)
	assert.Equal(t, []peer{
		peerA, peerB, peerA, peerB,
	}, selected[0].block.reattempt.attempted)
}

func TestSelectPeersFromPerPeerBlockMetadatasRetryOnFanoutConsistencyLevelFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions().
		SetFetchSeriesBlocksMaxBlockRetries(0).
		SetBootstrapConsistencyLevel(topology.ReadConsistencyLevelMajority)
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)

	var (
		metrics          = session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw)
		peerA            = NewMockpeer(ctrl)
		peerB            = NewMockpeer(ctrl)
		peerC            = NewMockpeer(ctrl)
		peers            = preparedMockPeers(peerA, peerB, peerC)
		enqueueCh        = NewMockenqueueChannel(ctrl)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		start     = timeZero
		checksums = []uint32{1, 2, 3}
		// This simulates a fanout fetch where the first peer A has returned successfully
		// and then retries are enqueued and processed for reselection for peer B and peer C
		// which eventually satisfies another fanout retry once processed.
		fanoutFetchState = &blockFanoutFetchState{numPending: 2, numSuccess: 1}
		initialPerPeer   = []receivedBlockMetadata{
			{
				peer: peerA,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksums[0],
				},
			},
			{
				peer: peerB,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksums[1],
				},
			},
			{
				peer: peerC,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksums[2],
				},
			},
		}
		firstRetry = []receivedBlockMetadata{
			{
				peer: peerB,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksums[1], reattempt: blockMetadataReattempt{
						attempt:              1,
						fanoutFetchState:     fanoutFetchState,
						attempted:            []peer{peerB},
						fetchedPeersMetadata: initialPerPeer,
					},
				},
			},
		}
		secondRetry = []receivedBlockMetadata{
			{
				peer: peerC,
				id:   fooID,
				block: blockMetadata{
					start: start, size: 2, checksum: &checksums[2], reattempt: blockMetadataReattempt{
						attempt:              1,
						fanoutFetchState:     fanoutFetchState,
						attempted:            []peer{peerC},
						fetchedPeersMetadata: initialPerPeer,
					},
				},
			},
		}
		pooled = selectPeersFromPerPeerBlockMetadatasPooledResources{}
	)

	// Perform first selection
	selected, _ := session.selectPeersFromPerPeerBlockMetadatas(
		firstRetry, peerBlocksQueues, enqueueCh,
		newStaticRuntimeReadConsistencyLevel(opts.BootstrapConsistencyLevel()),
		testPeers(peers), pooled, metrics)

	// Assert selection
	require.Equal(t, 0, len(selected))

	// Before second selection expect the re-enqueue of the block
	var wg sync.WaitGroup
	wg.Add(1)
	enqueueCh.EXPECT().
		enqueueDelayed(1).
		Return(func(reEnqueuedPerPeer []receivedBlockMetadata) {
			assert.Equal(t, len(initialPerPeer), len(reEnqueuedPerPeer))
			for i := range reEnqueuedPerPeer {
				expected := initialPerPeer[i]
				actual := reEnqueuedPerPeer[i]

				assert.True(t, expected.id.Equal(actual.id))
				assert.Equal(t, expected.peer, actual.peer)
				assert.Equal(t, expected.block.start, actual.block.start)
				assert.Equal(t, expected.block.size, actual.block.size)
				assert.Equal(t, expected.block.checksum, actual.block.checksum)

				// Ensure no reattempt data is attached
				assert.Equal(t, blockMetadataReattempt{}, actual.block.reattempt)
			}

			return
		}, func() {
			wg.Done()
		}, nil)

	// Perform second selection
	selected, _ = session.selectPeersFromPerPeerBlockMetadatas(
		secondRetry, peerBlocksQueues, enqueueCh,
		newStaticRuntimeReadConsistencyLevel(opts.BootstrapConsistencyLevel()),
		testPeers(peers), pooled, metrics)

	// Assert selection
	require.Equal(t, 0, len(selected))

	// Wait for re-enqueue of the block
	wg.Wait()
}

func TestStreamBlocksBatchFromPeerReenqueuesOnFailCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)
	session.reattemptStreamBlocksFromPeersFn = func(
		blocks []receivedBlockMetadata,
		enqueueCh enqueueChannel,
		attemptErr error,
		_ reason,
		reattemptType reattemptType,
		_ *streamFromPeersMetrics,
	) error {
		enqueue, done, err := enqueueCh.enqueueDelayed(len(blocks))
		require.NoError(t, err)
		session.streamBlocksReattemptFromPeersEnqueue(blocks, attemptErr,
			reattemptType, enqueue, done)
		return nil
	}

	mockHostQueues, mockClients := mockHostQueuesAndClientsForFetchBootstrapBlocks(ctrl, opts)
	session.newHostQueueFn = mockHostQueues.newHostQueueFn()
	require.NoError(t, session.Open())

	var (
		start   = time.Now().Truncate(blockSize).Add(blockSize * -(24 - 1))
		retrier = xretry.NewRetrier(xretry.NewOptions().
			SetMaxRetries(1).
			SetInitialBackoff(time.Millisecond))
		peerIdx   = len(mockHostQueues) - 1
		peer      = mockHostQueues[peerIdx]
		client    = mockClients[peerIdx]
		enqueueCh = newEnqueueChannel(session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw))
		batch     = []receivedBlockMetadata{
			{
				id: fooID,
				block: blockMetadata{
					start: start, size: 2, reattempt: blockMetadataReattempt{
						retryPeersMetadata: []receivedBlockMetadata{
							{block: blockMetadata{start: start, size: 2}},
						},
					},
				}},
			{
				id: barID,
				block: blockMetadata{
					start: start, size: 2, reattempt: blockMetadataReattempt{
						retryPeersMetadata: []receivedBlockMetadata{
							{block: blockMetadata{start: start, size: 2}},
						},
					},
				}},
		}
	)

	// Fail the call twice due to retry
	client.EXPECT().FetchBlocksRaw(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("an error")).Times(2)

	// Attempt stream blocks
	bopts := result.NewOptions()
	m := session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw)
	session.streamBlocksBatchFromPeer(testsNsMetadata(t), 0, peer, batch, bopts, nil, enqueueCh, retrier, m)

	// Assert result
	assertEnqueueChannel(t, batch, enqueueCh)

	assert.NoError(t, session.Close())
}

func TestStreamBlocksBatchFromPeerVerifiesBlockErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)
	session.reattemptStreamBlocksFromPeersFn = func(
		blocks []receivedBlockMetadata,
		enqueueCh enqueueChannel,
		attemptErr error,
		_ reason,
		reattemptType reattemptType,
		_ *streamFromPeersMetrics,
	) error {
		enqueue, done, err := enqueueCh.enqueueDelayed(len(blocks))
		require.NoError(t, err)
		session.streamBlocksReattemptFromPeersEnqueue(blocks, attemptErr,
			reattemptType, enqueue, done)
		return nil
	}

	mockHostQueues, mockClients := mockHostQueuesAndClientsForFetchBootstrapBlocks(ctrl, opts)
	session.newHostQueueFn = mockHostQueues.newHostQueueFn()
	require.NoError(t, session.Open())

	start := time.Now().Truncate(blockSize).Add(blockSize * -(24 - 1))
	enc := m3tsz.NewEncoder(start, nil, true, encoding.NewOptions())
	require.NoError(t, enc.Encode(ts.Datapoint{
		Timestamp: start.Add(10 * time.Second),
		Value:     42,
	}, xtime.Second, nil))

	ctx := context.NewContext()
	defer ctx.Close()

	reader, ok := enc.Stream(ctx)
	require.True(t, ok)
	segment, err := reader.Segment()
	require.NoError(t, err)
	rawBlockData := make([]byte, segment.Len())
	n, err := reader.Read(rawBlockData)
	require.NoError(t, err)
	require.Equal(t, len(rawBlockData), n)
	rawBlockLen := int64(len(rawBlockData))

	var (
		retrier = xretry.NewRetrier(xretry.NewOptions().
			SetMaxRetries(1).
			SetInitialBackoff(time.Millisecond))
		peerIdx       = len(mockHostQueues) - 1
		peer          = mockHostQueues[peerIdx]
		client        = mockClients[peerIdx]
		enqueueCh     = newEnqueueChannel(session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw))
		blockChecksum = digest.Checksum(rawBlockData)
		batch         = []receivedBlockMetadata{
			{
				id: fooID,
				block: blockMetadata{
					start: start, size: rawBlockLen, reattempt: blockMetadataReattempt{
						retryPeersMetadata: []receivedBlockMetadata{
							{block: blockMetadata{start: start, size: rawBlockLen, checksum: &blockChecksum}},
						},
					},
				},
			},
			{
				id: barID,
				block: blockMetadata{
					start: start, size: rawBlockLen, reattempt: blockMetadataReattempt{
						retryPeersMetadata: []receivedBlockMetadata{
							{block: blockMetadata{start: start, size: rawBlockLen, checksum: &blockChecksum}},
						},
					},
				},
			},
			{
				id: barID,
				block: blockMetadata{
					start: start.Add(blockSize), size: rawBlockLen, reattempt: blockMetadataReattempt{
						retryPeersMetadata: []receivedBlockMetadata{
							{block: blockMetadata{start: start.Add(blockSize), size: rawBlockLen, checksum: &blockChecksum}},
						},
					},
				},
			},
		}
	)

	// Fail the call twice due to retry
	client.EXPECT().
		FetchBlocksRaw(gomock.Any(), gomock.Any()).
		Return(&rpc.FetchBlocksRawResult_{
			Elements: []*rpc.Blocks{
				// First foo block intact
				&rpc.Blocks{ID: []byte("foo"), Blocks: []*rpc.Block{
					&rpc.Block{Start: start.UnixNano(), Segments: &rpc.Segments{
						Merged: &rpc.Segment{
							Head: rawBlockData[:len(rawBlockData)-1],
							Tail: []byte{rawBlockData[len(rawBlockData)-1]},
						},
					}},
				}},
				// First bar block intact, second with error
				&rpc.Blocks{ID: []byte("bar"), Blocks: []*rpc.Block{
					&rpc.Block{Start: start.UnixNano(), Segments: &rpc.Segments{
						Merged: &rpc.Segment{
							Head: rawBlockData[:len(rawBlockData)-1],
							Tail: []byte{rawBlockData[len(rawBlockData)-1]},
						},
					}},
				}},
				&rpc.Blocks{ID: []byte("bar"), Blocks: []*rpc.Block{
					&rpc.Block{Start: start.Add(blockSize).UnixNano(), Err: &rpc.Error{
						Type:    rpc.ErrorType_INTERNAL_ERROR,
						Message: "an error",
					}},
				}},
			},
		}, nil)

	// Attempt stream blocks
	bopts := result.NewOptions()
	m := session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw)
	r := newBulkBlocksResult(namespace.Context{}, opts, bopts, session.pools.tagDecoder, session.pools.id)
	session.streamBlocksBatchFromPeer(testsNsMetadata(t), 0, peer, batch, bopts, r, enqueueCh, retrier, m)

	// Assert result
	assertEnqueueChannel(t, batch[2:], enqueueCh)

	// Assert length of blocks result
	assert.Equal(t, 2, r.result.AllSeries().Len())
	fooBlocks, ok := r.result.AllSeries().Get(fooID)
	require.True(t, ok)
	assert.Equal(t, 1, fooBlocks.Blocks.Len())
	barBlocks, ok := r.result.AllSeries().Get(barID)
	require.True(t, ok)
	assert.Equal(t, 1, barBlocks.Blocks.Len())

	assert.NoError(t, session.Close())
}

// TODO: add test TestStreamBlocksBatchFromPeerDoesNotRetryOnUnreachable

// TODO: add test TestVerifyFetchedBlockSegmentsNil

// TODO: add test TestVerifyFetchedBlockSegmentsNoMergedOrUnmerged

func TestStreamBlocksBatchFromPeerVerifiesBlockChecksum(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	require.NoError(t, err)
	session := s.(*session)
	session.reattemptStreamBlocksFromPeersFn = func(
		blocks []receivedBlockMetadata,
		enqueueCh enqueueChannel,
		attemptErr error,
		_ reason,
		reattemptType reattemptType,
		_ *streamFromPeersMetrics,
	) error {
		enqueue, done, err := enqueueCh.enqueueDelayed(len(blocks))
		require.NoError(t, err)
		session.streamBlocksReattemptFromPeersEnqueue(blocks, attemptErr,
			reattemptType, enqueue, done)
		return nil
	}

	mockHostQueues, mockClients := mockHostQueuesAndClientsForFetchBootstrapBlocks(ctrl, opts)
	session.newHostQueueFn = mockHostQueues.newHostQueueFn()

	require.NoError(t, session.Open())

	start := time.Now().Truncate(blockSize).Add(blockSize * -(24 - 1))

	enc := m3tsz.NewEncoder(start, nil, true, encoding.NewOptions())
	require.NoError(t, enc.Encode(ts.Datapoint{
		Timestamp: start.Add(10 * time.Second),
		Value:     42,
	}, xtime.Second, nil))

	ctx := context.NewContext()
	defer ctx.Close()

	reader, ok := enc.Stream(ctx)
	require.True(t, ok)
	segment, err := reader.Segment()
	require.NoError(t, err)
	rawBlockData := make([]byte, segment.Len())
	n, err := reader.Read(rawBlockData)
	require.NoError(t, err)
	require.Equal(t, len(rawBlockData), n)
	rawBlockLen := int64(len(rawBlockData))

	var (
		retrier = xretry.NewRetrier(xretry.NewOptions().
			SetMaxRetries(1).
			SetInitialBackoff(time.Millisecond))
		peerIdx       = len(mockHostQueues) - 1
		peer          = mockHostQueues[peerIdx]
		client        = mockClients[peerIdx]
		enqueueCh     = newEnqueueChannel(session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw))
		blockChecksum = digest.Checksum(rawBlockData)
		batch         = []receivedBlockMetadata{
			{
				id: fooID,
				block: blockMetadata{
					start: start, size: rawBlockLen, reattempt: blockMetadataReattempt{
						retryPeersMetadata: []receivedBlockMetadata{
							{block: blockMetadata{start: start, size: rawBlockLen, checksum: &blockChecksum}},
						},
					},
				},
			},
			{
				id: barID,
				block: blockMetadata{
					start: start, size: rawBlockLen, reattempt: blockMetadataReattempt{
						retryPeersMetadata: []receivedBlockMetadata{
							{block: blockMetadata{start: start, size: rawBlockLen, checksum: &blockChecksum}},
						},
					},
				},
			},
			{
				id: barID,
				block: blockMetadata{
					start: start.Add(blockSize), size: rawBlockLen, reattempt: blockMetadataReattempt{
						retryPeersMetadata: []receivedBlockMetadata{
							{block: blockMetadata{start: start.Add(blockSize), size: rawBlockLen, checksum: &blockChecksum}},
						},
					},
				},
			},
		}
	)

	head := rawBlockData[:len(rawBlockData)-1]
	tail := []byte{rawBlockData[len(rawBlockData)-1]}
	d := digest.NewDigest().Update(head).Update(tail).Sum32()
	validChecksum := int64(d)
	invalidChecksum := 1 + validChecksum

	client.EXPECT().
		FetchBlocksRaw(gomock.Any(), gomock.Any()).
		Return(&rpc.FetchBlocksRawResult_{
			Elements: []*rpc.Blocks{
				// valid foo block
				&rpc.Blocks{ID: []byte("foo"), Blocks: []*rpc.Block{
					&rpc.Block{Start: start.UnixNano(), Checksum: &validChecksum, Segments: &rpc.Segments{
						Merged: &rpc.Segment{
							Head: head,
							Tail: tail,
						},
					}},
				}},
				&rpc.Blocks{ID: []byte("bar"), Blocks: []*rpc.Block{
					// invalid bar block
					&rpc.Block{Start: start.UnixNano(), Checksum: &invalidChecksum, Segments: &rpc.Segments{
						Merged: &rpc.Segment{
							Head: head,
							Tail: tail,
						},
					}},
				}},
				&rpc.Blocks{ID: []byte("bar"), Blocks: []*rpc.Block{
					// valid bar block, no checksum
					&rpc.Block{Start: start.Add(blockSize).UnixNano(), Segments: &rpc.Segments{
						Merged: &rpc.Segment{
							Head: head,
							Tail: tail,
						},
					}},
				}},
			},
		}, nil)

	// Attempt stream blocks
	bopts := result.NewOptions()
	m := session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw)
	r := newBulkBlocksResult(namespace.Context{}, opts, bopts, session.pools.tagDecoder, session.pools.id)
	session.streamBlocksBatchFromPeer(testsNsMetadata(t), 0, peer, batch, bopts, r, enqueueCh, retrier, m)

	// Assert enqueueChannel contents (bad bar block)
	assertEnqueueChannel(t, batch[1:2], enqueueCh)

	// Assert length of blocks result
	assert.Equal(t, 2, r.result.AllSeries().Len())

	fooBlocks, ok := r.result.AllSeries().Get(fooID)
	require.True(t, ok)
	assert.Equal(t, 1, fooBlocks.Blocks.Len())
	_, ok = fooBlocks.Blocks.BlockAt(start)
	assert.True(t, ok)

	barBlocks, ok := r.result.AllSeries().Get(barID)
	require.True(t, ok)
	assert.Equal(t, 1, barBlocks.Blocks.Len())
	_, ok = barBlocks.Blocks.BlockAt(start.Add(blockSize))
	assert.True(t, ok)

	assert.NoError(t, session.Close())
}

func TestBlocksResultAddBlockFromPeerReadMerged(t *testing.T) {
	opts := newSessionTestAdminOptions()
	bopts := newResultTestOptions()
	start := time.Now().Truncate(time.Hour)

	blockSize := time.Minute
	bs := int64(blockSize)
	rpcBlockSize := &bs

	bl := &rpc.Block{
		Start: start.UnixNano(),
		Segments: &rpc.Segments{Merged: &rpc.Segment{
			Head:      []byte{1, 2},
			Tail:      []byte{3},
			BlockSize: rpcBlockSize,
		}},
	}

	r := newBulkBlocksResult(namespace.Context{}, opts, bopts,
		testTagDecodingPool, testIDPool)
	r.addBlockFromPeer(fooID, fooTags, testHost, bl)

	series := r.result.AllSeries()
	assert.Equal(t, 1, series.Len())

	sl, ok := series.Get(fooID)
	assert.True(t, ok)
	blocks := sl.Blocks
	assert.Equal(t, 1, blocks.Len())
	result, ok := blocks.BlockAt(start)
	assert.True(t, ok)

	ctx := context.NewContext()
	defer ctx.Close()

	stream, err := result.Stream(ctx)
	require.NoError(t, err)
	require.NotNil(t, stream)

	// block reader has correct start time and block size
	assert.Equal(t, start, stream.Start)
	assert.Equal(t, blockSize, stream.BlockSize)

	seg, err := stream.Segment()
	require.NoError(t, err)

	// Assert block has data
	data, err := ioutil.ReadAll(xio.NewSegmentReader(seg))
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3}, data)
}

func TestBlocksResultAddBlockFromPeerReadUnmerged(t *testing.T) {
	var wrapEncoderFn func(enc encoding.Encoder) encoding.Encoder
	eops := encoding.NewOptions()
	intopt := true

	encoderPool := encoding.NewEncoderPool(nil)
	encoderPool.Init(func() encoding.Encoder {
		enc := m3tsz.NewEncoder(time.Time{}, nil, intopt, eops)
		if wrapEncoderFn != nil {
			enc = wrapEncoderFn(enc)
		}
		return enc
	})

	opts := newSessionTestAdminOptions()
	bopts := result.NewOptions()
	bopts = bopts.SetDatabaseBlockOptions(bopts.DatabaseBlockOptions().
		SetEncoderPool(encoderPool).
		SetMultiReaderIteratorPool(newSessionTestMultiReaderIteratorPool()))

	start := time.Now()

	vals0 := []testValue{
		{1.0, start, xtime.Second, []byte{1, 2, 3}},
		{4.0, start.Add(3 * time.Second), xtime.Second, nil},
	}

	vals1 := []testValue{
		{2.0, start.Add(1 * time.Second), xtime.Second, []byte{4, 5, 6}},
	}

	vals2 := []testValue{
		{3.0, start.Add(2 * time.Second), xtime.Second, []byte{7, 8, 9}},
	}

	var all []testValue
	bl := &rpc.Block{
		Start:    start.UnixNano(),
		Segments: &rpc.Segments{},
	}
	for _, vals := range [][]testValue{vals0, vals1, vals2} {
		nsCtx := namespace.NewContextFor(ident.StringID("default"), opts.SchemaRegistry())
		encoder := encoderPool.Get()
		encoder.Reset(start, 0, nsCtx.Schema)
		for _, val := range vals {
			dp := ts.Datapoint{Timestamp: val.t, Value: val.value}
			assert.NoError(t, encoder.Encode(dp, val.unit, val.annotation))
			all = append(all, val)
		}
		result := encoder.Discard()
		seg := &rpc.Segment{Head: result.Head.Bytes(), Tail: result.Tail.Bytes()}
		bl.Segments.Unmerged = append(bl.Segments.Unmerged, seg)
	}

	r := newBulkBlocksResult(namespace.Context{}, opts, bopts, testTagDecodingPool, testIDPool)
	r.addBlockFromPeer(fooID, fooTags, testHost, bl)

	series := r.result.AllSeries()
	assert.Equal(t, 1, series.Len())

	sl, ok := series.Get(fooID)
	assert.True(t, ok)
	blocks := sl.Blocks
	assert.Equal(t, 1, blocks.Len())
	result, ok := blocks.BlockAt(start)
	assert.True(t, ok)

	ctx := context.NewContext()
	defer ctx.Close()

	stream, err := result.Stream(ctx)
	assert.NoError(t, err)

	// Sort the test values
	sort.Sort(testValuesByTime(all))

	// Assert test values sorted match the block values
	iter := m3tsz.NewReaderIterator(stream, intopt, eops)
	defer iter.Close()
	asserted := 0
	for iter.Next() {
		idx := asserted
		dp, unit, annotation := iter.Current()
		assert.Equal(t, all[idx].value, dp.Value)
		assert.True(t, all[idx].t.Equal(dp.Timestamp))
		assert.Equal(t, all[idx].unit, unit)
		assert.Equal(t, all[idx].annotation, []byte(annotation))
		asserted++
	}
	assert.Equal(t, len(all), asserted)
	assert.NoError(t, iter.Err())
}

// TODO: add test TestBlocksResultAddBlockFromPeerMergeExistingResult

func TestBlocksResultAddBlockFromPeerErrorOnNoSegments(t *testing.T) {
	opts := newSessionTestAdminOptions()
	bopts := result.NewOptions()
	r := newBulkBlocksResult(namespace.Context{}, opts, bopts, testTagDecodingPool, testIDPool)

	bl := &rpc.Block{Start: time.Now().UnixNano()}
	err := r.addBlockFromPeer(fooID, fooTags, testHost, bl)
	assert.Error(t, err)
	assert.Equal(t, errSessionBadBlockResultFromPeer, err)
}

func TestBlocksResultAddBlockFromPeerErrorOnNoSegmentsData(t *testing.T) {
	opts := newSessionTestAdminOptions()
	bopts := result.NewOptions()
	r := newBulkBlocksResult(namespace.Context{}, opts, bopts, testTagDecodingPool, testIDPool)

	bl := &rpc.Block{Start: time.Now().UnixNano(), Segments: &rpc.Segments{}}
	err := r.addBlockFromPeer(fooID, fooTags, testHost, bl)
	assert.Error(t, err)
	assert.Equal(t, errSessionBadBlockResultFromPeer, err)
}

func TestEnqueueChannelEnqueueDelayed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)
	enqueueCh := newEnqueueChannel(session.newPeerMetadataStreamingProgressMetrics(0, resultTypeRaw))

	// Enqueue multiple blocks metadata
	numBlocks := 10
	blocks := make([][]receivedBlockMetadata, numBlocks)
	enqueueFn, enqueueDelayedDone, err := enqueueCh.enqueueDelayed(len(blocks))
	require.NoError(t, err)

	require.Equal(t, numBlocks, enqueueCh.unprocessedLen())
	enqueueChInputs := enqueueCh.read()
	require.Equal(t, 0, len(enqueueChInputs))

	// Actually enqueue the blocks
	for i := 0; i < numBlocks; i++ {
		enqueueFn(blocks[i])
	}
	enqueueDelayedDone()

	require.Equal(t, numBlocks, enqueueCh.unprocessedLen())
	enqueueChInputs = enqueueCh.read()
	require.Equal(t, numBlocks, len(enqueueChInputs))

	// Process the blocks
	require.NoError(t, err)
	for i := 0; i < numBlocks; i++ {
		<-enqueueChInputs
		enqueueCh.trackProcessed(1)
	}

	require.Equal(t, 0, enqueueCh.unprocessedLen())
	enqueueChInputs = enqueueCh.read()
	require.Equal(t, 0, len(enqueueChInputs))
}

func mockPeerBlocksQueues(peers []peer, opts AdminOptions) peerBlocksQueues {
	var (
		peerQueues peerBlocksQueues
		workers    = xsync.NewWorkerPool(opts.FetchSeriesBlocksBatchConcurrency())
	)
	for _, peer := range peers {
		size := opts.FetchSeriesBlocksBatchSize()
		drainEvery := 100 * time.Millisecond
		queue := newPeerBlocksQueue(peer, size, drainEvery, workers, func(batch []receivedBlockMetadata) {
			// No-op
		})
		peerQueues = append(peerQueues, queue)
	}
	return peerQueues
}

func preparedMockPeers(peers ...*Mockpeer) []peer {
	var result []peer
	for i, peer := range peers {
		id := fmt.Sprintf("mockpeer%d", i)
		addr := fmt.Sprintf("%s:9000", id)
		peer.EXPECT().Host().Return(topology.NewHost(id, addr)).AnyTimes()
		result = append(result, peer)
	}
	return result
}

type MockHostQueues []*MockhostQueue

func (qs MockHostQueues) newHostQueueFn() newHostQueueFn {
	idx := uint64(0)
	return func(
		host topology.Host,
		opts hostQueueOpts,
	) (hostQueue, error) {
		return qs[atomic.AddUint64(&idx, 1)-1], nil
	}
}

type MockTChanNodes []*rpc.MockTChanNode

func (c MockTChanNodes) expectFetchMetadataAndReturn(
	result []testBlocksMetadata,
	opts AdminOptions,
) {
	for _, client := range c {
		expectFetchMetadataAndReturn(client, result, opts)
	}
}

func mockHostQueuesAndClientsForFetchBootstrapBlocks(
	ctrl *gomock.Controller,
	opts AdminOptions,
) (MockHostQueues, MockTChanNodes) {
	var (
		hostQueues MockHostQueues
		clients    MockTChanNodes
	)
	hostShardSets := sessionTestHostAndShards(sessionTestShardSet())
	for i := 0; i < len(hostShardSets); i++ {
		host := hostShardSets[i].Host()
		hostQueue, client := defaultHostAndClientWithExpect(ctrl, host, opts)
		hostQueues = append(hostQueues, hostQueue)
		clients = append(clients, client)
	}
	return hostQueues, clients
}

func defaultHostAndClientWithExpect(
	ctrl *gomock.Controller,
	host topology.Host,
	opts AdminOptions,
) (*MockhostQueue, *rpc.MockTChanNode) {
	client := rpc.NewMockTChanNode(ctrl)
	connectionPool := NewMockconnectionPool(ctrl)
	connectionPool.EXPECT().NextClient().Return(client, &noopPooledChannel{}, nil).AnyTimes()

	hostQueue := NewMockhostQueue(ctrl)
	hostQueue.EXPECT().Open()
	hostQueue.EXPECT().Host().Return(host).AnyTimes()
	hostQueue.EXPECT().ConnectionCount().Return(opts.MinConnectionCount()).Times(sessionTestShards)
	hostQueue.EXPECT().ConnectionPool().Return(connectionPool).AnyTimes()
	hostQueue.EXPECT().BorrowConnection(gomock.Any()).Do(func(fn WithConnectionFn) {
		fn(client, &noopPooledChannel{})
	}).Return(nil).AnyTimes()
	hostQueue.EXPECT().Close()

	return hostQueue, client
}

func resultMetadataFromBlocks(
	blocks []testBlocks,
) []testBlocksMetadata {
	var result []testBlocksMetadata
	for _, b := range blocks {
		bm := []testBlockMetadata{}
		for _, bl := range b.blocks {
			size := int64(0)
			d := digest.NewDigest()
			if merged := bl.segments.merged; merged != nil {
				size += int64(len(merged.head) + len(merged.tail))
				d = d.Update(merged.head).Update(merged.tail)
			}
			for _, unmerged := range bl.segments.unmerged {
				size += int64(len(unmerged.head) + len(unmerged.tail))
				d = d.Update(unmerged.head).Update(unmerged.tail)
			}
			checksum := d.Sum32()
			m := testBlockMetadata{
				start:    bl.start,
				size:     &size,
				checksum: &checksum,
			}
			bm = append(bm, m)
		}
		m := testBlocksMetadata{
			id:     b.id,
			blocks: bm,
		}
		result = append(result, m)
	}
	return result
}

func expectedRepairFetchRequestsAndResponses(
	blocks []testBlocks,
	batchSize int,
) ([]fetchBlocksReq, [][]testBlocks) {
	requests := make([]fetchBlocksReq, 0, len(blocks))
	responses := make([][]testBlocks, 0, len(blocks))
	request := fetchBlocksReq{
		params: []fetchBlocksReqParam{},
	}
	response := []testBlocks{}
	for idx := 0; idx < len(blocks); idx++ {
		starts := make([]time.Time, 0, len(blocks[idx].blocks))
		for j := 0; j < len(blocks[idx].blocks); j++ {
			starts = append(starts, blocks[idx].blocks[j].start)
		}
		if idx != 0 && (idx%batchSize) == 0 {
			requests = append(requests, request)
			responses = append(responses, response)
			request = fetchBlocksReq{
				params: []fetchBlocksReqParam{},
			}
			response = []testBlocks{}
		}
		request.params = append(request.params,
			fetchBlocksReqParam{
				id:     blocks[idx].id,
				starts: starts,
			})
		response = append(response, blocks[idx])
	}
	if len(response) > 0 {
		responses = append(responses, response)
		requests = append(requests, request)
	}
	return requests, responses
}

func expectedReqsAndResultFromBlocks(
	t *testing.T,
	blocks []testBlocks,
	batchSize int,
	clientsParticipatingLen int,
	selectClientForBlockFn func(id ident.ID, blockIndex int) (clientIndex int),
) ([][]fetchBlocksReq, [][][]testBlocks) {
	var (
		clientsExpectReqs   [][]fetchBlocksReq
		clientsBlocksResult [][][]testBlocks
		blockIdx            = 0
	)
	for i := 0; i < clientsParticipatingLen; i++ {
		clientsExpectReqs = append(clientsExpectReqs, []fetchBlocksReq{})
		clientsBlocksResult = append(clientsBlocksResult, [][]testBlocks{})
	}

	// Round robin the blocks to clients to simulate our load balancing
	for len(blocks) > 0 {
		currBlock := blocks[0]

		clientIdx := selectClientForBlockFn(currBlock.id, blockIdx)
		if clientIdx >= clientsParticipatingLen {
			msg := "client selected for block (%d) " +
				"is greater than clients partipating (%d)"
			require.FailNow(t, fmt.Sprintf(msg, blockIdx, clientIdx))
		}

		expectReqs := clientsExpectReqs[clientIdx]
		blocksResult := clientsBlocksResult[clientIdx]

		// Extend if batch is full
		if len(expectReqs) == 0 ||
			len(expectReqs[len(expectReqs)-1].params) == batchSize {
			clientsExpectReqs[clientIdx] =
				append(clientsExpectReqs[clientIdx], fetchBlocksReq{})
			expectReqs = clientsExpectReqs[clientIdx]
			clientsBlocksResult[clientIdx] =
				append(clientsBlocksResult[clientIdx], []testBlocks{})
			blocksResult = clientsBlocksResult[clientIdx]
		}

		req := &expectReqs[len(expectReqs)-1]

		starts := []time.Time{}
		for i := 0; i < len(currBlock.blocks); i++ {
			starts = append(starts, currBlock.blocks[i].start)
		}
		param := fetchBlocksReqParam{
			id:     currBlock.id,
			starts: starts,
		}
		req.params = append(req.params, param)
		blocksResult[len(blocksResult)-1] = append(blocksResult[len(blocksResult)-1], currBlock)

		clientsBlocksResult[clientIdx] = blocksResult

		blocks = blocks[1:]
		blockIdx++
	}

	return clientsExpectReqs, clientsBlocksResult
}

func expectFetchMetadataAndReturn(
	client *rpc.MockTChanNode,
	result []testBlocksMetadata,
	opts AdminOptions,
) {
	batchSize := opts.FetchSeriesBlocksBatchSize()
	totalCalls := int(math.Ceil(float64(len(result)) / float64(batchSize)))
	includeSizes := true

	var calls []*gomock.Call
	for i := 0; i < totalCalls; i++ {
		var (
			ret      = &rpc.FetchBlocksMetadataRawV2Result_{}
			beginIdx = i * batchSize
		)
		for j := beginIdx; j < len(result) && j < beginIdx+batchSize; j++ {
			id := result[j].id.Bytes()
			for k := 0; k < len(result[j].blocks); k++ {
				bl := &rpc.BlockMetadataV2{}
				bl.ID = id
				bl.Start = result[j].blocks[k].start.UnixNano()
				bl.Size = result[j].blocks[k].size
				if result[j].blocks[k].checksum != nil {
					checksum := int64(*result[j].blocks[k].checksum)
					bl.Checksum = &checksum
				}
				ret.Elements = append(ret.Elements, bl)
			}
		}
		if i != totalCalls-1 {
			// Include next page token if not last page
			ret.NextPageToken = []byte(fmt.Sprintf("token_%d", i+1))
		}

		matcher := &fetchMetadataReqMatcher{
			shard:        0,
			limit:        int64(batchSize),
			includeSizes: &includeSizes,
			isV2:         true,
		}
		if i != 0 {
			matcher.pageToken = []byte(fmt.Sprintf("token_%d", i))
		}

		call := client.EXPECT().FetchBlocksMetadataRawV2(gomock.Any(), matcher).Return(ret, nil)
		calls = append(calls, call)
	}

	gomock.InOrder(calls...)
}

type fetchMetadataReqMatcher struct {
	shard        int32
	limit        int64
	pageToken    []byte
	includeSizes *bool
	isV2         bool
}

func (m *fetchMetadataReqMatcher) Matches(x interface{}) bool {
	req, ok := x.(*rpc.FetchBlocksMetadataRawV2Request)
	if !ok {
		return false
	}

	if m.shard != req.Shard {
		return false
	}

	if m.limit != req.Limit {
		return false
	}

	if m.pageToken == nil {
		if req.PageToken != nil {
			return false
		}
	} else {
		if req.PageToken == nil {
			return false
		}
		if !bytes.Equal(req.PageToken, m.pageToken) {
			return false
		}
	}

	if m.includeSizes == nil {
		if req.IncludeSizes != nil {
			return false
		}
	} else {
		if req.IncludeSizes == nil {
			return false
		}
		if *req.IncludeSizes != *m.includeSizes {
			return false
		}
	}

	return true
}

func (m *fetchMetadataReqMatcher) String() string {
	return "fetchMetadataReqMatcher"
}

func expectFetchBlocksAndReturn(
	client *rpc.MockTChanNode,
	expect []fetchBlocksReq,
	result [][]testBlocks,
) {
	for i := 0; i < len(expect); i++ {
		matcher := &fetchBlocksReqMatcher{req: expect[i]}
		ret := &rpc.FetchBlocksRawResult_{}
		for _, res := range result[i] {
			blocks := &rpc.Blocks{}
			blocks.ID = res.id.Bytes()
			for j := range res.blocks {
				bl := &rpc.Block{}
				bl.Start = res.blocks[j].start.UnixNano()
				if res.blocks[j].segments != nil {
					segs := &rpc.Segments{}
					if res.blocks[j].segments.merged != nil {
						segs.Merged = &rpc.Segment{
							Head: res.blocks[j].segments.merged.head,
							Tail: res.blocks[j].segments.merged.tail,
						}
					}
					for k := range res.blocks[j].segments.unmerged {
						segs.Unmerged = append(segs.Unmerged, &rpc.Segment{
							Head: res.blocks[j].segments.unmerged[k].head,
							Tail: res.blocks[j].segments.unmerged[k].tail,
						})
					}
					bl.Segments = segs
				}
				if res.blocks[j].err != nil {
					bl.Err = &rpc.Error{}
					bl.Err.Type = res.blocks[j].err.errorType
					bl.Err.Message = res.blocks[j].err.message
				}
				blocks.Blocks = append(blocks.Blocks, bl)
			}
			ret.Elements = append(ret.Elements, blocks)
		}

		client.EXPECT().FetchBlocksRaw(gomock.Any(), matcher).Do(func(_ interface{}, req *rpc.FetchBlocksRawRequest) {
			// The order of the elements in the request is non-deterministic (due to concurrency) so inspect the request
			// and re-order the response to match by trying to match up values (their may be duplicate entries for a
			// given series ID so comparing IDs is not sufficient).
			retElements := make([]*rpc.Blocks, 0, len(ret.Elements))
			for _, elem := range req.Elements {
			inner:
				for _, retElem := range ret.Elements {
					if !bytes.Equal(elem.ID, retElem.ID) {
						continue
					}
					if len(elem.Starts) != len(retElem.Blocks) {
						continue
					}

					for i, start := range elem.Starts {
						block := retElem.Blocks[i]
						if start != block.Start {
							continue inner
						}
					}

					retElements = append(retElements, retElem)
				}
			}
			ret.Elements = retElements
		}).Return(ret, nil)
	}
}

type fetchBlocksReqMatcher struct {
	req fetchBlocksReq
}

func (m *fetchBlocksReqMatcher) Matches(x interface{}) bool {
	req, ok := x.(*rpc.FetchBlocksRawRequest)
	if !ok {
		return false
	}

	params := m.req.params
	if len(params) != len(req.Elements) {
		return false
	}

	for i := range params {
		// Possible for slices to be in different orders so try and match
		// them up intelligently.
		var found = false
	inner:
		for reqIdx, element := range req.Elements {
			reqID := ident.BinaryID(checked.NewBytes(element.ID, nil))
			if !params[i].id.Equal(reqID) {
				continue
			}

			if len(params[i].starts) != len(req.Elements[reqIdx].Starts) {
				continue
			}
			for j := range params[i].starts {
				if params[i].starts[j].UnixNano() != req.Elements[reqIdx].Starts[j] {
					continue inner
				}
			}

			found = true
		}

		if !found {
			return false
		}
	}

	return true
}

func (m *fetchBlocksReqMatcher) String() string {
	return "fetchBlocksReqMatcher"
}

type fetchBlocksReq struct {
	params []fetchBlocksReqParam
}

type fetchBlocksReqParam struct {
	id     ident.ID
	starts []time.Time
}

type testBlocksMetadata struct {
	id     ident.ID
	blocks []testBlockMetadata
}

type testBlockMetadata struct {
	start    time.Time
	size     *int64
	checksum *uint32
}

type testBlocks struct {
	id     ident.ID
	blocks []testBlock
}

type testBlock struct {
	start    time.Time
	segments *testBlockSegments
	err      *testBlockError
}

type testBlockError struct {
	errorType rpc.ErrorType
	message   string
}

type testBlockSegments struct {
	merged   *testBlockSegment
	unmerged []*testBlockSegment
}

type testBlockSegment struct {
	head []byte
	tail []byte
}

func assertFetchBootstrapBlocksResult(
	t *testing.T,
	expected []testBlocks,
	actual result.ShardResult,
) {
	ctx := context.NewContext()
	defer ctx.Close()

	series := actual.AllSeries()
	require.Equal(t, len(expected), series.Len())

	for i := range expected {
		id := expected[i].id
		entry, ok := series.Get(id)
		if !ok {
			require.Fail(t, fmt.Sprintf("blocks for series '%s' not present", id.String()))
			continue
		}

		expectedLen := 0
		for _, block := range expected[i].blocks {
			if block.err != nil {
				continue
			}
			expectedLen++
		}
		require.Equal(t, expectedLen, entry.Blocks.Len())

		for _, block := range expected[i].blocks {
			actualBlock, ok := entry.Blocks.BlockAt(block.start)
			if !ok {
				require.Fail(t, fmt.Sprintf("block for series '%s' start %v not present", id.String(), block.start))
				continue
			}

			if block.segments.merged != nil {
				expectedData := append(block.segments.merged.head, block.segments.merged.tail...)
				stream, err := actualBlock.Stream(ctx)
				require.NoError(t, err)
				seg, err := stream.Segment()
				require.NoError(t, err)
				actualData := append(bytesFor(seg.Head), bytesFor(seg.Tail)...)
				require.Equal(t, expectedData, actualData)
			} else if block.segments.unmerged != nil {
				require.Fail(t, "unmerged comparison not supported")
			}
		}
	}
}

func bytesFor(data checked.Bytes) []byte {
	if data == nil {
		return nil
	}
	return data.Bytes()
}

func assertEnqueueChannel(
	t *testing.T,
	expected []receivedBlockMetadata,
	ch enqueueChannel,
) {
	enqueueCh, ok := ch.(*enqueueCh)
	require.True(t, ok)

	var distinct []receivedBlockMetadata
	for {
		var perPeerBlocksMetadata []receivedBlockMetadata
		enqueueChInputs := enqueueCh.read()

		select {
		case perPeerBlocksMetadata = <-enqueueChInputs:
		default:
		}
		if perPeerBlocksMetadata == nil {
			break
		}

		elem := perPeerBlocksMetadata[0]
		distinct = append(distinct, elem)
	}

	require.Equal(t, len(expected), len(distinct))
	matched := make([]bool, len(expected))
	for i, expected := range expected {
		for _, actual := range distinct {
			found := expected.id.Equal(actual.id) &&
				expected.block.start.Equal(actual.block.start) &&
				expected.block.size == actual.block.size
			if found {
				matched[i] = true
				continue
			}
		}
	}
	for _, m := range matched {
		assert.True(t, m)
	}

	close(enqueueCh.peersMetadataCh)
}
