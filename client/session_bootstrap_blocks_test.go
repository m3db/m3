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
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var (
	timeZero = time.Time{}
)

func newSessionTestAdminOptions() AdminOptions {
	opts := applySessionTestOptions(NewAdminOptions()).(AdminOptions)
	hostShardSets := sessionTestHostAndShards(sessionTestShardSet())
	host := hostShardSets[0].Host()
	return opts.
		Origin(host).
		FetchSeriesBlocksBatchSize(2).
		FetchSeriesBlocksMetadataBatchTimeout(time.Second).
		FetchSeriesBlocksBatchTimeout(time.Second).
		FetchSeriesBlocksBatchConcurrency(4)
}

func newBootstrapTestOptions() bootstrap.Options {
	return newBootstrapTestOptionsWithEncoderCallback(nil)
}

func newBootstrapTestOptionsWithEncoderCallback(fn func(e *testEncoder)) bootstrap.Options {
	opts := bootstrap.NewOptions()
	encoderPool := encoding.NewEncoderPool(0)
	encoderPool.Init(func() encoding.Encoder {
		enc := &testEncoder{}
		if fn != nil {
			fn(enc)
		}
		return enc
	})
	return opts.DatabaseBlockOptions(opts.GetDatabaseBlockOptions().
		EncoderPool(encoderPool))
}

func TestFetchBootstrapBlocksAllPeersSucceed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
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
		peer hostQueue,
		maxQueueSize int,
		_ time.Duration,
		workers pool.WorkerPool,
		processFn processFn,
	) *peerBlocksQueue {
		qsMutex.Lock()
		defer qsMutex.Unlock()
		q := newPeerBlocksQueue(peer, maxQueueSize, 0, workers, processFn)
		qs = append(qs, q)
		return q
	}

	assert.NoError(t, session.Open())

	batchSize := opts.GetFetchSeriesBlocksBatchSize()
	blockSize := 2 * time.Hour

	start := time.Now().Truncate(blockSize).Add(blockSize * -(24 - 1))

	blocks := []testBlocks{
		{
			id: "foo",
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
			id: "bar",
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
			id: "baz",
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

	// Expect the fetch metadata calls
	metadataResult := resultMetadataFromBlocks(blocks)
	// Skip the first client which is the client for the origin
	mockClients[1:].expectFetchMetadataAndReturn(metadataResult, opts)

	// Expect the fetch blocks calls
	participating := len(mockClients) - 1
	blocksExpectedReqs, blocksResult := expectedReqsAndResultFromBlocks(blocks, batchSize, participating)
	// Skip the first client which is the client for the origin
	for i, client := range mockClients[1:] {
		expectFetchBlocksAndReturn(client, blocksExpectedReqs[i], blocksResult[i])
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
	bootstrapOpts := newBootstrapTestOptions()
	result, err := session.FetchBootstrapBlocksFromPeers(0, rangeStart, rangeEnd, bootstrapOpts)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Assert result
	assertFetchBootstrapBlocksResult(t, blocks, result)

	assert.NoError(t, session.Close())
}

func TestSelectBlocksForSeriesFromPeerBlocksMetadataAllPeersSucceed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	var (
		peerA            = NewMockhostQueue(ctrl)
		peerB            = NewMockhostQueue(ctrl)
		peers            = preparedMockPeers(peerA, peerB)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		currStart, currEligible []*blocksMetadata
		blocksMetadataQueues    []blocksMetadataQueue
		start                   = timeZero
		perPeer                 = []*blocksMetadata{
			&blocksMetadata{
				peer: peerA,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 2},
				},
			},
			&blocksMetadata{
				peer: peerB,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 2},
				},
			},
		}
	)

	// Perform selection
	session.selectBlocksForSeriesFromPeerBlocksMetadata(
		perPeer, peerBlocksQueues,
		currStart, currEligible, blocksMetadataQueues)

	// Assert selection first peer
	assert.Equal(t, 1, len(perPeer[0].blocks))

	assert.Equal(t, start, perPeer[0].blocks[0].start)
	assert.Equal(t, int64(2), perPeer[0].blocks[0].size)

	assert.Equal(t, 1, perPeer[0].blocks[0].reattempt.attempt)
	assert.Equal(t, []hostQueue{peerA}, perPeer[0].blocks[0].reattempt.attempted)

	// Assert selection second peer
	assert.Equal(t, 0, len(perPeer[1].blocks))
}

func TestSelectBlocksForSeriesFromPeerBlocksMetadataTakeLargerBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	var (
		peerA            = NewMockhostQueue(ctrl)
		peerB            = NewMockhostQueue(ctrl)
		peers            = preparedMockPeers(peerA, peerB)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		currStart, currEligible []*blocksMetadata
		blocksMetadataQueues    []blocksMetadataQueue
		start                   = timeZero
		perPeer                 = []*blocksMetadata{
			&blocksMetadata{
				peer: peerA,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 2},
					{start: start.Add(time.Hour * 2), size: 1},
				},
			},
			&blocksMetadata{
				peer: peerB,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 1},
					{start: start.Add(time.Hour * 2), size: 2},
				},
			},
		}
	)

	// Perform selection
	session.selectBlocksForSeriesFromPeerBlocksMetadata(
		perPeer, peerBlocksQueues,
		currStart, currEligible, blocksMetadataQueues)

	// Assert selection first peer
	assert.Equal(t, 1, len(perPeer[0].blocks))

	assert.Equal(t, start, perPeer[0].blocks[0].start)
	assert.Equal(t, int64(2), perPeer[0].blocks[0].size)

	assert.Equal(t, 1, perPeer[0].blocks[0].reattempt.attempt)
	assert.Equal(t, []hostQueue{peerA}, perPeer[0].blocks[0].reattempt.attempted)

	// Assert selection second peer
	assert.Equal(t, 1, len(perPeer[1].blocks))

	assert.Equal(t, start.Add(time.Hour*2), perPeer[1].blocks[0].start)
	assert.Equal(t, int64(2), perPeer[1].blocks[0].size)

	assert.Equal(t, 1, perPeer[1].blocks[0].reattempt.attempt)
	assert.Equal(t, []hostQueue{peerA}, perPeer[1].blocks[0].reattempt.attempted)
}

func TestSelectBlocksForSeriesFromPeerBlocksMetadataTakeAvailableBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	var (
		peerA            = NewMockhostQueue(ctrl)
		peerB            = NewMockhostQueue(ctrl)
		peerC            = NewMockhostQueue(ctrl)
		peers            = preparedMockPeers(peerA, peerB, peerC)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		currStart, currEligible []*blocksMetadata
		blocksMetadataQueues    []blocksMetadataQueue
		start                   = timeZero
		perPeer                 = []*blocksMetadata{
			&blocksMetadata{
				peer: peerA,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 2},
				},
			},
			&blocksMetadata{
				peer: peerB,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start.Add(time.Hour * 2), size: 2},
				},
			},
			&blocksMetadata{
				peer: peerC,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start.Add(time.Hour * 4), size: 2},
				},
			},
		}
	)

	// Perform selection
	session.selectBlocksForSeriesFromPeerBlocksMetadata(
		perPeer, peerBlocksQueues,
		currStart, currEligible, blocksMetadataQueues)

	// Assert selection first peer
	assert.Equal(t, 1, len(perPeer[0].blocks))

	assert.Equal(t, start, perPeer[0].blocks[0].start)
	assert.Equal(t, int64(2), perPeer[0].blocks[0].size)

	assert.Equal(t, 1, perPeer[0].blocks[0].reattempt.attempt)
	assert.Equal(t, []hostQueue{peerA}, perPeer[0].blocks[0].reattempt.attempted)

	// Assert selection second peer
	assert.Equal(t, 1, len(perPeer[1].blocks))

	assert.Equal(t, start.Add(time.Hour*2), perPeer[1].blocks[0].start)
	assert.Equal(t, int64(2), perPeer[1].blocks[0].size)

	assert.Equal(t, 1, perPeer[1].blocks[0].reattempt.attempt)
	assert.Equal(t, []hostQueue{peerB}, perPeer[1].blocks[0].reattempt.attempted)

	// Assert selection third peer
	assert.Equal(t, 1, len(perPeer[2].blocks))

	assert.Equal(t, start.Add(time.Hour*4), perPeer[2].blocks[0].start)
	assert.Equal(t, int64(2), perPeer[2].blocks[0].size)

	assert.Equal(t, 1, perPeer[2].blocks[0].reattempt.attempt)
	assert.Equal(t, []hostQueue{peerC}, perPeer[2].blocks[0].reattempt.attempted)
}

func TestSelectBlocksForSeriesFromPeerBlocksMetadataAvoidsReattemptingFromAttemptedPeers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	var (
		peerA            = NewMockhostQueue(ctrl)
		peerB            = NewMockhostQueue(ctrl)
		peerC            = NewMockhostQueue(ctrl)
		peers            = preparedMockPeers(peerA, peerB, peerC)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		currStart, currEligible []*blocksMetadata
		blocksMetadataQueues    []blocksMetadataQueue
		start                   = timeZero
		reattempt               = blockMetadataReattempt{
			attempt:   1,
			id:        "foo",
			attempted: []hostQueue{peerA},
		}
		perPeer = []*blocksMetadata{
			&blocksMetadata{
				peer: peerA,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 2, reattempt: reattempt},
				},
			},
			&blocksMetadata{
				peer: peerB,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 2, reattempt: reattempt},
				},
			},
			&blocksMetadata{
				peer: peerC,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 2, reattempt: reattempt},
				},
			},
		}
	)

	// Track peer C as having an assigned block to ensure block ends up
	// under peer B which is just as eligible as peer C to receive the block
	// assignment
	peerBlocksQueues.findQueue(peerC).trackAssigned(1)

	// Perform selection
	session.selectBlocksForSeriesFromPeerBlocksMetadata(
		perPeer, peerBlocksQueues,
		currStart, currEligible, blocksMetadataQueues)

	// Assert selection first peer
	assert.Equal(t, 0, len(perPeer[0].blocks))

	// Assert selection second peer
	assert.Equal(t, 1, len(perPeer[1].blocks))

	assert.Equal(t, start, perPeer[1].blocks[0].start)
	assert.Equal(t, int64(2), perPeer[1].blocks[0].size)

	assert.Equal(t, 2, perPeer[1].blocks[0].reattempt.attempt)
	assert.Equal(t, []hostQueue{peerA, peerB}, perPeer[1].blocks[0].reattempt.attempted)

	// Assert selection third peer
	assert.Equal(t, 0, len(perPeer[2].blocks))
}

func TestSelectBlocksForSeriesFromPeerBlocksMetadataAvoidsExhaustedBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	var (
		peerA            = NewMockhostQueue(ctrl)
		peerB            = NewMockhostQueue(ctrl)
		peerC            = NewMockhostQueue(ctrl)
		peers            = preparedMockPeers(peerA, peerB, peerC)
		peerBlocksQueues = mockPeerBlocksQueues(peers, opts)
	)
	defer peerBlocksQueues.closeAll()

	var (
		currStart, currEligible []*blocksMetadata
		blocksMetadataQueues    []blocksMetadataQueue
		start                   = timeZero
		reattempt               = blockMetadataReattempt{
			attempt:   3,
			id:        "foo",
			attempted: []hostQueue{peerA, peerB, peerC},
		}
		perPeer = []*blocksMetadata{
			&blocksMetadata{
				peer: peerA,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 2},
					{start: start.Add(time.Hour * 2), size: 2, reattempt: reattempt},
				},
			},
			&blocksMetadata{
				peer: peerB,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 2},
					{start: start.Add(time.Hour * 2), size: 2, reattempt: reattempt},
				},
			},
			&blocksMetadata{
				peer: peerC,
				id:   "foo",
				blocks: []blockMetadata{
					{start: start, size: 2},
					{start: start.Add(time.Hour * 2), size: 2, reattempt: reattempt},
				},
			},
		}
	)

	// Perform selection
	session.selectBlocksForSeriesFromPeerBlocksMetadata(
		perPeer, peerBlocksQueues,
		currStart, currEligible, blocksMetadataQueues)

	// Assert selection first peer
	assert.Equal(t, 1, len(perPeer[0].blocks))

	assert.Equal(t, start, perPeer[0].blocks[0].start)
	assert.Equal(t, int64(2), perPeer[0].blocks[0].size)

	assert.Equal(t, 1, perPeer[0].blocks[0].reattempt.attempt)
	assert.Equal(t, []hostQueue{peerA}, perPeer[0].blocks[0].reattempt.attempted)

	// Assert selection second peer
	assert.Equal(t, 0, len(perPeer[1].blocks))

	// Assert selection third peer
	assert.Equal(t, 0, len(perPeer[2].blocks))
}

func TestStreamBlocksBatchFromPeerReenqueuesOnFailCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSessionTestAdminOptions()
	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	mockHostQueues, mockClients := mockHostQueuesAndClientsForFetchBootstrapBlocks(ctrl, opts)
	session.newHostQueueFn = mockHostQueues.newHostQueueFn()
	// Ensure work enqueued immediately so can test result of the reenqueue
	session.streamBlocksReattemptWorkers = newSynchronousWorkerPool()

	assert.NoError(t, session.Open())

	var (
		blockSize = 2 * time.Hour
		start     = time.Now().Truncate(blockSize).Add(blockSize * -(24 - 1))
		retrier   = xretry.NewRetrier(xretry.NewOptions().Max(1).InitialBackoff(time.Millisecond))
		peerIdx   = len(mockHostQueues) - 1
		peer      = mockHostQueues[peerIdx]
		client    = mockClients[peerIdx]
		enqueueCh = newEnqueueChannel()
		batch     = []*blocksMetadata{
			&blocksMetadata{id: "foo", blocks: []blockMetadata{
				{start: start, size: 2, reattempt: blockMetadataReattempt{
					id: "foo",
					peersMetadata: []blockMetadataReattemptPeerMetadata{
						{start: start, size: 2},
					},
				}},
				{start: start.Add(blockSize), size: 2, reattempt: blockMetadataReattempt{
					id: "foo",
					peersMetadata: []blockMetadataReattemptPeerMetadata{
						{start: start.Add(blockSize), size: 2},
					},
				}},
			}},
			&blocksMetadata{id: "bar", blocks: []blockMetadata{
				{start: start, size: 2, reattempt: blockMetadataReattempt{
					id: "foo",
					peersMetadata: []blockMetadataReattemptPeerMetadata{
						{start: start, size: 2},
					},
				}},
				{start: start.Add(blockSize), size: 2, reattempt: blockMetadataReattempt{
					id: "foo",
					peersMetadata: []blockMetadataReattemptPeerMetadata{
						{start: start.Add(blockSize), size: 2},
					},
				}},
			}},
		}
	)

	// Fail the call twice due to retry
	client.EXPECT().FetchBlocks(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("an error")).Times(2)

	// Attempt stream blocks
	session.streamBlocksBatchFromPeer(0, peer, batch, nil, enqueueCh, retrier)

	// Assert result
	assertEnqueueChannel(t, append(batch[0].blocks, batch[1].blocks...), enqueueCh)

	assert.NoError(t, session.Close())
}

func TestBlocksResultAddBlockFromPeerReadMerged(t *testing.T) {
	var encoders []*testEncoder
	opts := newSessionTestAdminOptions()
	bopts := newBootstrapTestOptionsWithEncoderCallback(func(enc *testEncoder) {
		encoders = append(encoders, enc)
	})

	start := time.Now()

	bl := &rpc.Block{
		Start: start.UnixNano(),
		Segments: &rpc.Segments{Merged: &rpc.Segment{
			Head: []byte{1, 2},
			Tail: []byte{3},
		}},
	}

	r := newBlocksResult(opts, bopts)
	r.addBlockFromPeer("foo", bl)

	series := r.result.GetAllSeries()
	assert.Equal(t, 1, len(series))

	blocks, ok := series["foo"]
	assert.True(t, ok)
	assert.Equal(t, 1, blocks.Len())
	result, ok := blocks.GetBlockAt(start)
	assert.True(t, ok)

	ctx := context.NewContext()
	defer ctx.Close()

	stream, err := result.Stream(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, stream)

	// Assert encoder has data
	assert.Equal(t, 1, len(encoders))
	assert.Equal(t, []byte{1, 2, 3}, encoders[0].data)

	// Ensure not sealed
	assert.False(t, encoders[0].sealed)
	assert.True(t, encoders[0].writable)
	assert.False(t, encoders[0].closed)
}

func TestBlocksResultAddBlockFromPeerReadUnmerged(t *testing.T) {
	var wrapEncoderFn func(enc encoding.Encoder) encoding.Encoder
	eops := encoding.NewOptions()
	intopt := true

	encoderPool := encoding.NewEncoderPool(0)
	encoderPool.Init(func() encoding.Encoder {
		enc := m3tsz.NewEncoder(time.Time{}, nil, intopt, eops)
		if wrapEncoderFn != nil {
			enc = wrapEncoderFn(enc)
		}
		return enc
	})

	opts := newSessionTestAdminOptions()
	bopts := bootstrap.NewOptions()
	bopts = bopts.DatabaseBlockOptions(bopts.GetDatabaseBlockOptions().
		EncoderPool(encoderPool))

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
		encoder := encoderPool.Get()
		encoder.Reset(start, 0)
		for _, val := range vals {
			dp := ts.Datapoint{Timestamp: val.t, Value: val.value}
			assert.NoError(t, encoder.Encode(dp, val.unit, val.annotation))
			all = append(all, val)
		}
		result := encoder.Stream().Segment()
		seg := &rpc.Segment{Head: result.Head, Tail: result.Tail}
		bl.Segments.Unmerged = append(bl.Segments.Unmerged, seg)
	}

	// Intercept encoder creation and wrap with a test encoder to introspect state
	var mergeEncoder *testPassthroughEncoder
	wrapEncoderFn = func(enc encoding.Encoder) encoding.Encoder {
		mergeEncoder = &testPassthroughEncoder{encoder: enc}
		return mergeEncoder
	}

	r := newBlocksResult(opts, bopts)
	r.addBlockFromPeer("foo", bl)

	series := r.result.GetAllSeries()
	assert.Equal(t, 1, len(series))

	blocks, ok := series["foo"]
	assert.True(t, ok)
	assert.Equal(t, 1, blocks.Len())
	result, ok := blocks.GetBlockAt(start)
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
		assert.Equal(t, all[idx].t, dp.Timestamp)
		assert.Equal(t, all[idx].unit, unit)
		assert.Equal(t, all[idx].annotation, []byte(annotation))
		asserted++
	}
	assert.Equal(t, len(all), asserted)
	assert.NoError(t, iter.Err())

	// Ensure not sealed
	assert.False(t, mergeEncoder.sealed)
	assert.True(t, mergeEncoder.writable)
	assert.False(t, mergeEncoder.closed)
}

func TestBlocksResultAddBlockFromPeerErrorOnNoSegments(t *testing.T) {
	opts := newSessionTestAdminOptions()
	bopts := bootstrap.NewOptions()
	r := newBlocksResult(opts, bopts)

	bl := &rpc.Block{Start: time.Now().UnixNano()}
	err := r.addBlockFromPeer("foo", bl)
	assert.Error(t, err)
	assert.Equal(t, errSessionBadBlockResultFromPeer, err)
}

func TestBlocksResultAddBlockFromPeerErrorOnNoSegmentsData(t *testing.T) {
	opts := newSessionTestAdminOptions()
	bopts := bootstrap.NewOptions()
	r := newBlocksResult(opts, bopts)

	bl := &rpc.Block{Start: time.Now().UnixNano(), Segments: &rpc.Segments{}}
	err := r.addBlockFromPeer("foo", bl)
	assert.Error(t, err)
	assert.Equal(t, errSessionBadBlockResultFromPeer, err)
}

func mockPeerBlocksQueues(peers []hostQueue, opts AdminOptions) peerBlocksQueues {
	var (
		peerQueues peerBlocksQueues
		workers    = pool.NewWorkerPool(opts.GetFetchSeriesBlocksBatchConcurrency())
	)
	for _, peer := range peers {
		size := opts.GetFetchSeriesBlocksBatchSize()
		drainEvery := 100 * time.Millisecond
		queue := newPeerBlocksQueue(peer, size, drainEvery, workers, func(batch []*blocksMetadata) {
			// No-op
		})
		peerQueues = append(peerQueues, queue)
	}
	return peerQueues
}

func preparedMockPeers(peers ...*MockhostQueue) []hostQueue {
	var result []hostQueue
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
		writeBatchRequestPool writeBatchRequestPool,
		writeRequestArrayPool writeRequestArrayPool,
		opts Options,
	) hostQueue {
		return qs[atomic.AddUint64(&idx, 1)-1]
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
		client := rpc.NewMockTChanNode(ctrl)
		connectionPool := NewMockconnectionPool(ctrl)
		connectionPool.EXPECT().NextClient().Return(client, nil).AnyTimes()
		hostQueue := NewMockhostQueue(ctrl)
		hostQueue.EXPECT().Open()
		hostQueue.EXPECT().Host().Return(host).AnyTimes()
		hostQueue.EXPECT().GetConnectionCount().Return(opts.GetMinConnectionCount()).Times(sessionTestShards)
		hostQueue.EXPECT().GetConnectionPool().Return(connectionPool).AnyTimes()
		hostQueue.EXPECT().Close()
		hostQueues = append(hostQueues, hostQueue)
		clients = append(clients, client)
	}
	return hostQueues, clients
}

func resultMetadataFromBlocks(
	blocks []testBlocks,
) []testBlocksMetadata {
	var result []testBlocksMetadata
	for _, b := range blocks {
		bm := []testBlockMetadata{}
		for _, bl := range b.blocks {
			size := int64(0)
			if merged := bl.segments.merged; merged != nil {
				size += int64(len(merged.head) + len(merged.tail))
			}
			for _, unmerged := range bl.segments.unmerged {
				size += int64(len(unmerged.head) + len(unmerged.tail))
			}
			m := testBlockMetadata{
				start: bl.start,
				size:  &size,
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

func expectedReqsAndResultFromBlocks(
	blocks []testBlocks,
	batchSize int,
	clientsParticipatingLen int,
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
		clientIdx := blockIdx % clientsParticipatingLen
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
		for i := 0; i < len(blocks[0].blocks); i++ {
			starts = append(starts, blocks[0].blocks[i].start)
		}
		param := fetchBlocksReqParam{
			id:     blocks[0].id,
			starts: starts,
		}
		req.params = append(req.params, param)
		blocksResult[len(blocksResult)-1] = append(blocksResult[len(blocksResult)-1], blocks[0])

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
	batchSize := opts.GetFetchSeriesBlocksBatchSize()
	totalCalls := int(math.Ceil(float64(len(result)) / float64(batchSize)))
	includeSizes := true

	var calls []*gomock.Call
	for i := 0; i < totalCalls; i++ {
		var (
			ret      = &rpc.FetchBlocksMetadataResult_{}
			beginIdx = i * batchSize
			nextIdx  = int64(0)
		)
		for j := beginIdx; j < len(result) && j < beginIdx+batchSize; j++ {
			elem := &rpc.BlocksMetadata{}
			elem.ID = result[j].id
			for k := 0; k < len(result[j].blocks); k++ {
				bl := &rpc.BlockMetadata{}
				bl.Start = result[j].blocks[k].start.UnixNano()
				bl.Size = result[j].blocks[k].size
				elem.Blocks = append(elem.Blocks, bl)
			}
			ret.Elements = append(ret.Elements, elem)
			nextIdx = int64(j) + 1
		}
		if i != totalCalls-1 {
			// Include next page token if not last page
			ret.NextPageToken = &nextIdx
		}

		matcher := &fetchMetadataReqMatcher{
			shard:        0,
			limit:        int64(batchSize),
			includeSizes: &includeSizes,
		}
		if i != 0 {
			expectPageToken := int64(beginIdx)
			matcher.pageToken = &expectPageToken
		}

		call := client.EXPECT().FetchBlocksMetadata(gomock.Any(), matcher).Return(ret, nil)
		calls = append(calls, call)
	}

	gomock.InOrder(calls...)
}

type fetchMetadataReqMatcher struct {
	shard        int32
	limit        int64
	pageToken    *int64
	includeSizes *bool
}

func (m *fetchMetadataReqMatcher) Matches(x interface{}) bool {
	req, ok := x.(*rpc.FetchBlocksMetadataRequest)
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
		if *req.PageToken != *m.pageToken {
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
	var calls []*gomock.Call
	for i := 0; i < len(expect); i++ {
		matcher := &fetchBlocksReqMatcher{req: expect[i]}
		ret := &rpc.FetchBlocksResult_{}
		for _, res := range result[i] {
			blocks := &rpc.Blocks{}
			blocks.ID = res.id
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
		call := client.EXPECT().FetchBlocks(gomock.Any(), matcher).Return(ret, nil)
		calls = append(calls, call)
	}

	gomock.InOrder(calls...)
}

type fetchBlocksReqMatcher struct {
	req fetchBlocksReq
}

func (m *fetchBlocksReqMatcher) Matches(x interface{}) bool {
	req, ok := x.(*rpc.FetchBlocksRequest)
	if !ok {
		return false
	}

	params := m.req.params
	if len(params) != len(req.Elements) {
		return false
	}

	for i := range params {
		if params[i].id != req.Elements[i].ID {
			return false
		}
		if len(params[i].starts) != len(req.Elements[i].Starts) {
			return false
		}
		for j := range params[i].starts {
			if params[i].starts[j].UnixNano() != req.Elements[i].Starts[j] {
				return false
			}
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
	id     string
	starts []time.Time
}

type testBlocksMetadata struct {
	id     string
	blocks []testBlockMetadata
}

type testBlockMetadata struct {
	start time.Time
	size  *int64
}

type testBlocks struct {
	id     string
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
	actual bootstrap.ShardResult,
) {
	ctx := context.NewContext()
	defer ctx.Close()

	series := actual.GetAllSeries()
	assert.Equal(t, len(expected), len(series))

	for i := range expected {
		id := expected[i].id
		entry, ok := series[id]
		if !ok {
			assert.Fail(t, fmt.Sprintf("blocks for series '%s' not present", id))
			continue
		}

		expectedLen := 0
		for _, block := range expected[i].blocks {
			if block.err != nil {
				continue
			}
			expectedLen++
		}
		assert.Equal(t, expectedLen, entry.Len())

		for _, block := range expected[i].blocks {
			actualBlock, ok := entry.GetBlockAt(block.start)
			if !ok {
				assert.Fail(t, fmt.Sprintf("block for series '%s' start %v not present", id, block.start))
				continue
			}

			if block.segments.merged != nil {
				expectedData := append(block.segments.merged.head, block.segments.merged.tail...)
				stream, err := actualBlock.Stream(ctx)
				assert.NoError(t, err)
				actualData := append(stream.Segment().Head, stream.Segment().Tail...)
				assert.Equal(t, expectedData, actualData)
			} else if block.segments.unmerged != nil {
				assert.Fail(t, "unmerged comparison not supported")
			}
		}
	}
}

func assertEnqueueChannel(
	t *testing.T,
	expected []blockMetadata,
	enqueueCh *enqueueChannel,
) {
	var distinct []blockMetadata
	for {
		var perPeerBlocksMetadata []*blocksMetadata
		select {
		case perPeerBlocksMetadata = <-enqueueCh.get():
		default:
		}
		if perPeerBlocksMetadata == nil {
			break
		}

		for {
			var earliestStart time.Time
			for _, blocksMetadata := range perPeerBlocksMetadata {
				if len(blocksMetadata.unselectedBlocks()) == 0 {
					continue
				}
				unselected := blocksMetadata.unselectedBlocks()
				if earliestStart.Equal(time.Time{}) ||
					unselected[0].start.Before(earliestStart) {
					earliestStart = unselected[0].start
				}
			}

			var currStart []*blocksMetadata
			for _, blocksMetadata := range perPeerBlocksMetadata {
				if len(blocksMetadata.unselectedBlocks()) == 0 {
					continue
				}
				unselected := blocksMetadata.unselectedBlocks()
				if !unselected[0].start.Equal(earliestStart) {
					continue
				}
				currStart = append(currStart, blocksMetadata)
			}

			if len(currStart) == 0 {
				break
			}

			for i := 1; i < len(currStart); i++ {
				// Remove from all others
				currStart[i].blocks = append(currStart[i].blocks[:currStart[i].idx], currStart[i].blocks[currStart[i].idx+1:]...)
			}
			// Append distinct and select from current
			block := currStart[0].unselectedBlocks()[0]
			assert.Equal(t, currStart[0].id, block.reattempt.id)
			distinct = append(distinct, block)
			currStart[0].idx++
		}
	}

	matchesLen := len(expected) == len(distinct)
	assert.True(t, matchesLen)
	if matchesLen {
		for i, expected := range expected {
			actual := distinct[i]
			assert.Equal(t, expected.start, actual.start)
			assert.Equal(t, expected.size, actual.size)
			assert.Equal(t, expected.reattempt.id, actual.reattempt.id)
		}
	}

	close(enqueueCh.peersMetadataCh)
}

type testEncoder struct {
	start    time.Time
	data     []byte
	writable bool
	sealed   bool
	closed   bool
}

func (e *testEncoder) Encode(dp ts.Datapoint, timeUnit xtime.Unit, annotation ts.Annotation) error {
	return fmt.Errorf("not implemented")
}

func (e *testEncoder) Stream() xio.SegmentReader {
	return xio.NewSegmentReader(ts.Segment{Head: e.data, Tail: nil})
}

func (e *testEncoder) Seal() {
	e.sealed = true
}

func (e *testEncoder) Unseal() error {
	e.sealed = false
	e.writable = true
	return nil
}

func (e *testEncoder) Reset(t time.Time, capacity int) {
	e.start = t
	e.data = nil
	e.writable = true
}

func (e *testEncoder) ResetSetData(t time.Time, data []byte, writable bool) {
	e.start = t
	e.data = data
	e.writable = writable
}

func (e *testEncoder) Close() {
	e.closed = true
}

type testPassthroughEncoder struct {
	encoder   encoding.Encoder
	start     time.Time
	resetData []byte
	writable  bool
	sealed    bool
	closed    bool
}

func (e *testPassthroughEncoder) Encode(dp ts.Datapoint, timeUnit xtime.Unit, annotation ts.Annotation) error {
	return e.encoder.Encode(dp, timeUnit, annotation)
}

func (e *testPassthroughEncoder) Stream() xio.SegmentReader {
	return e.encoder.Stream()
}

func (e *testPassthroughEncoder) Seal() {
	e.encoder.Seal()
	e.sealed = true
}

func (e *testPassthroughEncoder) Unseal() error {
	e.sealed = false
	e.writable = true
	return e.encoder.Unseal()
}

func (e *testPassthroughEncoder) Reset(t time.Time, capacity int) {
	e.encoder.Reset(t, capacity)
	e.start = t
	e.resetData = nil
	e.writable = true
}

func (e *testPassthroughEncoder) ResetSetData(t time.Time, data []byte, writable bool) {
	e.encoder.ResetSetData(t, data, writable)
	e.start = t
	e.resetData = data
	e.writable = writable
}

func (e *testPassthroughEncoder) Close() {
	e.encoder.Close()
	e.closed = true
}

type synchronousWorkerPool struct{}

func newSynchronousWorkerPool() *synchronousWorkerPool {
	return &synchronousWorkerPool{}
}

func (s *synchronousWorkerPool) Init() {
	// Noop
}

func (s *synchronousWorkerPool) Go(work pool.Work) {
	work()
}

func (s *synchronousWorkerPool) GoIfAvailable(work pool.Work) bool {
	work()
	return true
}
