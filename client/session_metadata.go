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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/ts"

	xerrors "github.com/m3db/m3x/errors"
	xlog "github.com/m3db/m3x/log"
	xretry "github.com/m3db/m3x/retry"

	"github.com/uber/tchannel-go/thrift"
)

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
		errCh <- s.streamBlocksMetadataFromPeers(namespace, shard, peers, start, end, metadataCh, m)
		close(metadataCh)
		close(errCh)
	}()

	return newMetadataIter(metadataCh, errCh), nil
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
		multiErr = xerrors.NewMultiError()
	)

	pending := int64(len(peers))
	m.metadataFetches.Update(pending)

	for _, peer := range peers {
		peer := peer

		wg.Add(1)
		go func() {
			err := s.streamBlocksMetadataFromOnePeer(namespace, shard, peer, start, end, ch, m)
			if err != nil {
				errLock.Lock()
				multiErr = multiErr.Add(err)
				errLock.Unlock()
			}
			m.metadataFetches.Update(atomic.AddInt64(&pending, -1))
			wg.Done()
		}()
	}

	wg.Wait()

	if multiErr.NumErrors() == len(peers) {
		return multiErr.FinalError()
	}
	return nil
}

func (s *session) streamBlocksMetadataFromOnePeer(
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
		moreResults = true
	)

	// Declare before loop to avoid redeclaring each iteration
	attemptFn := func() error {
		client, err := peer.ConnectionPool().NextClient()
		if err != nil {
			return err
		}

		tctx, _ := thrift.NewContext(s.streamBlocksMetadataBatchTimeout)
		req := s.newFetchBlocksMetadataRawRequest(namespace, shard, start, end, pageToken)

		m.metadataFetchBatchCall.Inc(1)
		result, err := client.FetchBlocksMetadataRaw(tctx, req)
		if err != nil {
			m.metadataFetchBatchError.Inc(1)
			return err
		}

		m.metadataFetchBatchSuccess.Inc(1)
		m.metadataReceived.Inc(int64(len(result.Elements)))

		if result.NextPageToken != nil {
			// Create space on the heap for the page token and take its
			// address to avoid having to keep the entire result around just
			// for the page token
			resultPageToken := *result.NextPageToken
			pageToken = &resultPageToken
		} else {
			moreResults = false
		}

		for _, elem := range result.Elements {
			blockMetas := make([]blockMetadata, 0, len(elem.Blocks))
			for _, b := range elem.Blocks {
				blockStart := time.Unix(0, b.Start)

				if b.Err != nil {
					// Error occurred retrieving block metadata, use default values
					blockMetas = append(blockMetas, blockMetadata{start: blockStart})
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

func (s *session) streamCollectedBlocksMetadata(
	peersLen int,
	ch <-chan blocksMetadata,
	enqueueCh *enqueueChannel,
) {
	metadata := make(map[ts.Hash]*receivedBlocks)

	// receive off of metadata channel
	for m := range ch {
		m := m
		received, ok := metadata[m.id.Hash()]
		if !ok {
			received = &receivedBlocks{results: make([]*blocksMetadata, 0, peersLen)}
			metadata[m.id.Hash()] = received
		} else if received.submitted { // Already submitted to enqueue channel
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
		if !received.submitted {
			enqueueCh.enqueue(received.results)
		}
	}
}

func freeRefs(pooledCurrStart, pooledCurrEligible []*blocksMetadata,
	pooledBlocksMetadataQueues []blocksMetadataQueue,
) {
	for i := range pooledCurrStart {
		pooledCurrStart[i] = nil
	}
	for i := range pooledCurrEligible {
		pooledCurrEligible[i] = nil
	}

	var zeroed blocksMetadataQueue
	for i := range pooledCurrEligible { // should this be pooledBlocksMetadataQueues?
		pooledBlocksMetadataQueues[i] = zeroed
	}
}

func earliestUnselectedStartTime(metaBlocks []*blocksMetadata) time.Time {
	var earliestStart = time.Now() // sentinel

	for _, blocksMetadata := range metaBlocks {
		unselected := blocksMetadata.unselectedBlocks()
		if len(unselected) > 0 && unselected[0].start.Before(earliestStart) {
			earliestStart = unselected[0].start
		}
	}

	return earliestStart
}

func (s *session) selectBlocksForSeriesFromPeerBlocksMetadata(
	perPeerBlocksMetadata []*blocksMetadata,
	peerQueues peerBlocksQueues,
	pooledCurrStart, pooledCurrEligible []*blocksMetadata,
	pooledBlocksMetadataQueues []blocksMetadataQueue,
) {
	// Free any references the pool still has
	freeRefs(pooledCurrStart, pooledCurrEligible, pooledBlocksMetadataQueues)

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
		blocksMetadata.idx = 0 // Reset the selection index
	}

	// Select blocks from peers
	for {
		earliestStart := earliestUnselectedStartTime(perPeerBlocksMetadata)

		// Find all with the earliest start time, ordered by time so must be first of each
		currStart = currStart[:0]
		for _, blocksMetadata := range perPeerBlocksMetadata {
			unselected := blocksMetadata.unselectedBlocks()

			if len(unselected) > 0 && unselected[0].start.Equal(earliestStart) {
				currStart = append(currStart, blocksMetadata)
			}
		}

		if len(currStart) == 0 { // No more blocks to select from any peers
			break
		}

		// Only select from peers not already attempted
		currEligible = currStart[:]
		currID := currStart[0].id
		for i := len(currEligible) - 1; i >= 0; i-- {
			unselected := currEligible[i].unselectedBlocks()
			if unselected[0].reattempt.attempt == 0 { // Not attempted yet
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
		for _, c := range currStart {
			unselected := c.unselectedBlocks()
			peersMetadata = append(peersMetadata, blockMetadataReattemptPeerMetadata{
				peer:     c.peer,
				start:    unselected[0].start,
				size:     unselected[0].size,
				checksum: unselected[0].checksum,
			})
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

		// If all of the peers have the same non-nil checksum, we pick the peer with the
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
			// todo@bl: this should find the min, not sort

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

				// Remove this block
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
