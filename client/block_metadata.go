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
	"strings"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/ts"
)

type blocksMetadata struct {
	peer   hostQueue
	id     ts.ID
	blocks []blockMetadata
	idx    int
}

type receivedBlocks struct {
	submitted bool
	results   []*blocksMetadata
}

type processFn func(batch []*blocksMetadata)

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
